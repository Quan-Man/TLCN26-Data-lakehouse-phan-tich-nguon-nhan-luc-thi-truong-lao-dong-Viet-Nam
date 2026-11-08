"""Small, bounded Trino HTTP client shared by operational scripts."""
from __future__ import annotations

from dataclasses import dataclass
import json
import os
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


class TrinoError(RuntimeError):
    """A Trino error with operation/query context and no credential or SQL values."""


@dataclass(frozen=True)
class TrinoResult:
    columns: tuple[str, ...]
    rows: tuple[tuple, ...]
    query_id: str | None
    update_type: str | None


class TrinoClient:
    def __init__(self, url=None, *, user=None, catalog=None,
                 schema=None, request_timeout=5.0, query_timeout=60.0,
                 startup_timeout=60.0, retry_interval=1.0):
        if url is None:
            url = os.getenv("TRINO_URL")
            if not url:
                url = "http://trino:8080" if os.getenv("AIRFLOW_HOME") else "http://localhost:8085"
        parsed = urlsplit(url.rstrip("/"))
        if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                or parsed.username or parsed.password or parsed.path not in {"", "/"}
                or parsed.query or parsed.fragment):
            raise ValueError("Trino URL must be an http(s) endpoint without credentials or path")
        _ = parsed.port
        for name, value in (("request_timeout", request_timeout),
                            ("query_timeout", query_timeout),
                            ("startup_timeout", startup_timeout),
                            ("retry_interval", retry_interval)):
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        self.url = f"{parsed.scheme}://{parsed.netloc}"
        self.user = user or os.getenv("TRINO_USER", "lakehouse")
        self.catalog = catalog
        self.schema = schema
        self.request_timeout = float(request_timeout)
        self.query_timeout = float(query_timeout)
        self.startup_timeout = float(startup_timeout)
        self.retry_interval = float(retry_interval)

    def _headers(self, *, sql=False):
        headers = {"X-Trino-User": self.user, "User-Agent": "lakehouse-trino-client/1.0"}
        if self.catalog:
            headers["X-Trino-Catalog"] = self.catalog
        if self.schema:
            headers["X-Trino-Schema"] = self.schema
        if sql:
            headers["Content-Type"] = "text/plain; charset=utf-8"
        return headers

    def _read_json(self, request: Request, timeout: float) -> dict:
        with urlopen(request, timeout=timeout) as response:
            value = json.load(response)
        if not isinstance(value, dict):
            raise ValueError("Trino returned a non-object response")
        return value

    def wait_until_ready(self, timeout=None) -> dict:
        """Wait for Trino's info endpoint; only transient connection/server errors retry."""
        deadline = time.monotonic() + (self.startup_timeout if timeout is None else timeout)
        last_error = None
        info_url = self.url + "/v1/info"
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            request = Request(info_url, headers={"User-Agent": "lakehouse-trino-client/1.0"})
            try:
                info = self._read_json(request, min(self.request_timeout, remaining))
                return info
            except HTTPError as exc:
                if exc.code not in {502, 503, 504}:
                    raise TrinoError(f"Trino readiness check failed (HTTP {exc.code})") from exc
                last_error = f"HTTP {exc.code}"
            except (URLError, TimeoutError, OSError, ValueError) as exc:
                last_error = type(exc).__name__
            time.sleep(min(self.retry_interval, max(0.0, deadline - time.monotonic())))
        raise TrinoError(
            f"Trino did not become ready at {self.url}/v1/info within "
            f"{self.startup_timeout:g}s (last error: {last_error or 'timeout'})"
        )

    @staticmethod
    def _statement_kind(sql: str) -> str:
        return sql.lstrip().split(None, 1)[0].upper()[:24] if sql.strip() else "EMPTY"

    def execute(self, sql: str, *, operation=None) -> TrinoResult:
        """Run one statement and consume pages before returning rows/update metadata."""
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("sql must be a non-empty string")
        kind = self._statement_kind(sql)
        context = operation or kind
        deadline = time.monotonic() + self.query_timeout
        request = Request(self.url + "/v1/statement", data=sql.encode("utf-8"), method="POST",
                          headers=self._headers(sql=True))
        try:
            result = self._read_json(request, min(self.request_timeout, self.query_timeout))
            query_id = result.get("id")
            columns: tuple[str, ...] = ()
            rows: list[tuple] = []
            while True:
                page_columns = result.get("columns")
                if page_columns and not columns:
                    columns = tuple(column.get("name", "") for column in page_columns)
                rows.extend(tuple(row) for row in result.get("data", []))
                error = result.get("error")
                if error:
                    name = error.get("errorName") or error.get("errorType") or "QueryFailed"
                    raise TrinoError(f"Trino {context} failed (query_id={query_id}, error={name})")
                next_uri = result.get("nextUri")
                if not next_uri:
                    return TrinoResult(columns, tuple(rows), query_id, result.get("updateType"))
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TrinoError(f"Trino {context} timed out (query_id={query_id})")
                parsed_next = urlsplit(next_uri)
                if (parsed_next.scheme != urlsplit(self.url).scheme
                        or parsed_next.netloc != urlsplit(self.url).netloc):
                    raise TrinoError(f"Trino {context} returned an unexpected result URL (query_id={query_id})")
                next_request = Request(next_uri, headers={"User-Agent": "lakehouse-trino-client/1.0"})
                result = self._read_json(next_request, min(self.request_timeout, remaining))
        except TrinoError:
            raise
        except HTTPError as exc:
            raise TrinoError(
                f"Trino {context} request failed (HTTP {exc.code}, query_id={locals().get('query_id')})"
            ) from exc
        except (URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
            raise TrinoError(
                f"Trino {context} request failed ({type(exc).__name__}, "
                f"query_id={locals().get('query_id')})"
            ) from exc


def quote_identifier(value: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("SQL identifier must be a non-empty string")
    return '"' + value.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    if not isinstance(value, str) or "\x00" in value:
        raise ValueError("SQL literal must be a string without NUL")
    return "'" + value.replace("'", "''") + "'"
