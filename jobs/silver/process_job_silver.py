#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import html
import json
import re
import os
from html.parser import HTMLParser
from urllib.parse import urlsplit, urlunsplit

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from zoneinfo import ZoneInfo

import csv
from pathlib import Path

import argparse
from collections import Counter
import logging
import shutil
import sys
import tempfile
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.normalization import normalize_whitespace

def canonical_hash(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()

def clean_text(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("Expected string, got " + type(value).__name__)
    return normalize_whitespace(value)

class _HTMLText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style"}:
            self.hidden += 1
        if tag in {"br", "p", "div", "li", "tr", "h1", "h2", "h3"}:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in {"script", "style"}:
            self.hidden = max(0, self.hidden - 1)
        if tag in {"p", "div", "li", "tr"}:
            self.parts.append("\n")

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)

def clean_multiline(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("Expected multiline string")
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    if re.search(r"</?(?:p|div|li|br|span|ul|ol|b|strong|script|style)\b", value, re.I):
        parser = _HTMLText()
        parser.feed(value)
        value = "".join(parser.parts)
    value = html.unescape(value)
    lines = [clean_text(line) or "" for line in value.split("\n")]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip() or None

def clean_array(value):
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError("Expected array of strings")
    return list(dict.fromkeys(x for item in value if (x := clean_text(item))))

def normalize_source(value):
    value = clean_text(value)
    if not value:
        return None
    value = value.casefold()
    return {"timviec365.vn": "timviec365", "careerviet.vn": "careerviet"}.get(value, value)

def normalize_url(value):
    value = clean_text(value)
    if not value:
        return None
    try:
        parsed = urlsplit(value)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
            return None
        if parsed.username or parsed.password or re.search(r"\s", value):
            return None
        port = parsed.port
        host = parsed.hostname.lower()
        if ":" in host:
            host = "[" + host + "]"
        netloc = host + (":" + str(port) if port else "")
        return urlunsplit((parsed.scheme.lower(), netloc, parsed.path, parsed.query, ""))
    except ValueError:
        return None

def job_identity(source, source_id, url):
    if not source:
        raise ValueError("SOURCE_NAME_MISSING")
    if source_id:
        return canonical_hash(["job", source, "source_id", source_id])
    if url:
        return canonical_hash(["job", source, "url", url])
    raise ValueError("JOB_IDENTITY_MISSING")

LOCAL_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
NUM = r"\d+(?:[.,]\d+)*"
MONEY = r"(?:triệu|tr|nghìn|ngàn|k|vnđ|vnd|đồng|₫|usd|eur|\$|€)"

def parse_date(value, flags=None, flag="DATE_PARSE_FAILED"):
    value = clean_text(value)
    if value is None:
        return None
    # Bắt ngày tuyệt đối; không suy ngày từ '3 giờ trước'.
    match = re.fullmatch(r"(\d{2}[/-]\d{2}[/-]\d{4}|\d{4}-\d{2}-\d{2})(?:\s*\([^\n]*\))?", value)
    if match:
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(match[1], fmt).date().isoformat()
            except ValueError:
                pass
    if flags is not None:
        flags.append(flag)
    return None

def parse_timestamp(value, flags=None):
    value = clean_text(value)
    if value is None:
        return None
    try:
        if not re.match(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}", value):
            raise ValueError("Not an ISO timestamp")
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=LOCAL_TZ)
            if flags is not None:
                flags.append("TIMEZONE_ASSUMED")
        return dt.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    except ValueError:
        if flags is not None:
            flags.append("CRAWLED_AT_PARSE_FAILED")
        return None

def local_date(timestamp):
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(LOCAL_TZ).date().isoformat() if timestamp else None

def decimal_string(value):
    return format(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), ".2f")

def _number(token, multiplier):
    if multiplier != 1:
        if not re.fullmatch(r"\d+(?:[.,]\d{1,2})?", token):
            raise ValueError("Ambiguous scaled amount")
        return Decimal(token.replace(",", ".")) * multiplier
    if re.fullmatch(r"\d+", token):
        return Decimal(token)
    if re.fullmatch(r"\d{1,3}(?:\.\d{3})+|\d{1,3}(?:,\d{3})+", token):
        return Decimal(token.replace(".", "").replace(",", ""))
    raise ValueError("Ambiguous amount")

def _normalize(value):
    return (clean_text(value) or "").casefold().replace("–", "-").replace("—", "-")

def _bounds(body, unit_pattern):
    # Fullmatch để không lấy nhầm tiền thưởng/năm kinh nghiệm khác trong chuỗi.
    unit = "(?:" + unit_pattern + ")?"
    pair = re.fullmatch(r"(?:từ\s+)?(" + NUM + r")\s*(" + unit_pattern + r")?\s*(?:-|đến|tới)\s*(" + NUM + r")\s*(" + unit_pattern + r")?", body)
    if pair:
        return "RANGE", pair[1], pair[3], True, True, pair[2], pair[4]
    single = re.fullmatch(r"(?:(từ|trên|hơn|ít nhất|tối thiểu|đến|tới|tối đa|dưới|>=|>|<=|<)\s*)?(" + NUM + r")\s*(" + unit_pattern + r")?", body)
    if not single:
        raise ValueError("Unsupported numeric text")
    prefix, value, raw_unit = single.groups()
    if prefix in {"từ", "trên", "hơn", "ít nhất", "tối thiểu", ">", ">="}:
        return "LOWER_BOUND", value, None, prefix not in {"trên", "hơn", ">"}, None, raw_unit, raw_unit
    if prefix in {"đến", "tới", "tối đa", "dưới", "<", "<="}:
        return "UPPER_BOUND", None, value, None, prefix not in {"dưới", "<"}, raw_unit, raw_unit
    return "FIXED", value, value, True, True, raw_unit, raw_unit

def parse_salary(value):
    body = _normalize(value)
    body = re.sub(r"^lương\s*:\s*", "", body)
    out = dict(salary_min_vnd=None, salary_max_vnd=None, salary_midpoint_vnd=None,
               salary_type="MISSING", salary_currency=None)
    flags = []
    if not body:
        return out, ["SALARY_MISSING"]
    if body in {"thỏa thuận", "thoả thuận", "cạnh tranh", "negotiable", "competitive"}:
        out["salary_type"] = "NEGOTIABLE"
        return out, flags
    else:
        flags.append("SALARY_PARSED")
    currencies = set()
    if re.search(r"\busd\b|\$", body): currencies.add("USD")
    if re.search(r"\beur\b|€", body): currencies.add("EUR")
    if re.search(r"\b(?:vnđ|vnd|đồng)\b|₫", body): currencies.add("VND")
    if len(currencies) > 1:
        out["salary_type"] = "UNPARSED"
        return out, flags + ["SALARY_MIXED_CURRENCIES"]
    currency = next(iter(currencies), None)
    scaled = bool(re.search(r"\b(?:triệu|tr|nghìn|ngàn|k)\b", body))
    if currency is None and scaled:
        currency = "VND"
        flags.append("SALARY_CURRENCY_INFERRED")
    out["salary_currency"] = currency
    if currency and currency != "VND":
        out["salary_type"] = "UNPARSED"
        return out, flags + ["SALARY_NON_VND"]
    # Bỏ tiền tệ, giữ đơn vị triệu/nghìn tại từng cận để xử lý '10 triệu - 15 triệu'.
    body = re.sub(r"\b(?:vnđ|vnd|đồng)\b|₫", "", body).strip()
    try:
        typ, low, high, li, hi, lu, hu = _bounds(body, r"triệu|tr|nghìn|ngàn|k")
        if not currency:
            raise ValueError("Currency unknown")
        def multiplier(unit):
            return Decimal(1000000 if unit in {"triệu", "tr"} else 1000 if unit in {"nghìn", "ngàn", "k"} else 1)
        lower = _number(low, multiplier(lu or hu)) if low else None
        upper = _number(high, multiplier(hu or lu)) if high else None
        if (lower is not None and upper is not None and lower > upper):
            raise ValueError("Invalid range")
        if any(x is not None and (x < 0 or x >= Decimal("1e16")) for x in [lower, upper]):
            raise ValueError("Decimal overflow")
        out.update(salary_type=typ,
                   salary_min_vnd=decimal_string(lower) if lower is not None else None,
                   salary_max_vnd=decimal_string(upper) if upper is not None else None,
                   salary_midpoint_vnd=decimal_string((lower+upper)/2) if lower is not None and upper is not None else None)
    except (ValueError, InvalidOperation):
        out["salary_type"] = "UNPARSED"
        flags.append("SALARY_UNPARSED")
    return out, flags

def parse_experience(value):
    body = _normalize(value)
    out = dict(experience_min_years=None, experience_max_years=None, experience_type="MISSING")
    if not body:
        return out, []
    if body in {"không yêu cầu", "chưa có kinh nghiệm", "không yêu cầu kinh nghiệm"}:
        out["experience_type"] = "NO_REQUIREMENT"
        return out, []
    try:
        # Chỉ parse trường kinh nghiệm riêng, không đoán từ đoạn requirement nhiều số.
        body = re.sub(r"\s+kinh nghiệm$", "", body)
        typ, lo, hi, li, ui, lu, hu = _bounds(body, r"năm|tháng")
        if not (lu or hu): raise ValueError("Missing unit")
        def years(s, unit):
            if s is None: return None
            number = Decimal(s.replace(",", ".")) / (12 if unit == "tháng" else 1)
            if not 0 <= number.quantize(Decimal('0.01'),rounding=ROUND_HALF_UP) <= Decimal('999.99'): raise ValueError("Out of range")
            return number
        lower, upper = years(lo, lu or hu), years(hi, hu or lu)
        if lower is not None and upper is not None and lower > upper: raise ValueError("Invalid range")
        out.update(experience_type=typ,
                   experience_min_years=decimal_string(lower) if lower is not None else None,
                   experience_max_years=decimal_string(upper) if upper is not None else None)
        return out, []
    except (ValueError, InvalidOperation):
        out["experience_type"] = "UNPARSED"
        return out, ["EXPERIENCE_UNPARSED"]

def normalize_company(source, value):
    name = clean_text(value)
    if not name: return None
    normalized = name.casefold()
    return dict(company_id=canonical_hash(["company", source, normalized]),
                company_name=name, normalized_name=normalized, source_name=source,
                identity_status="PROVISIONAL_NAME")

def read_csv(path, required):
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if not set(required).issubset(reader.fieldnames or []):
            raise ValueError(f"{path}: cần các cột {required}")
        return list(reader)

def as_bool(value, default=False):
    if value is None or value == "": return default
    if str(value).strip().casefold() in {"1", "true", "yes"}: return True
    if str(value).strip().casefold() in {"0", "false", "no"}: return False
    raise ValueError("Giá trị boolean CSV không hợp lệ: " + str(value))

class SkillNormalizer:
    def __init__(self, dictionary_path, aliases_path):
        self.skills = {}
        for row in read_csv(dictionary_path, ["skill_id", "skill_name", "skill_category", "skill_type", "is_active"]):
            sid, name = clean_text(row["skill_id"]), clean_text(row["skill_name"])
            if not sid or not name or sid in self.skills:
                raise ValueError("skill_id/skill_name rỗng hoặc trùng: " + str(sid))
            self.skills[sid] = {**row, "skill_id":sid, "skill_name":name}
        self.patterns = []
        self.skipped_context_aliases = 0
        seen = {}
        for row in read_csv(aliases_path, ["alias", "skill_id"]):
            alias = clean_text(row["alias"])
            sid = clean_text(row["skill_id"])
            if sid not in self.skills:
                raise ValueError("Alias tham chiếu skill_id không tồn tại: " + str(sid))
            if not alias: raise ValueError("Alias rỗng")
            if not as_bool(self.skills[sid].get("is_active"), True): continue
            if as_bool(row.get("requires_context")):
                self.skipped_context_aliases += 1
                continue
            case = as_bool(row.get("case_sensitive"))
            kind = row.get("match_type") or "token_boundary"
            if kind not in {"token_boundary", "exact"}:
                raise ValueError("match_type chưa hỗ trợ: " + kind)
            key = (alias if case else alias.casefold(), case, kind)
            if key in seen and seen[key] != sid:
                raise ValueError("Alias mơ hồ: " + alias)
            if key in seen: continue
            seen[key] = sid
            # Bao cả +/#/. ở ranh giới để 'C' không khớp C++, 'Java' không khớp JavaScript.
            pattern = re.compile(r"(?<![\w+#.])" + re.escape(alias) + r"(?![\w+#]|\.[\w])",
                                 0 if case else re.IGNORECASE)
            words = re.findall(r'\w+', alias.casefold())
            seed = words[0] if words else None
            self.patterns.append((alias, sid, kind, pattern, seed))
        self.patterns.sort(key=lambda item:(-len(item[0]), item[0], item[1]))

    def find(self, text):
        if not text: return []
        candidates = []
        words = set(re.findall(r'\w+', text.casefold()))
        for alias, sid, kind, pattern, seed in self.patterns:
            if seed is not None and seed not in words: continue
            for match in pattern.finditer(text):
                if kind == "exact" and text.strip() != match[0]: continue
                candidates.append(dict(skill_id=sid, alias=alias, matched_text=match[0],
                                       start=match.start(), end=match.end()))

        accepted = []
        for item in sorted(candidates, key=lambda c:(-(c['end']-c['start']), c['start'], c['skill_id'])):
            if not any(item['start'] < old['end'] and old['start'] < item['end'] for old in accepted):
                accepted.append(item)
        return sorted(accepted, key=lambda c:(c['start'], c['skill_id']))

    def normalize(self, raw_skill):
        """Chỉ nhận toàn bộ alias khi chuẩn hóa một nhãn skill có cấu trúc."""
        text = clean_text(raw_skill)
        if not text: return None
        matches = [m for m in self.find(text) if m['start'] == 0 and m['end'] == len(text)]
        return matches[0]['skill_id'] if len(matches) == 1 else None

def extract_job_info(job_id, payload, normalizer):
    fields = []
    raw_skills = payload.get("skills", payload.get("skills_raw"))
    for text in clean_array(raw_skills) or []:
        # Một số nguồn để tên nghề/tin trong skills; chỉ nhận exact alias.
        sid = normalizer.normalize(text)
        if sid:
            fields.append(("skills_raw", text, "STRUCTURED", [dict(skill_id=sid, start=0, end=len(text), matched_text=text)]))
    for field, source in [("requirements_raw", "requirements"), ("description_raw", "description")]:
        text = clean_multiline(payload.get(source, payload.get(field)))
        if text:
            fields.append((field, text, "DICTIONARY", normalizer.find(text)))
    selected = {}
    for field, text, method, matches in fields:
        for match in matches:
            sid = match['skill_id']
            if sid in selected: continue
            start, end = match['start'], match['end']
            # Lưu cửa sổ bằng chứng và span trên văn bản đã làm sạch của evidence_field.
            evidence = text[max(0,start-100):min(len(text),end+100)]
            selected[sid] = dict(job_id=job_id, skill_id=sid, raw_skill_text=evidence,
                is_required=None, required_level=None,
                years_required=None, confidence_score=None)
    return [selected[sid] for sid in sorted(selected)]

class LocationNormalizer:
    def __init__(self, dictionary, aliases, policy="strict"):
        if policy not in {"strict", "reference"}: raise ValueError("Unknown location policy")
        self.policy = policy
        self.locations = {}
        self.aliases = {}
        for row in read_csv(dictionary, ["location_id", "province_code", "location_name", "location_name_normalized", "location_type"]):
            lid = row["location_id"]
            if not lid or lid in self.locations: raise ValueError("location_id trùng/rỗng")
            self.locations[lid] = row
        for row in read_csv(aliases, ["alias", "location_id"]):
            if row['location_id'] not in self.locations: raise ValueError("Unknown location_id")
            if as_bool(row.get('requires_context')): continue
            key = clean_text(row['alias'])
            if not key: raise ValueError("Location alias empty")
            self.aliases.setdefault(key.casefold(), set()).add(row['location_id'])

    def lookup(self, text, crawl_date):
        text = clean_text(text)
        if not text: return None
        candidates = []
        for lid in self.aliases.get(text.casefold(), set()):
            row = self.locations[lid]
            if self.policy == 'strict':
                if not crawl_date: continue
                if row.get('valid_from'):
                    if crawl_date < row['valid_from'] or (row.get('valid_to') and crawl_date > row['valid_to']): continue
                elif row.get('reference_date') != crawl_date:
                    continue
            candidates.append(lid)
        return candidates[0] if len(candidates) == 1 else None

    def resolve(self, job_id, value, work_location, crawl_date):
        flags = []
        structured = None
        if work_location is not None:
            if not isinstance(work_location, dict): raise ValueError("work_location phải là object")
            structured = clean_text(work_location.get('province'))
        if value is not None and not isinstance(value, str): raise ValueError("location phải là string")
        tokens = [t for item in re.split(r"[,;\n|]+", value or "") if (t := clean_text(item))]
        if structured: tokens.append(structured)
        ids = {}
        unresolved = []
        for token in tokens:
            lid = self.lookup(token, crawl_date)
            if lid: ids[lid] = self.locations[lid]
            else: unresolved.append(token)
        primary = self.lookup(structured, crawl_date) if structured else None
        if primary is None and not structured and len(ids) == 1 and not unresolved:
            primary = next(iter(ids))
        if structured and len(tokens) == 2 and tokens[0].casefold() != structured.casefold():
            flags.append("LOCATION_CONFLICT")
        if unresolved or not tokens: flags.append("LOCATION_UNRESOLVED")
        if self.policy == 'reference' and ids: flags.append("LOCATION_REFERENCE_VINTAGE")
        rows = [dict(job_id=job_id, location_id=lid, location_name=r['location_name'],
                     is_primary=lid == primary, reference_date=r.get('reference_date') or None,
                     resolution_policy=self.policy) for lid,r in sorted(ids.items())]
        return rows, primary, unresolved, flags

def identify(envelope):
    payload = envelope['raw_payload']
    source = normalize_source(payload.get('source_name'))
    sid = payload.get('job_id')
    if isinstance(sid, int) and not isinstance(sid, bool): sid = str(sid)
    sid = clean_text(sid)
    title = clean_text(payload.get('job_title', payload.get('job_title_raw')))
    if not title: raise ValueError("JOB_TITLE_MISSING")
    url = normalize_url(payload.get('job_url'))
    return job_identity(source, sid, url), source, sid, title, url

def version_rank(envelope):
    p, m = envelope['raw_payload'], envelope['_metadata']
    return (parse_timestamp(p.get('crawled_at')) or '',
            parse_date(p.get('updated_at',p.get('updated_at_raw'))) or '',
            parse_timestamp(m.get('ingested_at')) or '',
            canonical_hash([m.get('source_file_sha256'),m.get('row_number'),m.get('record_sha256')]),
            canonical_hash(envelope))

def deduplicate_jobs(envelopes):
    winners, duplicates, quarantine = {}, [], []
    for envelope in envelopes:
        try:
            key = identify(envelope)[0]
            # Validate types for rank now; malformed row is reviewable, not fatal to batch.
            rank = version_rank(envelope)
        except (ValueError, TypeError, KeyError) as exc:
            quarantine.append(dict(reason=str(exc), bronze_record=envelope))
            continue
        old = winners.get(key)
        if old is not None:
            if rank > version_rank(old):
                loser, winners[key] = old, envelope
            else:
                loser = envelope
            duplicates.append(dict(job_id=key, loser_metadata=loser['_metadata']))
        else:
            winners[key] = envelope
    return winners, duplicates, quarantine

COMMON = {
    'job_postings': {
        'job_id':'string!', 'source_name':'string!', 'source_job_id':'string', 'job_title':'string!',
        'occupation_id':'string', 'company_id':'string', 'primary_location_id':'string',
        'salary_min_vnd':'decimal(18,2)', 'salary_max_vnd':'decimal(18,2)', 'salary_midpoint_vnd':'decimal(18,2)',
        'salary_type':'string!', 'salary_currency':'string',
        'experience_min_years':'decimal(5,2)', 'experience_max_years':'decimal(5,2)', 'experience_type':'string!',
        'education_level':'string', 'job_level':'string', 'employment_type':'string', 'working_model':'string',
        'working_time':'string', 'job_description':'string', 'job_requirements':'string',
        'posted_date':'date', 'expiration_date':'date', 'source_updated_date':'date', 'source_url':'string',
        'record_status':'string!', 'crawled_at':'timestamp', 'processed_at':'timestamp!',
        'category_name':'string', 'source_industry_labels':'array', 'source_field_labels':'array',
        'source_requirement_items':'array', 'raw_id':'string!', 'ingestion_batch_id':'string!', 'source_file':'string!',
        'source_record_index':'int!', 'ingested_at':'timestamp', 'data_quality_flags':'array!',
        'unresolved_location_labels':'array!', 'business_hash':'string!',
    },
    'companies': {'company_id':'string!', 'company_name':'string!', 'normalized_name':'string!', 'source_name':'string!', 'identity_status':'string!'},
    'job_locations': {'job_id':'string!', 'location_id':'string!', 'location_name':'string!'},
    'job_skills': {'job_id':'string!', 'skill_id':'string!', 'raw_skill_text':'string!',
                   'is_required':'boolean', 'required_level':'string', 'years_required':'decimal(5,2)', 'confidence_score':'double'},
}
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.table_catalog import table_spec
PRIMARY_KEYS = {name: table_spec(name)['primary_key'] for name in COMMON}

def validate_tables(tables, skill_ids, location_ids):
    from decimal import Decimal
    jobs = {r['job_id']:r for r in tables['job_postings']}
    companies = {r['company_id'] for r in tables['companies']}
    for name, columns in COMMON.items():
        keys = set()
        for row in tables[name]:
            if set(row) != set(columns):
                missing_columns = sorted(set(columns) - set(row))
                extra_columns = sorted(set(row) - set(columns))

                raise ValueError(
                    f"Schema field mismatch: {name}\n"
                    f"Missing columns: {missing_columns}\n"
                    f"Extra columns: {extra_columns}\n"
                    f"Expected columns: {columns}\n"
                    f"Actual columns: {list(row.keys())}"
                )
            key = tuple(row[k] for k in PRIMARY_KEYS[name])
            if key in keys: raise ValueError('Duplicate key: ' + name)
            keys.add(key)
            for col, spec in columns.items():
                v = row[col]
                if spec.endswith('!') and (v is None or v == ''):
                    raise ValueError('Required value missing: ' + name + '.' + col)
                if v is None: continue
                typ = spec.rstrip('!')
                if typ in {'string','date','timestamp'} and not isinstance(v,str): raise ValueError('String expected: '+col)
                if typ == 'boolean' and type(v) is not bool: raise ValueError('Boolean expected: '+col)
                if typ == 'int' and type(v) is not int: raise ValueError('Int expected: '+col)
                if typ == 'array' and (not isinstance(v,list) or any(not isinstance(x,str) for x in v)): raise ValueError('String array expected: '+col)
                if typ.startswith('decimal') and Decimal(v) < 0: raise ValueError('Negative decimal: '+col)
    source_keys = [(r['source_name'],r['source_job_id']) for r in jobs.values() if r['source_job_id']]
    if len(source_keys) != len(set(source_keys)): raise ValueError('Duplicate source job')
    for r in jobs.values():
        if r['company_id'] and r['company_id'] not in companies: raise ValueError('Company FK failed')
        if r['salary_currency'] != 'VND' and any(r[k] is not None for k in ['salary_min_vnd','salary_max_vnd','salary_midpoint_vnd']): raise ValueError('Non-VND salary')
        for prefix,suffix in [('salary','vnd'),('experience','years')]:
            lo,hi = r[f'{prefix}_min_{suffix}'],r[f'{prefix}_max_{suffix}']
            if lo is not None and hi is not None and Decimal(lo)>Decimal(hi): raise ValueError('Reversed range')
    locations = {(r['job_id'],r['location_id']) for r in tables['job_locations'] if r['is_primary']}
    for r in tables['job_locations']:
        if r['job_id'] not in jobs or r['location_id'] not in location_ids: raise ValueError('Location FK failed')
        if r['is_primary'] != (jobs[r['job_id']]['primary_location_id'] == r['location_id']): raise ValueError('Primary location mismatch')
    for r in jobs.values():
        if r['primary_location_id'] and (r['job_id'],r['primary_location_id']) not in locations: raise ValueError('Primary location absent')
    for r in tables['job_skills']:
        if r['job_id'] not in jobs or r['skill_id'] not in skill_ids: raise ValueError('Skill FK failed')
        if r['confidence_score'] is not None and not 0 <= r['confidence_score'] <= 1: raise ValueError('Invalid confidence')

def write_parquet(path, rows, columns):
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError('Parquet cần pyarrow; cài requirements-silver.txt hoặc dùng --format jsonl') from exc
    from datetime import date, datetime
    from decimal import Decimal
    import re
    types = {'string':pa.string(), 'date':pa.date32(), 'timestamp':pa.timestamp('us',tz='UTC'),
             'boolean':pa.bool_(), 'int':pa.int32(), 'double':pa.float64(), 'array':pa.list_(pa.string())}
    fields, typed = [], []
    for name,spec in columns.items():
        typ = spec.rstrip('!')
        dec = re.fullmatch(r'decimal\((\d+),(\d+)\)',typ)
        fields.append(pa.field(name,pa.decimal128(int(dec[1]),int(dec[2])) if dec else types[typ],nullable=not spec.endswith('!')))
    for row in rows:
        record = {}
        for name,spec in columns.items():
            val = row[name]
            if val is not None:
                if spec.startswith('decimal'): val = Decimal(val)
                elif spec.rstrip('!') == 'date': val = date.fromisoformat(val)
                elif spec.rstrip('!') == 'timestamp': val = datetime.fromisoformat(val.replace('Z','+00:00'))
            record[name] = val
        typed.append(record)
    pq.write_table(pa.Table.from_pylist(typed,schema=pa.schema(fields)),path,compression='snappy')

def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''): digest.update(chunk)
    return digest.hexdigest()

def write_json(path,value):
    Path(path).write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')

def write_jsonl(path,rows):
    with Path(path).open('w',encoding='utf-8',newline='\n') as stream:
        for row in rows:
            stream.write(json.dumps(row,ensure_ascii=False,sort_keys=True,allow_nan=False,separators=(',',':'))+'\n')

def read_jsonl(path):
    with Path(path).open(encoding='utf-8-sig') as stream:
        for index,line in enumerate(stream,1):
            if not line.strip(): continue
            try:
                row = json.loads(line)
            except ValueError as exc: raise ValueError(f'{path}:{index}: JSONL hỏng') from exc
            if not isinstance(row,dict) or not isinstance(row.get('raw_payload'),dict) or not isinstance(row.get('_metadata'),dict):
                raise ValueError(f'{path}:{index}: cần raw_payload object + _metadata object')
            yield row

def safe_child(root,relative):
    root = Path(root).resolve()
    path = (root/relative).resolve()
    if not path.is_relative_to(root) or path == root: raise ValueError('Đường dẫn manifest không hợp lệ')
    return path

def read_bronze(root):
    root = Path(root).resolve()
    if not root.is_dir(): raise FileNotFoundError('Không có thư mục Bronze: '+str(root))
    manifests = sorted(root.rglob('manifest.json'))
    records,inputs = [],[]
    for path in manifests:
        if any(part.startswith('.') for part in path.relative_to(root).parts): continue
        manifest = json.loads(path.read_text(encoding='utf-8'))
        if manifest.get('status') != 'completed' or manifest.get('dataset') != 'bronze.job_posting': continue
        count = 0
        for item in manifest['files']:
            data_path = safe_child(path.parent,item['records_path'])
            if file_hash(data_path) != item['records_sha256']: raise ValueError('Bronze checksum mismatch: '+str(data_path))
            rows = list(read_jsonl(data_path))
            if len(rows) != item['record_count']: raise ValueError('Bronze record_count mismatch')
            for row in rows:
                m = row['_metadata']
                if m.get('run_id') != manifest['run_id']: raise ValueError('Bronze run_id mismatch')
                if canonical_hash(row['raw_payload']) != m.get('record_sha256'): raise ValueError('Bronze payload checksum mismatch')
            records.extend(rows)
            count += len(rows)
        if count != manifest['total_records']: raise ValueError('Bronze total_records mismatch')
        inputs.append(dict(path=str(path),sha256=file_hash(path),run_id=manifest['run_id'],records=count))
    if not inputs: raise ValueError('Không tìm thấy Bronze manifest completed; chạy ingest_job_posting.py trước')
    return records,inputs

def minio_client():
    try:
        from minio import Minio
    except ImportError as exc: raise RuntimeError('Cần pip install minio khi dùng MinIO') from exc
    endpoint = os.getenv('MINIO_ENDPOINT','').strip()
    access = os.getenv('MINIO_ACCESS_KEY') or os.getenv('MINIO_ROOT_USER')
    secret = os.getenv('MINIO_SECRET_KEY') or os.getenv('MINIO_ROOT_PASSWORD')
    if not endpoint or not access or not secret: raise ValueError('Thiếu biến môi trường MINIO_ENDPOINT/ACCESS_KEY/SECRET_KEY')
    secure_text = os.getenv('MINIO_SECURE','true').strip().lower()
    if secure_text not in {'true','false','1','0'}: raise ValueError('MINIO_SECURE phải là true/false')
    secure = secure_text in {'true','1'}
    if '://' in endpoint:
        url = urlsplit(endpoint)
        if url.scheme not in {'http','https'} or not url.hostname or url.path not in {'','/'} or url.query or url.fragment or url.username or url.password:
            raise ValueError('MINIO_ENDPOINT không hợp lệ')
        secure,endpoint = url.scheme == 'https',url.netloc
    return Minio(endpoint,access_key=access,secret_key=secret,secure=secure)

def download_bronze(uri,destination):
    url = urlsplit(uri)
    if url.scheme != 's3' or not url.netloc or url.query or url.fragment: raise ValueError('Dùng s3://bucket/prefix')
    prefix = url.path.strip('/')
    if any(p in {'.','..'} for p in prefix.split('/')): raise ValueError('Unsafe S3 prefix')
    prefix = prefix+'/' if prefix else ''
    client = minio_client()
    found = False
    for obj in client.list_objects(url.netloc,prefix=prefix,recursive=True):
        if not obj.object_name.endswith('/manifest.json'): continue
        relative = obj.object_name[len(prefix):]
        manifest_path = safe_child(destination,relative)
        manifest_path.parent.mkdir(parents=True,exist_ok=True)
        client.fget_object(url.netloc,obj.object_name,str(manifest_path))
        manifest = json.loads(manifest_path.read_text())
        if manifest.get('status') != 'completed' or manifest.get('dataset') != 'bronze.job_posting':
            manifest_path.unlink()
            continue
        base = obj.object_name.rsplit('/',1)[0]
        for item in manifest['files']:
            target = safe_child(manifest_path.parent,item['records_path'])
            target.parent.mkdir(parents=True,exist_ok=True)
            client.fget_object(url.netloc,base+'/'+item['records_path'],str(target))
        found = True
    if not found: raise ValueError('Không có Bronze completed trong '+uri)

def upload_snapshot(snapshot,bucket,prefix,pointer,create_bucket=False):
    client = minio_client()
    prefix = prefix.strip('/')
    if not prefix or any(p in {'','.','..'} for p in prefix.split('/')): raise ValueError('Unsafe MinIO prefix')
    if not client.bucket_exists(bucket):
        if not create_bucket: raise ValueError('Bucket Silver chưa tồn tại; tạo bucket hoặc dùng --create-bucket')
        client.make_bucket(bucket)
    base = prefix+'/snapshots/'+snapshot.name
    files = sorted(p for p in snapshot.rglob('*') if p.is_file() and p.name!='manifest.json')
    files.append(snapshot/'manifest.json')
    for path in files:
        key = base+'/'+path.relative_to(snapshot).as_posix()
        client.fput_object(bucket,key,str(path))
        # Không công bố latest nếu object thiếu hoặc sai kích thước sau upload.
        if client.stat_object(bucket,key).size != path.stat().st_size: raise RuntimeError('MinIO size verification failed')
    import io
    payload = json.dumps(pointer,ensure_ascii=False).encode()
    client.put_object(bucket,prefix+'/latest.json',io.BytesIO(payload),len(payload),content_type='application/json')
    return 's3://'+bucket+'/'+base

EDUCATION = {
    'cao đẳng':'COLLEGE', 'đại học':'UNIVERSITY', 'trung cấp':'INTERMEDIATE',
    'thpt':'HIGH_SCHOOL', 'trung học':'HIGH_SCHOOL', 'cử nhân':'UNIVERSITY',
    'sau đại học':'POSTGRADUATE', 'thạc sĩ':'MASTER', 'tiến sĩ':'DOCTORATE',
    'chứng chỉ':'CERTIFICATE', 'không yêu cầu':'NOT_REQUIRED',
}
JOB_LEVEL = {'nhân viên':'STAFF','trưởng phòng':'DEPARTMENT_MANAGER', 'thực tập sinh':'INTERN',
             'trưởng nhóm':'TEAM_LEADER','giám đốc':'DIRECTOR'}
EMPLOYMENT = {'toàn thời gian cố định':'FULL_TIME','toàn thời gian':'FULL_TIME','full-time':'FULL_TIME',
              'bán thời gian':'PART_TIME','part-time':'PART_TIME','thực tập':'INTERNSHIP'}

def lookup(value, dictionary, flags, code):
    text = clean_text(value)
    if not text: return None
    result = dictionary.get(text.casefold())
    if result is None: flags.append(code)
    return result

def labels(value):
    if isinstance(value,str): return [clean_text(value)] if clean_text(value) else []
    return clean_array(value)

def transform_job(envelope, processed_at, skill_normalizer, location_normalizer):
    p, meta = envelope['raw_payload'],envelope['_metadata']
    job_id, source, sid, title, url = identify(envelope)
    flags = []
    if not sid: flags.append('SOURCE_JOB_ID_MISSING')
    if p.get('job_url') and not url: flags.append('SOURCE_URL_INVALID')
    company = normalize_company(source,p.get('company',p.get('company_raw')))
    if not company: flags.append('COMPANY_MISSING')
    row = {name:None for name in COMMON['job_postings']}
    def get(name): return p.get(name,p.get(name+'_raw'))
    crawl = parse_timestamp(p.get('crawled_at'),flags)
    if crawl is None: flags.append('CRAWLED_AT_MISSING')
    posted = parse_date(get('posted_date'), flags, 'POSTED_DATE_PARSE_FAILED')
    if not posted: flags.append('POSTED_DATE_MISSING')
    expiration = parse_date(get('expiration_date'),flags,'EXPIRATION_DATE_PARSE_FAILED')
    updated = parse_date(p.get('updated_at',p.get('updated_at_raw')), flags,'UPDATED_DATE_UNRESOLVED')
    salary,salary_flags = parse_salary(get('salary'))
    experience,experience_flags = parse_experience(get('experience'))
    flags.extend(salary_flags+experience_flags)
    location_rows,primary,unresolved,location_flags = location_normalizer.resolve(job_id,get('location'),p.get('work_location',p.get('work_location_raw')),local_date(crawl))
    flags.extend(location_flags)
    skills = extract_job_info(job_id,p,skill_normalizer)
    if not p.get('skills',p.get('skills_raw')): flags.append('SKILLS_NOT_PROVIDED')
    education = clean_text(get('education'))
    if education: education = education.casefold().removesuffix(' trở lên')
    edu_level = lookup(education,EDUCATION,flags,'EDUCATION_UNMAPPED')
    # Không suy nghề/ngành kinh tế từ tên nhóm tuyển dụng nếu thiếu crosswalk đã duyệt.
    flags.extend(['OCCUPATION_UNRESOLVED','INDUSTRY_UNRESOLVED'])
    model = lookup(p.get('working_model'),{'onsite':'ONSITE','remote':'REMOTE','hybrid':'HYBRID'},flags,'WORKING_MODEL_UNMAPPED')
    if not model: flags.append('WORKING_MODEL_UNKNOWN')
    ingestion_time = parse_timestamp(meta.get('ingested_at'),flags)
    if not meta.get('run_id') or not meta.get('source_file'): raise ValueError('BRONZE_LINEAGE_MISSING')
    if type(meta.get('row_number')) is not int or meta['row_number'] < 1: raise ValueError('BRONZE_ROW_NUMBER_INVALID')
    row.update(job_id=job_id,source_name=source,source_job_id=sid,job_title=title,
        company_id=company['company_id'] if company else None,primary_location_id=primary,
        crawled_at=crawl,processed_at=processed_at,posted_date=posted,expiration_date=expiration,
        source_updated_date=updated,source_url=url,education_level=edu_level,
        job_level=lookup(p.get('position_level',p.get('job_level_raw')),JOB_LEVEL,flags,'JOB_LEVEL_UNMAPPED'),
        employment_type=lookup(p.get('work_type',p.get('employment_type_raw')),EMPLOYMENT,flags,'EMPLOYMENT_TYPE_UNMAPPED'),
        working_time=clean_text(p.get('working_time',p.get('working_time_raw'))),
        job_description=clean_multiline(get('description')),job_requirements=clean_multiline(get('requirements')),
        category_name=clean_text(p.get('category_name') or p.get('category')),
        source_industry_labels=labels(p.get('industry',p.get('industry_raw'))),
        source_field_labels=labels(p.get('field',p.get('field_raw'))),
        source_requirement_items=clean_array(p.get('requirement_items',p.get('requirement_items_raw'))),
        raw_id=canonical_hash([meta.get('source_file_sha256'),meta['row_number']-1]),
        ingestion_batch_id=meta['run_id'],source_file=meta['source_file'],
        source_record_index=meta['row_number']-1,ingested_at=ingestion_time,
        unresolved_location_labels=unresolved,**salary,**experience)
    day = local_date(crawl)
    row['record_status'] = ('EXPIRED' if expiration < day else 'WITHIN_DEADLINE') if expiration and day else 'UNKNOWN'
    row['data_quality_flags'] = sorted(set(flags))
    excluded = {'processed_at','crawled_at','record_status','raw_id','ingestion_batch_id','source_file',
                'source_record_index','ingested_at','business_hash'}
    row['business_hash'] = canonical_hash({k:v for k,v in row.items() if k not in excluded})
    return row,company,location_rows,skills

ROOT = Path(__file__).resolve().parents[2]
LOG = logging.getLogger('silver.jobs')

def run_pipeline(bronze_dir=ROOT/'data/bronze/job_posting', output_dir=ROOT/'data/silver/jobs',
                 mapping_dir=ROOT/'configs/mappings', location_policy='strict', output_format='jsonl',
                 upload=False, bucket='silver', prefix='jobs', create_bucket=False):
    """API cho PythonOperator. Dừng khi kiểm tra dữ liệu/ghi/upload thất bại.

    Gộp winners của snapshot trước để batch cũ không ghi đè bản mới.
    Lock cục bộ: chỉ một writer dùng cùng output_dir; MinIO cần một writer ở Airflow.
    """
    output_dir, mapping_dir = Path(output_dir).resolve(),Path(mapping_dir).resolve()
    if output_format not in {'jsonl','parquet','both'}: raise ValueError('Unsupported output format')
    if output_format != 'jsonl':
        try: import pyarrow  # noqa: F401
        except ImportError as exc: raise RuntimeError('Cài pyarrow trong requirements-silver.txt trước khi xuất Parquet') from exc
    skills = SkillNormalizer(mapping_dir/'skill_dictionary.csv',mapping_dir/'skill_aliases.csv')
    locations = LocationNormalizer(mapping_dir/'location_dictionary.csv',mapping_dir/'location_aliases.csv',location_policy)
    incoming,inputs = read_bronze(bronze_dir)
    output_dir.mkdir(parents=True,exist_ok=True)
    lock = output_dir/'.writer.lock'
    try: lock.mkdir()
    except FileExistsError as exc: raise RuntimeError('Silver writer đang chạy hoặc còn lock sau crash: '+str(lock)) from exc
    stage = None
    try:
        previous = []
        latest = output_dir/'latest.json'
        if latest.exists():
            pointer = json.loads(latest.read_text())
            previous_dir = safe_child(output_dir,pointer['snapshot'])
            previous_manifest = json.loads((previous_dir/'manifest.json').read_text())
            if previous_manifest.get('status') != 'completed': raise ValueError('Previous Silver snapshot incomplete')
            state_path = previous_dir/'state/winners.jsonl'
            if file_hash(state_path) != previous_manifest['files']['state/winners.jsonl']['sha256']:
                raise ValueError('Previous Silver state checksum mismatch')
            previous = list(read_jsonl(state_path))
        # Bỏ envelope giống hệt do đọc lại batch cũ; business duplicate vẫn được báo.
        unique = {canonical_hash(row):row for row in previous+incoming}
        winners,duplicates,quarantine = deduplicate_jobs(unique.values())
        now = datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00','Z')
        snapshot_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'_'+uuid4().hex[:12]
        snapshots = output_dir/'snapshots'
        snapshots.mkdir(exist_ok=True)
        stage = Path(tempfile.mkdtemp(prefix='.pending_',dir=snapshots))
        tables = {name:[] for name in COMMON}
        company_versions,successful = {},[]
        for job_id,envelope in sorted(winners.items()):
            try:
                job,company,locs,job_skills = transform_job(envelope,now,skills,locations)
            except (ValueError,TypeError,KeyError) as exc:
                quarantine.append(dict(reason=str(exc),bronze_record=envelope))
                continue
            tables['job_postings'].append(job)
            tables['job_locations'].extend(locs)
            tables['job_skills'].extend(job_skills)
            successful.append(envelope)
            if company:
                cid,rank = company['company_id'],(version_rank(envelope),job_id)
                if cid not in company_versions or rank > company_versions[cid][0]:
                    company_versions[cid] = (rank,company)
        tables['companies'] = [company_versions[cid][1] for cid in sorted(company_versions)]
        if not tables['job_postings']: raise ValueError('Không có tin hợp lệ; giữ nguyên latest trước đó')
        validate_tables(tables,set(skills.skills),set(locations.locations))
        for name,rows in tables.items():
            if output_format in {'jsonl','both'}: write_jsonl(stage/(name+'.jsonl'),rows)
            if output_format in {'parquet','both'}: write_parquet(stage/(name+'.parquet'),rows,COMMON[name])
        (stage/'state').mkdir()
        # Giữ cả winner lỗi transform để batch cũ không phục hồi phiên bản lỗi thời.
        write_jsonl(stage/'state/winners.jsonl',[winners[k] for k in sorted(winners)])
        write_jsonl(stage/'quarantine.jsonl',quarantine)
        write_jsonl(stage/'duplicates.jsonl',duplicates)
        write_json(stage/'schema.json',dict(version='silver-jobs-v1',tables=COMMON,primary_keys=PRIMARY_KEYS))
        # Đóng băng danh mục/cấu hình dùng trong snapshot để tái lập kết quả.
        (stage/'references').mkdir()
        for name in ['skill_dictionary.csv','skill_aliases.csv','location_dictionary.csv','location_aliases.csv']:
            shutil.copy2(mapping_dir/name,stage/'references'/name)
        flags = Counter(f for r in tables['job_postings'] for f in r['data_quality_flags'])
        metrics = dict(input_records=len(incoming),previous_state_records=len(previous),
            unique_observations=len(unique),duplicate_observations=len(duplicates),
            quarantined_records=len(quarantine),table_counts={k:len(v) for k,v in tables.items()},
            source_counts=dict(Counter(r['source_name'] for r in tables['job_postings'])),
            quality_flags=dict(flags),salary_types=dict(Counter(r['salary_type'] for r in tables['job_postings'])),
            jobs_with_skill_candidates=len({r['job_id'] for r in tables['job_skills']}),
            skipped_context_skill_aliases=skills.skipped_context_aliases)
        write_json(stage/'metrics.json',metrics)
        files = {p.relative_to(stage).as_posix():dict(sha256=file_hash(p),bytes=p.stat().st_size)
                 for p in sorted(stage.rglob('*')) if p.is_file()}
        manifest = dict(dataset='silver.jobs',status='completed',snapshot_id=snapshot_id,
            processed_at=now,format=output_format,location_policy=location_policy,
            input_manifests=inputs,code_sha256=file_hash(Path(__file__)),
            table_counts=metrics['table_counts'],checks=dict(primary_keys=True,foreign_keys=True,ranges=True),files=files)
        write_json(stage/'manifest.json',manifest)
        final = snapshots/snapshot_id
        stage.rename(final)
        stage = None
        pointer = dict(snapshot='snapshots/'+snapshot_id,manifest='snapshots/'+snapshot_id+'/manifest.json',
                       status='completed',table_counts=metrics['table_counts'])
        if upload:
            uri = upload_snapshot(final,bucket,prefix,pointer,create_bucket)
            LOG.info('MinIO hoàn tất: %s',uri)
        temporary_pointer = output_dir/('.latest_'+uuid4().hex+'.json')
        write_json(temporary_pointer,pointer)
        os.replace(temporary_pointer,latest)
        LOG.info('Silver hoàn tất: %s',json.dumps(metrics,ensure_ascii=False))
        return final,metrics
    finally:
        if stage is not None: shutil.rmtree(stage,ignore_errors=True)
        lock.rmdir()

def main(argv=None):
    parser = argparse.ArgumentParser(description='Bronze job_posting → Silver snapshots')
    parser.add_argument('--input-dir',type=Path,default=ROOT/'data/bronze/job_posting')
    parser.add_argument('--output-dir',type=Path,default=ROOT/'data/silver/jobs')
    parser.add_argument('--mapping-dir',type=Path,default=ROOT/'configs/mappings')
    parser.add_argument('--location-policy',choices=['strict','reference'],default='strict')
    parser.add_argument('--format',choices=['jsonl','parquet','both'],default='jsonl')
    parser.add_argument('--bronze-uri',help='Đọc từ s3://bronze/job_posting; thay cho --input-dir')
    parser.add_argument('--minio',action='store_true',help='Upload snapshot và latest.json lên MinIO')
    parser.add_argument('--bucket',default='silver')
    parser.add_argument('--prefix',default='jobs')
    parser.add_argument('--create-bucket',action='store_true')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    try:
        options = dict(output_dir=args.output_dir,mapping_dir=args.mapping_dir,
            location_policy=args.location_policy,output_format=args.format,upload=args.minio,
            bucket=args.bucket,prefix=args.prefix,create_bucket=args.create_bucket)
        if args.bronze_uri:
            with tempfile.TemporaryDirectory(prefix='bronze_jobs_') as tmp:
                download_bronze(args.bronze_uri,Path(tmp))
                final,_ = run_pipeline(Path(tmp),**options)
        else:
            final,_ = run_pipeline(args.input_dir,**options)
        print('SUCCESS: '+str(final))
        return 0
    except Exception:
        LOG.exception('Silver thất bại; latest local chưa được chuyển sang snapshot mới')
        return 1

if __name__ == '__main__':
    raise SystemExit(main())
