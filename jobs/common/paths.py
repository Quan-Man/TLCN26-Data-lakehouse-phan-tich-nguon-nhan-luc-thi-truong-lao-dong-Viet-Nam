"""Đường dẫn object storage dùng s3a:// cho Spark; không dùng pathlib với URI."""
import re


def layer_path(settings, layer: str, *parts: str) -> str:
    if layer not in {"bronze", "silver", "gold"}:
        raise ValueError("layer phải là bronze, silver hoặc gold")
    bucket = getattr(settings, f"{layer}_bucket")
    if (not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket)
            or ".." in bucket or ".-" in bucket or "-." in bucket
            or re.fullmatch(r"\d+\.\d+\.\d+\.\d+", bucket)):
        raise ValueError("Tên bucket không hợp lệ")
    segments = []
    for part in parts:
        if not isinstance(part, str) or "://" in part or "\\" in part:
            raise ValueError("Thành phần object path không hợp lệ")
        for segment in part.strip("/").split("/"):
            if segment in {"", ".", ".."}:
                raise ValueError("Object path không được rỗng hoặc chứa . / ..")
            segments.append(segment)
    return f"s3a://{bucket}" + ("/" + "/".join(segments) if segments else "")
