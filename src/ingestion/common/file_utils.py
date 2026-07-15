from urllib.parse import urlparse


def guess_file_extension_from_url(url: str, content_type: str | None) -> str:
    path = urlparse(url).path.lower()

    if path.endswith(".csv"):
        return "csv"
    if path.endswith(".zip"):
        return "zip"
    if path.endswith(".json"):
        return "json"
    if path.endswith(".xml"):
        return "xml"

    if content_type:
        content_type_lower = content_type.lower()

        if "csv" in content_type_lower or "text/plain" in content_type_lower:
            return "csv"
        if "zip" in content_type_lower:
            return "zip"
        if "json" in content_type_lower:
            return "json"
        if "xml" in content_type_lower:
            return "xml"

    return "dat"


def detect_file_extension_from_content(file_path: str, fallback_extension: str) -> str:
    with open(file_path, "rb") as file:
        header = file.read(16)

    if header.startswith(b"PK\x03\x04"):
        return "zip"

    if header.startswith(b"{") or header.startswith(b"["):
        return "json"

    if header.startswith(b"<?xml"):
        return "xml"

    if header.startswith(b"\xef\xbb\xbf"):
        return "csv"

    return fallback_extension


def content_type_for_extension(extension: str) -> str:
    if extension == "csv":
        return "text/csv"
    if extension == "zip":
        return "application/zip"
    if extension == "json":
        return "application/json"
    if extension == "xml":
        return "application/xml"

    return "application/octet-stream"


def sanitize_for_object_key(value: str) -> str:
    return (
        value
        .replace(".", "_")
        .replace("/", "_")
        .replace(" ", "_")
        .replace(":", "_")
        .replace("?", "_")
        .replace("=", "_")
        .replace("&", "_")
    )