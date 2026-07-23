import re
from urllib.parse import urlparse

# Mappings für schnelle & saubere Mime-Type Lookups
MIME_TO_EXT = {
    "text/csv": "csv",
    "text/plain": "csv",
    "application/csv": "csv",
    "application/zip": "zip",
    "application/x-zip-compressed": "zip",
    "application/json": "json",
    "text/json": "json",
    "application/xml": "xml",
    "text/xml": "xml",
}

EXT_TO_MIME = {
    "csv": "text/csv",
    "zip": "application/zip",
    "json": "application/json",
    "xml": "application/xml",
}


def guess_file_extension_from_url(url: str, content_type: str | None) -> str:
    path = urlparse(url).path.lower()

    # 1. Endung direkt aus dem URL-Pfad prüfen
    for ext in ("csv", "zip", "json", "xml"):
        if path.endswith(f".{ext}"):
            return ext

    # 2. Falls keine eindeutige Endung in der URL, Content-Type Header prüfen
    if content_type:
        # Entfernt Parameter wie "; charset=utf-8"
        clean_ct = content_type.split(";")[0].strip().lower()
        if clean_ct in MIME_TO_EXT:
            return MIME_TO_EXT[clean_ct]

    return "dat"


def detect_file_extension_from_content(file_path: str, fallback_extension: str) -> str:
    # 512 Bytes lesen, um auch bei leading whitespaces / BOMs sicher zu testen
    with open(file_path, "rb") as file:
        header = file.read(512)

    if not header:
        return fallback_extension

    header_stripped = header.lstrip()

    # ZIP Magic Bytes (PK..)
    if header_stripped.startswith(b"PK\x03\x04"):
        return "zip"

    # JSON (Object/Array, auch mit UTF-8 BOM)
    if header_stripped.startswith((b"{", b"[", b"\xef\xbb\xbf{", b"\xef\xbb\xbf[")):
        return "json"

    # XML (mit oder ohne Header)
    if header_stripped.startswith(b"<?xml") or header_stripped.startswith(b"<"):
        return "xml"

    # UTF-8 BOM für CSV
    if header.startswith(b"\xef\xbb\xbf"):
        return "csv"

    # Heuristik: Reine Textdatei ohne Null-Bytes (0x00) ist im ETL-Kontext meist CSV
    if b"\x00" not in header:
        if fallback_extension not in ("json", "xml", "zip"):
            return "csv"

    return fallback_extension


def content_type_for_extension(extension: str) -> str:
    return EXT_TO_MIME.get(extension.lower(), "application/octet-stream")


def sanitize_for_object_key(value: str) -> str:
    # Ersetzt alle Nicht-Alphanumerischen Zeichen (außer - und _) durch _
    sanitized = re.sub(r"[^a-zA-Z0-9\-_]", "_", value)
    # Reif für S3: Führt mehrfache Unterstriche zu einem einzelnen zusammen ("___" -> "_")
    return re.sub(r"_+", "_", sanitized).strip("_")