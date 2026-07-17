import os
import csv
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import quote_plus
from typing import Any

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from ingestion.common.minio_client import MinioClient
from ingestion.common.downloader import HttpDownloader
from ingestion.common.file_utils import sanitize_for_object_key


# -------------------------------------------------------------------
# Environment configuration
# -------------------------------------------------------------------

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
APP_ENV = os.getenv("APP_ENV", "dev")

BAFIN_CANDIDATES_FILE = os.getenv(
    "BAFIN_CANDIDATES_FILE",
    "/app/config/bafin_candidates.csv",
)

BAFIN_COMPANY_SEARCH_URL_TEMPLATE = os.getenv("BAFIN_COMPANY_SEARCH_URL_TEMPLATE")

BAFIN_REQUEST_TIMEOUT_SECONDS = int(
    os.getenv("BAFIN_REQUEST_TIMEOUT_SECONDS", "120")
)

BAFIN_CHUNK_SIZE_BYTES = int(
    os.getenv("BAFIN_CHUNK_SIZE_BYTES", str(1024 * 1024))
)

BAFIN_SLEEP_SECONDS = float(
    os.getenv("BAFIN_SLEEP_SECONDS", "3")
)

BAFIN_MAX_PER_RUN = int(
    os.getenv("BAFIN_MAX_PER_RUN", "20")
)


# -------------------------------------------------------------------
# Runtime values
# -------------------------------------------------------------------

LOAD_DT = datetime.now(timezone.utc)
LOAD_DATE = LOAD_DT.strftime("%Y-%m-%d")
LOAD_TIMESTAMP_UTC = LOAD_DT.isoformat()


# -------------------------------------------------------------------
# Validation
# -------------------------------------------------------------------

def validate_environment() -> None:
    required_values = {
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ROOT_USER": MINIO_ROOT_USER,
        "MINIO_ROOT_PASSWORD": MINIO_ROOT_PASSWORD,
        "MINIO_BUCKET": MINIO_BUCKET,
        "BAFIN_CANDIDATES_FILE": BAFIN_CANDIDATES_FILE,
    }

    missing = [key for key, value in required_values.items() if not value]

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )

    candidates_path = Path(BAFIN_CANDIDATES_FILE)

    if not candidates_path.exists():
        raise FileNotFoundError(
            f"BaFin candidates file does not exist: {BAFIN_CANDIDATES_FILE}"
        )

# -------------------------------------------------------------------
# Candidate handling
# -------------------------------------------------------------------

def parse_bool(value: str | None) -> bool:
    if value is None:
        return False

    return value.strip().lower() in {"true", "1", "yes", "y"}


def load_candidates(file_path: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []

    with open(file_path, mode="r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        for row_number, row in enumerate(reader, start=1):
            enabled = parse_bool(row.get("enabled"))

            if not enabled:
                continue

            search_name = (
                row.get("search_name")
                or row.get("legal_name")
                or ""
            ).strip()

            if not search_name:
                continue

            candidate = {
                "candidate_id": row.get("candidate_id") or str(row_number),
                "lei": row.get("lei"),
                "legal_name": row.get("legal_name"),
                "search_name": search_name,
                "jurisdiction": row.get("jurisdiction"),
                "country": row.get("country"),
                "source_reason": row.get("source_reason"),
                "priority": row.get("priority"),
                "enabled": enabled,
                "bafin_institut_id": row.get("bafin_institut_id"),
                "bafin_detail_url": row.get("bafin_detail_url"),
            }

            candidates.append(candidate)

    return candidates

def build_bafin_url(candidate: dict[str, Any]) -> str:
    detail_url = (candidate.get("bafin_detail_url") or "").strip()

    if detail_url:
        return detail_url

    institut_id = (candidate.get("bafin_institut_id") or "").strip()

    if institut_id:
        return (
            "https://portal.mvp.bafin.de/database/InstInfo/institutDetails.do"
            f"?cmd=loadInstitutAction&institutId={institut_id}"
        )

    if not BAFIN_COMPANY_SEARCH_URL_TEMPLATE:
        raise RuntimeError(
            "No BaFin detail URL, no institut ID and no search URL template available."
        )

    if "{query}" not in BAFIN_COMPANY_SEARCH_URL_TEMPLATE:
        raise RuntimeError(
            "BAFIN_COMPANY_SEARCH_URL_TEMPLATE must contain the placeholder {query}."
        )

    return BAFIN_COMPANY_SEARCH_URL_TEMPLATE.replace(
        "{query}",
        quote_plus(candidate["search_name"]),
    )


def determine_bafin_extension(content_type: str | None, fallback_extension: str) -> str:
    if content_type and "html" in content_type.lower():
        return "html"

    return fallback_extension


def determine_bafin_content_type(extension: str, original_content_type: str | None) -> str:
    if extension == "html":
        return "text/html"

    if original_content_type:
        return original_content_type

    return "application/octet-stream"


# -------------------------------------------------------------------
# Main job
# -------------------------------------------------------------------

def main() -> None:
    validate_environment()

    candidates = load_candidates(BAFIN_CANDIDATES_FILE)

    if not candidates:
        raise RuntimeError("No enabled BaFin candidates found.")

    candidates_to_process = candidates[:BAFIN_MAX_PER_RUN]

    minio = MinioClient(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        bucket=MINIO_BUCKET,
    )

    downloader = HttpDownloader(
        timeout_seconds=BAFIN_REQUEST_TIMEOUT_SECONDS,
        chunk_size_bytes=BAFIN_CHUNK_SIZE_BYTES,
        user_agent="EntityRisk-Navigator/1.0 educational-project defensive-bafin-enrichment",
    )

    minio.ensure_bucket_exists()

    manifest = {
        "job_name": "download_bafin",
        "app_env": APP_ENV,
        "source": "BaFin Unternehmensdatenbank",
        "load_timestamp_utc": LOAD_TIMESTAMP_UTC,
        "load_date": LOAD_DATE,
        "bucket": MINIO_BUCKET,
        "candidate_file": BAFIN_CANDIDATES_FILE,
        "candidate_count_total_enabled": len(candidates),
        "candidate_count_processed": len(candidates_to_process),
        "max_per_run": BAFIN_MAX_PER_RUN,
        "sleep_seconds": BAFIN_SLEEP_SECONDS,
        "files": [],
        "status": "running",
    }

    temp_files = []

    for index, candidate in enumerate(candidates_to_process, start=1):
        try:
            print("------------------------------------------------------------")
            print(f"Candidate {index}/{len(candidates_to_process)}")
            print(f"Search name: {candidate['search_name']}")

            source_url = build_bafin_url(candidate)

            result = downloader.download_to_tempfile(source_url)
            temp_files.append(result.temp_file_path)

            extension = determine_bafin_extension(
                content_type=result.content_type,
                fallback_extension=result.extension,
            )

            content_type = determine_bafin_content_type(
                extension=extension,
                original_content_type=result.content_type,
            )

            safe_candidate_id = sanitize_for_object_key(str(candidate["candidate_id"]))
            safe_search_name = sanitize_for_object_key(candidate["search_name"])

            base_path = (
                f"bafin/company_detail/"
                f"snapshot_type=detail_page/"
                f"load_date={LOAD_DATE}"
            )

            file_stem = f"bafin_detail_{safe_candidate_id}_{safe_search_name}"

            data_object_key = f"{base_path}/{file_stem}.{extension}"
            metadata_object_key = f"{base_path}/{file_stem}.metadata.json"

            minio.upload_file(
                object_key=data_object_key,
                local_file_path=result.temp_file_path,
                content_type=content_type,
            )

            metadata = {
                "source": "BaFin Unternehmensdatenbank",
                "bafin_institut_id": candidate.get("bafin_institut_id"),
                "bafin_detail_url": candidate.get("bafin_detail_url"),
                "source_name": "bafin_company_search",
                "candidate_id": candidate["candidate_id"],
                "lei": candidate.get("lei"),
                "legal_name": candidate.get("legal_name"),
                "search_name": candidate["search_name"],
                "jurisdiction": candidate.get("jurisdiction"),
                "country": candidate.get("country"),
                "source_reason": candidate.get("source_reason"),
                "priority": candidate.get("priority"),
                "source_url": source_url,
                "http_status": result.http_status,
                "content_type": result.content_type,
                "content_length_header": result.content_length_header,
                "downloaded_bytes": result.downloaded_bytes,
                "sha256": result.sha256,
                "file_extension": extension,
                "load_timestamp_utc": LOAD_TIMESTAMP_UTC,
                "load_date": LOAD_DATE,
                "minio_bucket": MINIO_BUCKET,
                "minio_object_key": data_object_key,
            }

            minio.upload_bytes(
                object_key=metadata_object_key,
                content=json.dumps(metadata, indent=2).encode("utf-8"),
                content_type="application/json",
            )

            manifest["files"].append(
                {
                    "bafin_institut_id": candidate.get("bafin_institut_id"),
                    "candidate_id": candidate["candidate_id"],
                    "lei": candidate.get("lei"),
                    "legal_name": candidate.get("legal_name"),
                    "search_name": candidate["search_name"],
                    "source_url": source_url,
                    "data_object_key": data_object_key,
                    "metadata_object_key": metadata_object_key,
                    "downloaded_bytes": result.downloaded_bytes,
                    "sha256": result.sha256,
                    "status": "success",
                }
            )

            print(f"Uploaded data: s3://{MINIO_BUCKET}/{data_object_key}")
            print(f"Uploaded metadata: s3://{MINIO_BUCKET}/{metadata_object_key}")

        except Exception as exc:
            error_entry = {
                "candidate_id": candidate.get("candidate_id"),
                "lei": candidate.get("lei"),
                "legal_name": candidate.get("legal_name"),
                "search_name": candidate.get("search_name"),
                "status": "failed",
                "error": str(exc),
            }

            manifest["files"].append(error_entry)

            print("Download/upload failed:")
            print(json.dumps(error_entry, indent=2))

        if index < len(candidates_to_process):
            time.sleep(BAFIN_SLEEP_SECONDS)

    failed_files = [
        file for file in manifest["files"]
        if file.get("status") == "failed"
    ]

    manifest["status"] = "failed" if failed_files else "success"
    manifest["failed_count"] = len(failed_files)
    manifest["success_count"] = len(manifest["files"]) - len(failed_files)

    manifest_object_key = (
        f"bafin/_manifests/load_date={LOAD_DATE}/download_bafin_manifest.json"
    )

    minio.upload_bytes(
        object_key=manifest_object_key,
        content=json.dumps(manifest, indent=2).encode("utf-8"),
        content_type="application/json",
    )

    for temp_file_path in temp_files:
        try:
            os.remove(temp_file_path)
        except OSError:
            pass

    print("------------------------------------------------------------")
    print(f"Manifest uploaded: s3://{MINIO_BUCKET}/{manifest_object_key}")
    print(f"Job status: {manifest['status']}")
    print(f"Successful files: {manifest['success_count']}")
    print(f"Failed files: {manifest['failed_count']}")

    if failed_files:
        raise RuntimeError(
            f"BaFin download finished with {len(failed_files)} failed candidate(s)."
        )


if __name__ == "__main__":
    main()