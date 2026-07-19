import os
import json
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from ingestion.common.download_quality import (
    summarize_quality_checks,
    run_download_quality_checks,
)
from ingestion.common.minio_client import MinioClient
from ingestion.common.downloader import HttpDownloader
from ingestion.common.file_utils import (
    content_type_for_extension,
    sanitize_for_object_key,
)


# -------------------------------------------------------------------
# Environment configuration
# -------------------------------------------------------------------

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
APP_ENV = os.getenv("APP_ENV", "dev")

OPENSANCTIONS_SANCTIONS_TARGETS_URL = os.getenv("OPENSANCTIONS_SANCTIONS_TARGETS_URL")

OPENSANCTIONS_REQUEST_TIMEOUT_SECONDS = int(
    os.getenv("OPENSANCTIONS_REQUEST_TIMEOUT_SECONDS", "900")
)
OPENSANCTIONS_CHUNK_SIZE_BYTES = int(
    os.getenv("OPENSANCTIONS_CHUNK_SIZE_BYTES", str(1024 * 1024))
)
OPENSANCTIONS_MAX_RETRIES = int(os.getenv("OPENSANCTIONS_MAX_RETRIES", "3"))
OPENSANCTIONS_RETRY_SLEEP_SECONDS = int(os.getenv("OPENSANCTIONS_RETRY_SLEEP_SECONDS", "5"))

# -------------------------------------------------------------------
# Runtime values
# -------------------------------------------------------------------

LOAD_DT = datetime.now(timezone.utc)
LOAD_DATE = LOAD_DT.strftime("%Y-%m-%d")
LOAD_TIMESTAMP_UTC = LOAD_DT.isoformat()


# -------------------------------------------------------------------
# OpenSanctions source configuration
# -------------------------------------------------------------------

OPENSANCTIONS_SOURCES = [
    {
        "source_name": "opensanctions_sanctions_targets_simple",
        "dataset_group": "sanctions",
        "snapshot_type": "full",
        "url": OPENSANCTIONS_SANCTIONS_TARGETS_URL,
        "description": "OpenSanctions sanctions targets.simple.csv bulk download",
    },
]


# -------------------------------------------------------------------
# Validation
# -------------------------------------------------------------------

def validate_environment() -> None:
    required_values = {
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ROOT_USER": MINIO_ROOT_USER,
        "MINIO_ROOT_PASSWORD": MINIO_ROOT_PASSWORD,
        "MINIO_BUCKET": MINIO_BUCKET,
        "OPENSANCTIONS_SANCTIONS_TARGETS_URL": OPENSANCTIONS_SANCTIONS_TARGETS_URL,
    }

    missing = [key for key, value in required_values.items() if not value]

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )


# -------------------------------------------------------------------
# Main job
# -------------------------------------------------------------------

def main() -> None:
    validate_environment()

    minio = MinioClient(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        bucket=MINIO_BUCKET,
    )

    downloader = HttpDownloader(
        timeout_seconds=OPENSANCTIONS_REQUEST_TIMEOUT_SECONDS,
        chunk_size_bytes=OPENSANCTIONS_CHUNK_SIZE_BYTES,
        max_retries=OPENSANCTIONS_MAX_RETRIES,
        retry_sleep_seconds=OPENSANCTIONS_RETRY_SLEEP_SECONDS,
    )

    minio.ensure_bucket_exists()

    manifest = {
        "job_name": "download_opensanctions",
        "app_env": APP_ENV,
        "source": "OpenSanctions",
        "load_timestamp_utc": LOAD_TIMESTAMP_UTC,
        "load_date": LOAD_DATE,
        "bucket": MINIO_BUCKET,
        "source_count": len(OPENSANCTIONS_SOURCES),
        "files": [],
        "status": "running",
    }

    temp_files = []

    for source in OPENSANCTIONS_SOURCES:
        
        quality_checks = []
        quality_summary = {}
        
        try:
            print("------------------------------------------------------------")
            print(f"Source: {source['source_name']}")
            print(f"Description: {source['description']}")

            result = downloader.download_to_tempfile(source["url"])
            temp_files.append(result.temp_file_path)

            source_name_safe = sanitize_for_object_key(source["source_name"])
            extension = result.extension

            quality_expectations = source.get("quality_expectations", {})
            quality_checks = run_download_quality_checks(
                file_path=result.temp_file_path,
                extension=extension,
                expectations=quality_expectations,
            )
            quality_summary = summarize_quality_checks(quality_checks)
            if quality_summary["error_count"] > 0:
                raise RuntimeError(
                    "Download quality checks failed: "
                    + json.dumps(quality_checks, indent=2)
                )

            base_path = (
                f"opensanctions/{source['dataset_group']}/"
                f"snapshot_type={source['snapshot_type']}/"
                f"load_date={LOAD_DATE}"
            )

            file_stem = f"{source_name_safe}_{LOAD_DATE}"

            data_object_key = f"{base_path}/{file_stem}.{extension}"
            metadata_object_key = f"{base_path}/{file_stem}.metadata.json"

            minio.upload_file(
                object_key=data_object_key,
                local_file_path=result.temp_file_path,
                content_type=content_type_for_extension(extension),
            )

            metadata = {
                "source": "OpenSanctions",
                "source_name": source["source_name"],
                "dataset_group": source["dataset_group"],
                "snapshot_type": source["snapshot_type"],
                "description": source["description"],
                "source_url": source["url"],
                "http_status": result.http_status,
                "content_type": result.content_type,
                "content_length_header": result.content_length_header,
                "downloaded_bytes": result.downloaded_bytes,
                "sha256": result.sha256,
                "file_extension": extension,
                "quality_summary": quality_summary,
                "quality_checks": quality_checks,
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
                    "source_name": source["source_name"],
                    "dataset_group": source["dataset_group"],
                    "snapshot_type": source["snapshot_type"],
                    "source_url": source["url"],
                    "data_object_key": data_object_key,
                    "metadata_object_key": metadata_object_key,
                    "downloaded_bytes": result.downloaded_bytes,
                    "sha256": result.sha256,
                    "quality_summary": quality_summary,
                    "quality_checks": quality_checks,
                    "status": "success",
                }
            )

            print(f"Uploaded data: s3://{MINIO_BUCKET}/{data_object_key}")
            print(f"Uploaded metadata: s3://{MINIO_BUCKET}/{metadata_object_key}")

        except Exception as exc:
            error_entry = {
                "source_name": source.get("source_name"),
                "dataset_group": source.get("dataset_group"),
                "snapshot_type": source.get("snapshot_type"),
                "source_url": source.get("url"),
                "status": "failed",
                "quality_summary": quality_summary,
                "quality_checks": quality_checks,
                "error": str(exc),
            }

            manifest["files"].append(error_entry)

            print("Download/upload failed:")
            print(json.dumps(error_entry, indent=2))

    failed_files = [
        file for file in manifest["files"]
        if file.get("status") == "failed"
    ]

    warning_count = 0
    error_count = 0

    for file in manifest["files"]:
        file_quality_summary = file.get("quality_summary") or {}
        warning_count += int(file_quality_summary.get("warning_count", 0))
        error_count += int(file_quality_summary.get("error_count", 0))

    manifest["failed_count"] = len(failed_files)
    manifest["success_count"] = len(manifest["files"]) - len(failed_files)
    manifest["warning_count"] = warning_count
    manifest["error_count"] = error_count

    if failed_files or error_count > 0:
        manifest["status"] = "failed"
    elif warning_count > 0:
        manifest["status"] = "success_with_warnings"
    else:
        manifest["status"] = "success"

    manifest_object_key = (
        f"opensanctions/_manifests/load_date={LOAD_DATE}/download_opensanctions_manifest.json"
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
    print(f"Warnings: {manifest['warning_count']}")
    print(f"Errors: {manifest['error_count']}")

    if failed_files or error_count > 0:
        raise RuntimeError(
            f"OpenSanctions download finished with {len(failed_files)} failed source(s) and {error_count} quality error(s)."
        )

if __name__ == "__main__":
    main()