import os
import json
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from common.download_quality import (
        run_download_quality_checks, 
        summarize_quality_checks
)
from common.minio_client import MinioClient
from common.downloader import HttpDownloader
from common.file_utils import (
    content_type_for_extension,
    sanitize_for_object_key,
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
APP_ENV = os.getenv("APP_ENV", "dev")

GLEIF_LEI_FULL_URL = os.getenv("GLEIF_LEI_FULL_URL")
GLEIF_LEI_DELTA_LASTDAY_URL = os.getenv("GLEIF_LEI_DELTA_LASTDAY_URL")
GLEIF_RR_FULL_URL = os.getenv("GLEIF_RR_FULL_URL")

GLEIF_DOWNLOAD_LEI_FULL = os.getenv("GLEIF_DOWNLOAD_LEI_FULL", "true").lower() == "true"
GLEIF_DOWNLOAD_LEI_DELTA_LASTDAY = os.getenv("GLEIF_DOWNLOAD_LEI_DELTA_LASTDAY", "false").lower() == "true"
GLEIF_DOWNLOAD_RR_FULL = os.getenv("GLEIF_DOWNLOAD_RR_FULL", "true").lower() == "true"

GLEIF_REQUEST_TIMEOUT_SECONDS = int(os.getenv("GLEIF_REQUEST_TIMEOUT_SECONDS", "600"))
GLEIF_CHUNK_SIZE_BYTES = int(os.getenv("GLEIF_CHUNK_SIZE_BYTES", str(1024 * 1024)))
GLEIF_MAX_RETRIES = int(os.getenv("GLEIF_MAX_RETRIES", "3"))
GLEIF_RETRY_SLEEP_SECONDS = int(os.getenv("GLEIF_RETRY_SLEEP_SECONDS", "5"))

LOAD_DT = datetime.now(timezone.utc)
LOAD_DATE = LOAD_DT.strftime("%Y-%m-%d")
LOAD_TIMESTAMP_UTC = LOAD_DT.isoformat()


GLEIF_SOURCES = [
    {
        "source_name": "gleif_lei_full",
        "dataset_group": "lei",
        "snapshot_type": "full",
        "url": GLEIF_LEI_FULL_URL,
        "enabled": GLEIF_DOWNLOAD_LEI_FULL,
        "description": "GLEIF LEI Golden Copy full CSV",
        "quality_expectations": {
            "expected_extensions": ["zip"],
            "zip_must_contain_csv": True,
            "min_expected_bytes": 1000000,
            "check_utf8_sample": False,
        },
    },
    {
        "source_name": "gleif_lei_delta_lastday",
        "dataset_group": "lei",
        "snapshot_type": "delta_lastday",
        "url": GLEIF_LEI_DELTA_LASTDAY_URL,
        "enabled": GLEIF_DOWNLOAD_LEI_DELTA_LASTDAY,
        "description": "GLEIF LEI Golden Copy LastDay delta CSV",
        "quality_expectations": {
            "expected_extensions": ["zip", "csv"],
            "min_expected_bytes": 1000,
            "check_utf8_sample": False,
        },
    },
    {
        "source_name": "gleif_rr_full",
        "dataset_group": "rr",
        "snapshot_type": "full",
        "url": GLEIF_RR_FULL_URL,
        "enabled": GLEIF_DOWNLOAD_RR_FULL,
        "description": "GLEIF Relationship Records Golden Copy full CSV",
        "quality_expectations": {
            "expected_extensions": ["zip"],
            "zip_must_contain_csv": True,
            "min_expected_bytes": 1000000,
            "check_utf8_sample": False,
        },
    },
]


def validate_environment() -> None:
    required_values = {
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ROOT_USER": MINIO_ROOT_USER,
        "MINIO_ROOT_PASSWORD": MINIO_ROOT_PASSWORD,
        "MINIO_BUCKET": MINIO_BUCKET,
    }

    missing = [key for key, value in required_values.items() if not value]

    enabled_sources = [
        source for source in GLEIF_SOURCES
        if source.get("enabled")
    ]

    if not enabled_sources:
        raise RuntimeError("No GLEIF sources enabled. Check GLEIF_DOWNLOAD_* settings.")

    for source in enabled_sources:
        if not source.get("url"):
            missing.append(f"URL for {source['source_name']}")

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )


def main() -> None:
    validate_environment()

    enabled_sources = [
        source for source in GLEIF_SOURCES
        if source.get("enabled")
    ]

    minio = MinioClient(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        bucket=MINIO_BUCKET,
    )

    downloader = HttpDownloader(
        timeout_seconds=GLEIF_REQUEST_TIMEOUT_SECONDS,
        chunk_size_bytes=GLEIF_CHUNK_SIZE_BYTES,
        max_retries=GLEIF_MAX_RETRIES,
        retry_sleep_seconds=GLEIF_RETRY_SLEEP_SECONDS,
    )

    minio.ensure_bucket_exists()

    manifest = {
        "job_name": "download_gleif",
        "app_env": APP_ENV,
        "source": "GLEIF Golden Copy public downloads",
        "load_timestamp_utc": LOAD_TIMESTAMP_UTC,
        "load_date": LOAD_DATE,
        "bucket": MINIO_BUCKET,
        "source_count_configured": len(GLEIF_SOURCES),
        "source_count_enabled": len(enabled_sources),
        "files": [],
        "status": "running",
    }

    temp_files = []

    for source in enabled_sources:
        
        quality_checks = []
        quality_summary = []

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
                f"gleif/{source['dataset_group']}/"
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
                "source": "GLEIF Golden Copy public downloads",
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
        f"gleif/_manifests/load_date={LOAD_DATE}/download_gleif_manifest.json"
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
            f"GLEIF download finished with {len(failed_files)} failed source(s) and {error_count} quality error(s)."
        )

if __name__ == "__main__":
    main()