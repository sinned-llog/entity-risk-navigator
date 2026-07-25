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
    summarize_quality_checks,
)
from common.minio_client import MinioClient
from common.downloader import HttpDownloader
from common.file_utils import (
    content_type_for_extension,
    sanitize_for_object_key,
)
from common.postgres_client import PostgresClient
from common.audit_logger import (
    start_job_run,
    finish_job_run_success,
    finish_job_run_failure,
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
APP_ENV = os.getenv("APP_ENV", "dev")

EU_FSF_CSV_FULL_URL = os.getenv("EU_FSF_CSV_FULL_URL")
EU_FSF_REQUEST_TIMEOUT_SECONDS = int(os.getenv("EU_FSF_REQUEST_TIMEOUT_SECONDS", "300"))
EU_FSF_CHUNK_SIZE_BYTES = int(os.getenv("EU_FSF_CHUNK_SIZE_BYTES", str(1024 * 1024)))
EU_FSF_MAX_RETRIES = int(os.getenv("EU_FSF_MAX_RETRIES", "3"))
EU_FSF_RETRY_SLEEP_SECONDS = int(os.getenv("EU_FSF_RETRY_SLEEP_SECONDS", "5"))

LOAD_DT = datetime.now(timezone.utc)
LOAD_DATE = LOAD_DT.strftime("%Y-%m-%d")
LOAD_TIMESTAMP_UTC = LOAD_DT.isoformat()

EU_FSF_SOURCES = [
    {
        "enabled": True,
        "source_name": "eu_fsf_csv_full",
        "dataset_group": "sanctions",
        "snapshot_type": "full",
        "url": EU_FSF_CSV_FULL_URL,
        "description": "EU Financial Sanctions Files full consolidated CSV list",
        "quality_expectations": {
            "expected_extensions": ["csv"],
            "min_expected_bytes": 1000,
            "check_utf8_sample": True,
        }
    }
]

def validate_environment() -> None:
    required_values = {
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ROOT_USER": MINIO_ROOT_USER,
        "MINIO_ROOT_PASSWORD": MINIO_ROOT_PASSWORD,
        "MINIO_BUCKET": MINIO_BUCKET,
        "EU_FSF_CSV_FULL_URL": EU_FSF_CSV_FULL_URL,
    }

    missing = [key for key, value in required_values.items() if not value]

    enabled_sources = [source for source in EU_FSF_SOURCES if source.get("enabled")]
    
    if not enabled_sources:
        raise RuntimeError("No EU FSF sources enabled.")

    for source in enabled_sources:
        if not source.get("url"):
            missing.append(f"URL for {source['source_name']}")

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )

def main() -> None:
    validate_environment()

    postgres = PostgresClient.from_env()
    job_run_id = None
    manifest_object_key = None
    manifest = None
    temp_files = []
    failure_audited = False

    try:
        job_run_id = start_job_run(
            postgres=postgres,
            job_name="download_eu_fsf",
            job_type="download",
            source="EU Financial Sanctions Files",
            target_system="minio",
            app_env=APP_ENV,
            metadata_json={
                "bucket": MINIO_BUCKET,
                "request_timeout_seconds": EU_FSF_REQUEST_TIMEOUT_SECONDS,
                "chunk_size_bytes": EU_FSF_CHUNK_SIZE_BYTES,
                "max_retries": EU_FSF_MAX_RETRIES,
                "retry_sleep_seconds": EU_FSF_RETRY_SLEEP_SECONDS,
            },
        )

        enabled_sources = [source for source in EU_FSF_SOURCES if source.get("enabled")]

        minio = MinioClient(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ROOT_USER,
            secret_key=MINIO_ROOT_PASSWORD,
            bucket=MINIO_BUCKET,
        )

        downloader = HttpDownloader(
            timeout_seconds=EU_FSF_REQUEST_TIMEOUT_SECONDS,
            chunk_size_bytes=EU_FSF_CHUNK_SIZE_BYTES,
            max_retries=EU_FSF_MAX_RETRIES,
            retry_sleep_seconds=EU_FSF_RETRY_SLEEP_SECONDS,
        )

        minio.ensure_bucket_exists()

        manifest = {
            "job_name": "download_eu_fsf",
            "app_env": APP_ENV,
            "source": "EU Financial Sanctions Files",
            "load_timestamp_utc": LOAD_TIMESTAMP_UTC,
            "load_date": LOAD_DATE,
            "bucket": MINIO_BUCKET,
            "source_count_configured": len(EU_FSF_SOURCES),
            "source_count_enabled": len(enabled_sources),
            "files": [],
            "status": "running",
        }

        for source in enabled_sources:

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
                    f"eu_fsf/{source['dataset_group']}/"
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
                    "source": "EU Financial Sanctions Files",
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
                manifest["files"].append(
                    {
                        "source_name": source["source_name"],
                        "dataset_group": source["dataset_group"],
                        "snapshot_type": source["snapshot_type"],
                        "source_url": source["url"],
                        "status": "failed",
                        "quality_summary": quality_summary,
                        "quality_checks": quality_checks,
                        "error": str(exc),
                    }
                )

                print("Download/upload failed:")
                print(json.dumps(manifest["files"][-1], indent=2))

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

        if manifest["success_count"] > 0 and (failed_files or error_count > 0):
            manifest["status"] = "success_with_warnings"
        elif failed_files or error_count > 0:
            manifest["status"] = "failed"
        elif warning_count > 0:
            manifest["status"] = "success_with_warnings"
        else:
            manifest["status"] = "success"

        manifest_object_key = (
            f"eu_fsf/_manifests/load_date={LOAD_DATE}/download_eu_fsf_manifest.json"
        )
        minio.upload_bytes(
            object_key=manifest_object_key,
            content=json.dumps(manifest, indent=2).encode("utf-8"),
            content_type="application/json",
        )

        print("------------------------------------------------------------")
        print(f"Manifest uploaded: s3://{MINIO_BUCKET}/{manifest_object_key}")
        print(f"Job status: {manifest['status']}")
        print(f"Successful files: {manifest['success_count']}")
        print(f"Failed files: {manifest['failed_count']}")
        print(f"Warnings: {manifest['warning_count']}")
        print(f"Errors: {manifest['error_count']}")

        downloaded_bytes_total = sum(
            int(file.get("downloaded_bytes") or 0)
            for file in manifest.get("files", [])
            if file.get("status") == "success"
        )

        if manifest["status"] == "failed":
            error_message = (
                f"{manifest['job_name']} failed with "
                f"{len(failed_files)} failed source(s) "
                f"and {error_count} quality error(s)."
            )

            finish_job_run_failure(
                postgres=postgres,
                job_run_id=job_run_id,
                error_message=error_message,
                manifest_key=manifest_object_key,
                effective_load_date=LOAD_DATE,
                files_discovered=manifest.get("source_count_enabled") or manifest.get("candidate_count_processed") or manifest.get("series_count_enabled"),
                files_processed=len(manifest.get("files", [])),
                files_success=manifest.get("success_count"),
                files_failed=manifest.get("failed_count"),
                downloaded_bytes=downloaded_bytes_total,
                warning_count=manifest.get("warning_count"),
                error_count=manifest.get("error_count"),
                metadata_json={
                    "bucket": MINIO_BUCKET,
                    "source": manifest.get("source"),
                    "load_date": LOAD_DATE,
                    "manifest_status": manifest.get("status"),
                    },
            )

            failure_audited = True
            
            raise RuntimeError(error_message)
        
        finish_job_run_success(
            postgres=postgres,
            job_run_id=job_run_id,
            status=manifest["status"],
            manifest_key=manifest_object_key,
            effective_load_date=LOAD_DATE,
            files_discovered=manifest.get("source_count_enabled") or manifest.get("candidate_count_processed") or manifest.get("series_count_enabled"),
            files_processed=len(manifest.get("files", [])),
            files_success=manifest.get("success_count"),
            files_failed=manifest.get("failed_count"),
            downloaded_bytes=downloaded_bytes_total,
            warning_count=manifest.get("warning_count"),
            error_count=manifest.get("error_count"),
            metadata_json={
                "bucket": MINIO_BUCKET,
                "source": manifest.get("source"),
                "load_date": LOAD_DATE,
                "source_count_configured": manifest.get("source_count_configured"),
                "source_count_enabled": manifest.get("source_count_enabled"),
            },
        )
    except Exception as exc:
        if job_run_id and not failure_audited:
            finish_job_run_failure(
                postgres=postgres,
                job_run_id=job_run_id, 
                error_message=str(exc),
                manifest_key=manifest_object_key,
                effective_load_date=LOAD_DATE,
                metadata_json={
                    "bucket": MINIO_BUCKET,
                    "source": manifest.get("source") if manifest else None,
                    "manifest_status": manifest.get("status") if manifest else None,
                    "load_date": LOAD_DATE,
                },
            )
        raise
    finally:
        for temp_file_path in temp_files:
            try:
                os.remove(temp_file_path)
            except OSError:
                pass

        postgres.close()

if __name__ == "__main__":
    main()