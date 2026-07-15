import os
import json
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from ingestion.common.minio_client import MinioClient
from ingestion.common.downloader import HttpDownloader
from ingestion.common.file_utils import (
    content_type_for_extension,
    sanitize_for_object_key,
)


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
APP_ENV = os.getenv("APP_ENV", "dev")

EU_FSF_CSV_FULL_URL = os.getenv("EU_FSF_CSV_FULL_URL")
EU_FSF_REQUEST_TIMEOUT_SECONDS = int(os.getenv("EU_FSF_REQUEST_TIMEOUT_SECONDS", "300"))
EU_FSF_CHUNK_SIZE_BYTES = int(os.getenv("EU_FSF_CHUNK_SIZE_BYTES", str(1024 * 1024)))

LOAD_DT = datetime.now(timezone.utc)
LOAD_DATE = LOAD_DT.strftime("%Y-%m-%d")
LOAD_TIMESTAMP_UTC = LOAD_DT.isoformat()


def validate_environment() -> None:
    required_values = {
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ROOT_USER": MINIO_ROOT_USER,
        "MINIO_ROOT_PASSWORD": MINIO_ROOT_PASSWORD,
        "MINIO_BUCKET": MINIO_BUCKET,
        "EU_FSF_CSV_FULL_URL": EU_FSF_CSV_FULL_URL,
    }

    missing = [key for key, value in required_values.items() if not value]

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )


def main() -> None:
    validate_environment()

    minio = MinioClient(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        bucket=MINIO_BUCKET,
    )

    downloader = HttpDownloader(
        timeout_seconds=EU_FSF_REQUEST_TIMEOUT_SECONDS,
        chunk_size_bytes=EU_FSF_CHUNK_SIZE_BYTES,
    )

    minio.ensure_bucket_exists()

    source = {
        "source_name": "eu_fsf_csv_full",
        "dataset_group": "sanctions",
        "snapshot_type": "full",
        "url": EU_FSF_CSV_FULL_URL,
        "description": "EU Financial Sanctions Files full consolidated CSV list",
    }

    manifest = {
        "job_name": "download_eu_fsf",
        "app_env": APP_ENV,
        "source": "EU Financial Sanctions Files",
        "load_timestamp_utc": LOAD_TIMESTAMP_UTC,
        "load_date": LOAD_DATE,
        "bucket": MINIO_BUCKET,
        "files": [],
        "status": "running",
    }

    temp_file_path = None

    try:
        result = downloader.download_to_tempfile(source["url"])
        temp_file_path = result.temp_file_path

        source_name_safe = sanitize_for_object_key(source["source_name"])
        extension = result.extension

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
            local_file_path=temp_file_path,
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
                "status": "success",
            }
        )

        manifest["status"] = "success"
        manifest["success_count"] = 1
        manifest["failed_count"] = 0

        print(f"Uploaded data: s3://{MINIO_BUCKET}/{data_object_key}")
        print(f"Uploaded metadata: s3://{MINIO_BUCKET}/{metadata_object_key}")

    except Exception as exc:
        manifest["status"] = "failed"
        manifest["success_count"] = 0
        manifest["failed_count"] = 1
        manifest["files"].append(
            {
                "source_name": source["source_name"],
                "dataset_group": source["dataset_group"],
                "snapshot_type": source["snapshot_type"],
                "source_url": source["url"],
                "status": "failed",
                "error": str(exc),
            }
        )

        raise

    finally:
        if temp_file_path:
            try:
                os.remove(temp_file_path)
            except OSError:
                pass

        manifest_object_key = (
            f"eu_fsf/_manifests/load_date={LOAD_DATE}/download_eu_fsf_manifest.json"
        )

        minio.upload_bytes(
            object_key=manifest_object_key,
            content=json.dumps(manifest, indent=2).encode("utf-8"),
            content_type="application/json",
        )

        print(f"Manifest uploaded: s3://{MINIO_BUCKET}/{manifest_object_key}")


if __name__ == "__main__":
    main()