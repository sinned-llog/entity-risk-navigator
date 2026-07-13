import os
import json
import hashlib
import tempfile
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# -------------------------------------------------------------------
# Environment configuration
# -------------------------------------------------------------------

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "counterparty-risk-bronze")
APP_ENV = os.getenv("APP_ENV", "dev")

GLEIF_LEI_FULL_URL = os.getenv("GLEIF_LEI_FULL_URL")
GLEIF_LEI_DELTA_LASTDAY_URL = os.getenv("GLEIF_LEI_DELTA_LASTDAY_URL")
GLEIF_RR_FULL_URL = os.getenv("GLEIF_RR_FULL_URL")

# Optional switches. Default: load full LEI and RR. Delta is optional.
GLEIF_DOWNLOAD_LEI_FULL = os.getenv("GLEIF_DOWNLOAD_LEI_FULL", "true").lower() == "true"
GLEIF_DOWNLOAD_LEI_DELTA_LASTDAY = os.getenv("GLEIF_DOWNLOAD_LEI_DELTA_LASTDAY", "false").lower() == "true"
GLEIF_DOWNLOAD_RR_FULL = os.getenv("GLEIF_DOWNLOAD_RR_FULL", "true").lower() == "true"

# Network settings
REQUEST_TIMEOUT_SECONDS = int(os.getenv("GLEIF_REQUEST_TIMEOUT_SECONDS", "600"))
CHUNK_SIZE_BYTES = int(os.getenv("GLEIF_CHUNK_SIZE_BYTES", str(1024 * 1024)))


# -------------------------------------------------------------------
# Runtime values
# -------------------------------------------------------------------

LOAD_DT = datetime.now(timezone.utc)
LOAD_DATE = LOAD_DT.strftime("%Y-%m-%d")
LOAD_TIMESTAMP_UTC = LOAD_DT.isoformat()


# -------------------------------------------------------------------
# Source configuration
# -------------------------------------------------------------------

GLEIF_SOURCES = [
    {
        "source_name": "gleif_lei_full",
        "dataset_group": "lei",
        "snapshot_type": "full",
        "url": GLEIF_LEI_FULL_URL,
        "enabled": GLEIF_DOWNLOAD_LEI_FULL,
        "description": "GLEIF LEI Golden Copy full CSV",
    },
    {
        "source_name": "gleif_lei_delta_lastday",
        "dataset_group": "lei",
        "snapshot_type": "delta_lastday",
        "url": GLEIF_LEI_DELTA_LASTDAY_URL,
        "enabled": GLEIF_DOWNLOAD_LEI_DELTA_LASTDAY,
        "description": "GLEIF LEI Golden Copy LastDay delta CSV",
    },
    {
        "source_name": "gleif_rr_full",
        "dataset_group": "rr",
        "snapshot_type": "full",
        "url": GLEIF_RR_FULL_URL,
        "enabled": GLEIF_DOWNLOAD_RR_FULL,
        "description": "GLEIF Relationship Records Golden Copy full CSV",
    },
]


# -------------------------------------------------------------------
# Validation
# -------------------------------------------------------------------

def validate_environment() -> None:
    missing = []

    if not MINIO_ROOT_USER:
        missing.append("MINIO_ROOT_USER")
    if not MINIO_ROOT_PASSWORD:
        missing.append("MINIO_ROOT_PASSWORD")
    if not MINIO_ENDPOINT:
        missing.append("MINIO_ENDPOINT")
    if not MINIO_BUCKET:
        missing.append("MINIO_BUCKET")

    enabled_sources = [src for src in GLEIF_SOURCES if src["enabled"]]
    if not enabled_sources:
        raise RuntimeError("No GLEIF sources enabled. Check GLEIF_DOWNLOAD_* settings.")

    for source in enabled_sources:
        if not source.get("url"):
            missing.append(f"URL for {source['source_name']}")

    if missing:
        raise RuntimeError("Missing required configuration: " + ", ".join(missing))


# -------------------------------------------------------------------
# MinIO helper functions
# -------------------------------------------------------------------

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket_exists(s3_client, bucket_name: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket exists: {bucket_name}")
    except ClientError:
        print(f"Creating bucket: {bucket_name}")
        s3_client.create_bucket(Bucket=bucket_name)


def upload_file_to_minio(
    s3_client,
    bucket_name: str,
    object_key: str,
    local_file_path: str,
    content_type: str,
) -> None:
    s3_client.upload_file(
        Filename=local_file_path,
        Bucket=bucket_name,
        Key=object_key,
        ExtraArgs={"ContentType": content_type},
    )


def upload_bytes_to_minio(
    s3_client,
    bucket_name: str,
    object_key: str,
    content: bytes,
    content_type: str,
) -> None:
    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=content,
        ContentType=content_type,
    )


# -------------------------------------------------------------------
# Download helper functions
# -------------------------------------------------------------------

def guess_file_extension(url: str, content_type: str | None) -> str:
    path = urlparse(url).path.lower()

    if path.endswith(".csv"):
        return "csv"
    if path.endswith(".zip"):
        return "zip"
    if path.endswith(".json"):
        return "json"

    if content_type:
        ct = content_type.lower()
        if "zip" in ct:
            return "zip"
        if "json" in ct:
            return "json"
        if "csv" in ct or "text/plain" in ct:
            return "csv"

    return "dat"


def content_type_for_extension(extension: str) -> str:
    if extension == "csv":
        return "text/csv"
    if extension == "zip":
        return "application/zip"
    if extension == "json":
        return "application/json"
    return "application/octet-stream"


def stream_download_to_tempfile(url: str) -> dict:
    """Download URL as stream, write to temp file, compute sha256 and size."""
    print(f"Downloading: {url}")

    response = requests.get(
        url,
        stream=True,
        timeout=REQUEST_TIMEOUT_SECONDS,
        headers={
            "User-Agent": "EntityRisk-Navigator/1.0 educational-project",
            "Accept": "text/csv, application/zip, application/json, */*",
        },
    )
    response.raise_for_status()

    content_type = response.headers.get("Content-Type")
    extension = guess_file_extension(url, content_type)

    sha256 = hashlib.sha256()
    total_bytes = 0

    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{extension}")
    temp_file_path = temp_file.name

    try:
        with temp_file:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE_BYTES):
                if not chunk:
                    continue
                temp_file.write(chunk)
                sha256.update(chunk)
                total_bytes += len(chunk)

        if total_bytes == 0:
            raise RuntimeError(f"Empty response for URL: {url}")

        return {
            "temp_file_path": temp_file_path,
            "http_status": response.status_code,
            "content_type": content_type,
            "extension": extension,
            "content_length_header": response.headers.get("Content-Length"),
            "downloaded_bytes": total_bytes,
            "sha256": sha256.hexdigest(),
        }

    except Exception:
        try:
            os.remove(temp_file_path)
        except OSError:
            pass
        raise


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


# -------------------------------------------------------------------
# Main job
# -------------------------------------------------------------------

def main() -> None:
    validate_environment()

    enabled_sources = [source for source in GLEIF_SOURCES if source["enabled"]]

    s3_client = get_s3_client()
    ensure_bucket_exists(s3_client, MINIO_BUCKET)

    manifest = {
        "job_name": "download_gleif",
        "app_env": APP_ENV,
        "source": "GLEIF",
        "load_timestamp_utc": LOAD_TIMESTAMP_UTC,
        "load_date": LOAD_DATE,
        "bucket": MINIO_BUCKET,
        "source_count_configured": len(GLEIF_SOURCES),
        "source_count_enabled": len(enabled_sources),
        "files": [],
        "status": "running",
    }

    for source in enabled_sources:
        temp_file_path = None

        try:
            print("------------------------------------------------------------")
            print(f"Source: {source['source_name']}")
            print(f"Description: {source['description']}")

            download_result = stream_download_to_tempfile(source["url"])
            temp_file_path = download_result["temp_file_path"]

            source_name_safe = sanitize_for_object_key(source["source_name"])
            extension = download_result["extension"]

            base_path = (
                f"gleif/{source['dataset_group']}/"
                f"snapshot_type={source['snapshot_type']}/"
                f"load_date={LOAD_DATE}"
            )
            file_stem = f"{source_name_safe}_{LOAD_DATE}"

            data_object_key = f"{base_path}/{file_stem}.{extension}"
            metadata_object_key = f"{base_path}/{file_stem}.metadata.json"

            upload_file_to_minio(
                s3_client=s3_client,
                bucket_name=MINIO_BUCKET,
                object_key=data_object_key,
                local_file_path=temp_file_path,
                content_type=content_type_for_extension(extension),
            )

            metadata = {
                "source": "GLEIF",
                "source_name": source["source_name"],
                "dataset_group": source["dataset_group"],
                "snapshot_type": source["snapshot_type"],
                "description": source["description"],
                "source_url": source["url"],
                "http_status": download_result["http_status"],
                "content_type": download_result["content_type"],
                "content_length_header": download_result["content_length_header"],
                "downloaded_bytes": download_result["downloaded_bytes"],
                "sha256": download_result["sha256"],
                "file_extension": extension,
                "load_timestamp_utc": LOAD_TIMESTAMP_UTC,
                "load_date": LOAD_DATE,
                "minio_bucket": MINIO_BUCKET,
                "minio_object_key": data_object_key,
            }

            upload_bytes_to_minio(
                s3_client=s3_client,
                bucket_name=MINIO_BUCKET,
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
                    "downloaded_bytes": download_result["downloaded_bytes"],
                    "sha256": download_result["sha256"],
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
                "error": str(exc),
            }
            manifest["files"].append(error_entry)

            print("Download/upload failed:")
            print(json.dumps(error_entry, indent=2))

        finally:
            if temp_file_path:
                try:
                    os.remove(temp_file_path)
                except OSError:
                    pass

    failed_files = [file for file in manifest["files"] if file.get("status") == "failed"]

    manifest["status"] = "failed" if failed_files else "success"
    manifest["failed_count"] = len(failed_files)
    manifest["success_count"] = len(manifest["files"]) - len(failed_files)

    manifest_object_key = (
        f"gleif/_manifests/load_date={LOAD_DATE}/download_gleif_manifest.json"
    )

    upload_bytes_to_minio(
        s3_client=s3_client,
        bucket_name=MINIO_BUCKET,
        object_key=manifest_object_key,
        content=json.dumps(manifest, indent=2).encode("utf-8"),
        content_type="application/json",
    )

    print("------------------------------------------------------------")
    print(f"Manifest uploaded: s3://{MINIO_BUCKET}/{manifest_object_key}")
    print(f"Job status: {manifest['status']}")
    print(f"Successful files: {manifest['success_count']}")
    print(f"Failed files: {manifest['failed_count']}")

    if failed_files:
        raise RuntimeError(
            f"GLEIF download finished with {len(failed_files)} failed source(s)."
        )


if __name__ == "__main__":
    main()
