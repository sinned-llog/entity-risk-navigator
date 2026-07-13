import os
import json
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlencode

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

ECB_API_BASE_URL = os.getenv(
    "ECB_API_BASE_URL",
    "https://data-api.ecb.europa.eu/service"
).rstrip("/")

ECB_START_PERIOD = os.getenv("ECB_START_PERIOD", "2020-01-01")
ECB_RESPONSE_FORMAT = os.getenv("ECB_RESPONSE_FORMAT", "csvdata")

ECB_FLOW_MIR = os.getenv("ECB_FLOW_MIR", "MIR")
ECB_FLOW_BSI = os.getenv("ECB_FLOW_BSI", "BSI")
ECB_FLOW_EST = os.getenv("ECB_FLOW_EST", "EST")
ECB_FLOW_YC = os.getenv("ECB_FLOW_YC", "YC")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "counterparty-risk-bronze")

APP_ENV = os.getenv("APP_ENV", "dev")


# -------------------------------------------------------------------
# ECB series configuration
# -------------------------------------------------------------------

ECB_SERIES = [
    {
        "dataset_code": ECB_FLOW_MIR,
        "series_key": os.getenv(
            "ECB_MIR_COST_OF_BORROWING_CORP",
            "M.U2.B.A2I.AM.R.A.2240.EUR.N"
        ),
        "indicator_name": "Cost of borrowing for corporations",
        "frequency": "M",
        "unit": "Percent per annum",
        "enabled": True,
    },
    {
        "dataset_code": ECB_FLOW_EST,
        "series_key": os.getenv(
            "ECB_EST_EURO_SHORT_TERM_RATE",
            "B.EU000A2X2A25.WT"
        ),
        "indicator_name": "Euro short-term rate",
        "frequency": "B",
        "unit": "Percent",
        "enabled": True,
    },
    {
        "dataset_code": ECB_FLOW_YC,
        "series_key": os.getenv(
            "ECB_YC_AAA_2Y_SPOT",
            "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y"
        ),
        "indicator_name": "AAA yield curve 2Y spot rate",
        "frequency": "B",
        "unit": "Percent per annum",
        "enabled": True,
    },
    {
        "dataset_code": ECB_FLOW_YC,
        "series_key": os.getenv(
            "ECB_YC_AAA_10Y_SPOT",
            "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y"
        ),
        "indicator_name": "AAA yield curve 10Y spot rate",
        "frequency": "B",
        "unit": "Percent per annum",
        "enabled": True,
    },
    {
        "dataset_code": ECB_FLOW_BSI,
        "series_key": os.getenv("ECB_BSI_LOANS_NFC", ""),
        "indicator_name": "Loans to non-financial corporations",
        "frequency": "M",
        "unit": "To be defined",
        "enabled": bool(os.getenv("ECB_BSI_LOANS_NFC", "").strip()),
    },
    {
        "dataset_code": ECB_FLOW_BSI,
        "series_key": os.getenv("ECB_BSI_M3", ""),
        "indicator_name": "Monetary aggregate M3",
        "frequency": "M",
        "unit": "To be defined",
        "enabled": bool(os.getenv("ECB_BSI_M3", "").strip()),
    },
]


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
    missing = []

    if not MINIO_ROOT_USER:
        missing.append("MINIO_ROOT_USER")

    if not MINIO_ROOT_PASSWORD:
        missing.append("MINIO_ROOT_PASSWORD")

    if not MINIO_ENDPOINT:
        missing.append("MINIO_ENDPOINT")

    if not MINIO_BUCKET:
        missing.append("MINIO_BUCKET")

    if not ECB_API_BASE_URL:
        missing.append("ECB_API_BASE_URL")

    if missing:
        raise RuntimeError(
            "Missing required environment variables: "
            + ", ".join(missing)
        )


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
# ECB helper functions
# -------------------------------------------------------------------

def build_ecb_url(dataset_code: str, series_key: str) -> str:
    params = {
        "format": ECB_RESPONSE_FORMAT,
        "startPeriod": ECB_START_PERIOD,
    }

    return (
        f"{ECB_API_BASE_URL}/data/{dataset_code}/{series_key}"
        f"?{urlencode(params)}"
    )


def sanitize_for_object_key(value: str) -> str:
    return (
        value
        .replace(".", "_")
        .replace("/", "_")
        .replace(" ", "_")
        .replace(":", "_")
    )


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def download_ecb_series(series: dict) -> dict:
    dataset_code = series["dataset_code"]
    series_key = series["series_key"]
    indicator_name = series["indicator_name"]

    url = build_ecb_url(dataset_code, series_key)

    print("------------------------------------------------------------")
    print(f"Downloading ECB series: {dataset_code} / {series_key}")
    print(f"Indicator: {indicator_name}")
    print(f"URL: {url}")

    response = requests.get(
        url,
        timeout=90,
        headers={
            "Accept": "text/csv, application/vnd.sdmx.data+csv, */*",
            "User-Agent": "EntityRisk-Navigator/1.0 educational-project",
        },
    )

    response.raise_for_status()

    content = response.content

    if not content:
        raise RuntimeError(f"Empty response for {dataset_code}/{series_key}")

    checksum = sha256_bytes(content)

    return {
        "dataset_code": dataset_code,
        "series_key": series_key,
        "indicator_name": indicator_name,
        "frequency": series.get("frequency"),
        "unit": series.get("unit"),
        "source_url": url,
        "http_status": response.status_code,
        "content_type": response.headers.get("Content-Type"),
        "content_length": len(content),
        "sha256": checksum,
        "content": content,
    }


# -------------------------------------------------------------------
# Main job
# -------------------------------------------------------------------

def main() -> None:
    validate_environment()

    enabled_series = [
        series for series in ECB_SERIES
        if series.get("enabled") and series.get("series_key")
    ]

    if not enabled_series:
        raise RuntimeError("No ECB series enabled for download.")

    s3_client = get_s3_client()
    ensure_bucket_exists(s3_client, MINIO_BUCKET)

    manifest = {
        "job_name": "download_ecb",
        "app_env": APP_ENV,
        "source": "ECB Data Portal",
        "load_timestamp_utc": LOAD_TIMESTAMP_UTC,
        "load_date": LOAD_DATE,
        "bucket": MINIO_BUCKET,
        "ecb_api_base_url": ECB_API_BASE_URL,
        "start_period": ECB_START_PERIOD,
        "response_format": ECB_RESPONSE_FORMAT,
        "series_count_configured": len(ECB_SERIES),
        "series_count_enabled": len(enabled_series),
        "files": [],
        "status": "running",
    }

    for series in enabled_series:
        try:
            result = download_ecb_series(series)

            dataset_code = result["dataset_code"]
            safe_series_key = sanitize_for_object_key(result["series_key"])

            base_path = f"ecb/{dataset_code}/load_date={LOAD_DATE}"
            file_stem = f"{dataset_code}_{safe_series_key}"

            csv_object_key = f"{base_path}/{file_stem}.csv"
            metadata_object_key = f"{base_path}/{file_stem}.metadata.json"

            upload_bytes_to_minio(
                s3_client=s3_client,
                bucket_name=MINIO_BUCKET,
                object_key=csv_object_key,
                content=result["content"],
                content_type="text/csv",
            )

            metadata = {
                "source": "ECB Data Portal",
                "dataset_code": result["dataset_code"],
                "series_key": result["series_key"],
                "indicator_name": result["indicator_name"],
                "frequency": result["frequency"],
                "unit": result["unit"],
                "source_url": result["source_url"],
                "http_status": result["http_status"],
                "content_type": result["content_type"],
                "content_length": result["content_length"],
                "sha256": result["sha256"],
                "load_timestamp_utc": LOAD_TIMESTAMP_UTC,
                "load_date": LOAD_DATE,
                "minio_bucket": MINIO_BUCKET,
                "minio_object_key": csv_object_key,
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
                    "dataset_code": result["dataset_code"],
                    "series_key": result["series_key"],
                    "indicator_name": result["indicator_name"],
                    "frequency": result["frequency"],
                    "unit": result["unit"],
                    "csv_object_key": csv_object_key,
                    "metadata_object_key": metadata_object_key,
                    "sha256": result["sha256"],
                    "content_length": result["content_length"],
                    "status": "success",
                }
            )

            print(f"Uploaded CSV: s3://{MINIO_BUCKET}/{csv_object_key}")
            print(f"Uploaded metadata: s3://{MINIO_BUCKET}/{metadata_object_key}")

        except Exception as exc:
            error_entry = {
                "dataset_code": series.get("dataset_code"),
                "series_key": series.get("series_key"),
                "indicator_name": series.get("indicator_name"),
                "status": "failed",
                "error": str(exc),
            }

            manifest["files"].append(error_entry)

            print("Download/upload failed:")
            print(json.dumps(error_entry, indent=2))

    failed_files = [
        file for file in manifest["files"]
        if file.get("status") == "failed"
    ]

    manifest["status"] = "failed" if failed_files else "success"
    manifest["failed_count"] = len(failed_files)
    manifest["success_count"] = len(manifest["files"]) - len(failed_files)

    manifest_object_key = (
        f"ecb/_manifests/load_date={LOAD_DATE}/download_ecb_manifest.json"
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
            f"ECB download finished with {len(failed_files)} failed series."
        )


if __name__ == "__main__":
    main()