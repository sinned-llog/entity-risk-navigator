import os
import json
from datetime import datetime, timezone
from urllib.parse import urlencode

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from ingestion.common.download_quality import run_download_quality_checks, summarize_quality_checks
from ingestion.common.minio_client import MinioClient
from ingestion.common.downloader import HttpDownloader
from ingestion.common.file_utils import (
    content_type_for_extension,
    sanitize_for_object_key,
)


ECB_API_BASE_URL = os.getenv("ECB_API_BASE_URL", "https://data-api.ecb.europa.eu/service").rstrip("/")
ECB_START_PERIOD = os.getenv("ECB_START_PERIOD", "2020-01-01")
ECB_RESPONSE_FORMAT = os.getenv("ECB_RESPONSE_FORMAT", "csvdata")

ECB_FLOW_MIR = os.getenv("ECB_FLOW_MIR", "MIR")
ECB_FLOW_BSI = os.getenv("ECB_FLOW_BSI", "BSI")
ECB_FLOW_EST = os.getenv("ECB_FLOW_EST", "EST")
ECB_FLOW_YC = os.getenv("ECB_FLOW_YC", "YC")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
APP_ENV = os.getenv("APP_ENV", "dev")

ECB_REQUEST_TIMEOUT_SECONDS = int(os.getenv("ECB_REQUEST_TIMEOUT_SECONDS", "300"))
ECB_CHUNK_SIZE_BYTES = int(os.getenv("ECB_CHUNK_SIZE_BYTES", str(1024 * 1024)))

LOAD_DT = datetime.now(timezone.utc)
LOAD_DATE = LOAD_DT.strftime("%Y-%m-%d")
LOAD_TIMESTAMP_UTC = LOAD_DT.isoformat()


ECB_SERIES = [
    {
        "dataset_code": ECB_FLOW_MIR,
        "series_key": os.getenv(
            "ECB_MIR_COST_OF_BORROWING_CORP",
            "M.U2.B.A2I.AM.R.A.2240.EUR.N",
        ),
        "indicator_name": "Cost of borrowing for corporations",
        "frequency": "M",
        "unit": "Percent per annum",
        "enabled": True,
        "quality_expectations": {
            "expected_extensions": ["csv"],
            "required_columns": ["TIME_PERIOD", "OBS_VALUE"],
            "min_expected_bytes": 50,
            "check_utf8_sample": True,
    },
    },
    {
        "dataset_code": ECB_FLOW_EST,
        "series_key": os.getenv(
            "ECB_EST_EURO_SHORT_TERM_RATE",
            "B.EU000A2X2A25.WT",
        ),
        "indicator_name": "Euro short-term rate",
        "frequency": "B",
        "unit": "Percent",
        "enabled": True,
        "quality_expectations": {
            "expected_extensions": ["csv"],
            "required_columns": ["TIME_PERIOD", "OBS_VALUE"],
            "min_expected_bytes": 50,
            "check_utf8_sample": True,
        },
    },
    {
        "dataset_code": ECB_FLOW_YC,
        "series_key": os.getenv(
            "ECB_YC_AAA_2Y_SPOT",
            "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y",
        ),
        "indicator_name": "AAA yield curve 2Y spot rate",
        "frequency": "B",
        "unit": "Percent per annum",
        "enabled": True,
        "quality_expectations": {
            "expected_extensions": ["csv"],
            "required_columns": ["TIME_PERIOD", "OBS_VALUE"],
            "min_expected_bytes": 50,
            "check_utf8_sample": True,
        },
    },
    {
        "dataset_code": ECB_FLOW_YC,
        "series_key": os.getenv(
            "ECB_YC_AAA_10Y_SPOT",
            "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
        ),
        "indicator_name": "AAA yield curve 10Y spot rate",
        "frequency": "B",
        "unit": "Percent per annum",
        "enabled": True,
        "quality_expectations": {
            "expected_extensions": ["csv"],
            "required_columns": ["TIME_PERIOD", "OBS_VALUE"],
            "min_expected_bytes": 50,
            "check_utf8_sample": True,
        },
    },
    {
        "dataset_code": ECB_FLOW_BSI,
        "series_key": os.getenv("ECB_BSI_LOANS_NFC", ""),
        "indicator_name": "Loans to non-financial corporations",
        "frequency": "M",
        "unit": "To be defined",
        "enabled": bool(os.getenv("ECB_BSI_LOANS_NFC", "").strip()),
        "quality_expectations": {
            "expected_extensions": ["csv"],
            "required_columns": ["TIME_PERIOD", "OBS_VALUE"],
            "min_expected_bytes": 50,
            "check_utf8_sample": True,
        },
    },
    {
        "dataset_code": ECB_FLOW_BSI,
        "series_key": os.getenv("ECB_BSI_M3", ""),
        "indicator_name": "Monetary aggregate M3",
        "frequency": "M",
        "unit": "To be defined",
        "enabled": bool(os.getenv("ECB_BSI_M3", "").strip()),
        "quality_expectations": {
            "expected_extensions": ["csv"],
            "required_columns": ["TIME_PERIOD", "OBS_VALUE"],
            "min_expected_bytes": 50,
            "check_utf8_sample": True,
        },
    },
]


def validate_environment() -> None:
    required_values = {
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ROOT_USER": MINIO_ROOT_USER,
        "MINIO_ROOT_PASSWORD": MINIO_ROOT_PASSWORD,
        "MINIO_BUCKET": MINIO_BUCKET,
        "ECB_API_BASE_URL": ECB_API_BASE_URL,
    }

    missing = [key for key, value in required_values.items() if not value]

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )


def build_ecb_url(dataset_code: str, series_key: str) -> str:
    params = {
        "format": ECB_RESPONSE_FORMAT,
        "startPeriod": ECB_START_PERIOD,
    }

    return (
        f"{ECB_API_BASE_URL}/data/{dataset_code}/{series_key}"
        f"?{urlencode(params)}"
    )


def main() -> None:
    validate_environment()

    enabled_series = [
        series for series in ECB_SERIES
        if series.get("enabled") and series.get("series_key")
    ]

    if not enabled_series:
        raise RuntimeError("No ECB series enabled for download.")

    minio = MinioClient(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        bucket=MINIO_BUCKET,
    )

    downloader = HttpDownloader(
        timeout_seconds=ECB_REQUEST_TIMEOUT_SECONDS,
        chunk_size_bytes=ECB_CHUNK_SIZE_BYTES,
    )

    minio.ensure_bucket_exists()

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

    temp_files = []

    for series in enabled_series:

        quality_checks = []
        quality_summary = {}

        try:
            dataset_code = series["dataset_code"]
            series_key = series["series_key"]
            indicator_name = series["indicator_name"]
            source_url = build_ecb_url(dataset_code, series_key)

            print("------------------------------------------------------------")
            print(f"Dataset: {dataset_code}")
            print(f"Series key: {series_key}")
            print(f"Indicator: {indicator_name}")

            result = downloader.download_to_tempfile(source_url)
            temp_files.append(result.temp_file_path)

            safe_series_key = sanitize_for_object_key(series_key)
            extension = result.extension

            quality_expectations = series.get("quality_expectations", {})

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

            base_path = f"ecb/{dataset_code}/load_date={LOAD_DATE}"
            file_stem = f"{dataset_code}_{safe_series_key}"

            data_object_key = f"{base_path}/{file_stem}.{extension}"
            metadata_object_key = f"{base_path}/{file_stem}.metadata.json"

            minio.upload_file(
                object_key=data_object_key,
                local_file_path=result.temp_file_path,
                content_type=content_type_for_extension(extension),
            )

            metadata = {
                "source": "ECB Data Portal",
                "dataset_code": dataset_code,
                "series_key": series_key,
                "indicator_name": indicator_name,
                "frequency": series.get("frequency"),
                "unit": series.get("unit"),
                "source_url": source_url,
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
                    "dataset_code": dataset_code,
                    "series_key": series_key,
                    "indicator_name": indicator_name,
                    "frequency": series.get("frequency"),
                    "unit": series.get("unit"),
                    "source_url": source_url,
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
                "dataset_code": series.get("dataset_code"),
                "series_key": series.get("series_key"),
                "indicator_name": series.get("indicator_name"),
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
        f"ecb/_manifests/load_date={LOAD_DATE}/download_ecb_manifest.json"
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
            f"ECB download finished with {len(failed_files)} failed series and {error_count} quality check errors."
        )


if __name__ == "__main__":
    main()