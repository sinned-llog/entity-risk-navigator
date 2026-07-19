import csv
import os
import zipfile
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional


@dataclass
class QualityCheckResult:
    check_name: str
    status: str
    severity: str
    message: str
    expected: Optional[Any] = None
    actual: Optional[Any] = None


def _passed(
    check_name: str,
    message: str,
    expected: Any = None,
    actual: Any = None,
) -> QualityCheckResult:
    return QualityCheckResult(
        check_name=check_name,
        status="passed",
        severity="info",
        message=message,
        expected=expected,
        actual=actual,
    )


def _failed(
    check_name: str,
    message: str,
    severity: str = "error",
    expected: Any = None,
    actual: Any = None,
) -> QualityCheckResult:
    return QualityCheckResult(
        check_name=check_name,
        status="failed",
        severity=severity,
        message=message,
        expected=expected,
        actual=actual,
    )


def check_min_file_size(
    file_path: str,
    min_expected_bytes: int,
) -> QualityCheckResult:
    actual_size = os.path.getsize(file_path)

    if actual_size >= min_expected_bytes:
        return _passed(
            check_name="min_file_size",
            message="Downloaded file size is above minimum threshold.",
            expected=min_expected_bytes,
            actual=actual_size,
        )

    return _failed(
        check_name="min_file_size",
        message="Downloaded file is smaller than expected.",
        expected=min_expected_bytes,
        actual=actual_size,
    )


def check_expected_extension(
    extension: str,
    expected_extensions: List[str],
) -> QualityCheckResult:
    if extension in expected_extensions:
        return _passed(
            check_name="expected_extension",
            message="Detected file extension is allowed.",
            expected=expected_extensions,
            actual=extension,
        )

    return _failed(
        check_name="expected_extension",
        message="Detected file extension is not allowed.",
        expected=expected_extensions,
        actual=extension,
    )


def check_zip_contains_csv(file_path: str) -> QualityCheckResult:
    try:
        with zipfile.ZipFile(file_path, "r") as zip_file:
            csv_members = [
                name
                for name in zip_file.namelist()
                if name.lower().endswith(".csv")
            ]

        if csv_members:
            return _passed(
                check_name="zip_contains_csv",
                message="ZIP archive contains at least one CSV file.",
                expected="at least one .csv member",
                actual=csv_members,
            )

        return _failed(
            check_name="zip_contains_csv",
            message="ZIP archive does not contain a CSV file.",
            expected="at least one .csv member",
            actual=[],
        )

    except zipfile.BadZipFile as exc:
        return _failed(
            check_name="zip_is_readable",
            message=f"File is not a readable ZIP archive: {exc}",
        )


def _read_csv_header_from_file(file_path: str) -> List:
    with open(file_path, mode="r", encoding="utf-8-sig", newline="") as file:
        reader = csv.reader(file)
        return next(reader, [])


def _read_csv_header_from_zip(file_path: str) -> List:
    with zipfile.ZipFile(file_path, "r") as zip_file:
        csv_members = [
            name
            for name in zip_file.namelist()
            if name.lower().endswith(".csv")
        ]

        if not csv_members:
            return []

        first_csv = csv_members[0]

        with zip_file.open(first_csv, "r") as raw_file:
            text = raw_file.readline().decode("utf-8-sig", errors="replace")
            return next(csv.reader([text]), [])


def check_csv_required_columns(
    file_path: str,
    extension: str,
    required_columns: List[str],
) -> QualityCheckResult:
    try:
        if extension == "zip":
            header = _read_csv_header_from_zip(file_path)
        else:
            header = _read_csv_header_from_file(file_path)

        missing = [
            column
            for column in required_columns
            if column not in header
        ]

        if not missing:
            return _passed(
                check_name="csv_required_columns",
                message="All required CSV columns are present.",
                expected=required_columns,
                actual=header,
            )

        return _failed(
            check_name="csv_required_columns",
            message="One or more required CSV columns are missing.",
            expected=required_columns,
            actual={
                "header": header,
                "missing": missing,
            },
        )

    except Exception as exc:
        return _failed(
            check_name="csv_required_columns",
            message=f"Could not inspect CSV header: {exc}",
        )


def check_html_contains_markers(
    file_path: str,
    markers: List[str],
) -> QualityCheckResult:
    try:
        with open(
            file_path,
            mode="r",
            encoding="utf-8",
            errors="replace",
        ) as file:
            content = file.read()

        missing = [
            marker
            for marker in markers
            if marker not in content
        ]

        if not missing:
            return _passed(
                check_name="html_required_markers",
                message="All required HTML markers are present.",
                expected=markers,
                actual="all markers found",
            )

        return _failed(
            check_name="html_required_markers",
            message="One or more required HTML markers are missing.",
            expected=markers,
            actual={
                "missing": missing,
            },
        )

    except Exception as exc:
        return _failed(
            check_name="html_required_markers",
            message=f"Could not inspect HTML file: {exc}",
        )


def check_utf8_readable(
    file_path: str,
    sample_bytes: int = 4096,
) -> QualityCheckResult:
    try:
        with open(file_path, "rb") as file:
            sample = file.read(sample_bytes)

        sample.decode("utf-8")

        return _passed(
            check_name="utf8_readable_sample",
            message="File sample is readable as UTF-8.",
        )

    except UnicodeDecodeError as exc:
        return _failed(
            check_name="utf8_readable_sample",
            message=f"File sample is not valid UTF-8: {exc}",
            severity="warning",
        )


def run_download_quality_checks(
    file_path: str,
    extension: str,
    expectations: Dict[str, Any],
) -> List[Dict[str, Any]]:
    checks: List[QualityCheckResult] = []

    min_expected_bytes = expectations.get("min_expected_bytes")
    if min_expected_bytes is not None:
        checks.append(
            check_min_file_size(
                file_path=file_path,
                min_expected_bytes=int(min_expected_bytes),
            )
        )

    expected_extensions = expectations.get("expected_extensions")
    if expected_extensions:
        checks.append(
            check_expected_extension(
                extension=extension,
                expected_extensions=list(expected_extensions),
            )
        )

    if expectations.get("zip_must_contain_csv"):
        checks.append(
            check_zip_contains_csv(file_path)
        )

    required_columns = expectations.get("required_columns")
    if required_columns:
        checks.append(
            check_csv_required_columns(
                file_path=file_path,
                extension=extension,
                required_columns=list(required_columns),
            )
        )

    html_required_markers = expectations.get("html_required_markers")
    if html_required_markers:
        checks.append(
            check_html_contains_markers(
                file_path=file_path,
                markers=list(html_required_markers),
            )
        )

    if expectations.get("check_utf8_sample", True):
        checks.append(
            check_utf8_readable(file_path)
        )

    return [asdict(check) for check in checks]


def summarize_quality_checks(
    checks: List[Dict[str, Any]],
) -> Dict[str, int]:
    error_count = 0
    warning_count = 0
    passed_count = 0

    for check in checks:
        if check.get("status") == "passed":
            passed_count += 1
        elif check.get("severity") == "warning":
            warning_count += 1
        else:
            error_count += 1

    return {
        "passed_count": passed_count,
        "warning_count": warning_count,
        "error_count": error_count,
        "total_count": len(checks),
    }