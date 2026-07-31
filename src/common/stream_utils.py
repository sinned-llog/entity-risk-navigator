import csv
import zipfile
import io
from typing import Iterator
from io import TextIOWrapper


def iter_csv_rows_from_zip_stream(
    stream,
    encoding: str = "utf-8-sig",
    delimiter: str | None = None,
) -> Iterator:
    """
    Reads the first CSV file within a ZIP archive row by row.

    This avoids loading all CSV rows into memory.
    The ZIP stream/file handle must be seekable. For large ZIP files,
    prefer downloading the object to a temporary local file first.
    """

    with zipfile.ZipFile(stream, "r") as zip_file:
        csv_members = [
            name
            for name in zip_file.namelist()
            if name.lower().endswith(".csv")
        ]

        if not csv_members:
            raise RuntimeError("ZIP archive does not contain a CSV file.")

        csv_member = csv_members[0]
        print(f"CSV member in ZIP: {csv_member}")

        with zip_file.open(csv_member, "r") as raw_file:
            text_file = TextIOWrapper(
                raw_file,
                encoding=encoding,
                errors="replace",
                newline="",
            )

            if delimiter:
                reader = csv.DictReader(text_file, delimiter=delimiter)
            else:
                reader = csv.DictReader(text_file)

            for row in reader:
                yield row


def iter_csv_rows_from_text_stream(
    stream,
    encoding: str = "utf-8-sig",
    delimiter: str | None = None,
) -> Iterator:
    """
    Reads a text stream row by row as CSV.

    This avoids loading the full file into memory.
    """

    text_file = TextIOWrapper(
        stream,
        encoding=encoding,
        errors="replace",
        newline="",
    )

    if delimiter:
        reader = csv.DictReader(text_file, delimiter=delimiter)
    else:
        reader = csv.DictReader(text_file)

    for row in reader:
        yield row

def iter_csv_rows_from_zip_stream_v1(
    stream,
    encoding: str = "utf-8-sig",
    delimiter: str | None = None,
) -> Iterator:
    """
    Reads the first CSV file within a ZIP archive row by row from a stream,
    buffering the ZIP stream in memory (io.BytesIO) to make it seekable.
    """
    if not hasattr(stream, "seek") or not stream.seekable():
        zip_buffer = io.BytesIO(stream.read())
    else:
        zip_buffer = stream

    with zipfile.ZipFile(zip_buffer, "r") as zip_file:
        csv_members = [
            name
            for name in zip_file.namelist()
            if name.lower().endswith(".csv")
        ]

        if not csv_members:
            raise RuntimeError("ZIP archive does not contain a CSV file.")

        csv_member = csv_members[0]
        print(f"CSV member in ZIP: {csv_member}")

        with zip_file.open(csv_member, "r") as raw_file:
            text_file = TextIOWrapper(
                raw_file,
                encoding=encoding,
                errors="replace",
                newline="",
            )

            if delimiter:
                reader = csv.DictReader(text_file, delimiter=delimiter)
            else:
                reader = csv.DictReader(text_file)

            for row in reader:
                yield row