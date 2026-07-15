import os
import hashlib
import tempfile
from dataclasses import dataclass

import requests

from ingestion.common.file_utils import (
    guess_file_extension_from_url,
    detect_file_extension_from_content,
)


@dataclass
class DownloadResult:
    temp_file_path: str
    http_status: int
    content_type: str | None
    extension: str
    content_length_header: str | None
    downloaded_bytes: int
    sha256: str


class HttpDownloader:
    def __init__(
        self,
        timeout_seconds: int = 300,
        chunk_size_bytes: int = 1024 * 1024,
        user_agent: str = "EntityRisk-Navigator/1.0 educational-project",
    ):
        self.timeout_seconds = timeout_seconds
        self.chunk_size_bytes = chunk_size_bytes
        self.user_agent = user_agent

    def download_to_tempfile(self, url: str) -> DownloadResult:
        print(f"Downloading: {url}")

        response = requests.get(
            url,
            stream=True,
            timeout=self.timeout_seconds,
            headers={
                "User-Agent": self.user_agent,
                "Accept": "text/csv, application/zip, application/json, application/xml, */*",
            },
        )

        response.raise_for_status()

        content_type = response.headers.get("Content-Type")
        fallback_extension = guess_file_extension_from_url(url, content_type)

        sha256 = hashlib.sha256()
        total_bytes = 0

        temp_file = tempfile.NamedTemporaryFile(
            delete=False,
            suffix=f".{fallback_extension}",
        )
        temp_file_path = temp_file.name

        try:
            with temp_file:
                for chunk in response.iter_content(chunk_size=self.chunk_size_bytes):
                    if not chunk:
                        continue

                    temp_file.write(chunk)
                    sha256.update(chunk)
                    total_bytes += len(chunk)

            if total_bytes == 0:
                raise RuntimeError(f"Empty response for URL: {url}")

            actual_extension = detect_file_extension_from_content(
                temp_file_path,
                fallback_extension,
            )

            return DownloadResult(
                temp_file_path=temp_file_path,
                http_status=response.status_code,
                content_type=content_type,
                extension=actual_extension,
                content_length_header=response.headers.get("Content-Length"),
                downloaded_bytes=total_bytes,
                sha256=sha256.hexdigest(),
            )

        except Exception:
            try:
                os.remove(temp_file_path)
            except OSError:
                pass

            raise