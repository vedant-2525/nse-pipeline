"""Official National Stock Exchange of India (NSE) Bhavcopy source.

Why this design:
NSE provides daily end-of-day market summaries (Bhavcopy) published after market close
(~6:30 PM IST). NSE transitioned to the UDiFF (Unified Distilled File Format) standard
on 2024-07-08, changing both file nomenclature and URLs. We support both modern and legacy
archives with automatic endpoint resolution based on the requested trade date.
"""

import io
import zipfile
from datetime import date
from pathlib import Path

import requests

from nse_pipeline.ingest.base import BhavcopySource
from nse_pipeline.ingest.metadata import write_file_metadata
from nse_pipeline.utils.logging import logger

UDIFF_TRANSITION_DATE = date(2024, 7, 8)


class NseSource(BhavcopySource):
    """Primary source fetching official daily bhavcopy files directly from NSE archives."""

    def __init__(self, session: requests.Session | None = None, timeout: int = 30):
        self.session = session or requests.Session()
        self.timeout = timeout

        # NSE blocks default python-requests User-Agent; emulate a standard browser
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

    @property
    def source_name(self) -> str:
        return "nse_official"

    def get_candidate_urls(self, trade_date: date) -> list[str]:
        """Resolves ordered candidate download URLs for the given date.

        Why this design:
        NSE maintains multiple archive mirrors and file formats. If a primary mirror
        is experiencing downtime or the file is formatted under an alternative scheme,
        falling through candidate URLs ensures ingestion resilience.
        """
        date_yyyymmdd = trade_date.strftime("%Y%m%d")
        date_dd = trade_date.strftime("%d")
        date_mmm = trade_date.strftime("%b").upper()
        date_yyyy = trade_date.strftime("%Y")

        if trade_date >= UDIFF_TRANSITION_DATE:
            # Modern UDiFF standard (post July 2024)
            return [
                f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_yyyymmdd}_F_0000.csv.zip",
                f"https://archives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_yyyymmdd}_F_0000.csv.zip",
            ]
        else:
            # Legacy Bhavcopy standard (prior to July 2024)
            return [
                f"https://archives.nseindia.com/content/historical/EQUITIES/{date_yyyy}/{date_mmm}/cm{date_dd}{date_mmm}{date_yyyy}bhav.csv.zip",
                f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{date_yyyy}/{date_mmm}/cm{date_dd}{date_mmm}{date_yyyy}bhav.csv.zip",
            ]

    def fetch(self, trade_date: date, output_dir: Path) -> Path:
        """Downloads the official Bhavcopy file for trade_date and saves it to output_dir."""
        output_dir.mkdir(parents=True, exist_ok=True)
        urls = self.get_candidate_urls(trade_date)

        last_status = None
        for url in urls:
            logger.info("Attempting NSE Bhavcopy download from: %s", url)
            try:
                response = self.session.get(url, timeout=self.timeout)
                last_status = response.status_code

                if response.status_code == 200:
                    content = response.content
                    if not content:
                        logger.warning("Received empty body from %s", url)
                        continue

                    # If response is a zip archive, extract the inner CSV
                    if url.endswith(".zip") or content.startswith(b"PK"):
                        with zipfile.ZipFile(io.BytesIO(content)) as z:
                            csv_files = [n for n in z.namelist() if n.lower().endswith(".csv")]
                            if not csv_files:
                                raise ValueError(f"Zip archive from {url} contained no CSV files")
                            csv_name = csv_files[0]
                            target_path = output_dir / csv_name
                            target_path.write_bytes(z.read(csv_name))
                            write_file_metadata(target_path, source_name=self.source_name, url=url)
                            logger.info(
                                "Successfully extracted raw CSV to: %s (%d bytes)",
                                target_path,
                                target_path.stat().st_size,
                            )
                            return target_path
                    else:
                        # Plain uncompressed CSV file
                        filename = Path(url).name
                        target_path = output_dir / filename
                        target_path.write_bytes(content)
                        write_file_metadata(target_path, source_name=self.source_name, url=url)
                        logger.info(
                            "Successfully saved raw CSV to: %s (%d bytes)",
                            target_path,
                            target_path.stat().st_size,
                        )
                        return target_path

                elif response.status_code in (403, 429):
                    raise PermissionError(
                        f"NSE blocked access with HTTP {response.status_code}. Rate limit reached or IP restricted."
                    )
                elif response.status_code == 404:
                    logger.debug("URL %s returned 404 Not Found", url)
                    continue
                else:
                    logger.warning("URL %s returned unexpected HTTP %d", url, response.status_code)

            except (requests.RequestException, zipfile.BadZipFile) as e:
                logger.warning("Error fetching %s: %s", url, e)
                continue

        if last_status == 404:
            raise FileNotFoundError(
                f"NSE Bhavcopy not found for {trade_date}. Market may have been closed (holiday/weekend) "
                "or data is not yet published."
            )

        raise ConnectionError(
            f"Failed to download NSE Bhavcopy for {trade_date} from all candidate URLs."
        )
