import logging
from pathlib import Path

import pandas as pd
import pyreadr
import requests

from Utils.log_utils import setup_logging

setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

CACHE_DIR = Path("data_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _download_rda(dataset_name: str, base_raw_url: str, force_refresh: bool = False) -> Path:
    """Download a survivoR .rda file, caching the result on disk."""
    file_name = f"{dataset_name}.rda"
    local_path = CACHE_DIR / file_name

    if local_path.exists() and not force_refresh:
        return local_path

    if not base_raw_url:
        raise ValueError("Base raw URL is required to download survivoR datasets.")

    url = f"{base_raw_url.rstrip('/')}/{file_name}"
    logger.info("Downloading %s from %s", file_name, url)

    response = requests.get(url, timeout=60)
    if response.status_code != requests.codes.ok:
        raise RuntimeError(
            f"Failed to download dataset '{dataset_name}' from {url}. "
            f"HTTP status: {response.status_code}"
        )

    content_type = response.headers.get("Content-Type", "").lower()
    if "json" in content_type:
        raise RuntimeError(
            f"Expected binary .rda content for '{dataset_name}', "
            f"but received Content-Type '{content_type}'"
        )

    content_snippet = response.content[:4]
    if content_snippet not in (b"RDX2", b"RDX3", b"RDA2", b"RDA3"):
        logger.warning(
            "Downloaded file for '%s' does not begin with a known RDA signature (saw %s)",
            dataset_name,
            content_snippet,
        )

    if response.content.startswith(b"{") or response.content.startswith(b"["):
        raise RuntimeError(
            f"Downloaded payload for '{dataset_name}' appears to be JSON rather than an RDA binary."
        )

    local_path.write_bytes(response.content)
    logger.info("Saved %s to %s", file_name, local_path)
    return local_path


def load_dataset(dataset_name: str, base_raw_url: str, force_refresh: bool = False) -> pd.DataFrame:
    """
    Download (if needed) and load a survivoR .rda dataset into a DataFrame.

    Parameters
    ----------
    dataset_name:
        Name of the dataset inside the survivoR package (e.g., 'castaways').
    base_raw_url:
        Base GitHub raw URL pointing at the survivoR `data/` directory.
    force_refresh:
        When True, bypass the local cache and re-download the file.

    Returns
    -------
    pandas.DataFrame
        A copy of the dataset ready for downstream processing.
    """
    local_path = _download_rda(dataset_name, base_raw_url, force_refresh=force_refresh)

    try:
        read_result = pyreadr.read_r(str(local_path))
    except Exception as exc:
        raise RuntimeError(f"Failed to read {local_path} via pyreadr") from exc

    if dataset_name in read_result:
        df = read_result[dataset_name]
    else:
        df = next(iter(read_result.values()), None)

    if df is None or not isinstance(df, pd.DataFrame):
        raise RuntimeError(f"Dataset '{dataset_name}' did not yield a pandas DataFrame.")

    return df.copy()
