"""Read-only access to the medallion lake (Gold layer served by this API)."""

import logging

import pandas as pd

from app.config.settings import settings

logger = logging.getLogger(__name__)


class LakeUnavailable(Exception):
    """The lake could not be reached (offline, bad credentials, permission denied)."""


def dataset_uri(*parts: str) -> str:
    root = settings.lake_root.rstrip("/")
    tail = "/".join(p.strip("/") for p in parts if p)
    return f"{root}/{tail}" if tail else root


def read_dataset(*parts: str, columns: list[str] | None = None) -> pd.DataFrame:
    """Read a (possibly partitioned) parquet dataset from the lake.

    Returns an empty DataFrame when the dataset does not exist yet, so callers
    can distinguish "no data produced yet" from "lake unreachable"
    (which raises LakeUnavailable).
    """
    path = dataset_uri(*parts)
    storage_options = settings.storage_options or None
    try:
        return pd.read_parquet(path, columns=columns, storage_options=storage_options)
    except (FileNotFoundError, OSError) as exc:
        # pyarrow raises FileNotFoundError for a missing local/S3 prefix;
        # s3fs raises a few OSError subclasses for the same case.
        if _looks_like_missing(exc):
            logger.info("Dataset not produced yet: %s", path)
            return pd.DataFrame()
        logger.exception("Lake read failed: %s", path)
        raise LakeUnavailable(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - normalise everything else to 503
        logger.exception("Lake read failed: %s", path)
        raise LakeUnavailable(str(exc)) from exc


def _looks_like_missing(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return (
        isinstance(exc, FileNotFoundError)
        or "no such file" in msg
        or "path does not exist" in msg
        or "not found" in msg
    )
