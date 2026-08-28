"""s3fs filesystem for the Wasabi bucket behind LAKE_ROOT.

Same library the serving layer uses (via pandas.read_parquet), so the repo has
a single S3 stack. Paths are "bucket/key" (no s3:// prefix needed).
"""

import s3fs

from app.config.settings import settings


def get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(**settings.storage_options)
