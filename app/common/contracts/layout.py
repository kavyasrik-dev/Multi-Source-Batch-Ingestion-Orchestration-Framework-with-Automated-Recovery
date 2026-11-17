"""Pure partition validation and row-count file planning."""

import re
from datetime import date
from pathlib import Path
from uuid import UUID

_DATASET_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def partition_path(root: Path, dataset: str, ingest_date: date, batch_id: UUID) -> Path:
    """Build a validated Hive-style partition without accepting path traversal."""
    if not _DATASET_NAME.fullmatch(dataset):
        raise ValueError("dataset must match [a-z][a-z0-9_]{0,62}")
    return (
        root
        / f"dataset={dataset}"
        / f"ingest_date={ingest_date.isoformat()}"
        / f"batch_id={batch_id}"
    )


def plan_file_sizes(total: int, target_rows: int, min_rows: int) -> list[int]:
    """Plan parts while merging a tiny final remainder into the prior part."""
    if total < 0:
        raise ValueError("total rows must be non-negative")
    if target_rows <= 0 or min_rows <= 0 or min_rows > target_rows:
        raise ValueError("require 0 < min_rows_per_file <= target_rows_per_file")
    if total == 0:
        return []
    full, remainder = divmod(total, target_rows)
    sizes = [target_rows] * full
    if remainder:
        if sizes and remainder < min_rows:
            sizes[-1] += remainder
        else:
            sizes.append(remainder)
    return sizes
