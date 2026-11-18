"""Safe, consistent object naming for the four ingestion data zones."""

import re
from datetime import date
from enum import StrEnum
from uuid import UUID

_SOURCE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")
_FILENAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class DataZone(StrEnum):
    """Each zone maps directly to the identically named MinIO bucket."""

    RAW = "raw"
    STAGING = "staging"
    CURATED = "curated"
    REJECTED = "rejected"

    @property
    def bucket(self) -> str:
        """Return the MinIO bucket used by this zone."""
        return self.value


def batch_prefix(source: str, logical_date: date, batch_id: UUID) -> str:
    """Return the zone-independent prefix shared by every processing stage."""
    if not _SOURCE.fullmatch(source):
        raise ValueError("source must match [a-z0-9][a-z0-9_-]{0,62}")
    return f"source={source}/logical_date={logical_date.isoformat()}/batch_id={batch_id}"


def data_object_key(source: str, logical_date: date, batch_id: UUID, filename: str) -> str:
    """Return a safe object key below the batch's data prefix."""
    if not _FILENAME.fullmatch(filename) or filename == "manifest.v1.json":
        raise ValueError("filename must be a safe 1-128 character leaf name")
    return f"{batch_prefix(source, logical_date, batch_id)}/data/{filename}"


def manifest_object_key(source: str, logical_date: date, batch_id: UUID) -> str:
    """Return the single deterministic manifest key for a logical batch."""
    return f"{batch_prefix(source, logical_date, batch_id)}/manifest.v1.json"
