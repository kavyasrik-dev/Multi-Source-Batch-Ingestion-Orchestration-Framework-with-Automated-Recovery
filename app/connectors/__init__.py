"""Source adapter contracts and concrete ingestion adapters."""

from app.connectors.base import (
    ExtractedBatch,
    SourceAdapter,
    SourceMetadata,
    SourceObject,
    SourceRecord,
)
from app.connectors.csv import CSVAdapter, CSVAdapterConfig

__all__ = [
    "CSVAdapter",
    "CSVAdapterConfig",
    "ExtractedBatch",
    "SourceAdapter",
    "SourceMetadata",
    "SourceObject",
    "SourceRecord",
]
