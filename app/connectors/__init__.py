"""Source adapter contracts and concrete ingestion adapters."""

from app.connectors.base import (
    ExtractedBatch,
    SourceAdapter,
    SourceMetadata,
    SourceObject,
    SourceRecord,
)
from app.connectors.csv import CSVAdapter, CSVAdapterConfig
from app.connectors.http_json import HTTPJSONAdapter, HTTPJSONConfig, RequestMetadata

__all__ = [
    "CSVAdapter",
    "CSVAdapterConfig",
    "ExtractedBatch",
    "HTTPJSONAdapter",
    "HTTPJSONConfig",
    "RequestMetadata",
    "SourceAdapter",
    "SourceMetadata",
    "SourceObject",
    "SourceRecord",
]
