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
from app.connectors.postgres import (
    ExtractionWindow,
    PostgresAdapter,
    PostgresAdapterConfig,
    PsycopgSourceReader,
    WatermarkState,
)

__all__ = [
    "CSVAdapter",
    "CSVAdapterConfig",
    "ExtractedBatch",
    "HTTPJSONAdapter",
    "HTTPJSONConfig",
    "ExtractionWindow",
    "PostgresAdapter",
    "PostgresAdapterConfig",
    "PsycopgSourceReader",
    "RequestMetadata",
    "SourceAdapter",
    "SourceMetadata",
    "SourceObject",
    "SourceRecord",
    "WatermarkState",
]
