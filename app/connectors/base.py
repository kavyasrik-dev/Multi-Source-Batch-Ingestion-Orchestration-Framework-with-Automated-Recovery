"""Common source adapter data types and behavioral contract."""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable
from uuid import UUID


@dataclass(frozen=True, slots=True)
class SourceObject:
    """A source item discovered by an adapter."""

    locator: str
    name: str


@dataclass(frozen=True, slots=True)
class SourceMetadata:
    """Immutable identity and physical metadata for one source revision."""

    identity: UUID
    checksum_sha256: str
    byte_count: int
    modified_at: datetime
    media_type: str


@dataclass(frozen=True, slots=True)
class SourceRecord:
    """One extracted source row with its physical line for diagnostics."""

    values: Mapping[str, str]
    line_number: int


@dataclass(frozen=True, slots=True)
class ExtractedBatch:
    """A bounded, replay-stable group of source records."""

    batch_id: UUID
    source: SourceObject
    source_metadata: SourceMetadata
    start_record: int
    records: tuple[SourceRecord, ...]

    def __post_init__(self) -> None:
        if not self.records:
            raise ValueError("ExtractedBatch cannot be empty")


@runtime_checkable
class SourceAdapter(Protocol):
    """Contract implemented by all source-specific adapters."""

    def discover(self) -> tuple[SourceObject, ...]:
        """Discover source objects in deterministic order."""
        ...

    def metadata(self, source: SourceObject) -> SourceMetadata:
        """Return checksum-backed identity and physical metadata."""
        ...

    def extract(self, source: SourceObject) -> Iterator[SourceRecord]:
        """Stream validated source records without materializing the full input."""
        ...

    def iter_batches(self, source: SourceObject, *, max_records: int) -> Iterator[ExtractedBatch]:
        """Yield non-empty batches containing at most max_records records."""
        ...
