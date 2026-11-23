"""Read-only incremental PostgreSQL adapter with publication-gated watermarks."""

import hashlib
import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Literal, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from app.connectors.base import ExtractedBatch, SourceMetadata, SourceObject, SourceRecord


class PostgresAdapterError(RuntimeError):
    """Base error for source connectivity, rows, windows, or publication acknowledgement."""


class PostgresConnectionError(PostgresAdapterError):
    """The dedicated source database could not complete a read-only extraction."""


class PostgresRowError(PostgresAdapterError):
    """A source row violates the configured incremental contract."""


class WatermarkAdvanceError(PostgresAdapterError):
    """A window cannot safely advance its stored source watermark."""


@dataclass(frozen=True, slots=True)
class PostgresAdapterConfig:
    """Safe SQL identifiers and incremental extraction policy for one source table."""

    source_id: UUID
    source_name: str
    schema: str
    table: str
    columns: tuple[str, ...]
    watermark_column: str
    tie_breaker_column: str
    tie_breaker_type: Literal["integer", "text"] = "integer"
    lookback_seconds: float = 0
    max_window_records: int = 10_000

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, UUID):
            raise TypeError("source_id must be a UUID")
        identifiers = (
            self.source_name,
            self.schema,
            self.table,
            self.watermark_column,
            self.tie_breaker_column,
            *self.columns,
        )
        if not all(isinstance(value, str) and value.strip() for value in identifiers):
            raise ValueError("source and SQL identifier values must be non-empty strings")
        if not self.columns or len(set(self.columns)) != len(self.columns):
            raise ValueError("columns must be non-empty and unique")
        required = {self.watermark_column, self.tie_breaker_column}
        if not required.issubset(self.columns):
            raise ValueError("columns must include watermark and tie-breaker columns")
        if self.tie_breaker_type not in {"integer", "text"}:
            raise ValueError("tie_breaker_type must be 'integer' or 'text'")
        if self.lookback_seconds < 0:
            raise ValueError("lookback_seconds must be non-negative")
        if (
            isinstance(self.max_window_records, bool)
            or not isinstance(self.max_window_records, int)
            or self.max_window_records < 1
        ):
            raise ValueError("max_window_records must be a positive integer")


@dataclass(frozen=True, slots=True)
class WatermarkState:
    """Stored high-water position and optimistic-lock version."""

    watermark: datetime | None
    tie_breaker: int | str | None
    version: int

    def __post_init__(self) -> None:
        if (self.watermark is None) != (self.tie_breaker is None):
            raise ValueError("watermark and tie_breaker must both be null or non-null")
        if self.watermark is not None and (
            self.watermark.tzinfo is None or self.watermark.utcoffset() is None
        ):
            raise ValueError("watermark must be timezone-aware")
        if self.version < 0:
            raise ValueError("watermark version cannot be negative")


class WatermarkRepository(Protocol):
    """Metadata operations required for guarded watermark advancement."""

    def load_source_watermark(self, source_id: UUID) -> Mapping[str, object]: ...

    def advance_source_watermark(
        self,
        source_id: UUID,
        *,
        expected_version: int,
        watermark: datetime,
        tie_breaker: object,
        publication_uri: str,
    ) -> Mapping[str, object]: ...


class SourceReader(Protocol):
    """Read operation separated for deterministic adapter tests."""

    def read(self, config: PostgresAdapterConfig, state: WatermarkState) -> Sequence[Mapping]: ...


class PsycopgSourceReader:
    """Execute parameterized incremental SQL in a read-only repeatable-read transaction."""

    def __init__(self, connection_factory: Callable[[], psycopg.Connection]) -> None:
        self.connection_factory = connection_factory

    def read(
        self, config: PostgresAdapterConfig, state: WatermarkState
    ) -> Sequence[Mapping[str, object]]:
        connection: psycopg.Connection | None = None
        try:
            connection = self.connection_factory()
            with connection.transaction(), connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                columns = sql.SQL(", ").join(map(sql.Identifier, config.columns))
                query = sql.SQL("SELECT {} FROM {}.{}").format(
                    columns,
                    sql.Identifier(config.schema),
                    sql.Identifier(config.table),
                )
                parameters: list[object] = []
                watermark = sql.Identifier(config.watermark_column)
                tie_breaker = sql.Identifier(config.tie_breaker_column)
                query += sql.SQL(" WHERE {} IS NOT NULL AND {} IS NOT NULL").format(
                    watermark, tie_breaker
                )
                if state.watermark is not None:
                    if config.lookback_seconds:
                        lower = state.watermark - timedelta(seconds=config.lookback_seconds)
                        inner = query + sql.SQL(" AND {} >= %s").format(watermark)
                        inner += sql.SQL(
                            " ORDER BY CASE WHEN ({}, {}) > (%s, %s) THEN 0 ELSE 1 END, "
                            "{} ASC, {} ASC LIMIT %s"
                        ).format(watermark, tie_breaker, watermark, tie_breaker)
                        parameters.extend(
                            (
                                lower,
                                state.watermark,
                                state.tie_breaker,
                                config.max_window_records,
                            )
                        )
                        query = sql.SQL(
                            "SELECT {} FROM ({}) AS extracted_window ORDER BY {} ASC, {} ASC"
                        ).format(columns, inner, watermark, tie_breaker)
                    else:
                        query += sql.SQL(" AND ({}, {}) > (%s, %s)").format(watermark, tie_breaker)
                        parameters.extend((state.watermark, state.tie_breaker))
                        query += sql.SQL(" ORDER BY {} ASC, {} ASC LIMIT %s").format(
                            watermark, tie_breaker
                        )
                        parameters.append(config.max_window_records)
                else:
                    query += sql.SQL(" ORDER BY {} ASC, {} ASC LIMIT %s").format(
                        watermark, tie_breaker
                    )
                    parameters.append(config.max_window_records)
                cursor.execute(query, parameters)
                return cursor.fetchall()
        except psycopg.Error as exc:
            raise PostgresConnectionError(
                f"read-only extraction failed for {config.schema}.{config.table}"
            ) from exc
        finally:
            if connection is not None:
                connection.close()


@dataclass(frozen=True, slots=True)
class ExtractionWindow:
    """Immutable extraction candidate that may be acknowledged only after publication."""

    token: UUID
    source: SourceObject
    base_state: WatermarkState
    candidate_watermark: datetime | None
    candidate_tie_breaker: int | str | None
    metadata: SourceMetadata
    records: tuple[SourceRecord, ...]


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value)


class PostgresAdapter:
    """Extract deterministic incremental windows without advancing state during reads."""

    def __init__(
        self,
        config: PostgresAdapterConfig,
        reader: SourceReader,
        watermarks: WatermarkRepository,
    ) -> None:
        self.config = config
        self.reader = reader
        self.watermarks = watermarks
        self._window: ExtractionWindow | None = None

    def discover(self) -> tuple[SourceObject, ...]:
        """Expose the configured table without revealing connection credentials."""
        locator = f"postgresql:///{self.config.schema}/{self.config.table}"
        return (SourceObject(locator=locator, name=self.config.source_name),)

    def metadata(self, source: SourceObject) -> SourceMetadata:
        """Return metadata for the current immutable extraction window."""
        return self.extract_window(source).metadata

    def extract(self, source: SourceObject) -> Iterator[SourceRecord]:
        """Iterate the current window; reading alone never advances the watermark."""
        return iter(self.extract_window(source).records)

    def iter_batches(self, source: SourceObject, *, max_records: int) -> Iterator[ExtractedBatch]:
        """Yield stable non-empty batches from one extraction window."""
        if isinstance(max_records, bool) or not isinstance(max_records, int) or max_records <= 0:
            raise ValueError("max_records must be a positive integer")
        window = self.extract_window(source)
        for start in range(0, len(window.records), max_records):
            records = window.records[start : start + max_records]
            end = start + len(records)
            batch_id = uuid5(NAMESPACE_URL, f"postgres-batch\0{window.token}\0{start}\0{end}")
            yield ExtractedBatch(
                batch_id=batch_id,
                source=source,
                source_metadata=window.metadata,
                start_record=start,
                records=records,
            )

    def extract_window(self, source: SourceObject) -> ExtractionWindow:
        """Read and validate one bounded incremental window, cached for safe replay."""
        if source != self.discover()[0]:
            raise PostgresAdapterError("source was not discovered by this PostgreSQL adapter")
        if self._window is not None:
            return self._window
        state = self._load_state()
        rows = self.reader.read(self.config, state)
        records, candidate = self._validate_rows(rows, state)
        payload = json.dumps(
            [dict(record.values) for record in records],
            sort_keys=True,
            separators=(",", ":"),
            default=_json_default,
        ).encode()
        checksum = hashlib.sha256(payload).hexdigest()
        candidate_watermark, candidate_tie = candidate if candidate else (None, None)
        identity_text = (
            f"postgres-window\0{source.locator}\0{state.version}\0{state.watermark}\0"
            f"{state.tie_breaker}\0{candidate_watermark}\0{candidate_tie}\0{checksum}"
        )
        token = uuid5(NAMESPACE_URL, identity_text)
        metadata = SourceMetadata(
            identity=token,
            checksum_sha256=checksum,
            byte_count=len(payload),
            modified_at=datetime.now(UTC),
            media_type="application/json",
        )
        self._window = ExtractionWindow(
            token=token,
            source=source,
            base_state=state,
            candidate_watermark=candidate_watermark,
            candidate_tie_breaker=candidate_tie,
            metadata=metadata,
            records=records,
        )
        return self._window

    def acknowledge_published(
        self,
        window: ExtractionWindow,
        *,
        validation_passed: bool,
        publication_uri: str,
    ) -> Mapping[str, object] | None:
        """Advance state only for this validated window after durable publication."""
        if self._window is None or window.token != self._window.token:
            raise WatermarkAdvanceError("window is not the adapter's active extraction")
        if not validation_passed:
            raise WatermarkAdvanceError("validation must pass before watermark advancement")
        if not publication_uri or not publication_uri.strip():
            raise WatermarkAdvanceError("a durable publication URI is required")
        if window.candidate_watermark is None or window.candidate_tie_breaker is None:
            return None
        candidate = (window.candidate_watermark, window.candidate_tie_breaker)
        base = (window.base_state.watermark, window.base_state.tie_breaker)
        if window.base_state.watermark is not None and candidate <= base:
            return None
        try:
            result = self.watermarks.advance_source_watermark(
                self.config.source_id,
                expected_version=window.base_state.version,
                watermark=window.candidate_watermark,
                tie_breaker=window.candidate_tie_breaker,
                publication_uri=publication_uri,
            )
        except (ValueError, KeyError) as exc:
            raise WatermarkAdvanceError("watermark changed concurrently or would regress") from exc
        self._window = None
        return result

    def _load_state(self) -> WatermarkState:
        raw = self.watermarks.load_source_watermark(self.config.source_id)
        tie_breaker = raw["tie_breaker"]
        expected_type = int if self.config.tie_breaker_type == "integer" else str
        if tie_breaker is not None and (
            not isinstance(tie_breaker, expected_type) or isinstance(tie_breaker, bool)
        ):
            raise PostgresRowError("stored tie-breaker type does not match adapter configuration")
        return WatermarkState(
            watermark=raw["watermark"],
            tie_breaker=tie_breaker,
            version=raw["version"],
        )

    def _validate_rows(
        self, rows: Sequence[Mapping], state: WatermarkState
    ) -> tuple[tuple[SourceRecord, ...], tuple[datetime, int | str] | None]:
        records: list[SourceRecord] = []
        previous: tuple[datetime, int | str] | None = None
        expected_tie_type = int if self.config.tie_breaker_type == "integer" else str
        for index, row in enumerate(rows, start=1):
            if set(row) != set(self.config.columns):
                raise PostgresRowError(
                    f"row {index}: expected columns {self.config.columns!r}, "
                    f"received {tuple(row)!r}"
                )
            watermark = row[self.config.watermark_column]
            tie_breaker = row[self.config.tie_breaker_column]
            if not isinstance(watermark, datetime) or (
                watermark.tzinfo is None or watermark.utcoffset() is None
            ):
                raise PostgresRowError(f"row {index}: watermark must be a non-null timestamptz")
            if not isinstance(tie_breaker, expected_tie_type) or isinstance(tie_breaker, bool):
                raise PostgresRowError(
                    f"row {index}: tie-breaker must be {self.config.tie_breaker_type}"
                )
            position = (watermark, tie_breaker)
            if previous is not None and position <= previous:
                raise PostgresRowError(
                    f"row {index}: source ordering is not strictly deterministic"
                )
            previous = position
            records.append(SourceRecord(values=MappingProxyType(dict(row)), line_number=index))
        candidate = previous
        if state.watermark is not None and (
            candidate is None
            or candidate
            < (
                state.watermark,
                state.tie_breaker,
            )
        ):
            candidate = (state.watermark, state.tie_breaker)
        return tuple(records), candidate
