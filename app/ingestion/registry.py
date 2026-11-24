"""Typed source registry, deterministic batch discovery, and safe registration."""

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg

from app.common.metadata.repository import DuplicateBatch
from app.common.storage.minio import MinioObjectStore
from app.connectors.base import ExtractedBatch, SourceAdapter, SourceObject
from app.connectors.csv import CSVAdapter, CSVAdapterConfig
from app.connectors.http_json import HTTPJSONAdapter, HTTPJSONConfig
from app.connectors.postgres import PostgresAdapter, PostgresAdapterConfig, PsycopgSourceReader


class RegistryError(RuntimeError):
    """Base error for source loading, validation, discovery, or registration."""


class InvalidRegistryConfig(RegistryError, ValueError):
    """Runtime or metadata source settings are missing, unknown, or malformed."""


class DuplicateDiscovery(RegistryError):
    """An adapter emitted the same deterministic logical batch more than once."""


@dataclass(frozen=True, slots=True)
class RuntimeSourceConfig:
    """Typed runtime controls and connector-setting overrides for one source."""

    enabled: bool = True
    batch_size: int | None = None
    batch_limit: int | None = None
    overrides: Mapping[str, object] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise InvalidRegistryConfig("runtime source enabled must be boolean")
        for name, value in (
            ("batch_size", self.batch_size),
            ("batch_limit", self.batch_limit),
        ):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 1
            ):
                raise InvalidRegistryConfig(f"{name} must be a positive integer")
        if not isinstance(self.overrides, Mapping):
            raise InvalidRegistryConfig("overrides must be a mapping")
        object.__setattr__(self, "overrides", MappingProxyType(dict(self.overrides)))


@dataclass(frozen=True, slots=True)
class RegistryConfig:
    """Runtime settings keyed by metadata source name plus safe defaults."""

    sources: Mapping[str, RuntimeSourceConfig] = field(default_factory=dict)
    default_batch_size: int = 1000
    default_batch_limit: int = 100

    def __post_init__(self) -> None:
        for name, value in (
            ("default_batch_size", self.default_batch_size),
            ("default_batch_limit", self.default_batch_limit),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise InvalidRegistryConfig(f"{name} must be a positive integer")
        if not isinstance(self.sources, Mapping) or not all(
            isinstance(name, str) and name and isinstance(value, RuntimeSourceConfig)
            for name, value in self.sources.items()
        ):
            raise InvalidRegistryConfig(
                "sources must map non-empty names to RuntimeSourceConfig values"
            )
        object.__setattr__(self, "sources", MappingProxyType(dict(self.sources)))

    @classmethod
    def from_mapping(cls, raw: Mapping[str, object]) -> "RegistryConfig":
        """Parse strict environment/file-decoded data into validated typed settings."""
        if not isinstance(raw, Mapping):
            raise InvalidRegistryConfig("registry configuration must be a mapping")
        allowed = {"sources", "default_batch_size", "default_batch_limit"}
        _reject_unknown(raw, allowed, "registry")
        source_values = raw.get("sources", {})
        if not isinstance(source_values, Mapping):
            raise InvalidRegistryConfig("registry.sources must be a mapping")
        sources: dict[str, RuntimeSourceConfig] = {}
        for name, value in source_values.items():
            if not isinstance(name, str) or not name or not isinstance(value, Mapping):
                raise InvalidRegistryConfig("runtime source entries must be named mappings")
            _reject_unknown(
                value,
                {"enabled", "batch_size", "batch_limit", "overrides"},
                f"runtime source {name}",
            )
            sources[name] = RuntimeSourceConfig(
                enabled=value.get("enabled", True),
                batch_size=value.get("batch_size"),
                batch_limit=value.get("batch_limit"),
                overrides=value.get("overrides", {}),
            )
        return cls(
            sources=sources,
            default_batch_size=raw.get("default_batch_size", 1000),
            default_batch_limit=raw.get("default_batch_limit", 100),
        )


@dataclass(frozen=True, slots=True)
class CSVSourceSettings:
    paths: tuple[Path, ...]
    delimiter: str = ","
    encoding: str = "utf-8"
    has_header: bool = True
    expected_headers: tuple[str, ...] | None = None
    recursive: bool = False


@dataclass(frozen=True, slots=True)
class HTTPSourceSettings:
    endpoint: str
    records_path: tuple[str, ...] = ("items",)
    next_cursor_path: tuple[str, ...] = ("next_cursor",)
    cursor_parameter: str = "cursor"
    parameters: Mapping[str, str] = field(default_factory=dict, repr=False)
    headers: Mapping[str, str] = field(default_factory=dict, repr=False)
    expected_fields: tuple[str, ...] | None = None
    timeout_seconds: float = 10.0
    max_attempts: int = 3
    backoff_seconds: float = 0.25
    max_pages: int = 1000
    raw_bucket: str = "raw"


@dataclass(frozen=True, slots=True)
class PostgresSourceSettings:
    schema: str
    table: str
    columns: tuple[str, ...]
    watermark_column: str
    tie_breaker_column: str
    tie_breaker_type: str = "integer"
    lookback_seconds: float = 0
    max_window_records: int = 10_000


ConnectorSettings = CSVSourceSettings | HTTPSourceSettings | PostgresSourceSettings


@dataclass(frozen=True, slots=True)
class SourceDefinition:
    """Validated metadata plus typed connector and execution settings."""

    source_id: UUID
    name: str
    connector_type: str
    settings: ConnectorSettings
    batch_size: int
    batch_limit: int


@dataclass(frozen=True, slots=True)
class DiscoveredBatch:
    """Stable registry-level identity for an adapter batch."""

    logical_batch_key: str
    source: SourceDefinition
    source_object: SourceObject
    extracted: ExtractedBatch

    @property
    def object_uri(self) -> str:
        """Use the immutable input locator until publication registers its output manifest."""
        return self.source_object.locator


@dataclass(frozen=True, slots=True)
class BatchRegistration:
    """Result of concurrency-safe registration; duplicates are explicit and harmless."""

    discovered: DiscoveredBatch
    created: bool
    run: Mapping[str, object] | None
    batch: Mapping[str, object] | None


class RegistryRepository(Protocol):
    def list_sources(self, *, enabled_only: bool = True) -> tuple[dict, ...]: ...

    def create_run(
        self,
        source_id: UUID,
        logical_batch_key: str,
        object_uri: str,
        *,
        record_count: int = 0,
    ) -> tuple[dict, dict]: ...


class AdapterFactory(Protocol):
    def build(self, source: SourceDefinition) -> SourceAdapter: ...


class DefaultAdapterFactory:
    """Construct existing adapters from validated definitions and injected dependencies."""

    def __init__(
        self,
        repository: RegistryRepository,
        *,
        object_store: MinioObjectStore | None = None,
        postgres_connection_factory: Callable[[], psycopg.Connection] | None = None,
    ) -> None:
        self.repository = repository
        self.object_store = object_store
        self.postgres_connection_factory = postgres_connection_factory

    def build(self, source: SourceDefinition) -> SourceAdapter:
        settings = source.settings
        if isinstance(settings, CSVSourceSettings):
            return CSVAdapter(
                CSVAdapterConfig(
                    paths=settings.paths,
                    delimiter=settings.delimiter,
                    encoding=settings.encoding,
                    has_header=settings.has_header,
                    expected_headers=settings.expected_headers,
                    recursive=settings.recursive,
                )
            )
        if isinstance(settings, HTTPSourceSettings):
            if self.object_store is None:
                raise InvalidRegistryConfig("HTTP source requires an object store dependency")
            return HTTPJSONAdapter(
                HTTPJSONConfig(
                    source_name=source.name,
                    endpoint=settings.endpoint,
                    records_path=settings.records_path,
                    next_cursor_path=settings.next_cursor_path,
                    cursor_parameter=settings.cursor_parameter,
                    parameters=settings.parameters,
                    headers=settings.headers,
                    expected_fields=settings.expected_fields,
                    timeout_seconds=settings.timeout_seconds,
                    max_attempts=settings.max_attempts,
                    backoff_seconds=settings.backoff_seconds,
                    max_pages=settings.max_pages,
                    raw_bucket=settings.raw_bucket,
                ),
                self.object_store,
            )
        if isinstance(settings, PostgresSourceSettings):
            if self.postgres_connection_factory is None:
                raise InvalidRegistryConfig(
                    "PostgreSQL source requires a connection factory dependency"
                )
            return PostgresAdapter(
                PostgresAdapterConfig(
                    source_id=source.source_id,
                    source_name=source.name,
                    schema=settings.schema,
                    table=settings.table,
                    columns=settings.columns,
                    watermark_column=settings.watermark_column,
                    tie_breaker_column=settings.tie_breaker_column,
                    tie_breaker_type=settings.tie_breaker_type,
                    lookback_seconds=settings.lookback_seconds,
                    max_window_records=settings.max_window_records,
                ),
                PsycopgSourceReader(self.postgres_connection_factory),
                self.repository,
            )
        raise InvalidRegistryConfig(f"unsupported settings type for source {source.name}")


class SourceRegistry:
    """Resolve enabled sources, discover stable batches, and register one winner."""

    def __init__(
        self,
        repository: RegistryRepository,
        config: RegistryConfig,
        adapter_factory: AdapterFactory,
    ) -> None:
        self.repository = repository
        self.config = config
        self.adapter_factory = adapter_factory

    def load_enabled(self) -> tuple[SourceDefinition, ...]:
        """Merge enabled metadata rows with typed runtime settings and validate all sources."""
        rows = self.repository.list_sources(enabled_only=True)
        definitions: list[SourceDefinition] = []
        seen_names: set[str] = set()
        for row in rows:
            name = row.get("name")
            if not isinstance(name, str) or not name:
                raise InvalidRegistryConfig("metadata source name is required")
            if name in seen_names:
                raise InvalidRegistryConfig(f"duplicate metadata source name: {name}")
            seen_names.add(name)
            runtime = self.config.sources.get(name, RuntimeSourceConfig())
            if not runtime.enabled:
                continue
            definitions.append(self._resolve(row, runtime))
        return tuple(sorted(definitions, key=lambda source: (source.name, str(source.source_id))))

    def discover(self) -> tuple[DiscoveredBatch, ...]:
        """Discover deterministic batches across every enabled source in stable order."""
        discovered: list[DiscoveredBatch] = []
        seen_keys: set[str] = set()
        for definition in self.load_enabled():
            adapter = self.adapter_factory.build(definition)
            source_objects = sorted(adapter.discover(), key=lambda item: (item.locator, item.name))
            remaining = definition.batch_limit
            for source_object in source_objects:
                if remaining == 0:
                    break
                for extracted in adapter.iter_batches(
                    source_object, max_records=definition.batch_size
                ):
                    logical_key = self._logical_key(definition, source_object, extracted)
                    if logical_key in seen_keys:
                        raise DuplicateDiscovery(
                            f"adapter emitted duplicate logical batch {logical_key}"
                        )
                    seen_keys.add(logical_key)
                    discovered.append(
                        DiscoveredBatch(
                            logical_batch_key=logical_key,
                            source=definition,
                            source_object=source_object,
                            extracted=extracted,
                        )
                    )
                    remaining -= 1
                    if remaining == 0:
                        break
        return tuple(
            sorted(
                discovered,
                key=lambda batch: (
                    batch.source.name,
                    batch.source_object.locator,
                    batch.extracted.start_record,
                    str(batch.extracted.batch_id),
                ),
            )
        )

    def register(self, discovered: DiscoveredBatch) -> BatchRegistration:
        """Atomically claim a logical batch; database uniqueness chooses one worker."""
        try:
            run, batch = self.repository.create_run(
                discovered.source.source_id,
                discovered.logical_batch_key,
                discovered.object_uri,
                record_count=len(discovered.extracted.records),
            )
        except DuplicateBatch:
            return BatchRegistration(discovered, False, None, None)
        return BatchRegistration(discovered, True, run, batch)

    def register_discovered(self) -> tuple[BatchRegistration, ...]:
        """Discover and attempt registration in deterministic order."""
        return tuple(self.register(batch) for batch in self.discover())

    def _resolve(self, row: Mapping[str, object], runtime: RuntimeSourceConfig) -> SourceDefinition:
        source_id = row.get("id")
        connector_type = row.get("connector_type")
        metadata_config = row.get("config")
        if not isinstance(source_id, UUID):
            raise InvalidRegistryConfig("metadata source id must be UUID")
        if connector_type not in {"file", "http", "postgres"}:
            raise InvalidRegistryConfig(
                f"source {row.get('name')}: unsupported connector_type {connector_type!r}"
            )
        if not isinstance(metadata_config, Mapping):
            raise InvalidRegistryConfig(f"source {row.get('name')}: config must be an object")
        merged = dict(metadata_config)
        merged.update(runtime.overrides)
        metadata_batch_size = merged.pop("batch_size", self.config.default_batch_size)
        metadata_batch_limit = merged.pop("batch_limit", self.config.default_batch_limit)
        batch_size = runtime.batch_size or metadata_batch_size
        batch_limit = runtime.batch_limit or metadata_batch_limit
        for label, value in (("batch_size", batch_size), ("batch_limit", batch_limit)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise InvalidRegistryConfig(
                    f"source {row.get('name')}: {label} must be a positive integer"
                )
        settings = _parse_settings(connector_type, merged, str(row["name"]))
        _validate_connector_settings(source_id, str(row["name"]), settings)
        return SourceDefinition(
            source_id=source_id,
            name=str(row["name"]),
            connector_type=connector_type,
            settings=settings,
            batch_size=batch_size,
            batch_limit=batch_limit,
        )

    @staticmethod
    def _logical_key(
        source: SourceDefinition,
        source_object: SourceObject,
        extracted: ExtractedBatch,
    ) -> str:
        identity = {
            "version": 1,
            "source_id": str(source.source_id),
            "connector_type": source.connector_type,
            "locator": source_object.locator,
            "adapter_batch_id": str(extracted.batch_id),
            "checksum_sha256": extracted.source_metadata.checksum_sha256,
            "start_record": extracted.start_record,
            "record_count": len(extracted.records),
        }
        canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"))
        return f"v1:{uuid5(NAMESPACE_URL, canonical)}"


def _reject_unknown(raw: Mapping, allowed: set[str], label: str) -> None:
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise InvalidRegistryConfig(f"{label}: unknown setting(s): {', '.join(unknown)}")


def _required(raw: Mapping[str, object], key: str, source: str) -> object:
    value = raw.get(key)
    if value is None or value == "":
        raise InvalidRegistryConfig(f"source {source}: missing required setting '{key}'")
    return value


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise InvalidRegistryConfig(f"{label} must be a non-empty string")
    return value


def _boolean(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise InvalidRegistryConfig(f"{label} must be boolean")
    return value


def _positive_integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise InvalidRegistryConfig(f"{label} must be a positive integer")
    return value


def _number(value: object, label: str, *, positive: bool) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InvalidRegistryConfig(f"{label} must be numeric")
    if (positive and value <= 0) or (not positive and value < 0):
        qualifier = "positive" if positive else "non-negative"
        raise InvalidRegistryConfig(f"{label} must be {qualifier}")
    return value


def _strings(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value:
        raise InvalidRegistryConfig(f"{label} must be a non-empty string list")
    if not all(isinstance(item, str) and item for item in value):
        raise InvalidRegistryConfig(f"{label} must contain non-empty strings")
    return tuple(value)


def _string_mapping(value: object, label: str) -> Mapping[str, str]:
    if not isinstance(value, Mapping) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in value.items()
    ):
        raise InvalidRegistryConfig(f"{label} must map strings to strings")
    return MappingProxyType(dict(value))


def _parse_settings(
    connector_type: str, raw: Mapping[str, object], source: str
) -> ConnectorSettings:
    if connector_type == "file":
        allowed = {
            "paths",
            "delimiter",
            "encoding",
            "has_header",
            "expected_headers",
            "recursive",
        }
        _reject_unknown(raw, allowed, f"source {source}")
        paths = _strings(_required(raw, "paths", source), f"source {source}.paths")
        headers = raw.get("expected_headers")
        return CSVSourceSettings(
            paths=tuple(Path(path) for path in paths),
            delimiter=_string(raw.get("delimiter", ","), f"source {source}.delimiter"),
            encoding=_string(raw.get("encoding", "utf-8"), f"source {source}.encoding"),
            has_header=_boolean(raw.get("has_header", True), f"source {source}.has_header"),
            expected_headers=(
                _strings(headers, f"source {source}.expected_headers")
                if headers is not None
                else None
            ),
            recursive=_boolean(raw.get("recursive", False), f"source {source}.recursive"),
        )
    if connector_type == "http":
        allowed = {
            "endpoint",
            "records_path",
            "next_cursor_path",
            "cursor_parameter",
            "parameters",
            "headers",
            "expected_fields",
            "timeout_seconds",
            "max_attempts",
            "backoff_seconds",
            "max_pages",
            "raw_bucket",
        }
        _reject_unknown(raw, allowed, f"source {source}")
        fields = raw.get("expected_fields")
        return HTTPSourceSettings(
            endpoint=_string(_required(raw, "endpoint", source), f"source {source}.endpoint"),
            records_path=_strings(raw.get("records_path", ["items"]), "records_path"),
            next_cursor_path=_strings(
                raw.get("next_cursor_path", ["next_cursor"]), "next_cursor_path"
            ),
            cursor_parameter=_string(
                raw.get("cursor_parameter", "cursor"), f"source {source}.cursor_parameter"
            ),
            parameters=_string_mapping(raw.get("parameters", {}), "parameters"),
            headers=_string_mapping(raw.get("headers", {}), "headers"),
            expected_fields=(
                _strings(fields, f"source {source}.expected_fields") if fields is not None else None
            ),
            timeout_seconds=_number(
                raw.get("timeout_seconds", 10.0),
                f"source {source}.timeout_seconds",
                positive=True,
            ),
            max_attempts=_positive_integer(
                raw.get("max_attempts", 3), f"source {source}.max_attempts"
            ),
            backoff_seconds=_number(
                raw.get("backoff_seconds", 0.25),
                f"source {source}.backoff_seconds",
                positive=False,
            ),
            max_pages=_positive_integer(raw.get("max_pages", 1000), f"source {source}.max_pages"),
            raw_bucket=_string(raw.get("raw_bucket", "raw"), f"source {source}.raw_bucket"),
        )
    if connector_type == "postgres":
        allowed = {
            "schema",
            "table",
            "columns",
            "watermark_column",
            "tie_breaker_column",
            "tie_breaker_type",
            "lookback_seconds",
            "max_window_records",
        }
        _reject_unknown(raw, allowed, f"source {source}")
        return PostgresSourceSettings(
            schema=_string(_required(raw, "schema", source), f"source {source}.schema"),
            table=_string(_required(raw, "table", source), f"source {source}.table"),
            columns=_strings(_required(raw, "columns", source), f"source {source}.columns"),
            watermark_column=_string(
                _required(raw, "watermark_column", source),
                f"source {source}.watermark_column",
            ),
            tie_breaker_column=_string(
                _required(raw, "tie_breaker_column", source),
                f"source {source}.tie_breaker_column",
            ),
            tie_breaker_type=_string(
                raw.get("tie_breaker_type", "integer"),
                f"source {source}.tie_breaker_type",
            ),
            lookback_seconds=_number(
                raw.get("lookback_seconds", 0),
                f"source {source}.lookback_seconds",
                positive=False,
            ),
            max_window_records=_positive_integer(
                raw.get("max_window_records", 10_000),
                f"source {source}.max_window_records",
            ),
        )
    raise InvalidRegistryConfig(f"source {source}: unsupported connector_type {connector_type}")


def _validate_connector_settings(
    source_id: UUID, source_name: str, settings: ConnectorSettings
) -> None:
    """Run concrete adapter configuration validation before any adapter executes."""
    try:
        if isinstance(settings, CSVSourceSettings):
            CSVAdapterConfig(
                paths=settings.paths,
                delimiter=settings.delimiter,
                encoding=settings.encoding,
                has_header=settings.has_header,
                expected_headers=settings.expected_headers,
                recursive=settings.recursive,
            )
        elif isinstance(settings, HTTPSourceSettings):
            HTTPJSONConfig(
                source_name=source_name,
                endpoint=settings.endpoint,
                records_path=settings.records_path,
                next_cursor_path=settings.next_cursor_path,
                cursor_parameter=settings.cursor_parameter,
                parameters=settings.parameters,
                headers=settings.headers,
                expected_fields=settings.expected_fields,
                timeout_seconds=settings.timeout_seconds,
                max_attempts=settings.max_attempts,
                backoff_seconds=settings.backoff_seconds,
                max_pages=settings.max_pages,
                raw_bucket=settings.raw_bucket,
            )
        elif isinstance(settings, PostgresSourceSettings):
            PostgresAdapterConfig(
                source_id=source_id,
                source_name=source_name,
                schema=settings.schema,
                table=settings.table,
                columns=settings.columns,
                watermark_column=settings.watermark_column,
                tie_breaker_column=settings.tie_breaker_column,
                tie_breaker_type=settings.tie_breaker_type,
                lookback_seconds=settings.lookback_seconds,
                max_window_records=settings.max_window_records,
            )
    except (TypeError, ValueError) as exc:
        raise InvalidRegistryConfig(f"source {source_name}: {exc}") from exc
