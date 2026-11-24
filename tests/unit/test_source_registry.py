"""Typed source registry, deterministic discovery, limits, and registration tests."""

from datetime import UTC, datetime
from types import MappingProxyType
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest

from app.common.metadata.repository import DuplicateBatch
from app.connectors.base import ExtractedBatch, SourceMetadata, SourceObject, SourceRecord
from app.ingestion.registry import (
    DuplicateDiscovery,
    InvalidRegistryConfig,
    RegistryConfig,
    RuntimeSourceConfig,
    SourceRegistry,
)


class FakeAdapter:
    def __init__(self, locator, batches):
        self.source = SourceObject(locator=locator, name=locator.rsplit("/", 1)[-1])
        self.batches = tuple(batches)
        self.bounds = []

    def discover(self):
        return (self.source,)

    def iter_batches(self, source, *, max_records):
        assert source == self.source
        self.bounds.append(max_records)
        yield from self.batches


class FakeFactory:
    def __init__(self, adapters):
        self.adapters = adapters
        self.built = []

    def build(self, source):
        self.built.append(source.name)
        return self.adapters[source.name]


class FakeRepository:
    def __init__(self, rows):
        self.rows = rows
        self.claimed = set()
        self.calls = []

    def list_sources(self, *, enabled_only=True):
        self.calls.append(("list", enabled_only))
        rows = [row for row in self.rows if row["enabled"] or not enabled_only]
        return tuple(rows)

    def create_run(self, source_id, logical_batch_key, object_uri, *, record_count=0):
        claim = (source_id, logical_batch_key)
        if claim in self.claimed:
            raise DuplicateBatch(logical_batch_key)
        self.claimed.add(claim)
        self.calls.append(("create", source_id, logical_batch_key, object_uri, record_count))
        return {"id": uuid4()}, {"id": uuid4(), "logical_batch_key": logical_batch_key}


def metadata_row(name, *, enabled=True, connector_type="file", config=None):
    return {
        "id": uuid5(NAMESPACE_URL, "test-source:" + name),
        "name": name,
        "connector_type": connector_type,
        "config": config
        or {
            "paths": [f"tests/fixtures/{name}.csv"],
            "expected_headers": ["id"],
        },
        "enabled": enabled,
    }


def extracted(source, number, *, start=0):
    metadata = SourceMetadata(
        identity=uuid5(NAMESPACE_URL, f"metadata:{source.locator}:{number}"),
        checksum_sha256=f"{number:064x}",
        byte_count=10,
        modified_at=datetime(2026, 11, 24, tzinfo=UTC),
        media_type="text/csv",
    )
    return ExtractedBatch(
        batch_id=uuid5(NAMESPACE_URL, f"batch:{source.locator}:{number}"),
        source=source,
        source_metadata=metadata,
        start_record=start,
        records=(SourceRecord(MappingProxyType({"id": str(number)}), number),),
    )


def adapter_for(name, count):
    locator = f"file:///fixtures/{name}.csv"
    source = SourceObject(locator, f"{name}.csv")
    return FakeAdapter(locator, [extracted(source, index, start=index) for index in range(count)])


def test_disabled_metadata_and_runtime_sources_never_build_or_discover():
    rows = [
        metadata_row("enabled"),
        metadata_row("database-disabled", enabled=False),
        metadata_row("runtime-disabled"),
    ]
    repository = FakeRepository(rows)
    factory = FakeFactory({"enabled": adapter_for("enabled", 1)})
    config = RegistryConfig(sources={"runtime-disabled": RuntimeSourceConfig(enabled=False)})
    registry = SourceRegistry(repository, config, factory)
    discovered = registry.discover()
    assert [batch.source.name for batch in discovered] == ["enabled"]
    assert factory.built == ["enabled"]
    assert repository.calls[0] == ("list", True)


def test_multi_source_discovery_is_sorted_and_stable_across_repetition():
    rows = [metadata_row("zeta"), metadata_row("alpha")]
    repository = FakeRepository(rows)
    factory = FakeFactory({"alpha": adapter_for("alpha", 2), "zeta": adapter_for("zeta", 1)})
    registry = SourceRegistry(repository, RegistryConfig(), factory)
    first = registry.discover()
    second = registry.discover()
    assert [batch.source.name for batch in first] == ["alpha", "alpha", "zeta"]
    assert [batch.logical_batch_key for batch in first] == [
        batch.logical_batch_key for batch in second
    ]
    assert all(batch.logical_batch_key.startswith("v1:") for batch in first)


def test_per_source_batch_size_and_limit_are_enforced():
    rows = [
        metadata_row(
            "orders",
            config={
                "paths": ["tests/fixtures/orders.csv"],
                "expected_headers": ["id"],
                "batch_size": 50,
                "batch_limit": 4,
            },
        )
    ]
    adapter = adapter_for("orders", 5)
    runtime = RegistryConfig(sources={"orders": RuntimeSourceConfig(batch_size=2, batch_limit=2)})
    registry = SourceRegistry(FakeRepository(rows), runtime, FakeFactory({"orders": adapter}))
    discovered = registry.discover()
    assert len(discovered) == 2
    assert adapter.bounds == [2]
    assert all(
        batch.source.batch_size == 2 and batch.source.batch_limit == 2 for batch in discovered
    )


def test_duplicate_batch_from_adapter_fails_before_registration():
    row = metadata_row("orders")
    adapter = adapter_for("orders", 1)
    adapter.batches = (adapter.batches[0], adapter.batches[0])
    repository = FakeRepository([row])
    registry = SourceRegistry(repository, RegistryConfig(), FakeFactory({"orders": adapter}))
    with pytest.raises(DuplicateDiscovery, match="duplicate logical batch"):
        registry.discover()
    assert not repository.claimed


def test_duplicate_registration_is_idempotent_and_database_owned():
    repository = FakeRepository([metadata_row("orders")])
    registry = SourceRegistry(
        repository,
        RegistryConfig(),
        FakeFactory({"orders": adapter_for("orders", 1)}),
    )
    [batch] = registry.discover()
    first = registry.register(batch)
    duplicate = registry.register(batch)
    assert first.created is True
    assert first.run is not None and first.batch is not None
    assert duplicate.created is False
    assert duplicate.run is None and duplicate.batch is None
    create = [call for call in repository.calls if call[0] == "create"]
    assert create[0][3] == batch.source_object.locator
    assert create[0][4] == 1


@pytest.mark.parametrize(
    ("row", "message"),
    [
        (metadata_row("missing", config={"expected_headers": ["id"]}), "missing.*paths"),
        (
            metadata_row(
                "unknown", config={"paths": ["x.csv"], "expected_headers": ["id"], "typo": 1}
            ),
            "unknown setting",
        ),
        (
            metadata_row("untyped", config={"paths": ["x.csv"], "has_header": "true"}),
            "has_header must be boolean",
        ),
        (
            metadata_row("unsupported", connector_type="minio", config={"bucket": "raw"}),
            "unsupported",
        ),
        (metadata_row("bad-limit", config={"paths": ["x.csv"], "batch_limit": 0}), "batch_limit"),
    ],
)
def test_invalid_metadata_config_fails_before_adapter_execution(row, message):
    factory = FakeFactory({})
    registry = SourceRegistry(FakeRepository([row]), RegistryConfig(), factory)
    with pytest.raises(InvalidRegistryConfig, match=message):
        registry.discover()
    assert not factory.built


def test_runtime_mapping_is_strict_and_typed():
    config = RegistryConfig.from_mapping(
        {
            "default_batch_size": 25,
            "sources": {
                "orders": {
                    "batch_limit": 3,
                    "overrides": {"delimiter": ";"},
                }
            },
        }
    )
    assert config.default_batch_size == 25
    assert config.sources["orders"].batch_limit == 3
    with pytest.raises(InvalidRegistryConfig, match="unknown setting"):
        RegistryConfig.from_mapping({"unexpected": True})
    with pytest.raises(InvalidRegistryConfig, match="positive integer"):
        RegistryConfig.from_mapping({"default_batch_limit": 0})
