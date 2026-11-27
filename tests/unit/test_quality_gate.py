"""Reusable rules, thresholds, persistence, rejected evidence, and publication gate tests."""

import hashlib
from datetime import UTC, datetime
from decimal import Decimal
from types import MappingProxyType
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from app.common.storage.minio import StoredObject
from app.connectors.base import ExtractedBatch, SourceMetadata, SourceObject, SourceRecord
from app.transformation.orders import OrderMapping, OrderTransformer
from app.transformation.pipeline import TransformationPipeline
from app.validation.gate import QualityCheck, QualityGate, RuleOutcome, RuleThresholds
from app.validation.rules import (
    RangeRule,
    ReferentialRule,
    RequiredFieldsRule,
    SchemaTypesRule,
    UniquenessRule,
)

OUTPUT_BATCH_ID = UUID("22222222-2222-4222-8222-222222222222")
METADATA_BATCH_ID = UUID("33333333-3333-4333-8333-333333333333")
INGESTED_AT = datetime(2026, 11, 27, 15, tzinfo=UTC)


class MemoryStore:
    def __init__(self):
        self.objects = {}

    def upload_if_absent(self, bucket, key, data, *, content_type):
        existing = self.objects.setdefault((bucket, key), (data, content_type))
        assert existing == (data, content_type)
        return StoredObject(bucket, key, hashlib.sha256(data).hexdigest(), len(data), None)


class ValidationRepository:
    def __init__(self):
        self.records = {}

    def register_validation_summary(
        self,
        batch_id,
        rule_name,
        status,
        checked_count,
        failed_count,
        *,
        attempt=1,
        details=None,
    ):
        key = (batch_id, rule_name, attempt)
        value = {
            "batch_id": batch_id,
            "rule_name": rule_name,
            "status": status,
            "checked_count": checked_count,
            "failed_count": failed_count,
            "details": details or {},
        }
        existing = self.records.setdefault(key, value)
        assert existing == value
        return existing


class StagingPublisher:
    def __init__(self):
        self.calls = []

    def publish(self, records, *, source, ingest_date, batch_id):
        self.calls.append((tuple(records), source, ingest_date, batch_id))
        if not records:
            return ()
        return (StoredObject("staging", f"{source}/{batch_id}.parquet", "a" * 64, 10, None),)


def mapping():
    return OrderMapping(
        source_system="orders-api",
        order_id_field="id",
        customer_id_field="customer",
        order_timestamp_field="timestamp",
        amount_field="amount",
        currency_field="currency",
        status_field="status",
    )


def source_row(index=1, **changes):
    row = {
        "id": f"11111111-1111-4111-8111-{index:012d}",
        "customer": f"customer-{index}",
        "timestamp": "2026-11-27T10:00:00-05:00",
        "amount": "19.90",
        "currency": "USD",
        "status": "confirmed",
    }
    row.update(changes)
    return row


def extracted(rows):
    source = SourceObject("https://local.test/orders", "orders-api")
    metadata = SourceMetadata(
        uuid5(NAMESPACE_URL, "quality-source"),
        "b" * 64,
        100,
        INGESTED_AT,
        "application/json",
    )
    records = tuple(
        SourceRecord(MappingProxyType(row), line_number=index + 1) for index, row in enumerate(rows)
    )
    return ExtractedBatch(OUTPUT_BATCH_ID, source, metadata, 0, records)


def standard_checks(extra=()):
    return (
        QualityCheck(
            "required_fields",
            RequiredFieldsRule(("order_id", "customer_id", "amount", "currency")),
        ),
        QualityCheck(
            "schema_types",
            SchemaTypesRule(
                {
                    "order_id": str,
                    "customer_id": str,
                    "amount": Decimal,
                    "order_timestamp": datetime,
                }
            ),
        ),
        QualityCheck("unique_order", UniquenessRule(("order_id",))),
        QualityCheck(
            "amount_range",
            RangeRule("amount", Decimal("0.00"), Decimal("100.00")),
        ),
        QualityCheck(
            "known_currency",
            ReferentialRule("currency", frozenset({"USD", "EUR"})),
        ),
        *extra,
    )


def pipeline(checks, *, store=None, repository=None, staging=None):
    store = store or MemoryStore()
    repository = repository or ValidationRepository()
    staging = staging or StagingPublisher()
    gate = QualityGate(checks, repository, store)
    return (
        TransformationPipeline(OrderTransformer(mapping()), store, staging, gate),
        store,
        repository,
        staging,
    )


def process(instance, rows):
    return instance.process(
        extracted(rows),
        source="orders-api",
        ingested_at=INGESTED_AT,
        metadata_batch_id=METADATA_BATCH_ID,
    )


def test_clean_batch_passes_every_rule_and_publishes_staging():
    instance, store, repository, staging = pipeline(standard_checks())
    result = process(instance, [source_row(1), source_row(2, currency="EUR")])
    assert result.publication_blocked is False
    assert len(result.staging_objects) == 1
    assert len(staging.calls) == 1
    assert all(summary.outcome is RuleOutcome.PASSED for summary in result.quality_report.summaries)
    assert result.quality_report.issue_object is None
    assert len(repository.records) == len(standard_checks()) + 1
    assert not store.objects


def test_warning_only_batch_persists_issues_but_still_publishes():
    warning = QualityCheck(
        "high_value_warning",
        RangeRule("amount", maximum=Decimal("50.00")),
        RuleThresholds(warning_at=1, blocking_at=None),
    )
    instance, store, repository, staging = pipeline((warning,))
    result = process(instance, [source_row(amount="75.00")])
    summary = next(item for item in result.quality_report.summaries if item.name == warning.name)
    assert summary.outcome is RuleOutcome.WARNING
    assert summary.failed_count == 1
    assert result.publication_blocked is False
    assert len(staging.calls) == 1
    assert result.quality_report.issue_object.bucket == "rejected"
    assert (
        result.quality_report.issue_object.uri
        in summary.database_record["details"]["rejected_uris"]
    )
    assert len(store.objects) == 1


def test_blocking_duplicate_prevents_any_staging_publication():
    instance, store, repository, staging = pipeline(
        (QualityCheck("unique_order", UniquenessRule(("order_id",))),)
    )
    duplicate_id = "11111111-1111-4111-8111-000000000001"
    result = process(instance, [source_row(id=duplicate_id), source_row(2, id=duplicate_id)])
    summary = next(item for item in result.quality_report.summaries if item.name == "unique_order")
    assert summary.outcome is RuleOutcome.FAILED
    assert summary.failed_count == 2
    assert result.publication_blocked is True
    assert result.staging_objects == ()
    assert staging.calls == []
    assert len(store.objects) == 1


def test_all_transformation_rejected_batch_fails_non_empty_gate_and_keeps_reference():
    instance, store, repository, staging = pipeline(standard_checks())
    result = process(instance, [source_row(amount="malformed")])
    assert result.accepted_count == 0 and result.rejected_count == 1
    assert result.rejected_object is not None
    assert result.publication_blocked is True
    assert staging.calls == []
    non_empty = next(
        item for item in result.quality_report.summaries if item.name == "batch_non_empty"
    )
    assert non_empty.outcome is RuleOutcome.FAILED
    assert non_empty.checked_count == 0 and non_empty.failed_count == 0
    assert non_empty.violation_count == 1
    assert result.rejected_object.uri in non_empty.database_record["details"]["rejected_uris"]
    assert len(store.objects) == 2


def test_quality_rerun_is_deterministic_and_summary_registration_is_idempotent():
    store = MemoryStore()
    repository = ValidationRepository()
    warning = QualityCheck(
        "high_value_warning",
        RangeRule("amount", maximum=Decimal("50")),
        RuleThresholds(warning_at=1, blocking_at=None),
    )
    first_pipeline, _, _, _ = pipeline((warning,), store=store, repository=repository)
    second_pipeline, _, _, _ = pipeline((warning,), store=store, repository=repository)
    first = process(first_pipeline, [source_row(amount="75")])
    second = process(second_pipeline, [source_row(amount="75")])
    assert first.quality_report.issue_object == second.quality_report.issue_object
    assert len(repository.records) == 2
    assert len(store.objects) == 1


def test_applicable_referential_rule_and_schema_bool_handling():
    rows = [
        {"kind": "external", "customer_id": "known", "count": 1},
        {"kind": "external", "customer_id": "missing", "count": True},
        {"kind": "internal", "customer_id": "ignored", "count": 2},
    ]
    reference = ReferentialRule(
        "customer_id",
        frozenset({"known"}),
        when_field="kind",
        when_values=frozenset({"external"}),
    )
    schema = SchemaTypesRule({"count": int})
    assert [item.record_index for item in reference.evaluate(rows)] == [1]
    assert [item.record_index for item in schema.evaluate(rows)] == [1]


def test_threshold_configuration_and_required_rule_validation():
    with pytest.raises(ValueError, match="warning_at cannot exceed"):
        RuleThresholds(warning_at=3, blocking_at=2)
    with pytest.raises(ValueError, match="at least one"):
        RuleThresholds(warning_at=None, blocking_at=None)
    with pytest.raises(ValueError, match="unique"):
        RequiredFieldsRule(("id", "id"))


def test_quality_gate_requires_metadata_batch_id_before_any_output():
    instance, store, repository, staging = pipeline(standard_checks())
    with pytest.raises(ValueError, match="metadata_batch_id"):
        instance.process(extracted([source_row()]), source="orders-api", ingested_at=INGESTED_AT)
    with pytest.raises(ValueError, match="validation_attempt"):
        instance.process(
            extracted([source_row()]),
            source="orders-api",
            ingested_at=INGESTED_AT,
            metadata_batch_id=METADATA_BATCH_ID,
            validation_attempt=0,
        )
    assert not store.objects and not repository.records and not staging.calls
