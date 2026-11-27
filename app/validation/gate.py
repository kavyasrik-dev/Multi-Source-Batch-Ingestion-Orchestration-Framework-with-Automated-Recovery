"""Threshold evaluation, rejected issue storage, and PostgreSQL summary persistence."""

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from app.common.contracts.orders import OrderRecord
from app.common.storage.minio import MinioObjectStore, StoredObject
from app.common.storage.naming import DataZone, batch_prefix
from app.validation.rules import QualityRule, RuleViolation


class RuleOutcome(StrEnum):
    PASSED = "passed"
    WARNING = "warning"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RuleThresholds:
    """Trigger warning/failure when violation count reaches configured values."""

    warning_at: int | None = None
    blocking_at: int | None = 1

    def __post_init__(self) -> None:
        for name, value in (("warning_at", self.warning_at), ("blocking_at", self.blocking_at)):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 1
            ):
                raise ValueError(f"{name} must be null or a positive integer")
        if self.warning_at is None and self.blocking_at is None:
            raise ValueError("at least one warning or blocking threshold is required")
        if (
            self.warning_at is not None
            and self.blocking_at is not None
            and self.warning_at > self.blocking_at
        ):
            raise ValueError("warning_at cannot exceed blocking_at")

    def outcome(self, violation_count: int) -> RuleOutcome:
        if self.blocking_at is not None and violation_count >= self.blocking_at:
            return RuleOutcome.FAILED
        if self.warning_at is not None and violation_count >= self.warning_at:
            return RuleOutcome.WARNING
        return RuleOutcome.PASSED


@dataclass(frozen=True, slots=True)
class QualityCheck:
    name: str
    rule: QualityRule
    thresholds: RuleThresholds = RuleThresholds()

    def __post_init__(self) -> None:
        if not self.name or not self.name.replace("_", "").isalnum():
            raise ValueError("quality check name must be non-empty alphanumeric/underscore")


@dataclass(frozen=True, slots=True)
class ValidationSummary:
    name: str
    outcome: RuleOutcome
    checked_count: int
    failed_count: int
    violation_count: int
    violations: tuple[RuleViolation, ...]
    thresholds: RuleThresholds
    database_record: Mapping[str, object] | None = None


@dataclass(frozen=True, slots=True)
class QualityReport:
    summaries: tuple[ValidationSummary, ...]
    issue_object: StoredObject | None
    rejected_uris: tuple[str, ...]

    @property
    def blocking_failed(self) -> bool:
        return any(summary.outcome is RuleOutcome.FAILED for summary in self.summaries)


class ValidationRepository(Protocol):
    def register_validation_summary(
        self,
        batch_id: UUID,
        rule_name: str,
        status: str,
        checked_count: int,
        failed_count: int,
        *,
        attempt: int = 1,
        details: dict | None = None,
    ) -> Mapping[str, object]: ...


class QualityGate:
    """Evaluate reusable checks, retain issues, persist summaries, and decide publication."""

    def __init__(
        self,
        checks: Sequence[QualityCheck],
        repository: ValidationRepository,
        store: MinioObjectStore,
        *,
        require_non_empty: bool = True,
    ) -> None:
        self.checks = tuple(checks)
        if len({check.name for check in self.checks}) != len(self.checks):
            raise ValueError("quality check names must be unique")
        self.repository = repository
        self.store = store
        self.require_non_empty = require_non_empty

    def validate_and_persist(
        self,
        records: Sequence[OrderRecord],
        *,
        metadata_batch_id: UUID,
        source: str,
        ingest_date: date,
        output_batch_id: UUID,
        existing_rejected_uris: Sequence[str] = (),
        attempt: int = 1,
    ) -> QualityReport:
        """Run checks and persist deterministic evidence before returning a gate decision."""
        rows = tuple(record.as_dict() for record in records)
        pending: list[ValidationSummary] = []
        if self.require_non_empty:
            violations = () if rows else (RuleViolation(None, "batch has no accepted records"),)
            pending.append(
                self._summary(
                    "batch_non_empty", violations, len(rows), RuleThresholds(blocking_at=1)
                )
            )
        for check in self.checks:
            violations = check.rule.evaluate(rows)
            pending.append(self._summary(check.name, violations, len(rows), check.thresholds))
        issue_object = self._persist_issues(
            pending,
            rows,
            source=source,
            ingest_date=ingest_date,
            batch_id=output_batch_id,
        )
        rejected_uris = list(dict.fromkeys(uri for uri in existing_rejected_uris if uri))
        if issue_object is not None:
            rejected_uris.append(issue_object.uri)
        persisted = []
        for summary in pending:
            details = {
                "outcome": summary.outcome.value,
                "violation_count": summary.violation_count,
                "warning_at": summary.thresholds.warning_at,
                "blocking_at": summary.thresholds.blocking_at,
                "rejected_uris": rejected_uris,
            }
            row = self.repository.register_validation_summary(
                metadata_batch_id,
                summary.name,
                summary.outcome.value,
                summary.checked_count,
                summary.failed_count,
                attempt=attempt,
                details=details,
            )
            persisted.append(
                ValidationSummary(
                    name=summary.name,
                    outcome=summary.outcome,
                    checked_count=summary.checked_count,
                    failed_count=summary.failed_count,
                    violation_count=summary.violation_count,
                    violations=summary.violations,
                    thresholds=summary.thresholds,
                    database_record=row,
                )
            )
        return QualityReport(tuple(persisted), issue_object, tuple(rejected_uris))

    @staticmethod
    def _summary(
        name: str,
        violations: tuple[RuleViolation, ...],
        checked_count: int,
        thresholds: RuleThresholds,
    ) -> ValidationSummary:
        affected = len({item.record_index for item in violations if item.record_index is not None})
        return ValidationSummary(
            name=name,
            outcome=thresholds.outcome(len(violations)),
            checked_count=checked_count,
            failed_count=affected,
            violation_count=len(violations),
            violations=violations,
            thresholds=thresholds,
        )

    def _persist_issues(
        self,
        summaries: Sequence[ValidationSummary],
        rows: Sequence[Mapping[str, object]],
        *,
        source: str,
        ingest_date: date,
        batch_id: UUID,
    ) -> StoredObject | None:
        documents = []
        for summary in summaries:
            for violation in summary.violations:
                document = {
                    "rule": summary.name,
                    "outcome": summary.outcome.value,
                    "record_index": violation.record_index,
                    "reason": violation.reason,
                    "record": (
                        _json_value(rows[violation.record_index])
                        if violation.record_index is not None
                        else None
                    ),
                }
                documents.append(document)
        if not documents:
            return None
        payload = (
            "\n".join(
                json.dumps(document, sort_keys=True, separators=(",", ":"))
                for document in documents
            )
            + "\n"
        ).encode()
        digest = hashlib.sha256(payload).hexdigest()[:16]
        key = f"{batch_prefix(source, ingest_date, batch_id)}/rejections/quality-{digest}.jsonl"
        return self.store.upload_if_absent(
            DataZone.REJECTED.bucket,
            key,
            payload,
            content_type="application/x-ndjson",
        )


def _json_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return str(value)
