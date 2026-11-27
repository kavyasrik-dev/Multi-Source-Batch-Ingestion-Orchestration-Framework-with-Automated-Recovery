# Data quality rules and validation results (Day 12)

The quality gate runs after source records have been mapped into canonical orders and
before any staging Parquet is published. It evaluates pure reusable rules, writes
row-level evidence to the rejected zone, persists immutable summaries in PostgreSQL and
returns an explicit publication decision.

## Reusable rules

Rules accept sequences of mappings and return deterministic record-indexed violations:

- `RequiredFieldsRule` rejects missing, null and blank required values.
- `SchemaTypesRule` checks configured Python types and does not treat `bool` as `int`.
- `UniquenessRule` reports every member of a duplicate composite key group.
- `RangeRule` enforces inclusive minimum/maximum values and reports incomparable data.
- `ReferentialRule` checks membership in a reference set, optionally only when another
  field has one of the configured applicable values.

The canonical transformation contract already prevents many type/schema errors, but the
rules remain generic so they can validate canonical records, reference snapshots and
future datasets without embedding storage or orchestration behavior.

## Thresholds and outcomes

Each named `QualityCheck` has `RuleThresholds`. `warning_at` and `blocking_at` are
positive violation counts; either may be disabled with `None`. A blocking threshold is
evaluated first, then warning, producing one of `passed`, `warning` or `failed`.
Warnings retain evidence but allow publication. Any failed summary blocks all staging
publication for that batch.

The gate also enables `batch_non_empty` by default. If transformation rejects every input
row, this check fails even though its affected-record count is zero; its violation count
is one and the transformation rejection URI remains attached to the summary.

## Evidence and persistence

Every warning or failure with a record index is written as deterministic JSONL below:

```text
s3://rejected/source=<source>/logical_date=YYYY-MM-DD/batch_id=<output-uuid>/
  rejections/quality-<sha256-prefix>.jsonl
```

Documents include rule, outcome, record index, reason and the canonical record. Batch
issues such as an empty accepted set use a null record index. Existing transformation
rejection URIs and the quality issue URI are deduplicated and stored in each PostgreSQL
summary's `details.rejected_uris`; invalid rows are therefore never silently discarded.

Migration 0005 adds the `warning` validation status. `register_validation_summary`
uses `(batch_id, rule_name, attempt)` as immutable identity: identical retries return the
existing row while conflicting content raises `ValidationConflict`. The summary stores
checked and affected-record counts; details also retain violation count and thresholds.

## Publication gate

When a `QualityGate` is configured, `TransformationPipeline.process` requires the
PostgreSQL batch UUID. It persists transformation rejections, evaluates/persists quality
results, and only then calls the staging Parquet publisher. A blocking report returns
`publication_blocked=True` and no staging objects. Warning-only and clean reports proceed.
Storage or database failures remain publication errors and do not become row failures.

Tests cover clean, warning-only, duplicate blocking, all-transformation-rejected and
deterministic retry scenarios, plus rule edge cases, threshold validation, rejected
references and fail-before-output behavior when the metadata batch identity is missing.
