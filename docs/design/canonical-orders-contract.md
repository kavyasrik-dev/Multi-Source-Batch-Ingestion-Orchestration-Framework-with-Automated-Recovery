# Canonical orders contract and Parquet layout (Day 5)

Canonical orders use schema version `1`. Producers convert source records through
`parse_order`; storage uses the matching Arrow schema in `parquet.py`. Readers reject
files whose field order, types, nullability, or schema metadata differ. Schema changes
require a new contract version and explicit migration or multi-version reader.

| Field | Arrow/Python type | Nullable | Policy |
| --- | --- | --- | --- |
| `schema_version` | int16 / int | no | exactly `1` |
| `order_id` | UTF-8 / UUID | no | canonical UUID text |
| `customer_id` | UTF-8 / str | no | trimmed, non-empty |
| `order_timestamp` | timestamp[us, UTC] / datetime | no | explicit offset, normalized to UTC |
| `amount` | decimal128(18,2) / Decimal | no | nonnegative; no float or implicit rounding |
| `currency` | UTF-8 / str | no | uppercased 3-letter ASCII code |
| `status` | UTF-8 / str | yes | controlled order status or null |
| `source_system` | UTF-8 / str | no | trimmed, non-empty |
| `ingested_at` | timestamp[us, UTC] / datetime | no | explicit offset, normalized to UTC |
| `batch_id` | UTF-8 / UUID | no | canonical UUID text |
| `notes` | UTF-8 / str | yes | string or null |

Unknown fields fail closed so upstream drift is visible. Batch parsing reports all
invalid records using paths such as `records[3].amount`. Integer, Decimal and decimal
strings are accepted for amounts; binary floating-point values and booleans are
rejected. Decimal scale above two is rejected rather than silently rounded. Naive
timestamps are rejected rather than interpreted in the machine timezone.

Files use Zstandard level 6 with statistics and dictionaries for repeated categorical
columns. The Hive layout is:

```text
dataset=orders/ingest_date=YYYY-MM-DD/batch_id=<uuid>/part-00000-<content-hash>.parquet
```

Dataset names are validated to prevent path traversal. The ingest date must match
every record's normalized `ingested_at` UTC date, and all records must match the
partition batch UUID. Content-derived names make retrying the same write idempotent.
Writes use a temporary file and atomic same-directory rename. A changed payload gets
a different name; callers must use batch metadata/state transitions to decide whether
changed content is permissible for an existing logical batch.

The default target is 100,000 rows per file with a 10,000-row minimum. A final
remainder below the minimum is merged into the previous part, allowing that part to
exceed the target. A whole small batch remains one file. Empty batches return no paths
and create no directories, so they must still be recorded explicitly in metadata if
their occurrence matters. These row thresholds are operational defaults; production
tuning should measure compressed byte size and query behavior.

`read_order_file` reads a single canonical file and returns immutable `OrderRecord`
objects with UUID, Decimal, nullable values, and UTC datetimes restored. It is not a
general dataset scanner. Callers should enumerate files from a trusted manifest and
handle schema versions deliberately.
