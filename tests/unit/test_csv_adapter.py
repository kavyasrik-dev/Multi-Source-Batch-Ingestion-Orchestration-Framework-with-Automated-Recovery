"""CSV adapter discovery, validation, identity, encoding, and batching tests."""

import hashlib
from pathlib import Path

import pytest

from app.connectors import SourceAdapter
from app.connectors.csv import (
    CSVAdapter,
    CSVAdapterConfig,
    CSVAdapterError,
    CSVHeaderError,
    CSVRecordError,
    DuplicateInputError,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "csv"
ORDER_HEADERS = ("order_id", "customer_id", "amount")


def adapter(name: str, **changes) -> CSVAdapter:
    values = {"path": FIXTURES / name, "expected_headers": ORDER_HEADERS}
    values.update(changes)
    path = values.pop("path")
    return CSVAdapter(CSVAdapterConfig.from_path(path, **values))


def test_adapter_implements_common_contract_and_discovers_canonical_file_uri():
    instance = adapter("valid_orders.csv")
    assert isinstance(instance, SourceAdapter)
    [source] = instance.discover()
    assert source.name == "valid_orders.csv"
    assert source.locator == (FIXTURES / "valid_orders.csv").resolve().as_uri()


def test_metadata_captures_checksum_bytes_timestamp_and_stable_identity():
    instance = adapter("valid_orders.csv")
    [source] = instance.discover()
    first = instance.metadata(source)
    second = instance.metadata(source)
    content = (FIXTURES / "valid_orders.csv").read_bytes()
    assert first == second
    assert first.checksum_sha256 == hashlib.sha256(content).hexdigest()
    assert first.byte_count == len(content)
    assert first.modified_at.tzinfo is not None
    assert first.media_type == "text/csv"


def test_bounded_batches_and_ids_are_stable_across_replay():
    instance = adapter("valid_orders.csv")
    [source] = instance.discover()
    first = list(instance.iter_batches(source, max_records=2))
    replay = list(instance.iter_batches(source, max_records=2))
    assert [len(batch.records) for batch in first] == [2, 1]
    assert [batch.start_record for batch in first] == [0, 2]
    assert [batch.batch_id for batch in first] == [batch.batch_id for batch in replay]
    assert first[0].records[0].line_number == 2
    assert dict(first[0].records[0].values) == {
        "order_id": "o-1",
        "customer_id": "c-1",
        "amount": "10.00",
    }


@pytest.mark.parametrize("name", ["empty.csv", "header_only.csv"])
def test_empty_inputs_produce_no_empty_batches(name):
    instance = adapter(name)
    [source] = instance.discover()
    assert list(instance.iter_batches(source, max_records=10)) == []


def test_malformed_row_reports_file_line_expected_actual_and_row():
    instance = adapter("malformed_rows.csv")
    [source] = instance.discover()
    with pytest.raises(CSVRecordError) as caught:
        list(instance.iter_batches(source, max_records=10))
    message = str(caught.value)
    assert "malformed_rows.csv" in message
    assert "line 3" in message
    assert "expected 3 fields, received 2" in message
    assert "o-2" in message


def test_unexpected_headers_are_rejected_before_data_rows():
    instance = CSVAdapter(
        CSVAdapterConfig.from_path(
            FIXTURES / "valid_orders.csv", expected_headers=("id", "customer", "total")
        )
    )
    [source] = instance.discover()
    with pytest.raises(CSVHeaderError, match="unexpected headers"):
        list(instance.extract(source))


def test_configurable_utf8_encoding_and_actionable_decode_failure():
    good = CSVAdapter(
        CSVAdapterConfig.from_path(
            FIXTURES / "unicode.csv", encoding="utf-8", expected_headers=("name", "city")
        )
    )
    [source] = good.discover()
    [record] = list(good.extract(source))
    assert dict(record.values) == {"name": "café", "city": "Montréal"}

    wrong = CSVAdapter(
        CSVAdapterConfig.from_path(
            FIXTURES / "unicode.csv", encoding="ascii", expected_headers=("name", "city")
        )
    )
    [source] = wrong.discover()
    with pytest.raises(CSVAdapterError, match=r"cannot decode with ascii at byte \d+"):
        list(wrong.extract(source))


def test_configurable_delimiter_and_headerless_mode():
    semicolon = adapter("semicolon.csv", delimiter=";")
    [source] = semicolon.discover()
    assert dict(next(semicolon.extract(source)).values)["amount"] == "10.00"

    headerless = adapter("no_header.csv", has_header=False)
    [source] = headerless.discover()
    records = list(headerless.extract(source))
    assert len(records) == 2
    assert records[0].line_number == 1
    assert dict(records[0].values)["order_id"] == "o-1"


def test_duplicate_configured_input_is_rejected():
    path = FIXTURES / "valid_orders.csv"
    instance = CSVAdapter(CSVAdapterConfig(paths=(path, path), expected_headers=ORDER_HEADERS))
    with pytest.raises(DuplicateInputError, match="more than once"):
        instance.discover()


def test_directory_and_explicit_file_duplicate_is_rejected():
    instance = CSVAdapter(
        CSVAdapterConfig(
            paths=(FIXTURES, FIXTURES / "valid_orders.csv"), expected_headers=ORDER_HEADERS
        )
    )
    with pytest.raises(DuplicateInputError, match="valid_orders.csv"):
        instance.discover()


def test_changed_content_changes_identity_and_batch_ids(tmp_path):
    path = tmp_path / "input.csv"
    path.write_text("id\n1\n", encoding="utf-8")
    instance = CSVAdapter(CSVAdapterConfig.from_path(path, expected_headers=("id",)))
    [source] = instance.discover()
    before_metadata = instance.metadata(source)
    [before_batch] = instance.iter_batches(source, max_records=10)
    path.write_text("id\n2\n", encoding="utf-8")
    after_metadata = instance.metadata(source)
    [after_batch] = instance.iter_batches(source, max_records=10)
    assert before_metadata.identity != after_metadata.identity
    assert before_batch.batch_id != after_batch.batch_id


@pytest.mark.parametrize("max_records", [0, -1, True, 1.5])
def test_invalid_batch_bounds_are_rejected(max_records):
    instance = adapter("valid_orders.csv")
    [source] = instance.discover()
    with pytest.raises(ValueError, match="positive integer"):
        list(instance.iter_batches(source, max_records=max_records))


def test_missing_input_is_actionable(tmp_path):
    instance = CSVAdapter(
        CSVAdapterConfig.from_path(tmp_path / "missing.csv", expected_headers=ORDER_HEADERS)
    )
    with pytest.raises(CSVAdapterError, match="does not exist"):
        instance.discover()


def test_configuration_rejects_unknown_encoding_and_headerless_without_names():
    with pytest.raises(ValueError, match="unknown encoding"):
        CSVAdapterConfig.from_path(FIXTURES, encoding="no-such-codec")
    with pytest.raises(ValueError, match="expected_headers"):
        CSVAdapterConfig.from_path(FIXTURES, has_header=False)
