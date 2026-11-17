"""Pure tests for deterministic partition and anti-tiny-file planning."""

from datetime import date
from pathlib import Path
from uuid import UUID

import pytest

from app.common.contracts.layout import partition_path, plan_file_sizes

BATCH_ID = UUID("22222222-2222-4222-8222-222222222222")


def test_partition_path_is_exact_and_safe():
    path = partition_path(Path("lake"), "orders", date(2026, 11, 17), BATCH_ID)
    assert path.as_posix() == (f"lake/dataset=orders/ingest_date=2026-11-17/batch_id={BATCH_ID}")
    with pytest.raises(ValueError, match="dataset must match"):
        partition_path(Path("lake"), "../orders", date(2026, 11, 17), BATCH_ID)


@pytest.mark.parametrize(
    ("total", "expected"),
    [(0, []), (1, [1]), (10, [10]), (12, [12]), (14, [10, 4]), (25, [10, 10, 5])],
)
def test_file_plan_avoids_tiny_remainders(total, expected):
    assert plan_file_sizes(total, target_rows=10, min_rows=4) == expected


@pytest.mark.parametrize(
    ("total", "target", "minimum"), [(-1, 10, 1), (1, 0, 1), (1, 10, 0), (1, 10, 11)]
)
def test_invalid_file_plan_is_rejected(total, target, minimum):
    with pytest.raises(ValueError):
        plan_file_sizes(total, target, minimum)
