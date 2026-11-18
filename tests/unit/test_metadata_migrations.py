"""Offline checks for migration packaging and fail-fast version validation."""

from unittest.mock import Mock

import pytest

from app.common.metadata.migrate import MIGRATIONS, migrate


def test_migration_assets_are_packaged_as_paired_versions():
    assert {p.name for p in MIGRATIONS.glob("*.sql")} == {
        "0001_metadata.up.sql",
        "0001_metadata.down.sql",
        "0002_states_indexes.up.sql",
        "0002_states_indexes.down.sql",
        "0003_manifest_registration.up.sql",
        "0003_manifest_registration.down.sql",
    }


def test_invalid_target_does_not_open_transaction():
    conn = Mock()
    with pytest.raises(ValueError, match="Unknown target"):
        migrate(conn, target=99)
    conn.transaction.assert_not_called()


def test_missing_migration_version_does_not_open_transaction(tmp_path):
    (tmp_path / "0002_gap.up.sql").write_text("SELECT 1;", encoding="utf-8")
    (tmp_path / "0002_gap.down.sql").write_text("SELECT 1;", encoding="utf-8")
    conn = Mock()
    with pytest.raises(ValueError, match="contiguous"):
        migrate(conn, directory=tmp_path)
    conn.transaction.assert_not_called()
