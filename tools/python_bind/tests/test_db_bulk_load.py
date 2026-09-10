#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest

from neug import BulkLoadSession
from neug.database import Database


def _current_checkpoint_id(db_dir):
    return int((db_dir / "checkpoint" / "CURRENT").read_text().strip())


def _copy_query(label, path):
    return f"COPY {label} FROM '{path.as_posix()}' " "(HEADER=true, DELIMITER=',');"


def test_bulk_load_context_commits_multiple_copies_once(tmp_path):
    db_dir = tmp_path / "bulk-context"
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    first.write_text("id,name\n1,Alice\n")
    second.write_text("id,name\n2,Bob\n")

    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE BulkPerson(id INT64, name STRING, PRIMARY KEY(id));"
    )
    checkpoint_before = _current_checkpoint_id(db_dir)

    with conn.bulk_load() as loader:
        assert isinstance(loader, BulkLoadSession)
        loader.execute(_copy_query("BulkPerson", first))
        with pytest.raises(RuntimeError, match="ExecuteBulkLoadQuery"):
            conn.execute(_copy_query("BulkPerson", second))
        loader.execute(_copy_query("BulkPerson", second))
        assert loader.active
        assert conn.has_active_bulk_load
        assert _current_checkpoint_id(db_dir) == checkpoint_before

    assert not loader.active
    assert not conn.has_active_bulk_load
    assert _current_checkpoint_id(db_dir) == checkpoint_before + 1
    assert list(
        conn.execute("MATCH (n:BulkPerson) RETURN n.id, n.name ORDER BY n.id;")
    ) == [[1, "Alice"], [2, "Bob"]]

    conn.close()
    db.close()
    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    assert len(conn.execute("MATCH (n:BulkPerson) RETURN n;")) == 2
    conn.close()
    db.close()


def test_bulk_load_context_exception_rolls_back(tmp_path):
    db_dir = tmp_path / "bulk-context-rollback"
    data = tmp_path / "rollback.csv"
    data.write_text("id\n1\n")

    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    conn.execute("CREATE NODE TABLE BulkRollback(id INT64, PRIMARY KEY(id));")
    checkpoint_before = _current_checkpoint_id(db_dir)

    with pytest.raises(ValueError, match="stop load"):
        with conn.bulk_load() as loader:
            loader.execute(_copy_query("BulkRollback", data))
            raise ValueError("stop load")

    assert not loader.active
    assert not conn.has_active_bulk_load
    assert _current_checkpoint_id(db_dir) == checkpoint_before
    assert len(conn.execute("MATCH (n:BulkRollback) RETURN n;")) == 0
    conn.close()
    db.close()


def test_bulk_load_failure_requires_manual_rollback(tmp_path):
    db_dir = tmp_path / "bulk-failure"
    data = tmp_path / "failure.csv"
    data.write_text("id\n1\n")

    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    conn.execute("CREATE NODE TABLE BulkFailure(id INT64, PRIMARY KEY(id));")

    loader = conn.begin_bulk_load()
    loader.execute(_copy_query("BulkFailure", data))
    with pytest.raises(RuntimeError, match="Only persistent COPY FROM"):
        loader.execute("MATCH (n:BulkFailure) RETURN n;")
    assert loader.failed
    with pytest.raises(RuntimeError, match="rollback is required"):
        loader.commit()
    loader.rollback()
    assert not conn.has_active_bulk_load
    assert len(conn.execute("MATCH (n:BulkFailure) RETURN n;")) == 0
    with pytest.raises(RuntimeError, match="no longer active"):
        loader.execute(_copy_query("BulkFailure", data))

    empty = conn.begin_bulk_load()
    empty.commit()

    with pytest.raises(RuntimeError, match="rollback is required"):
        with conn.bulk_load() as failed:
            try:
                failed.execute("MATCH (n:BulkFailure) RETURN n;")
            except RuntimeError:
                pass
    assert not conn.has_active_bulk_load

    conn.close()
    db.close()
