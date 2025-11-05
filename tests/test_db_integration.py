"""
Integration tests for core.db.

How to run:
  python -m tests.test_db_integration
"""



from __future__ import annotations

import threading            
import time                 
from typing import Any      

from core.db import (      
    connect_db,
    ensure_db_ready,
    ensure_schema,
    execute_write,
    fetch_one,
    fetch_all,
    transaction,
)

# ---------- Helpers (small utilities) ----------

def _wipe_expenses() -> None:
    """
    Deletes all rows from the 'expenses' table.
    - Uses a write helper and commits.
    """
    with connect_db() as conn:
        execute_write(conn, "DELETE FROM expenses;", ())


def _row_count() -> int:
    """
    Counts rows in 'expenses' table. Returns integer count.
    - Uses fetch_one() for a single aggregate row.
    """
    with connect_db() as conn:
        row = fetch_one(conn, "SELECT COUNT(*) AS n FROM expenses;", ())
        return int(row["n"]) if row else 0


def _make_sample_rows() -> list[tuple]:
    """
    Returns a small batch of sample rows for insertion.
    - Each tuple matches (dt, category, amount, note).
    """
    return [
        ("2025-10-30", "Food", 12.50, "Coffee"),
        ("2025-10-31", "Transport", 3.40, "Bus"),
        ("2025-11-01", "Bills", 45.99, "Electricity"),
    ]


# ---------- Test cases ----------

def test_00_bootstrap() -> None:
    """
    Ensures DB file exists and schema is up-to-date.
    - ensure_db_ready(touch=True) forces file creation on first run.
    - ensure_schema() runs migrations to the target version.
    """
    ensure_db_ready(touch=True)
    ensure_schema()
    print("[OK] Bootstrap ready (file + schema).")


def test_10_write_and_read() -> None:
    """
    Inserts one row and reads it back.
    - Asserts basic fields are persisted and retrievable.
    """
    _wipe_expenses()
    with connect_db() as conn:
        # insert one row
        rc = execute_write(
            conn,
            "INSERT INTO expenses (dt, category, amount, note) VALUES (?, ?, ?, ?);",
            ("2025-10-30", "Food", 12.50, "Coffee"),
        )
        assert rc == 1, "Expected 1 affected row on insert"

    # read it back
    with connect_db() as conn:
        row = fetch_one(conn,
                        "SELECT dt, category, amount, note FROM expenses WHERE category=?;",
                        ("Food",))
        assert row is not None, "Inserted row not found"
        assert row["dt"] == "2025-10-30"
        assert row["category"] == "Food"
        assert abs(row["amount"] - 12.50) < 1e-6
        assert row["note"] == "Coffee"
    print("[OK] Single write/read.")


def test_20_transaction_commit() -> None:
    """
    Performs a multi-insert within a transaction and commits.
    - Verifies that total rows increased by len(batch).
    """
    _wipe_expenses()
    base = _row_count()
    batch = _make_sample_rows()

    with connect_db() as conn:
        # Group multiple writes atomically
        with transaction(conn, begin_mode="DEFERRED"):
            for params in batch:
                conn.execute(
                    "INSERT INTO expenses (dt, category, amount, note) VALUES (?, ?, ?, ?);",
                    params,
                )
        # commit happens on context exit

    after = _row_count()
    assert after - base == len(batch), "Transaction commit should persist all rows"
    print("[OK] Transaction commit.")


def test_30_transaction_rollback() -> None:
    """
    Validates rollback on error:
    - Starts a transaction, inserts one row, then raises an exception on purpose.
    - Ensures that no partial data remained (atomicity).
    """
    _wipe_expenses()
    base = _row_count()

    try:
        with connect_db() as conn:
            with transaction(conn, begin_mode="DEFERRED"):
                conn.execute(
                    "INSERT INTO expenses (dt, category, amount, note) VALUES (?, ?, ?, ?);",
                    ("2025-11-02", "Test", 1.23, "First"),
                )
                # simulate an error to trigger rollback
                raise RuntimeError("Simulated failure")
    except RuntimeError:
        pass  # expected

    after = _row_count()
    assert after == base, "Rollback should leave zero inserts"
    print("[OK] Transaction rollback.")


def test_40_retry_on_locked() -> None:
    """
    Simulates a short-lived write lock on the database and ensures retry works:
    - conn1 begins an IMMEDIATE transaction and holds the write lock.
    - a background thread will release that lock after ~0.4s.
    - meanwhile, execute_write() (decorated with retry) should wait and succeed.
    """
    _wipe_expenses()

    lock_ready = threading.Event()

    def hold_lock_then_release() -> None:
        # Open connection INSIDE this thread
        conn = connect_db()
        try:
            conn.execute("BEGIN IMMEDIATE;")  # acquire write lock
            lock_ready.set()                  # signal: lock acquired
            time.sleep(0.4)                   # hold the lock for a short while
            conn.commit()                     # release lock
        finally:
            conn.close()

    t = threading.Thread(target=hold_lock_then_release, daemon=True)
    t.start()

    # Wait until the lock is actually held
    assert lock_ready.wait(timeout=2.0), "Lock was not acquired in time"

    # Now attempt a write on another connection (should retry and succeed)
    with connect_db() as conn2:
        rc = execute_write(
            conn2,
            "INSERT INTO expenses (dt, category, amount, note) VALUES (?, ?, ?, ?);",
            ("2025-11-03", "LockTest", 9.99, "Retry should succeed"),
        )
        assert rc == 1, "Expected retry to succeed after lock release"

    t.join(timeout=2.0)
    assert _row_count() >= 1, "Row should be present after successful retry"
    print("[OK] Retry on locked worked.")

# ---------- Runner ----------

def main() -> None:
    """
    Simple runner to execute tests in order and show clear progress.
    """
    tests = [
        test_00_bootstrap,
        test_10_write_and_read,
        test_20_transaction_commit,
        test_30_transaction_rollback,
        test_40_retry_on_locked,
    ]
    for fn in tests:
        fn()
    print("\nAll integration tests passed.")

if __name__ == "__main__":
    main()
