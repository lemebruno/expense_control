
"""
Integration tests for core.repo_expense (repository layer).

How to run:
  python -m tests.test_repo_expense
"""



from __future__ import annotations

from datetime import date
from typing import Any

import core.validators as V

from core.db import (
    connect_db,
    ensure_db_ready,
    ensure_schema,
    fetch_one,
    execute_write,
    transaction,
)
from core.models import Expense
from core import repo_expense as repo
from core.validators import CATEGORY_TREE, validate_expense


# ---------- Helpers ----------

def _wipe_expenses() -> None:
    """Remove all rows from 'expenses' table (clean slate)."""
    with connect_db() as conn:
        execute_write(conn, "DELETE FROM expenses;", ())


def _count() -> int:
    """Return number of rows in 'expenses' table."""
    with connect_db() as conn:
        row = fetch_one(conn, "SELECT COUNT(*) AS n FROM expenses;", ())
        return int(row["n"]) if row else 0


def _sample_expense(
    *,
    dt: str = "2025-11-01",
    category: str = "Food",
    subcategory: str | None = "Groceries",
    amount: float = 12.34,
    note: str | None = "Sample",
) -> Expense:
    return Expense(
        dt=dt,
        category=category,
        subcategory=subcategory,
        amount=amount,
        note=note,
    )


# ---------- Tests ----------

def test_00_bootstrap() -> None:
    """DB file exists and schema is at target version."""
    ensure_db_ready(touch=True)
    ensure_schema()
    print("[OK] DB bootstrap (file + schema).")


def test_10_category_tree_validation() -> None:
    """
    Domain rule: subcategories are tied to categories.
    Setting CATEGORY_TREE enforces (category, subcategory) pairs.
    """
    global CATEGORY_TREE
    V.CATEGORY_TREE = {
        "Food": ["Groceries", "Dining"],
        "Transport": ["Bus", "Fuel"],
        "Other": [],
    }
        # Sanity-check: the validator must reject ("Food", "Fuel") immediately.
    try:
        V.validate_subcategory("Fuel", category="Food")
        assert False, "Expected ValueError from validator sanity-check"
    except ValueError:
        pass

    # Proceed with DB-backed tests

    _wipe_expenses()

    # Valid pair should pass and insert
    e_ok = _sample_expense(category="Food", subcategory="Groceries")
    new_id = repo.insert(e_ok)
    assert new_id > 0

    # Invalid pair should raise BEFORE hitting the DB
    e_bad = _sample_expense(category="Food", subcategory="Fuel")
    raised = False
    try:
        repo.insert(e_bad)
    except ValueError:
        raised = True
    assert raised, "Expected ValueError for invalid (category, subcategory) pair"
    print("[OK] CATEGORY_TREE validation.")


def test_20_crud_get_update_delete() -> None:
    """CRUD via repository: insert → get_by_id → update → delete."""
    _wipe_expenses()

    e = _sample_expense(dt="2025-11-02", category="Transport", subcategory="Bus", amount=3.4, note="Ticket")
    new_id = repo.insert(e)
    assert new_id > 0

    got = repo.get_by_id(new_id)
    assert got is not None
    assert got.category == "Transport"
    assert got.subcategory == "Bus"

    got.amount = 4.0
    rows = repo.update(got)
    assert rows == 1

    again = repo.get_by_id(new_id)
    assert again is not None and abs(again.amount - 4.0) < 1e-6

    deleted = repo.delete(new_id)
    assert deleted == 1
    assert repo.get_by_id(new_id) is None
    print("[OK] CRUD.")


def test_30_list_between_dates_with_filters() -> None:
    """list_between_dates supports date range + category/subcategory filters."""
    _wipe_expenses()
    items = [
        _sample_expense(dt="2025-10-30", category="Food", subcategory="Dining", amount=20.0, note="Dinner"),
        _sample_expense(dt="2025-10-31", category="Food", subcategory="Groceries", amount=15.0, note="Market"),
        _sample_expense(dt="2025-11-01", category="Transport", subcategory="Fuel", amount=40.0, note="Gas"),
        _sample_expense(dt="2025-11-02", category="Transport", subcategory="Bus", amount=3.0, note="Ticket"),
    ]
    repo.bulk_insert(items)

    all_oct31_to_nov2 = repo.list_between_dates("2025-10-31", "2025-11-02")
    assert len(all_oct31_to_nov2) == 3

    only_food = repo.list_between_dates("2025-10-30", "2025-11-02", category="Food")
    assert len(only_food) == 2

    only_bus = repo.list_between_dates("2025-10-30", "2025-11-02", category="Transport", subcategory="Bus")
    assert len(only_bus) == 1 and only_bus[0].note == "Ticket"
    print("[OK] list_between_dates with filters.")


def test_40_aggregations() -> None:
    """sum_by_month(year) and sum_by_category(range) return expected aggregates."""
    _wipe_expenses()
    repo.bulk_insert(
        [
            _sample_expense(dt="2025-01-10", category="Food", subcategory="Groceries", amount=10.0),
            _sample_expense(dt="2025-01-15", category="Food", subcategory="Dining", amount=20.0),
            _sample_expense(dt="2025-02-02", category="Transport", subcategory="Fuel", amount=30.0),
            _sample_expense(dt="2025-02-05", category="Transport", subcategory="Bus", amount=5.0),
            _sample_expense(dt="2025-02-28", category="Food", subcategory="Groceries", amount=7.0),
        ]
    )

    by_month = dict(repo.sum_by_month(2025))
    assert abs(by_month["2025-01"] - 30.0) < 1e-6
    assert abs(by_month["2025-02"] - 42.0) < 1e-6

    by_cat = dict(repo.sum_by_category("2025-02-01", "2025-02-28"))
    # February has 30 (Fuel) + 5 (Bus) + 7 (Groceries) = 42 total
    assert abs(by_cat["Transport"] - 35.0) < 1e-6
    assert abs(by_cat["Food"] - 7.0) < 1e-6
    print("[OK] Aggregations.")


def test_50_bulk_insert_transactionality() -> None:
    """bulk_insert validates first and inserts atomically."""
    _wipe_expenses()
    base = _count()
    n = repo.bulk_insert(
        [
            _sample_expense(dt="2025-03-01", category="Food", subcategory="Dining", amount=12.0),
            _sample_expense(dt="2025-03-02", category="Transport", subcategory="Bus", amount=2.5),
        ]
    )
    assert n == 2
    assert _count() == base + 2
    print("[OK] bulk_insert.")


# ---------- Runner ----------

def main() -> None:
    tests = [
        test_00_bootstrap,
        test_10_category_tree_validation,
        test_20_crud_get_update_delete,
        test_30_list_between_dates_with_filters,
        test_40_aggregations,
        test_50_bulk_insert_transactionality,
    ]
    for fn in tests:
        fn()
    print("\nAll repository integration tests passed.")


if __name__ == "__main__":
    main()
