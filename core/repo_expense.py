

"""
core.repo_expense
Repository layer for the Expense domain model.

Responsibilities:
- Provide typed CRUD and query functions over the `expenses` table.
- Enforce domain validation BEFORE any write.
- Rely on core.db helpers for connection, transactions, read/write, and retries.

Schema reference (v1):
    expenses(id INTEGER PK AUTOINCREMENT,
             dt TEXT NOT NULL,           -- ISO YYYY-MM-DD
             category TEXT NOT NULL,
             subcategory TEXT NULL,
             amount REAL NOT NULL,
             note TEXT NULL)
"""

from __future__ import annotations
from typing import Any

from dataclasses import asdict
from datetime import date
from typing import Iterable, Optional, Sequence

from .db import (
    connect_db,
    execute_write,
    fetch_one,
    fetch_all,
    transaction,
)  # retry on locked + helpers 
from .models import Expense  # domain model (to/from row) 
from . import validators as V  # domain validation (cat↔subcat rule) 


# -----------------------------------------------------------------------------
# Write operations
# -----------------------------------------------------------------------------
def insert(expense: Expense) -> int:
    """
    Insert a single Expense and return the assigned row id.
    Validation is performed before touching the database.
    """
    V.validate_expense(expense)
    sql = """
        INSERT INTO expenses (dt, category, subcategory, amount, note)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
    """
    with connect_db() as conn:
        # Use RETURNING to get the new id
        with conn.cursor() as cur:
            cur.execute(sql, (
                expense.dt,
                expense.category,
                expense.subcategory,
                expense.amount,
                expense.note,
            ))
            row = cur.fetchone()
            conn.commit()
            if not row or "id" not in row:
                raise RuntimeError("Insert did not return a new id.")
            return int(row["id"])


def update(expense: Expense) -> int:
    """
    Update an existing Expense by id. Returns affected row count (0 or 1).
    Requires `expense.id` to be not None.
    """
    if not expense.id:
        raise ValueError("update() requires an id on the Expense object.")
    V.validate_expense(expense)
    sql = """
        UPDATE expenses
           SET dt=%s, category=%s, subcategory=%s,
               amount=%s, note=%s
         WHERE id=%s;
    """
    with connect_db() as conn:
        return execute_write(conn, sql, (
            expense.dt,
            expense.category,
            expense.subcategory,
            expense.amount,
            expense.note,
            expense.id,
        ))


def delete(expense_id: int) -> int:
    """
    Delete an expense by id. Returns affected row count (0 or 1).
    """
    sql = "DELETE FROM expenses WHERE id=%s;"
    with connect_db() as conn:
        return execute_write(conn, sql, (expense_id,))


# -----------------------------------------------------------------------------
# Read operations
# -----------------------------------------------------------------------------
def get_by_id(expense_id: int) -> Optional[Expense]:
    """
    Fetch a single Expense by id. Returns Expense or None.
    """
    sql = """
        SELECT id, dt, category, subcategory, amount, note
          FROM expenses
         WHERE id=%s;
    """
    with connect_db() as conn:
        row = fetch_one(conn, sql, (expense_id,))
    return Expense.from_row(row) if row else None


def list_between_dates(
    dt_start: date | str,
    dt_end: date | str,
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
) -> list[Expense]:
    where = ["dt >= %s", "dt <= %s"]
    params: list = [dt_start, dt_end]

    if category:
        where.append("category = %s")
        params.append(category)
    if subcategory:
        where.append("subcategory = %s")
        params.append(subcategory)

    sql = f"""
        SELECT id, dt, category, subcategory, amount, note
          FROM expenses
         WHERE {' AND '.join(where)}
         ORDER BY dt ASC, id ASC;
    """
    with connect_db() as conn:
        rows = fetch_all(conn, sql, tuple(params))
    return [Expense.from_row(r) for r in rows]


# -----------------------------------------------------------------------------
# Aggregations
# -----------------------------------------------------------------------------
def sum_by_month(year: int) -> list[tuple[str, float]]:
    """
        Return a list of (year_month, total_amount) for the given year.
    """
    sql = """
        SELECT TO_CHAR(dt, 'YYYY-MM') AS year_month,
               ROUND(SUM(amount), 2) AS total
          FROM expenses
         WHERE EXTRACT(YEAR FROM dt) = %s
         GROUP BY TO_CHAR(dt, 'YYYY-MM')
         ORDER BY year_month ASC;
    """
    with connect_db() as conn:
        rows = fetch_all(conn, sql, (year,))
    return [(r["year_month"], float(r["total"])) for r in rows]


def sum_by_category(
    dt_start: str | date,
    dt_end: str | date,
    *,
    only_within_range: bool = True,
) -> list[tuple[str, float]]:
    """
    Return a list of (category, total_amount) within the given date range.
    """
    sql = """
        SELECT category,
               ROUND(SUM(amount), 2) AS total
          FROM expenses
         WHERE dt >= %s AND dt <= %s
         GROUP BY category
         ORDER BY total DESC, category ASC;
    """
    with connect_db() as conn:
        rows = fetch_all(conn, sql, (dt_start, dt_end))
    return [(r["category"], float(r["total"])) for r in rows]


# -----------------------------------------------------------------------------
# Batch helpers (optional)
# -----------------------------------------------------------------------------
def bulk_insert(expenses: Sequence[Expense]) -> int:
    """
    Insert a batch of expenses atomically. Returns the number of inserted rows.
    Each expense is validated before the transaction begins.
    """
    if not expenses:
        return 0
    # validate first (fail-fast)
    validated = [V.validate_expense(e) for e in expenses]
    sql = """
        INSERT INTO expenses (dt, category, subcategory, amount, note)
        VALUES (%s, %s, %s, %s, %s);
    """
    # perform batch insert in a transaction
    with connect_db() as conn:
        with transaction(conn):
            total = 0
            with conn.cursor() as cur:
                for e in validated:
                    cur.execute(sql, (
                        e.dt,
                        e.category,
                        e.subcategory,
                        e.amount,
                        e.note,
                    ))
                    total += 1
           
    return total
