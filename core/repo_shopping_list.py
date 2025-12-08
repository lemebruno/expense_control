from __future__ import annotations

from typing import Sequence, List
from .db import connect_db, execute_write, fetch_all
from .models import ShoppingItem

def insert_item(item: str) -> int:
    item_clean = (item or "").strip()
    if not item_clean:
        raise ValueError("Item must not be empty.")

    sql = """
        INSERT INTO shopping_list (item)
        VALUES (%s)
        RETURNING id;
    """
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, ( item_clean,))
            row = cur.fetchone()
            conn.commit()
            return int(row["id"])

def list_items() -> List[ShoppingItem]:
    sql = """
        SELECT id, item, created_at
          FROM shopping_list         
         ORDER BY created_at ASC, id ASC;
    """
    with connect_db() as conn:
        rows = fetch_all(conn, sql)
    return [ShoppingItem.from_row(r) for r in rows]

def delete_items(ids: Sequence[int]) -> int:
    if not ids:
        return 0
    sql = "DELETE FROM shopping_list WHERE id = ANY(%s);"
    with connect_db() as conn:
        return execute_write(conn, sql, (list(ids),))
