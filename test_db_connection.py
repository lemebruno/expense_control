from datetime import date
from core.db import connect_db, ensure_db_ready, ensure_schema  # change to 'from db' if not using package 'core'
import os
from core import db as dbmod  # change to 'import db as dbmod' if not using package 'core'
def main():
    print(">> Connecting to PostgreSQL (Supabase)...")
    conn = connect_db()

    print("conn is None?", conn is None)
    # 1) Ping
    with conn.cursor() as cur:
    # --- Sanity check: ensure we are importing the correct module and .env is loaded
        print("DBG using:", dbmod.connect_db.__module__, dbmod.connect_db.__code__.co_filename)
        print("DBG HOST:", os.getenv("SUPABASE_DB_HOST"))
        print("DBG PW_LEN:", len(os.getenv("SUPABASE_DB_PASSWORD") or ""))

        conn = connect_db()

    # 2) Readiness + Migrations
    print(">> Checking DB readiness...")
    ensure_db_ready(touch=True)

    print(">> Ensuring schema (migrations)...")
    ensure_schema()

    # 3) CRUD básico
    print(">> Inserting a smoke row into expenses...")
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO expenses (dt, category, subcategory, amount, note)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
        """, (date.today(), "Test", "Smoke", 12.34, "hello from smoke test"))
        rid = cur.fetchone()["id"]
        conn.commit()
        print("Inserted id:", rid)

    print(">> Reading back the inserted row...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, dt, category, subcategory, amount, note
              FROM expenses
             WHERE id=%s;
        """, (rid,))
        print("Row:", cur.fetchone())

    conn.close()
    print("OK! Connection, schema and CRUD look good.")

if __name__ == "__main__":
    main()
