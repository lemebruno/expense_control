# test_db_connection.py
from datetime import date
from core.db import connect_db, ensure_db_ready, ensure_schema

def main():
    print(">> Connecting to PostgreSQL (Supabase)...")
    conn = connect_db()

    # 1) Basic reachability
    with conn.cursor() as cur:
        cur.execute("SELECT version() AS ver, current_user AS usr;")
        row = cur.fetchone()
        print("version:", row["ver"])
        print("current_user:", row["usr"])

    # 2) DB readiness + schema migrations
    print(">> Checking DB readiness...")
    ensure_db_ready(touch=True)

    print(">> Applying/validating schema...")
    ensure_schema()

    # 3) Insert + read back from 'expenses'
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
