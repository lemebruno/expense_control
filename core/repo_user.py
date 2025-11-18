from typing import Optional

from .db import connect_db, fetch_one
from .models import User


def insert(email: str, password_hash: str) -> int:
    """
    Insert a new user into the users table and return the generated id.
    """
    email_norm = (email or "").strip().lower()
    if not email_norm:
        raise ValueError("Email must not be empty.")
    if not password_hash:
        raise ValueError("Password hash must not be empty.")

    sql = """
        INSERT INTO users (email, password_hash)
        VALUES (%s, %s)
        RETURNING id;
    """
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (email_norm, password_hash))
            row = cur.fetchone()
            conn.commit()

    if not row or "id" not in row:
        raise RuntimeError("Insert did not return a new id.")
    return int(row["id"])


def get_by_email(email: str) -> Optional[User]:
    """
    Fetch a user by email. Returns a User instance or None.
    """
    email_norm = (email or "").strip().lower()
    if not email_norm:
        return None

    sql = """
        SELECT id, email, password_hash
          FROM users
         WHERE email = %s;
    """
    with connect_db() as conn:
        row = fetch_one(conn, sql, (email_norm,))

    if not row:
        return None
    return User.from_row(row)