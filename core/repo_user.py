from typing import Optional
from .db import connect_db, fetch_one
from .models import User

def insert(user: User) -> int:
    sql = """
        INSERT INTO users (email, password_hash)
        VALUES (%s, %s)
        RETURNING id;
    """
    # executa o INSERT e retorna o id

def get_by_email(email: str) -> Optional[User]:
    sql = """
        SELECT id, email, password_hash
          FROM users
         WHERE email = %s;
    """