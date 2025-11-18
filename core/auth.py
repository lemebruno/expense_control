"""
core.auth
Authentication utilities for the Expense Control application.

This module uses Argon2id (via argon2-cffi) to hash and verify passwords.
The UI layer should call these helpers instead of dealing with password
hashing logic directly.
"""

from __future__ import annotations

from typing import Optional, Tuple

from argon2 import PasswordHasher
from argon2 import exceptions as argon2_exceptions

from .models import User
from . import repo_user


# Single PasswordHasher instance reused across calls.
_ph = PasswordHasher()


def hash_password(password: str) -> str:
    """
    Hash a plain-text password using Argon2id.

    Args:
        password: Plain-text password provided by the user.

    Returns:
        Argon2id hash string (includes parameters and salt).
    """
    if password is None:
        raise ValueError("Password must not be None.")
    password = password.strip()
    if not password:
        raise ValueError("Password must not be empty.")
    return _ph.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    """
    Verify that a provided password matches a stored Argon2 hash.

    Args:
        password: Plain-text password to verify.
        password_hash: Stored Argon2 hash string.

    Returns:
        True if the password is valid, False otherwise.
    """
    if not password or not password_hash:
        return False

    try:
        return _ph.verify(password_hash, password)
    except (
        argon2_exceptions.VerifyMismatchError,
        argon2_exceptions.VerificationError,
        argon2_exceptions.InvalidHash,
    ):
        return False


def authenticate(email: str, password: str) -> Tuple[bool, Optional[User], str]:
    """
    Authenticate a user by email and password.

    Args:
        email: User email (login identifier).
        password: Plain-text password typed by the user.

    Returns:
        (is_authenticated, user, message)

        - If authentication succeeds:
            is_authenticated = True
            user = User instance
            message = ""

        - If user does not exist:
            is_authenticated = False
            user = None
            message = "Invalid login"

        - If password does not match:
            is_authenticated = False
            user = None
            message = "Password does not match login"
    """
    email_normalized = (email or "").strip().lower()
    if not email_normalized:
        return False, None, "Email is required."
    if not password:
        return False, None, "Password is required."

    user = repo_user.get_by_email(email_normalized)
    if user is None:
        return False, None, "Invalid login"

    if not verify_password(password, user.password_hash):
        return False, None, "Password does not match login"

    return True, user, ""