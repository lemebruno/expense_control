"""
core.validators
Validation utilities for domain models (categories, subcategories, dates, amounts).
This module centralizes business rules so the GUI and repositories can trust
that data has already been normalized and validated.
"""

from __future__ import annotations

from datetime import date
from typing import Iterable, Optional, Mapping, Sequence

from .models import _to_iso, Expense

# -----------------------------------------------------------------------------
# Source of truth for categories and subcategories
# -----------------------------------------------------------------------------
# If CATEGORY_TREE is set, it defines the allowed categories AND which
# subcategories belong to each category.
# Example:
# CATEGORY_TREE = {
#     "Food": ["Groceries", "Dining"],
#     "Transport": ["Bus", "Fuel"],
#     "Other": []
# }
CATEGORY_TREE: Optional[Mapping[str, Sequence[str]]] = None

# Backward-compatible lists:
# If CATEGORY_TREE is None, these two lists can still be used for simple checks.
ALLOWED_CATEGORIES: Optional[Iterable[str]] = None
ALLOWED_SUBCATEGORIES: Optional[Iterable[str]] = None


# -----------------------------------------------------------------------------
# Basic field validators
# -----------------------------------------------------------------------------
def validate_date(dt: date | str) -> str:
    """
    Normalize and validate a date into ISO 'YYYY-MM-DD'.
    Accepts a datetime.date or a string in common formats.
    """
    return _to_iso(dt)


def validate_amount(x) -> float:
    """
    Validate that amount is numeric and strictly greater than 0.
    """
    try:
        v = float(x)
    except Exception as e:
        raise ValueError(f"amount must be numeric. Got {x!r}") from e
    if not (v > 0):
        raise ValueError("amount must be greater than 0.")
    return v


def validate_category(cat: str) -> str:
    """
    Validate that a category is present and allowed by the configured source of truth.
    If CATEGORY_TREE is set, it takes precedence over the plain allowed lists.
    """
    cat = (cat or "").strip()
    if not cat:
        raise ValueError("category is required.")

    if CATEGORY_TREE is not None:
        if cat not in CATEGORY_TREE.keys():
            raise ValueError(f"category {cat!r} is not allowed.")
        return cat

    if ALLOWED_CATEGORIES is not None and cat not in ALLOWED_CATEGORIES:
        raise ValueError(f"category {cat!r} is not allowed.")
    return cat


def validate_subcategory(sub: Optional[str], *, category: Optional[str] = None) -> Optional[str]:
    """
    Validate subcategory. If CATEGORY_TREE is set, the (category, subcategory) pair must be valid.
    If subcategory is None or empty, it is treated as not provided.
    """
    if sub is None:
        return None

    sub = sub.strip()
    if sub == "":
        return None

    if CATEGORY_TREE is not None:
        if not category:
            raise ValueError(
                "subcategory validation requires category when CATEGORY_TREE is set."
            )
        allowed = set(CATEGORY_TREE.get(category, ()))
        if sub not in allowed:
            raise ValueError(
                f"subcategory {sub!r} is not allowed for category {category!r}."
            )
        return sub

    if ALLOWED_SUBCATEGORIES is not None and sub not in ALLOWED_SUBCATEGORIES:
        raise ValueError(f"subcategory {sub!r} is not allowed.")
    return sub


def validate_note(note: Optional[str], *, max_len: int = 500) -> Optional[str]:
    """
    Validate note length and normalize whitespace. Empty strings become None.
    """
    if note is None:
        return None
    note = note.strip()
    if note == "":
        return None
    if len(note) > max_len:
        raise ValueError(f"note must be <= {max_len} characters.")
    return note


# -----------------------------------------------------------------------------
# Aggregate validator for the domain model
# -----------------------------------------------------------------------------
def validate_expense(expense: Expense) -> Expense:
    """
    Validate and normalize an Expense instance in-place, then return it.
    Ensures (category, subcategory) is a valid pair when CATEGORY_TREE is configured.
    """
    expense.dt = validate_date(expense.dt)
    expense.amount = validate_amount(expense.amount)
    expense.category = validate_category(expense.category)
    expense.subcategory = validate_subcategory(expense.subcategory, category=expense.category)
    expense.note = validate_note(expense.note)
    return expense


# -----------------------------------------------------------------------------
# Helper used by the GUI (dependent dropdowns)
# -----------------------------------------------------------------------------
def list_subcategories(category: str) -> Sequence[str]:
    """
    Return the allowed subcategories for a given category.

    GUI usage:
    - After the user selects a category, call this helper to populate the
      subcategory dropdown.
    - If CATEGORY_TREE is not set, falls back to ALLOWED_SUBCATEGORIES (flat list),
      or an empty list when not configured.
    """
    if CATEGORY_TREE is not None:
        return list(CATEGORY_TREE.get(category, ()))
    if ALLOWED_SUBCATEGORIES is not None:
        return list(ALLOWED_SUBCATEGORIES)
    return []
