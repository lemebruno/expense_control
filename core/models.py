"""
core.models
Domain layer models used by the Expense Control app.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional, Any, Mapping

ISO_FMT = "%Y-%m-%d"


def _to_iso(d: date | str) -> str:
    """
    Normalize an input date (date or 'YYYY-MM-DD' string) to ISO string.
    """
    if isinstance(d, date):
        return d.strftime(ISO_FMT)
    if isinstance(d, str):
        s = d.strip()
        try:
            dt = datetime.strptime(s, ISO_FMT)
            return dt.strftime(ISO_FMT)
        except ValueError:
            for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
                try:
                    dt = datetime.strptime(s, fmt)
                    return dt.strftime(ISO_FMT)
                except ValueError:
                    pass
    raise ValueError(f"Invalid date: {d!r}. Expected date or 'YYYY-MM-DD'.")
 
 
@dataclass(slots=True)
class Expense:
    """
    Canonical expense record as used in the domain layer.
    """
    dt: str  # ISO 'YYYY-MM-DD'
    category: str
    subcategory: Optional[str]
    amount: float
    note: Optional[str] = None
    id: Optional[int] = field(default=None, compare=False)

    def __post_init__(self) -> None:
        self.dt = _to_iso(self.dt)
        self.category = (self.category or '').strip()
        if self.subcategory is not None:
            self.subcategory = self.subcategory.strip() or None
        if self.note is not None:
            self.note = self.note.strip() or None
        try:
            self.amount = float(self.amount)
        except Exception as e:
            raise ValueError(f"amount must be numeric. Got {self.amount!r}") from e

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "Expense":
        """
        Build an Expense from a sqlite3.Row or mapping with keys:
        id, dt, category, subcategory, amount, note.
        """
        return cls(
            id=row.get("id") if hasattr(row, "get") else row["id"],
            dt=row.get("dt") if hasattr(row, "get") else row["dt"],
            category=row.get("category") if hasattr(row, "get") else row["category"],
            subcategory=row.get("subcategory") if hasattr(row, "get") else row["subcategory"],
            amount=row.get("amount") if hasattr(row, "get") else row["amount"],
            note=row.get("note") if hasattr(row, "get") else row["note"],
        )

    def to_params(self) -> dict[str, Any]:
        """Named params for INSERT/UPDATE."""
        return {
            "id": self.id,
            "dt": self.dt,
            "category": self.category,
            "subcategory": self.subcategory,
            "amount": self.amount,
            "note": self.note,
        }

    def to_tuple(self) -> tuple[Any, ...]:
        """Tuple in schema order (without id)."""
        return (self.dt, self.category, self.subcategory, self.amount, self.note)


# -----------------------------------------------------------------------------
# User model
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class User:
    """
    User domain model used for authentication.
    """
    email: str
    password_hash: str
    id: Optional[int] = field(default=None, compare=False)

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "User":
        return cls(
            id=row.get("id") if hasattr(row, "get") else row["id"],
            email=row.get("email") if hasattr(row, "get") else row["email"],
            password_hash=row.get("password_hash") if hasattr(row, "get") else row["password_hash"],
        )

    def to_params(self) -> dict[str, Any]:
        return {"id": self.id, "email": self.email, "password_hash": self.password_hash}


@dataclass(slots=True)
class ShoppingItem:
    item: str
    id: Optional[int] = field(default=None,compare=False)
    created_at: Optional[str] = field(default=None,compare=False)


    @classmethod
    def from_row(cls,row:Mapping[str, Any]) -> "ShoppingItem":
        return cls(
            id=row.get("id") if hasattr(row, "get") else row["id"],
            item=row.get("item") if hasattr(row, "get") else row["item"],
            created_at=row.get("created_at") if hasattr(row, "get") else row["created_at"],
        )
    
    def to_params(self) -> dict[str, Any]:
        return {"id":self.id,
                "item":self.item,
                "created_at":self.created_at}