"""
Streamlit GUI for the Expense Control application.

This app provides:
- An \"Insert Data\" tab to register new expenses.
- A \"View Data\" tab to inspect stored expenses.

It relies on the core package for:
- Domain model (Expense)
- Validation (validators)
- Repository (repo_expense)
- DB initialization and migrations (sync_cycle)
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

import streamlit as st
import pandas as pd

from core import validators as _validators
from core.validators import (
    list_subcategories,
    validate_amount,
    validate_category,
    validate_date,
    validate_subcategory,
    validate_note,
)
from core.models import Expense
from core import repo_expense
from core import sync_cycle

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Configure default category tree (only if not already provided elsewhere)
# -----------------------------------------------------------------------------
if _validators.CATEGORY_TREE is None:
    DEFAULT_CATEGORY_TREE: Dict[str, List[str]] = {        
        "Housing": ["Groceries","Rent", "Utilities", "Maintenance","Energy Bills","Internet","Phone", "Streaming / Subscriptions"],
        "Transportation": ["Public Transport", "Fuel", "Car taxes", "Maintenance / Repairs","Parking","Tolls"],
        "Savings / Investments": ["Savings", "Retirement", "Stocks"],
        "Leisure / Entertainment": ["Movies", "Tours / Concerts", "Games","Restaurants","Coffee Shops"],
        "Health": ["Gym", "Doctor", "Pharmacy","Supplements","Exams"],
        "Pets": ["Food", "Vet", "Grooming","Toys","Remedies"],
        "Other": ["Clothing", "Items","Home Decor"],
    }
    _validators.CATEGORY_TREE = DEFAULT_CATEGORY_TREE
    _validators.ALLOWED_CATEGORIES = list(DEFAULT_CATEGORY_TREE.keys())
    flat_subs: List[str] = []
    for lst in DEFAULT_CATEGORY_TREE.values():
        flat_subs.extend(lst)
    _validators.ALLOWED_SUBCATEGORIES = flat_subs


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def ensure_db_ready() -> None:
    """
    Run sync_before_db_use once per Streamlit session and record any error.
    """
    if "db_init_done" in st.session_state:
        return

    try:
        sync_cycle.sync_before_db_use()
        st.session_state["db_init_done"] = True
        st.session_state["db_error"] = None
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to run sync_before_db_use: %s", exc)
        st.session_state["db_init_done"] = False
        st.session_state["db_error"] = str(exc)


def expenses_to_dataframe(expenses: List[Expense]) -> pd.DataFrame:
    """
    Convert a list of Expense domain objects into a DataFrame suitable for display.
    """
    if not expenses:
        return pd.DataFrame(columns=["id", "date", "category", "subcategory", "amount", "notes"])

    rows: List[Dict[str, Any]] = []
    for e in expenses:
        rows.append(
            {
                "id": e.id,
                "date": e.dt,
                "category": e.category,
                "subcategory": e.subcategory,
                "amount": float(e.amount),
                "notes": e.note,
            }
        )
    return pd.DataFrame(rows)


# -----------------------------------------------------------------------------
# Pages
# -----------------------------------------------------------------------------
def page_insert() -> None:
    """
    Tab/page for inserting a new expense.
    """
    st.header("Insert Expense")

    ensure_db_ready()
    db_error = st.session_state.get("db_error")
    if db_error:
        st.warning(f"Database is not ready: {db_error}")

    # -------------------------------------------------------------------------
    # Persistent state: init + reset (if requested)
    # -------------------------------------------------------------------------
    # Inicializa valores padrão na primeira execução
    if "ins_dt" not in st.session_state:
        st.session_state["ins_dt"] = date.today()
    if "ins_category" not in st.session_state:
        st.session_state["ins_category"] = ""
    if "ins_subcategory" not in st.session_state:
        st.session_state["ins_subcategory"] = ""
    if "ins_amount_str" not in st.session_state:        
        st.session_state["ins_amount_str"] = ""
    if "ins_note" not in st.session_state:
        st.session_state["ins_note"] = ""

    
    if st.session_state.get("ins_reset"):
        st.session_state["ins_dt"] = date.today()
        st.session_state["ins_category"] = ""
        st.session_state["ins_subcategory"] = ""
        st.session_state["ins_amount_str"] = ""
        st.session_state["ins_note"] = ""
        st.session_state["ins_reset"] = False

    # -------------------------------------------------------------------------
    # Input widgets
    last_success = st.session_state.pop("ins_last_success", None)
    
    if last_success:
        st.success(last_success)

    categories = list(_validators.CATEGORY_TREE.keys()) if _validators.CATEGORY_TREE else []

    # --- Reactive widgets (no st.form) ---
    col1, col2 = st.columns(2)
    with col1:
        dt_input: date = st.date_input("Date", key="ins_dt")
    with col2:
        st.write("")  # keeps layout aligned

    category = st.selectbox("Category", [""] + categories, key="ins_category")

    # Subcategories depend on the selected category
    subcategories: List[str] = list_subcategories(category) if category else []
    subcat = st.selectbox(
        "Subcategory",
        [""] + subcategories if subcategories else [""],        
        disabled=not bool(category),
        key="ins_subcategory",
    )

    # Only numbers allowed
    amount_str = st.text_input("Value", key="ins_amount_str")
    note = st.text_area("Notes", height=80, max_chars=15, key="ins_note")

    save_clicked = st.button("Save", key="ins_save")

    if not save_clicked:
        return

    # -------------------------------------------------------------------------
    # Required-field check
    # -------------------------------------------------------------------------
    missing: List[str] = []
    if not dt_input:
        missing.append("Date")
    if not category:
        missing.append("Category")
    # Subcategory required only when there are options for the chosen category
    if subcategories and not subcat:
        missing.append("Subcategory")
    raw_amount_check = (st.session_state.get("ins_amount_str") or "").strip()
    if not raw_amount_check:
        missing.append("Value (> 0)")
        missing.append("Value (> 0)")

    if missing:
        st.error("Please fill out all required fields:\n- " + "\n- ".join(missing))
        return

    # -------------------------------------------------------------------------
    # Business validation
    # -------------------------------------------------------------------------
    try:
        dt_iso = validate_date(dt_input)
        cat_valid = validate_category(category)
        sub_valid = validate_subcategory(subcat or None, category=cat_valid)
        raw_amount = (st.session_state.get("ins_amount_str") or "").strip()
        if not raw_amount:
            raise ValueError("Amount is required.")        
        normalized_amount = raw_amount.replace(",", ".")
        try:
            amount_value = float(normalized_amount)
        except ValueError as exc:
            raise ValueError("Amount must be a number.") from exc

        amount_valid = validate_amount(amount_value)

        note_valid = validate_note(note)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Validation error: {exc}")
        return

    exp = Expense(
        dt=dt_iso,
        category=cat_valid,
        subcategory=sub_valid,
        amount=amount_valid,
        note=note_valid,
    )

    # -------------------------------------------------------------------------
    # Persist to DB
    # -------------------------------------------------------------------------
    try:
        new_id = repo_expense.insert(exp)

        # guarda a mensagem de sucesso para ser exibida no próximo rerun
        st.session_state["ins_last_success"] = (
            f"Expense inserted successfully"
        )

        # sinaliza que o formulário deve ser limpo antes de renderizar de novo
        st.session_state["ins_reset"] = True

        # dispara um novo rerun; na próxima execução o topo de page_insert()
        # vai limpar os campos e mostrar a mensagem
        st.rerun()  # se sua versão não tiver, use st.experimental_rerun()

    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to insert expense: %s", exc)
        st.error(f"Failed to insert expense: {exc}")



def page_view() -> None:
    """
    Tab/page for viewing expenses with simple filters.
    """
    st.header("View Expenses")

    ensure_db_ready()
    db_error = st.session_state.get("db_error")
    if db_error:
        st.warning(f"Database is not ready: {db_error}")

    categories = list(_validators.CATEGORY_TREE.keys()) if _validators.CATEGORY_TREE else []

    with st.form("view_form"):
        col1, col2 = st.columns(2)
        with col1:
            start_date: date = st.date_input("Start date", value=date.today().replace(day=1))
        with col2:
            end_date: date = st.date_input("End date", value=date.today())

        category_filter = st.selectbox("Filter by category", [""] + categories, index=0)

        subcategories_filter: List[str] = list_subcategories(category_filter) if category_filter else []
        subcategory_filter = st.selectbox(
            "Filter by subcategory",
            [""] + subcategories_filter if subcategories_filter else [""],
            index=0,
            disabled=not bool(category_filter),
        )

        submitted = st.form_submit_button("Load data")

    if not submitted:
        return

    try:
        dt_start_iso = validate_date(start_date)
        dt_end_iso = validate_date(end_date)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Validation error on dates: {exc}")
        return

    cat_filter_value: Optional[str] = category_filter or None
    sub_filter_value: Optional[str] = subcategory_filter or None

    try:
        expenses = repo_expense.list_between_dates(
            dt_start_iso,
            dt_end_iso,
            category=cat_filter_value,
            subcategory=sub_filter_value,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to load expenses: %s", exc)
        st.error(f"Failed to load expenses: {exc}")
        return

    if not expenses:
        st.info("No expenses found for the selected filters.")
        return

    df = expenses_to_dataframe(expenses)
    st.dataframe(df, use_container_width=True)


# -----------------------------------------------------------------------------
# Main entry point for Streamlit
# -----------------------------------------------------------------------------
def main() -> None:
    st.set_page_config(page_title="Expense Control", layout="wide")

    st.sidebar.title("Expense Control")
    

    tab = st.sidebar.radio("Select page", ["Insert Data", "View Data"])

    if tab == "Insert Data":
        page_insert()
    else:
        page_view()


if __name__ == "__main__":
    main()
