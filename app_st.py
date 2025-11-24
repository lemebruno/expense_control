"""
Streamlit GUI for the Expense Control application.

This app provides:
- An \"Insert Data\" tab to register new expenses.


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
import os
import streamlit as st
import pandas as pd
import plotly.express as px  # For charts in the analysis page
from datetime import datetime
from calendar import monthrange
from zoneinfo import ZoneInfo

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
from core import repo_expense, repo_user
from core import sync_cycle
from core import auth



logger = logging.getLogger(__name__)


def page_login() -> None:
    st.header("Login")
    ensure_db_ready()
    db_error = st.session_state.get("db_error")
    if db_error:
        st.warning(f"Database is not ready: {db_error}")
        return

    # Flag to toggle between login view and registration view
    if "show_register" not in st.session_state:
        st.session_state["show_register"] = False

    # -------------------------------------------------------------------------
    # Login view
    # -------------------------------------------------------------------------
    if not st.session_state["show_register"]:
        email = st.text_input("Email", key="login_email")
        password = st.text_input("Password", type="password", key="login_password")

        if st.button("Login", key="login_submit"):
            success, user, message = auth.authenticate(email, password)
            if success and user is not None:
                st.session_state["auth_user"] = user.email
                st.session_state["user_id"] = user.id
                st.success("Login successful.")
                st.rerun()
            else:
                st.error(message or "Authentication failed.")

        # “Link” to open registration form
        st.markdown("")
        st.text("Don't have an account yet?")
        if st.button("Create an account", key="show_register_btn"):
            st.session_state["show_register"] = True
            st.rerun()

    # -------------------------------------------------------------------------
    # Registration view
    # -------------------------------------------------------------------------
    else:
        st.subheader("Create a new account")

        reg_email = st.text_input("New email", key="register_email")
        reg_password = st.text_input("New password", type="password", key="register_password")
        reg_password_confirm = st.text_input(
            "Confirm password",
            type="password",
            key="register_password_confirm",
        )

        if st.button("Register", key="register_submit"):
            email_norm = (reg_email or "").strip().lower()
            if not email_norm:
                st.error("Email is required for registration.")
                return
            if not reg_password:
                st.error("Password is required for registration.")
                return
            if reg_password != reg_password_confirm:
                st.error("Password confirmation does not match.")
                return

            # Check if user already exists
            try:
                existing = repo_user.get_by_email(email_norm)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed to check existing user: %s", exc)
                st.error("Failed to check existing user. Please try again.")
                return

            if existing is not None:
                st.error("An account with this email already exists.")
                return

            # Create user with Argon2id hash
            try:
                password_hash = auth.hash_password(reg_password)
                new_id = repo_user.insert(email_norm, password_hash)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed to register user: %s", exc)
                st.error(f"Failed to register user: {exc}")
                return

            st.success("User registered successfully. You can now log in.")
            # Pre-fill login email and go back to login view
            st.session_state["login_email"] = email_norm
            st.session_state["show_register"] = False
            st.rerun()

        # “Link” to go back to login
        st.markdown("")
        if st.button("Back to login", key="back_to_login"):
            st.session_state["show_register"] = False
            st.rerun()          

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
# Analysis page
# -----------------------------------------------------------------------------
def page_analysis() -> None:
    """
    Tab/page for visualising expenses with pie and bar charts.

    On first entry, this page takes a snapshot of all expense data from
    the PostgreSQL/Supabase database and stores it in Streamlit's session
    state. Subsequent interactions operate on this in‑memory dataset to
    avoid multiple database queries. The snapshot includes every row of
    the `expenses` table without any filtering. Users can then filter
    by month and choose to aggregate by category or subcategory without
    hitting the database again.
    """
    st.header("Expenses Analysis")
    # Ensure the database is ready before attempting to read any data
    ensure_db_ready()
    db_error = st.session_state.get("db_error")
    if db_error:
        st.warning(f"Database is not ready: {db_error}")
        return
    # -------------------------------------------------------------------------
    # Snapshot loading: load all expenses once per session
    # -------------------------------------------------------------------------
    if "analysis_df" not in st.session_state:
        try:
            # Retrieve all expenses from the database in one query
            all_expenses = repo_expense.list_between_dates(
                "1900-01-01",
                "2100-12-31",
                category=None,
                subcategory=None,
            )
            # Convert to DataFrame
            snapshot_df = expenses_to_dataframe(all_expenses)
            if not snapshot_df.empty:
                snapshot_df["date"] = pd.to_datetime(snapshot_df["date"])
                snapshot_df["month_label"] = snapshot_df["date"].dt.strftime("%B %Y")
            # Store snapshot in session_state
            st.session_state["analysis_df"] = snapshot_df
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to load snapshot of expenses: %s", exc)
            st.error(f"Failed to load expenses snapshot: {exc}")
            return
    # Get snapshot from session_state
    df = st.session_state.get("analysis_df")
    # Guard if snapshot is empty
    if df is None or df.empty:
        st.info("No expense records found. Please insert expenses before using the analysis page.")
        return
    # -------------------------------------------------------------------------
    # Build list of available months from the snapshot
    # -------------------------------------------------------------------------
    # Compute the first date of each month to sort chronologically
    month_min_dates = (
        df.groupby("month_label")["date"].min().reset_index().rename(columns={"date": "min_date"})
        .sort_values("min_date")
    )
    month_labels = month_min_dates["month_label"].tolist()
    # Map month label to the start date (first of month)
    label_to_start_date = {row["month_label"]: row["min_date"].replace(day=1) for _, row in month_min_dates.iterrows()}
    # Determine default month: use current month if present, else most recent month available
    now_label = datetime.now(ZoneInfo("Europe/Dublin")).strftime("%B %Y")
    if now_label in month_labels:
        default_selection = [now_label]
    else:
        default_selection = [month_labels[-1]] if month_labels else []
    # Sidebar controls for analysis
    st.sidebar.header("Filters (Analysis)")
    selected_months = st.sidebar.multiselect(
        "Select months to analyse:",
        options=month_labels,
        default=default_selection,
        help="Only months present in the dataset snapshot are shown. You may choose multiple months."
    )
    # Button to select all months
    if st.sidebar.button("Display all months"):
        selected_months = month_labels.copy()
    # Radio button to choose grouping (applies to both charts)
    view_by = st.sidebar.radio(
        "Group charts by:",
        options=["Category", "Subcategory"],
        index=0,
        help="Select whether to display expenses by category or by subcategory in both charts."
    )
    # -------------------------------------------------------------------------
    # Filter the snapshot by selected months
    # -------------------------------------------------------------------------
    filtered_df = df.copy()
    if selected_months:
        filtered_df = filtered_df[filtered_df["month_label"].isin(selected_months)]
        if filtered_df.empty:
            st.warning("No expenses match the selected months.")
            return
    # Determine grouping column
    group_col = "category" if view_by == "Category" else "subcategory"
    # Remove rows without subcategory if grouping by subcategory
    if group_col == "subcategory":
        filtered_df = filtered_df[filtered_df["subcategory"].notna()]
        if filtered_df.empty:
            st.warning("No subcategory data found for the selected months.")
            return
    # Aggregate data for pie chart
    agg_pie = filtered_df.groupby(group_col)["amount"].sum().reset_index(name="Total")
    agg_pie["Percentage"] = (agg_pie["Total"] / agg_pie["Total"].sum()) * 100
    # Aggregate data for bar chart (same grouping as view_by)
    agg_bar = (
        filtered_df.groupby(group_col)["amount"]
        .sum()
        .reset_index(name="Total")
        .sort_values("Total", ascending=False)
    )
    # -------------------------------------------------------------------------
    # Render charts

    st.subheader(f"Expenses by {view_by} (Percent)")
    pie_fig = px.pie(
        agg_pie,
        names=group_col,
        values="Percentage",
        hole=0.4,
        title=f"Percentage of expenses by {view_by.lower()}",
    )
    pie_fig.update_traces(textinfo="percent+label")
    pie_fig.update_layout(legend_title_text=view_by)
    st.plotly_chart(pie_fig, use_container_width=True)

    x_label = "Category" if view_by == "Category" else "Subcategory"
    st.subheader(f"Spending per {view_by} (EUR)")
    bar_fig = px.bar(
        agg_bar,
        x=group_col,
        y="Total",
        labels={group_col: x_label, "Total": "Amount (€)"},
        title=f"Total spending by {x_label.lower()}",
        hover_data={group_col: False},
    )
    
    st.plotly_chart(bar_fig, use_container_width=True)


# -----------------------------------------------------------------------------
# Main entry point for Streamlit
# -----------------------------------------------------------------------------
def main() -> None:
    st.set_page_config(page_title="Expense Control", layout="wide")

    # Authentication check
    if "auth_user" not in st.session_state:
        page_login()
        return

    st.sidebar.title("Expense Control")
    if st.sidebar.button("Logout"):
        st.session_state.pop("auth_user", None)
        st.session_state.pop("user_id", None)
        st.rerun()

    tab = st.sidebar.radio("Select page", ["Insert Data", "Analysis"], index=0)
    if tab == "Insert Data":
        page_insert()
    else:
        page_analysis()


if __name__ == "__main__":
    main()