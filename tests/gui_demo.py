# gui_demo.py
# Minimal Streamlit demo: dependent dropdown (category -> subcategory) + save


import os, sys
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import streamlit as st
import core.validators as V
from core.models import Expense
from core import repo_expense as repo
from core.db import ensure_db_ready, ensure_schema

# Configure allowed categories/subcategories (source of truth)
V.CATEGORY_TREE = {
    "Food": ["Groceries", "Dining"],
    "Transport": ["Bus", "Fuel"],
    "Other": [],
}

def main():
    st.title("Expense Demo — Dependent Dropdown")

    # Ensure local DB & schema
    ensure_db_ready(touch=True)
    ensure_schema()

    # Category dropdown
    categories = list(V.CATEGORY_TREE.keys())
    category = st.selectbox("Category", options=categories)

    # Subcategory dropdown (dependent)
    subcats = V.list_subcategories(category)
    sub_options = subcats if subcats else ["(none)"]
    subcategory = st.selectbox("Subcategory", options=sub_options)
    if subcategory == "(none)":
        subcategory = None

    # Other fields
    dt = st.date_input("Date")
    amount = st.number_input("Amount", min_value=0.01, step=0.01, format="%.2f")
    note = st.text_input("Note", value="")

    if st.button("Save"):
        try:
            exp = Expense(
                dt=str(dt),
                category=category,
                subcategory=subcategory,
                amount=amount,
                note=note or None,
            )
            new_id = repo.insert(exp)
            st.success(f"Saved with id={new_id}")
        except Exception as e:
            st.error(f"Validation or save error: {e}")

    st.divider()
    st.caption("Tip: Change category to see subcategories update.")

if __name__ == "__main__":
    main()