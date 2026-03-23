from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from deltalake import DeltaTable

st.set_page_config(page_title="Customer360 Dashboard", page_icon=":bar_chart:", layout="wide")


@st.cache_data(ttl=60)
def load_segments_df(table_path: str) -> pd.DataFrame:
    dt = DeltaTable(table_path)
    return dt.to_pyarrow_table().to_pandas()


def main() -> None:
    st.title("Customer360 Dashboard")
    st.caption("Operational analytics for customer segmentation outputs")

    default_table_path = "data/delta/customer_segments"
    table_path = st.sidebar.text_input("Delta table path", value=default_table_path)
    refresh = st.sidebar.button("Refresh data")

    if refresh:
        st.cache_data.clear()

    path_obj = Path(table_path)
    if not path_obj.exists():
        st.error(f"Delta table path not found: {table_path}")
        st.stop()

    try:
        df = load_segments_df(table_path)
    except Exception as exc:
        st.error("Failed to read Delta table. Check table path and package versions.")
        st.exception(exc)
        st.stop()

    if df.empty:
        st.warning("No rows found in customer segment table.")
        st.stop()

    # Expected columns from your segmentation output.
    required_cols = {
        "customer_id",
        "segment",
        "state",
        "total_balance",
        "total_loan_amount",
        "account_count",
        "loan_count",
    }
    missing_cols = sorted(required_cols.difference(df.columns))
    if missing_cols:
        st.error("Missing expected columns: " + ", ".join(missing_cols))
        st.stop()

    segment_options = ["All"] + sorted(df["segment"].dropna().unique().tolist())
    state_options = ["All"] + sorted(df["state"].dropna().unique().tolist())

    selected_segment = st.sidebar.selectbox("Segment", options=segment_options, index=0)
    selected_state = st.sidebar.selectbox("State", options=state_options, index=0)

    filtered = df.copy()
    if selected_segment != "All":
        filtered = filtered[filtered["segment"] == selected_segment]
    if selected_state != "All":
        filtered = filtered[filtered["state"] == selected_state]

    total_customers = int(filtered["customer_id"].nunique())
    total_balance = float(filtered["total_balance"].fillna(0).sum())
    total_loan = float(filtered["total_loan_amount"].fillna(0).sum())

    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric("Customers", f"{total_customers:,}")
    kpi2.metric("Total Balance", f"${total_balance:,.2f}")
    kpi3.metric("Total Loan Amount", f"${total_loan:,.2f}")

    segment_dist = (
        filtered.groupby("segment", as_index=False)
        .agg(customer_count=("customer_id", "nunique"))
        .sort_values("customer_count", ascending=False)
    )

    state_dist = (
        filtered.groupby("state", as_index=False)
        .agg(customer_count=("customer_id", "nunique"))
        .sort_values("customer_count", ascending=False)
        .head(15)
    )

    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Customers by Segment")
        fig_seg = px.pie(
            segment_dist,
            names="segment",
            values="customer_count",
            hole=0.45,
        )
        fig_seg.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_seg, use_container_width=True)

    with c2:
        st.subheader("Top States by Customers")
        fig_state = px.bar(
            state_dist,
            x="state",
            y="customer_count",
            color="customer_count",
            color_continuous_scale="Blues",
        )
        fig_state.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_state, use_container_width=True)

    with st.expander("View data sample"):
        visible_cols = [
            "customer_id",
            "segment",
            "state",
            "account_count",
            "loan_count",
            "total_balance",
            "total_loan_amount",
        ]
        st.dataframe(filtered[visible_cols].head(100), use_container_width=True)


if __name__ == "__main__":
    main()
