import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# =========================
# Page Configuration
# =========================
st.set_page_config(
    page_title="E-Commerce Batch Analytics Dashboard",
    layout="wide"
)

# =========================
# File Paths
# =========================
DASHBOARD_DIR = "output/dashboard"

EVENT_TYPE_PATH = f"{DASHBOARD_DIR}/event_type_count_table.csv"
CATEGORY_REVENUE_PATH = f"{DASHBOARD_DIR}/category_revenue_table.csv"
BRAND_REVENUE_PATH = f"{DASHBOARD_DIR}/brand_revenue_table.csv"
MOST_VIEWED_CATEGORY_PATH = f"{DASHBOARD_DIR}/most_viewed_category_table.csv"
CONVERSION_RATE_PATH = f"{DASHBOARD_DIR}/conversion_rate_table.csv"
FUNNEL_SUMMARY_PATH = f"{DASHBOARD_DIR}/funnel_summary_table.csv"


# =========================
# Color Configuration
# =========================
EVENT_COLOR_MAP = {
    "view": "#4A90E2",
    "cart": "#F5A623",
    "purchase": "#2ECC71"
}

CHART_COLORS = {
    "category_revenue": "#4A90E2",
    "brand_revenue": "#F5A623",
    "most_viewed": "#9B59B6"
}


# =========================
# Helper Functions
# =========================
def load_csv(path):
    if not os.path.exists(path):
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def get_event_count(df, event_type):
    if df.empty:
        return 0

    if "event_type" not in df.columns:
        return 0

    if event_type not in df["event_type"].values:
        return 0

    return int(df[df["event_type"] == event_type]["event_count"].iloc[0])


def format_number(value):
    return f"{int(value):,}"


def format_currency(value):
    return f"${float(value):,.2f}"


def format_million_currency(value):
    return f"${float(value) / 1_000_000:.1f}M"


def add_rank_column(df, rank_column_name="No"):
    display_df = df.reset_index(drop=True).copy()
    display_df.insert(0, rank_column_name, display_df.index + 1)
    return display_df


# =========================
# Load Processed Tables
# =========================
event_type_df = load_csv(EVENT_TYPE_PATH)
category_revenue_df = load_csv(CATEGORY_REVENUE_PATH)
brand_revenue_df = load_csv(BRAND_REVENUE_PATH)
most_viewed_category_df = load_csv(MOST_VIEWED_CATEGORY_PATH)
conversion_rate_df = load_csv(CONVERSION_RATE_PATH)
funnel_summary_df = load_csv(FUNNEL_SUMMARY_PATH)


# =========================
# Dashboard Header
# =========================
st.title("E-Commerce Batch Analytics Dashboard")
st.caption(
    "Historical customer behavior analysis using HDFS, Spark SQL Batch Processing, and Streamlit."
)


# =========================
# Validate Required Tables
# =========================
required_tables = {
    "Event Type Count Table": event_type_df,
    "Category Revenue Table": category_revenue_df,
    "Brand Revenue Table": brand_revenue_df,
    "Most Viewed Category Table": most_viewed_category_df,
    "Conversion Rate Table": conversion_rate_df,
    "Funnel Summary Table": funnel_summary_df,
}

missing_tables = [
    table_name for table_name, df in required_tables.items()
    if df.empty
]

if missing_tables:
    st.warning("Some processed tables are missing.")
    st.write("Please run the Spark SQL batch job first.")

    st.code(
        "docker exec -it bdp-spark /opt/spark/bin/spark-submit /app/jobs/batch_hdfs_sql_analysis.py",
        language="bash"
    )

    st.write("Missing tables:")
    for table in missing_tables:
        st.write(f"- {table}")

else:
    # =========================
    # Overview
    # =========================
    st.header("Overview")

    total_views = int(conversion_rate_df["total_views"].iloc[0])
    total_purchases = int(conversion_rate_df["total_purchases"].iloc[0])
    conversion_rate = float(conversion_rate_df["conversion_rate_percentage"].iloc[0])

    total_cart = get_event_count(funnel_summary_df, "cart")

    top_category = category_revenue_df.iloc[0]
    top_brand = brand_revenue_df.iloc[0]
    most_viewed_category = most_viewed_category_df.iloc[0]

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Views", format_number(total_views))
    col2.metric("Cart Additions", format_number(total_cart))
    col3.metric("Total Purchases", format_number(total_purchases))
    col4.metric("Conversion Rate", f"{conversion_rate:.2f}%")

    st.divider()

    col5, col6, col7 = st.columns(3)

    col5.metric(
        "Top Revenue Category",
        str(top_category["category_code"]),
        format_currency(top_category["total_revenue"])
    )

    col6.metric(
        "Top Revenue Brand",
        str(top_brand["brand"]),
        format_currency(top_brand["total_revenue"])
    )

    col7.metric(
        "Most Viewed Category",
        str(most_viewed_category["category_code"]),
        f"{format_number(most_viewed_category['view_count'])} views"
    )

    st.divider()

    # =========================
    # Customer Behavior Funnel
    # =========================
    st.header("Customer Behavior Funnel")

    funnel_order = ["view", "cart", "purchase"]

    funnel_chart_df = funnel_summary_df.copy()

    funnel_chart_df["event_type"] = pd.Categorical(
        funnel_chart_df["event_type"],
        categories=funnel_order,
        ordered=True
    )

    funnel_chart_df = funnel_chart_df.sort_values("event_type")
    funnel_chart_df = funnel_chart_df.set_index("event_type").loc[
        funnel_order
    ].reset_index()

    view_count = get_event_count(funnel_chart_df, "view")
    cart_count = get_event_count(funnel_chart_df, "cart")
    purchase_count = get_event_count(funnel_chart_df, "purchase")

    view_to_cart_rate = 0
    cart_to_purchase_rate = 0
    view_to_purchase_rate = 0

    if view_count > 0:
        view_to_cart_rate = (cart_count / view_count) * 100
        view_to_purchase_rate = (purchase_count / view_count) * 100

    if cart_count > 0:
        cart_to_purchase_rate = (purchase_count / cart_count) * 100

    funnel_col1, funnel_col2, funnel_col3 = st.columns(3)

    funnel_col1.metric(
        "View-to-Cart Rate",
        f"{view_to_cart_rate:.2f}%"
    )

    funnel_col2.metric(
        "Cart-to-Purchase Rate",
        f"{cart_to_purchase_rate:.2f}%"
    )

    funnel_col3.metric(
        "View-to-Purchase Rate",
        f"{view_to_purchase_rate:.2f}%"
    )

    funnel_fig = go.Figure(
        go.Funnel(
            y=["View", "Cart", "Purchase"],
            x=[
                view_count,
                cart_count,
                purchase_count
            ],
            textinfo="value+percent initial",
            marker={
                "color": [
                    EVENT_COLOR_MAP["view"],
                    EVENT_COLOR_MAP["cart"],
                    EVENT_COLOR_MAP["purchase"]
                ]
            }
        )
    )

    funnel_fig.update_layout(
        title="Customer Journey Funnel: View → Cart → Purchase",
        xaxis_title="Event Count",
        yaxis_title="Customer Journey Stage",
        height=500
    )

    st.plotly_chart(
        funnel_fig,
        use_container_width=True
    )

    st.dataframe(
        add_rank_column(funnel_chart_df, "No"),
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # =========================
    # Revenue Analysis
    # =========================
    st.header("Revenue Analysis")

    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Top Product Categories by Purchase Revenue")

        category_chart_df = category_revenue_df.head(10).copy()

        category_chart_df["revenue_label"] = category_chart_df["total_revenue"].apply(
            format_million_currency
        )

        category_chart_df = category_chart_df.sort_values(
            "total_revenue",
            ascending=True
        )

        category_fig = px.bar(
            category_chart_df,
            x="total_revenue",
            y="category_code",
            orientation="h",
            text="revenue_label",
            title="Top 10 Categories by Purchase Revenue"
        )

        category_fig.update_traces(
            marker_color=CHART_COLORS["category_revenue"],
            textposition="outside",
            cliponaxis=False
        )

        category_fig.update_layout(
            xaxis_title="Total Revenue",
            yaxis_title="Product Category",
            height=520,
            margin=dict(l=180, r=100, t=80, b=60)
        )

        st.plotly_chart(
            category_fig,
            use_container_width=True
        )

        st.dataframe(
            add_rank_column(category_revenue_df, "Rank"),
            use_container_width=True,
            hide_index=True
        )

    with right_col:
        st.subheader("Top Brands by Purchase Revenue")

        brand_chart_df = brand_revenue_df.head(10).copy()

        brand_chart_df["revenue_label"] = brand_chart_df["total_revenue"].apply(
            format_million_currency
        )

        brand_chart_df = brand_chart_df.sort_values(
            "total_revenue",
            ascending=True
        )

        brand_fig = px.bar(
            brand_chart_df,
            x="total_revenue",
            y="brand",
            orientation="h",
            text="revenue_label",
            title="Top 10 Brands by Purchase Revenue"
        )

        brand_fig.update_traces(
            marker_color=CHART_COLORS["brand_revenue"],
            textposition="outside",
            cliponaxis=False
        )

        brand_fig.update_layout(
            xaxis_title="Total Revenue",
            yaxis_title="Brand",
            height=520,
            margin=dict(l=120, r=100, t=80, b=60)
        )

        st.plotly_chart(
            brand_fig,
            use_container_width=True
        )

        st.dataframe(
            add_rank_column(brand_revenue_df, "Rank"),
            use_container_width=True,
            hide_index=True
        )

    st.divider()

    # =========================
    # Browsing Behavior
    # =========================
    st.header("Browsing Behavior")

    st.subheader("Most Viewed Product Categories")

    viewed_chart_df = most_viewed_category_df.head(10).copy()

    viewed_chart_df["view_label"] = viewed_chart_df["view_count"].apply(
        lambda x: f"{int(x) / 1_000_000:.1f}M"
    )

    viewed_chart_df = viewed_chart_df.sort_values(
        "view_count",
        ascending=True
    )

    viewed_fig = px.bar(
        viewed_chart_df,
        x="view_count",
        y="category_code",
        orientation="h",
        text="view_label",
        title="Top 10 Most Viewed Product Categories"
    )

    viewed_fig.update_traces(
        marker_color=CHART_COLORS["most_viewed"],
        textposition="outside",
        cliponaxis=False
    )

    viewed_fig.update_layout(
        xaxis_title="View Count",
        yaxis_title="Product Category",
        height=520,
        margin=dict(l=180, r=100, t=80, b=60)
    )

    st.plotly_chart(
        viewed_fig,
        use_container_width=True
    )

    st.dataframe(
        add_rank_column(most_viewed_category_df, "Rank"),
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # =========================
    # Event Type Distribution
    # =========================
    st.header("Event Type Distribution")

    event_chart_df = event_type_df.copy()

    total_events_sum = event_chart_df["total_events"].sum()

    event_chart_df["percentage"] = (
        event_chart_df["total_events"] / total_events_sum * 100
    ).round(2)

    event_order = ["view", "cart", "purchase"]

    event_chart_df["event_type"] = pd.Categorical(
        event_chart_df["event_type"],
        categories=event_order,
        ordered=True
    )

    event_chart_df = event_chart_df.sort_values("event_type")

    event_fig = px.bar(
        event_chart_df,
        x="total_events",
        y="event_type",
        color="event_type",
        color_discrete_map=EVENT_COLOR_MAP,
        text="total_events",
        orientation="h",
        title="Event Type Distribution by Total Events"
    )

    event_fig.update_traces(
        texttemplate="%{x:,}",
        textposition="outside",
        cliponaxis=False
    )

    event_fig.update_layout(
        xaxis_title="Total Events (Log Scale)",
        yaxis_title="Event Type",
        xaxis_type="log",
        showlegend=False,
        height=420,
        margin=dict(l=120, r=100, t=80, b=60)
    )

    st.plotly_chart(
        event_fig,
        use_container_width=True
    )

    event_display_df = event_chart_df.copy()

    event_display_df["total_events"] = event_display_df["total_events"].apply(
        lambda x: f"{int(x):,}"
    )

    event_display_df["percentage"] = event_display_df["percentage"].apply(
        lambda x: f"{x:.2f}%"
    )

    st.dataframe(
        add_rank_column(event_display_df, "No"),
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # =========================
    # Findings Summary
    # =========================
    st.header("Findings Summary")

    st.write(
        f"The product category with the highest purchase revenue is "
        f"**{top_category['category_code']}**, generating "
        f"**{format_currency(top_category['total_revenue'])}** from "
        f"**{format_number(top_category['purchase_count'])} purchases**."
    )

    st.write(
        f"The brand with the highest purchase revenue is "
        f"**{top_brand['brand']}**, generating "
        f"**{format_currency(top_brand['total_revenue'])}** from "
        f"**{format_number(top_brand['purchase_count'])} purchases**."
    )

    st.write(
        f"The most viewed product category is "
        f"**{most_viewed_category['category_code']}**, with "
        f"**{format_number(most_viewed_category['view_count'])} views**."
    )

    st.write(
        f"The overall conversion rate from product views to purchases is "
        f"**{conversion_rate:.2f}%**, based on "
        f"**{format_number(total_views)} views** and "
        f"**{format_number(total_purchases)} purchases**."
    )

    st.write(
        f"The customer funnel shows a View-to-Cart Rate of "
        f"**{view_to_cart_rate:.2f}%**, a Cart-to-Purchase Rate of "
        f"**{cart_to_purchase_rate:.2f}%**, and a View-to-Purchase Rate of "
        f"**{view_to_purchase_rate:.2f}%**."
    )

    st.write(
        "Overall, the data shows that customer activity is dominated by product views. "
        "Only a small portion of browsing activity progresses into cart additions and purchases, "
        "which indicates a high browsing volume but a relatively low purchase conversion rate."
    )