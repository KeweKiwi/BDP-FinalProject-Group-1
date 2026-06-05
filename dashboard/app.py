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
    page_icon=":bar_chart:",
    layout="wide"
)

# =========================
# File Paths
# =========================
DASHBOARD_DIR = "output/dashboard"

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

EVENT_LABEL_MAP = {
    "view": "Product Views",
    "cart": "Cart Additions",
    "purchase": "Purchases"
}

BRAND_LABEL_OVERRIDES = {
    "lg": "LG",
    "hp": "HP",
    "oppo": "OPPO",
    "asus": "ASUS",
    "msi": "MSI"
}

CHART_COLORS = {
    "category_revenue": "#4A90E2",
    "brand_revenue": "#F5A623",
    "most_viewed": "#9B59B6",
    "product_value": "#00A6A6",
    "brand_value": "#EF476F"
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


def render_metric_card(label, value, delta=None):
    with st.container(border=True):
        st.metric(label, value, delta)


def apply_chart_layout(fig, height=520, left_margin=140, right_margin=90):
    fig.update_layout(
        height=height,
        margin=dict(l=left_margin, r=right_margin, t=80, b=60),
        bargap=0.28,
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title=dict(x=0, xanchor="left"),
        hoverlabel=dict(font_size=13)
    )


def format_label_token(value):
    return str(value).replace("_", " ").replace("-", " ").strip().title()


def format_category_label(category_code):
    if pd.isna(category_code):
        return "Unknown Product"

    parts = [
        part.strip()
        for part in str(category_code).split(".")
        if part.strip()
    ]

    if not parts:
        return "Unknown Product"

    return format_label_token(parts[-1])


def format_brand_label(brand):
    if pd.isna(brand):
        return "Unknown Brand"

    normalized_brand = str(brand).strip()

    if not normalized_brand:
        return "Unknown Brand"

    brand_key = normalized_brand.lower()

    if brand_key in BRAND_LABEL_OVERRIDES:
        return BRAND_LABEL_OVERRIDES[brand_key]

    return format_label_token(normalized_brand)


def format_event_type_label(event_type):
    event_key = str(event_type).strip().lower()
    return EVENT_LABEL_MAP.get(event_key, format_label_token(event_key))


def build_revenue_display_table(df, source_column, display_column, label_formatter):
    return pd.DataFrame({
        display_column: df[source_column].apply(label_formatter),
        "Total Revenue": df["total_revenue"].apply(format_currency),
        "Purchases": df["purchase_count"].apply(format_number),
        "Average Purchase Price": df["average_purchase_price"].apply(format_currency)
    })


def build_viewed_category_display_table(df):
    return pd.DataFrame({
        "Product": df["category_code"].apply(format_category_label),
        "Views": df["view_count"].apply(format_number)
    })


def build_purchase_value_display_table(
    df,
    source_column,
    display_column,
    label_formatter
):
    return pd.DataFrame({
        display_column: df[source_column].apply(label_formatter),
        "Average Purchase Price": df["average_purchase_price"].apply(format_currency),
        "Purchases": df["purchase_count"].apply(format_number),
        "Total Revenue": df["total_revenue"].apply(format_currency)
    })


def build_funnel_display_table(df):
    return pd.DataFrame({
        "Journey Stage": df["event_type"].apply(format_event_type_label),
        "Event Count": df["event_count"].apply(format_number)
    })


def add_rank_column(df, rank_column_name="No"):
    display_df = df.reset_index(drop=True).copy()
    display_df.insert(0, rank_column_name, display_df.index + 1)
    return display_df


# =========================
# Load Processed Tables
# =========================
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
    "Product Revenue Table": category_revenue_df,
    "Brand Revenue Table": brand_revenue_df,
    "Most Viewed Product Table": most_viewed_category_df,
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

    top_category_label = format_category_label(top_category["category_code"])
    top_brand_label = format_brand_label(top_brand["brand"])
    most_viewed_category_label = format_category_label(
        most_viewed_category["category_code"]
    )

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        render_metric_card("Total Views", format_number(total_views))

    with col2:
        render_metric_card("Cart Additions", format_number(total_cart))

    with col3:
        render_metric_card("Total Purchases", format_number(total_purchases))

    with col4:
        render_metric_card("Conversion Rate", f"{conversion_rate:.2f}%")

    st.divider()

    col5, col6, col7 = st.columns(3)

    with col5:
        render_metric_card(
            "Top Revenue Product",
            top_category_label,
            format_currency(top_category["total_revenue"])
        )

    with col6:
        render_metric_card(
            "Top Revenue Brand",
            top_brand_label,
            format_currency(top_brand["total_revenue"])
        )

    with col7:
        render_metric_card(
            "Most Viewed Product",
            most_viewed_category_label,
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

    with funnel_col1:
        render_metric_card(
            "View-to-Cart Rate",
            f"{view_to_cart_rate:.2f}%"
        )

    with funnel_col2:
        render_metric_card(
            "Cart-to-Purchase Rate",
            f"{cart_to_purchase_rate:.2f}%"
        )

    with funnel_col3:
        render_metric_card(
            "View-to-Purchase Rate",
            f"{view_to_purchase_rate:.2f}%"
        )

    funnel_stage_labels = [
        format_event_type_label(event_type)
        for event_type in funnel_order
    ]

    funnel_fig = go.Figure(
        go.Funnel(
            y=funnel_stage_labels,
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

    funnel_fig.update_traces(
        hovertemplate="<b>%{y}</b><br>Events: %{x:,}<extra></extra>"
    )

    apply_chart_layout(funnel_fig, height=500, left_margin=150)

    funnel_fig.update_layout(
        title="Customer Journey Funnel: View → Cart → Purchase",
        xaxis_title="Event Count",
        yaxis_title="Customer Journey Stage"
    )

    st.plotly_chart(
        funnel_fig,
        width="stretch"
    )

    funnel_display_df = build_funnel_display_table(funnel_chart_df)

    st.dataframe(
        add_rank_column(funnel_display_df, "No"),
        width="stretch",
        hide_index=True
    )

    st.divider()

    # =========================
    # Revenue Analysis
    # =========================
    st.header("Revenue Analysis")

    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Top Products by Purchase Revenue")

        category_chart_df = category_revenue_df.head(10).copy()

        category_chart_df["category_label"] = category_chart_df[
            "category_code"
        ].apply(format_category_label)

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
            y="category_label",
            orientation="h",
            text="revenue_label",
            title="Top 10 Products by Purchase Revenue",
            labels={
                "total_revenue": "Total Revenue",
                "category_label": "Product"
            }
        )

        category_fig.update_traces(
            marker_color=CHART_COLORS["category_revenue"],
            textposition="outside",
            cliponaxis=False,
            hovertemplate="<b>%{y}</b><br>Total Revenue: %{x:$,.2f}<extra></extra>"
        )

        apply_chart_layout(category_fig, height=520, left_margin=180)

        category_fig.update_layout(
            xaxis_title="Total Revenue",
            yaxis_title="Product"
        )

        category_fig.update_xaxes(tickprefix="$", separatethousands=True)

        st.plotly_chart(
            category_fig,
            width="stretch"
        )

        category_display_df = build_revenue_display_table(
            category_revenue_df,
            "category_code",
            "Product",
            format_category_label
        )

        st.dataframe(
            add_rank_column(category_display_df, "Rank"),
            width="stretch",
            hide_index=True
        )

    with right_col:
        st.subheader("Top Brands by Purchase Revenue")

        brand_chart_df = brand_revenue_df.head(10).copy()

        brand_chart_df["brand_label"] = brand_chart_df["brand"].apply(
            format_brand_label
        )

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
            y="brand_label",
            orientation="h",
            text="revenue_label",
            title="Top 10 Brands by Purchase Revenue",
            labels={
                "total_revenue": "Total Revenue",
                "brand_label": "Brand"
            }
        )

        brand_fig.update_traces(
            marker_color=CHART_COLORS["brand_revenue"],
            textposition="outside",
            cliponaxis=False,
            hovertemplate="<b>%{y}</b><br>Total Revenue: %{x:$,.2f}<extra></extra>"
        )

        apply_chart_layout(brand_fig, height=520, left_margin=120)

        brand_fig.update_layout(
            xaxis_title="Total Revenue",
            yaxis_title="Brand"
        )

        brand_fig.update_xaxes(tickprefix="$", separatethousands=True)

        st.plotly_chart(
            brand_fig,
            width="stretch"
        )

        brand_display_df = build_revenue_display_table(
            brand_revenue_df,
            "brand",
            "Brand",
            format_brand_label
        )

        st.dataframe(
            add_rank_column(brand_display_df, "Rank"),
            width="stretch",
            hide_index=True
        )

    st.divider()

    # =========================
    # Browsing Behavior
    # =========================
    st.header("Browsing Behavior")

    st.subheader("Most Viewed Products")

    viewed_chart_df = most_viewed_category_df.head(10).copy()

    viewed_chart_df["category_label"] = viewed_chart_df[
        "category_code"
    ].apply(format_category_label)

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
        y="category_label",
        orientation="h",
        text="view_label",
        title="Top 10 Most Viewed Products",
        labels={
            "view_count": "View Count",
            "category_label": "Product"
        }
    )

    viewed_fig.update_traces(
        marker_color=CHART_COLORS["most_viewed"],
        textposition="outside",
        cliponaxis=False,
        hovertemplate="<b>%{y}</b><br>Views: %{x:,}<extra></extra>"
    )

    apply_chart_layout(viewed_fig, height=520, left_margin=180)

    viewed_fig.update_layout(
        xaxis_title="View Count",
        yaxis_title="Product"
    )

    viewed_fig.update_xaxes(separatethousands=True)

    st.plotly_chart(
        viewed_fig,
        width="stretch"
    )

    viewed_display_df = build_viewed_category_display_table(
        most_viewed_category_df
    )

    st.dataframe(
        add_rank_column(viewed_display_df, "Rank"),
        width="stretch",
        hide_index=True
    )

    st.divider()

    # =========================
    # Purchase Value Analysis
    # =========================
    st.header("Purchase Value Analysis")

    value_col1, value_col2 = st.columns(2)

    with value_col1:
        st.subheader("Average Purchase Price by Product")

        product_value_df = category_revenue_df.copy()

        product_value_df["product_label"] = product_value_df[
            "category_code"
        ].apply(format_category_label)

        product_value_df["price_label"] = product_value_df[
            "average_purchase_price"
        ].apply(format_currency)

        product_value_chart_df = product_value_df.sort_values(
            "average_purchase_price",
            ascending=False
        ).head(10).sort_values(
            "average_purchase_price",
            ascending=True
        )

        product_value_fig = px.bar(
            product_value_chart_df,
            x="average_purchase_price",
            y="product_label",
            orientation="h",
            text="price_label",
            title="Average Purchase Price of Top Revenue Products",
            labels={
                "average_purchase_price": "Average Purchase Price",
                "product_label": "Product"
            }
        )

        product_value_fig.update_traces(
            marker_color=CHART_COLORS["product_value"],
            textposition="outside",
            cliponaxis=False,
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Average Purchase Price: %{x:$,.2f}<extra></extra>"
            )
        )

        apply_chart_layout(product_value_fig, height=520, left_margin=150)

        product_value_fig.update_layout(
            xaxis_title="Average Purchase Price",
            yaxis_title="Product"
        )

        product_value_fig.update_xaxes(tickprefix="$", separatethousands=True)

        st.plotly_chart(
            product_value_fig,
            width="stretch"
        )

        product_value_display_df = build_purchase_value_display_table(
            product_value_df.sort_values(
                "average_purchase_price",
                ascending=False
            ),
            "category_code",
            "Product",
            format_category_label
        )

        st.dataframe(
            add_rank_column(product_value_display_df, "Rank"),
            width="stretch",
            hide_index=True
        )

    with value_col2:
        st.subheader("Average Purchase Price by Brand")

        brand_value_df = brand_revenue_df.copy()

        brand_value_df["brand_label"] = brand_value_df["brand"].apply(
            format_brand_label
        )

        brand_value_df["price_label"] = brand_value_df[
            "average_purchase_price"
        ].apply(format_currency)

        brand_value_chart_df = brand_value_df.sort_values(
            "average_purchase_price",
            ascending=False
        ).head(10).sort_values(
            "average_purchase_price",
            ascending=True
        )

        brand_value_fig = px.bar(
            brand_value_chart_df,
            x="average_purchase_price",
            y="brand_label",
            orientation="h",
            text="price_label",
            title="Average Purchase Price of Top Revenue Brands",
            labels={
                "average_purchase_price": "Average Purchase Price",
                "brand_label": "Brand"
            }
        )

        brand_value_fig.update_traces(
            marker_color=CHART_COLORS["brand_value"],
            textposition="outside",
            cliponaxis=False,
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Average Purchase Price: %{x:$,.2f}<extra></extra>"
            )
        )

        apply_chart_layout(brand_value_fig, height=520, left_margin=120)

        brand_value_fig.update_layout(
            xaxis_title="Average Purchase Price",
            yaxis_title="Brand"
        )

        brand_value_fig.update_xaxes(tickprefix="$", separatethousands=True)

        st.plotly_chart(
            brand_value_fig,
            width="stretch"
        )

        brand_value_display_df = build_purchase_value_display_table(
            brand_value_df.sort_values(
                "average_purchase_price",
                ascending=False
            ),
            "brand",
            "Brand",
            format_brand_label
        )

        st.dataframe(
            add_rank_column(brand_value_display_df, "Rank"),
            width="stretch",
            hide_index=True
        )

    st.divider()

    # =========================
    # Findings Summary
    # =========================
    st.header("Findings Summary")

    st.write(
        f"The product with the highest purchase revenue is "
        f"**{top_category_label}**, generating "
        f"**{format_currency(top_category['total_revenue'])}** from "
        f"**{format_number(top_category['purchase_count'])} purchases**."
    )

    st.write(
        f"The brand with the highest purchase revenue is "
        f"**{top_brand_label}**, generating "
        f"**{format_currency(top_brand['total_revenue'])}** from "
        f"**{format_number(top_brand['purchase_count'])} purchases**."
    )

    st.write(
        f"The most viewed product is "
        f"**{most_viewed_category_label}**, with "
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
