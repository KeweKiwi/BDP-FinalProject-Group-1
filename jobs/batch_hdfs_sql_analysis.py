from pyspark.sql import SparkSession
import os
import shutil

spark = SparkSession.builder \
    .appName("EcommerceHDFSSQLBatchAnalysis") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_INPUT_PATH = "hdfs://namenode:9000/user/bdp/raw/2019-Oct.csv"
HDFS_PROCESSED_BASE = "hdfs://namenode:9000/user/bdp/processed"

LOCAL_DASHBOARD_DIR = "output/dashboard"

os.makedirs(LOCAL_DASHBOARD_DIR, exist_ok=True)


def clean_local_path(path):
    if os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)


def export_local_single_csv(spark_df, temp_dir, final_csv_path):
    clean_local_path(temp_dir)
    clean_local_path(final_csv_path)

    spark_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(temp_dir)

    part_file = None

    for file in os.listdir(temp_dir):
        if file.startswith("part-") and file.endswith(".csv"):
            part_file = file
            break

    if part_file is None:
        raise FileNotFoundError(f"No part CSV found in {temp_dir}")

    shutil.move(os.path.join(temp_dir, part_file), final_csv_path)
    shutil.rmtree(temp_dir)


def save_table(table_df, table_name):
    hdfs_table_path = f"{HDFS_PROCESSED_BASE}/{table_name}"
    local_temp_dir = f"{LOCAL_DASHBOARD_DIR}/{table_name}_temp"
    local_final_csv = f"{LOCAL_DASHBOARD_DIR}/{table_name}.csv"

    table_df.write \
        .mode("overwrite") \
        .parquet(hdfs_table_path)

    export_local_single_csv(
        table_df,
        local_temp_dir,
        local_final_csv
    )

    print(f"Saved HDFS processed table: {hdfs_table_path}")
    print(f"Saved local dashboard CSV: {local_final_csv}")


df = spark.read.csv(
    HDFS_INPUT_PATH,
    header=True,
    inferSchema=True
)

df.createOrReplaceTempView("ecommerce_events")

print("=== Dataset Schema ===")
df.printSchema()


print("=== Event Type Count Table ===")
event_type_count_df = spark.sql("""
    SELECT
        event_type,
        COUNT(*) AS total_events
    FROM ecommerce_events
    GROUP BY event_type
    ORDER BY total_events DESC
""")

event_type_count_df.show(truncate=False)
save_table(event_type_count_df, "event_type_count_table")


# Keep raw category_code for traceability, and add product_name for dashboard labels.
print("=== Query 1: Product Revenue Table ===")
category_revenue_df = spark.sql("""
    WITH purchase_events AS (
        SELECT
            category_code,
            INITCAP(REPLACE(REGEXP_EXTRACT(category_code, '[^.]+$', 0), '_', ' ')) AS product_name,
            price
        FROM ecommerce_events
        WHERE event_type = 'purchase'
          AND category_code IS NOT NULL
    )
    SELECT
        category_code,
        product_name,
        ROUND(SUM(price), 2) AS total_revenue,
        COUNT(*) AS purchase_count,
        ROUND(AVG(price), 2) AS average_purchase_price
    FROM purchase_events
    GROUP BY category_code, product_name
    ORDER BY total_revenue DESC
    LIMIT 20
""")

category_revenue_df.show(20, truncate=False)
save_table(category_revenue_df, "category_revenue_table")


print("=== Query 2: Brand Revenue Table ===")
brand_revenue_df = spark.sql("""
    SELECT
        brand,
        ROUND(SUM(price), 2) AS total_revenue,
        COUNT(*) AS purchase_count,
        ROUND(AVG(price), 2) AS average_purchase_price
    FROM ecommerce_events
    WHERE event_type = 'purchase'
      AND brand IS NOT NULL
    GROUP BY brand
    ORDER BY total_revenue DESC
    LIMIT 20
""")

brand_revenue_df.show(20, truncate=False)
save_table(brand_revenue_df, "brand_revenue_table")


print("=== Query 3: Most Viewed Product Table ===")
most_viewed_category_df = spark.sql("""
    WITH view_events AS (
        SELECT
            category_code,
            INITCAP(REPLACE(REGEXP_EXTRACT(category_code, '[^.]+$', 0), '_', ' ')) AS product_name
        FROM ecommerce_events
        WHERE event_type = 'view'
          AND category_code IS NOT NULL
    )
    SELECT
        category_code,
        product_name,
        COUNT(*) AS view_count
    FROM view_events
    GROUP BY category_code, product_name
    ORDER BY view_count DESC
    LIMIT 20
""")

most_viewed_category_df.show(20, truncate=False)
save_table(most_viewed_category_df, "most_viewed_category_table")


print("=== Query 4: Conversion Rate Table ===")
conversion_rate_df = spark.sql("""
    SELECT
        SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS total_views,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
        ROUND(
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)
            /
            SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END)
            * 100,
            2
        ) AS conversion_rate_percentage
    FROM ecommerce_events
""")

conversion_rate_df.show(truncate=False)
save_table(conversion_rate_df, "conversion_rate_table")


print("=== Query 5: Funnel Summary Table ===")
funnel_summary_df = spark.sql("""
    SELECT
        event_type,
        COUNT(*) AS event_count
    FROM ecommerce_events
    WHERE event_type IN ('view', 'cart', 'purchase')
    GROUP BY event_type
    ORDER BY
        CASE event_type
            WHEN 'view' THEN 1
            WHEN 'cart' THEN 2
            WHEN 'purchase' THEN 3
            ELSE 4
        END
""")

funnel_summary_df.show(truncate=False)
save_table(funnel_summary_df, "funnel_summary_table")


print("=== Batch SQL Processing Completed ===")
print("Processed tables saved to HDFS under:")
print("/user/bdp/processed")

print("Local dashboard CSV files saved to:")
print(LOCAL_DASHBOARD_DIR)

spark.stop()
