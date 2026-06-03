from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, round

spark = SparkSession.builder \
    .appName("EcommerceBatchAnalysis") \
    .getOrCreate()

df = spark.read.csv(
    "data/sample_data.csv",
    header=True,
    inferSchema=True
)

print("=== Dataset Schema ===")
df.printSchema()

print("=== Event Type Count ===")
df.groupBy("event_type") \
    .agg(count("*").alias("total_events")) \
    .orderBy(col("total_events").desc()) \
    .show(truncate=False)

purchase_df = df.filter(col("event_type") == "purchase")

print("=== Top 10 Categories by Purchase Revenue ===")
purchase_df.groupBy("category_code") \
    .agg(
        round(spark_sum("price"), 2).alias("total_revenue"),
        count("*").alias("purchase_count")
    ) \
    .orderBy(col("total_revenue").desc()) \
    .show(10, truncate=False)

print("=== Top 10 Brands by Purchase Revenue ===")
purchase_df.groupBy("brand") \
    .agg(
        round(spark_sum("price"), 2).alias("total_revenue"),
        count("*").alias("purchase_count")
    ) \
    .orderBy(col("total_revenue").desc()) \
    .show(10, truncate=False)

print("=== Most Viewed Categories ===")
df.filter(col("event_type") == "view") \
    .groupBy("category_code") \
    .agg(count("*").alias("view_count")) \
    .orderBy(col("view_count").desc()) \
    .show(10, truncate=False)

total_views = df.filter(col("event_type") == "view").count()
total_purchases = df.filter(col("event_type") == "purchase").count()

conversion_rate = 0
if total_views > 0:
    conversion_rate = (total_purchases / total_views) * 100

print("=== Conversion Rate ===")
print(f"Total Views: {total_views}")
print(f"Total Purchases: {total_purchases}")
print(f"Conversion Rate: {conversion_rate:.2f}%")

spark.stop()