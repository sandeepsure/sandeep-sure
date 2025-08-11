from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, year, month, dayofmonth, when, count, rank
from pyspark.sql.window import Window
import time

# Step 1: Create SparkSession
spark = SparkSession.builder \
    .appName("E-Commerce Data Pipeline") \
    .master("local[*]") \
    .getOrCreate()

# Step 2: Load CSVs
ecom_df = spark.read.option("header", True).option(
    "inferSchema", True).csv("ecommerce_transactions.csv")
region_df = spark.read.option("header", True).option(
    "inferSchema", True).csv("country_region.csv")

# Step 3: Clean Data
ecom_clean = ecom_df.dropDuplicates() \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("quantity") > 0)

# Step 4: Enrich Data
ecom_enriched = ecom_clean.withColumn("order_value", expr("quantity * unit_price")) \
    .withColumn("year", year("order_date")) \
    .withColumn("month", month("order_date")) \
    .withColumn("day", dayofmonth("order_date"))
ecom_enriched.show()  # 🔹 Triggers DAG

# Step 5: Country Revenue Summary
country_revenue = ecom_enriched.groupBy("country") \
    .sum("order_value") \
    .withColumnRenamed("sum(order_value)", "total_revenue")
country_revenue.show()  # 🔹 Triggers DAG

# Step 6: Top Customer per Country
window_spec = Window.partitionBy("country").orderBy(col("order_value").desc())
ranked_df = ecom_enriched.withColumn("rank", rank().over(window_spec))
ranked_df.show()  # 🔹 Triggers DAG
top_customer = ranked_df.filter(col("rank") == 1).select(
    "country", "customer_id", "order_value")
top_customer.show()  # 🔹 Triggers DAG

# Step 7: Region Join & Summary
region_joined = ecom_enriched.join(region_df, on="country", how="left")
region_summary = region_joined.groupBy("region") \
    .sum("order_value") \
    .withColumnRenamed("sum(order_value)", "region_revenue")
region_summary.show()  # 🔹 Triggers DAG

# Step 8: Monthly Category Pivot
monthly_pivot = ecom_enriched.groupBy("category", "month") \
    .sum("order_value") \
    .groupBy("category") \
    .pivot("month") \
    .sum("sum(order_value)")
monthly_pivot.show()  # 🔹 Triggers DAG

# Step 9: Price Band Counts
price_bands = ecom_enriched.withColumn("price_band", when(col("order_value") < 50, "Low")
                                       .when(col("order_value") < 200, "Medium")
                                       .otherwise("High")) \
                           .groupBy("price_band").agg(count("*").alias("count"))
price_bands.show()  # 🔹 Triggers DAG

# Step 10: Partition Tuning Check
print("Initial partitions:", ecom_enriched.rdd.getNumPartitions())
ecom_repart = ecom_enriched.repartition(4)
print("Repartitioned to 4 partitions:", ecom_repart.rdd.getNumPartitions())

# Step 11: Cache vs. Recompute
start = time.time()
ecom_enriched.groupBy("customer_id").sum("order_value").collect()
print("Without cache:", time.time() - start)

ecom_enriched.cache()
start = time.time()
ecom_enriched.groupBy("customer_id").sum("order_value").collect()
print("With cache:", time.time() - start)

# Step 12: Write Output(Parquet + CSV)
country_revenue.write.mode("overwrite").parquet(
    "gold_parquet/country_revenue", compression="snappy")
country_revenue.write.mode("overwrite").option(
    "header", True).csv("gold_csv/country_revenue")

region_summary.write.mode("overwrite").parquet(
    "gold_parquet/region_summary", compression="snappy")
region_summary.write.mode("overwrite").option(
    "header", True).csv("gold_csv/region_summary")

top_customer.write.mode("overwrite").parquet(
    "gold_parquet/top_customer", compression="snappy")
top_customer.write.mode("overwrite").option(
    "header", True).csv("gold_csv/top_customer")

print("✅ Output written to /gold_parquet (Parquet) and /gold_csv (CSV)")

# 🕒 Keep Spark UI Alive for DAG Visualization
print("⏳ Spark UI active at http://localhost:4040 for 2 minutes...")
time.sleep(120)

# Step 13: Stop SparkSession
spark.stop()