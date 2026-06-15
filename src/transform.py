from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# Create Spark Session
spark = SparkSession.builder \
    .appName("RetailLakehouse") \
    .getOrCreate()

print("Spark Session created successfully")

# Load Orders Dataset
order_df = spark.read.csv(
    "data/bronze/orders/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)

print("Orders dataset loaded successfully")
print("Total records in orders dataset:", order_df.count())

# Remove Duplicates
print("Records before removing duplicates:", order_df.count())

order_df = order_df.dropDuplicates()

print("Records after removing duplicates:", order_df.count())

# Check Null Values
null_counts = order_df.select(
    [
        count(when(col(c).isNull(), c)).alias(c)
        for c in order_df.columns
    ]
)

print("Null value count:")
null_counts.show()

# Load Customers Dataset
customer_df = spark.read.csv(
    "data/bronze/customers/olist_customers_dataset.csv",
    header=True,
    inferSchema=True
)

print("Customers dataset loaded successfully")
print("Total records in customers dataset:", customer_df.count())

# Join Orders + Customers
silver_df = order_df.join(
    customer_df,
    on="customer_id",
    how="left"
)

print("First join completed successfully")
print("Total records in silver dataset:", silver_df.count())

silver_df.printSchema()

# Load Order Items Dataset
order_item_df = spark.read.csv(
    "data/bronze/order_items/olist_order_items_dataset.csv",
    header=True,
    inferSchema=True
)

print("Order Items dataset loaded successfully")
print("Total records in order items dataset:", order_item_df.count())

# Join Silver + Order Items
silver_df = silver_df.join(
    order_item_df,
    on="order_id",
    how="left"
)

print("Second join completed successfully")
print("Total records in silver dataset after second join:", silver_df.count())

silver_df.printSchema()

# Preview Data
silver_df.select(
    "order_id",
    "customer_id",
    "price",
    "freight_value"
).show(5)

# Create Revenue Column
silver_df = silver_df.withColumn(
    "revenue",
    col("price") + col("freight_value")
)

print("Revenue column created successfully")

silver_df.select(
    "order_id",
    "price",
    "freight_value",
    "revenue"
).show(5)

# Write Silver Layer
silver_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("data/silver/sales")

print("Silver dataset ready")

# Stop Spark Session
spark.stop()