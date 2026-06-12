from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RetailLakehouseGold") \
    .getOrCreate()

print("Spark Session Created")
order_df = spark.read.csv(
    "data/bronze/orders/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)

print("Orders dataset loaded successfully")

print(
    "Total Orders:",

    order_df.count()

)
order_items_df = spark.read.csv(
    "data/bronze/order_items/olist_order_items_dataset.csv",
    header=True,
    inferSchema=True
)

print("Order Items dataset loaded successfully")

print(
    "Total Order Items:",
    order_items_df.count()
)
gold_df = order_df.join(
    order_items_df,
    on="order_id",
    how="inner"
)

print("Gold join completed successfully")

print(
    "Total records after join:",
    gold_df.count()
)
from pyspark.sql.functions import col

gold_df = gold_df.withColumn(
    "revenue",
    col("price") + col("freight_value")
)

print("Revenue column created")
gold_df.select(
    "order_id",
    "price",
    "freight_value",
    "revenue"
).show(5)
from pyspark.sql.functions import sum, to_date

daily_revenue_df = gold_df.groupBy(
    to_date("order_purchase_timestamp").alias("order_date")
).agg(
    sum("revenue").alias("daily_revenue")
)

print("Daily Revenue KPI Created")

daily_revenue_df.show(10)
products_df = spark.read.csv(
    "data/bronze/products/olist_products_dataset.csv",
    header=True,
    inferSchema=True
)

print("Products dataset loaded successfully")

print(
    "Total Products:",
    products_df.count()
)
translation_df = spark.read.csv(
    "data/bronze/translation/product_category_name_translation.csv",
    header=True,
    inferSchema=True
)

print("Translation dataset loaded successfully")

print(
    "Total Categories:",
    translation_df.count()
)
gold_product_df = gold_df.join(
    products_df,
    on="product_id",
    how="left"
)

print("Products joined successfully")

print(
    "Total records after products join:",
    gold_product_df.count()
)
gold_product_df = gold_product_df.join(
    translation_df,
    on="product_category_name",
    how="left"
)

print("Translation joined successfully")
from pyspark.sql.functions import sum

top_category_df = (
    gold_product_df
    .groupBy("product_category_name_english")
    .agg(
        sum("revenue").alias("total_revenue")
    )
    .orderBy(
        "total_revenue",
        ascending=False
    )
)

print("Top Category KPI Created")

top_category_df.show(10, False)

customers_df = spark.read.csv(
    "data/bronze/customers/olist_customers_dataset.csv",
    header=True,
    inferSchema=True
)

print("Customers loaded")
gold_df = gold_df.join(
    customers_df,
    on="customer_id",
    how="left"
)

print("Customers joined")

state_revenue_df = (
    gold_df
    .groupBy("customer_state")
    .agg(
        sum("revenue").alias("total_revenue")
    )
    .orderBy("total_revenue", ascending=False)
)

state_revenue_df.show(10, False)

from pyspark.sql.functions import month, year

monthly_revenue_df = (
    gold_df
    .groupBy(
        year("order_purchase_timestamp").alias("year"),
        month("order_purchase_timestamp").alias("month")
    )
    .agg(
        sum("revenue").alias("monthly_revenue")
    )
    .orderBy("year", "month")
)

print("Monthly Revenue KPI Created")

monthly_revenue_df.show(20, False)