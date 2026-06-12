from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RetailLakehouse").getOrCreate()
print("Spark Session created successfully")
order_df = spark.read.csv("data/bronze/orders/olist_orders_dataset.csv", header=True, inferSchema=True)
print("Orders dataset loaded successfully")
print("total records in orders dataset:", order_df.count())
print("records before removing duplicates:", order_df.count())
order_df = order_df.dropDuplicates()
print("records after removing duplicates:", order_df.count())

from pyspark.sql.functions import col, when, count
null_counts = order_df.select([count(when(col(c).isNull(), c)).alias(c) for c in order_df.columns])
null_counts.show()

customer_df = spark.read.csv("data/bronze/customers/olist_customers_dataset.csv", header=True, inferSchema=True)
print("Customers dataset loaded successfully")
print("total records in customers dataset:", customer_df.count())

silver_df = order_df.join(customer_df, on = "customer_id", how="left")
print("Join completed successfully")
print("total records in silver dataset:", silver_df.count())
silver_df.printSchema()

order_item_df = spark.read.csv("data/bronze/order_items/olist_order_items_dataset.csv", header=True, inferSchema=True)
print("Order Items dataset loaded successfully")
print("total records in order items dataset:", order_item_df.count())

silver_df= silver_df.join(order_item_df, on = "order_id", how="left")
print("Join completed successfully")
print("total records in silver dataset after second join:", silver_df.count())
silver_df.printSchema()

silver_df.select("order_id", "customer_id", "price","freight_value").show(5)
from pyspark.sql.functions import col

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

# silver_df.write.mode("overwrite").option(
#     "header",
#     True
# ).csv(
#     "data/silver/sales"
# )

print("Silver dataset ready")
