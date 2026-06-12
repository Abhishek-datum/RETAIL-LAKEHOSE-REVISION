import os
import shutil

landing_path = "data/landing"
bronze_path = "data/bronze"

folder_mapping = {
    "olist_order_items_dataset.csv": "order_items",
    "olist_orders_dataset.csv": "orders",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_order_payments_dataset.csv": "payments",
    "olist_order_reviews_dataset.csv": "reviews",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "translation"
}

for file in os.listdir(landing_path):

    if file in folder_mapping:

        target_folder = (
            f"{bronze_path}/{folder_mapping[file]}"
        )

        os.makedirs(
            target_folder,
            exist_ok=True
        )

        source_file = (
            f"{landing_path}/{file}"
        )

        destination_file = (
            f"{target_folder}/{file}"
        )

        shutil.copy(
            source_file,
            destination_file
        )

        print(f"Loaded {file}")

print("Landing to Bronze Completed!")