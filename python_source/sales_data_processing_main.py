# Databricks notebook source
# MAGIC %pip install --upgrade openpyxl

# COMMAND ----------

# MAGIC %run ./databricks_data_objects_setup

# COMMAND ----------

# MAGIC %run ./sales_data_processing_helper

# COMMAND ----------

customer_data_file_path = "/Volumes/sales/bronze/raw_data_files/Customer.xlsx"
orders_data_file_path = "/Volumes/sales/bronze/raw_data_files/Orders.json"
products_data_file_path = "/Volumes/sales/bronze/raw_data_files/Products.csv"

catalog_name = "sales"

raw_database_name = "bronze"

enriched_database_name = "silver"

aggregated_database_name = "gold"

raw_customers_table_name = f"{catalog_name}.{raw_database_name}.customers_raw"
raw_orders_table_name = f"{catalog_name}.{raw_database_name}.orders_raw"
raw_products_table_name = f"{catalog_name}.{raw_database_name}.products_raw"

cleaned_customers_table_name = f"{catalog_name}.{enriched_database_name}.customers_cleaned"
cleaned_orders_table_name = f"{catalog_name}.{enriched_database_name}.orders_cleaned"
cleaned_products_table_name = f"{catalog_name}.{enriched_database_name}.products_cleaned"

enriched_orders_table_name = f"{catalog_name}.{enriched_database_name}.sales_enriched_master"

aggregated_profit_table_name = f"{catalog_name}.{aggregated_database_name}.aggregated_profit"

# COMMAND ----------

# Task 1
raw_customers_df = read_raw_customers(customer_data_file_path)
write_df_to_table(raw_customers_df, raw_customers_table_name)
raw_orders_df = read_raw_orders(orders_data_file_path)
write_df_to_table(raw_orders_df, raw_orders_table_name)
raw_products_df = read_raw_products(products_data_file_path)
write_df_to_table(raw_products_df, raw_products_table_name)

# Task 2
cleaned_customers_df = clean_customers(raw_customers_df)
write_df_to_table(cleaned_customers_df, cleaned_customers_table_name)
cleaned_orders_df = clean_orders(raw_orders_df)
write_df_to_table(cleaned_orders_df, cleaned_orders_table_name)
cleaned_products_df = clean_products(raw_products_df)
write_df_to_table(cleaned_products_df, cleaned_products_table_name)

# Task 3
enriched_orders_master_df = create_enriched_orders_df(cleaned_customers_df, cleaned_orders_df, cleaned_products_df)
write_df_to_table(enriched_orders_master_df, enriched_orders_table_name)

# Task 4
aggregated_profit_df = create_aggregated_profit_df(enriched_orders_master_df)
write_df_to_table(aggregated_profit_df, aggregated_profit_table_name)

# COMMAND ----------

# Task 5
# a) Profit by Year
spark.sql("""
SELECT year, SUM(total_profit) AS total_profit
FROM sales.gold.aggregated_profit
GROUP BY year
ORDER BY year
""").display()

# COMMAND ----------

# Task 5
# b) Profit by Year + Product Category
spark.sql("""
SELECT year, category, SUM(total_profit) AS total_profit
FROM sales.gold.aggregated_profit
GROUP BY year, category
ORDER BY year, category
""").display()

# COMMAND ----------

# Task 5
# c) Profit by Customer
spark.sql("""
SELECT customer_id, customer_name, SUM(total_profit) AS total_profit
FROM sales.gold.aggregated_profit
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC
""").display()

# COMMAND ----------

# Task 5
# d) Profit by Customer + Year
spark.sql("""
SELECT year, customer_id, customer_name, SUM(total_profit) AS total_profit
FROM sales.gold.aggregated_profit
GROUP BY year, customer_id, customer_name
ORDER BY year, total_profit DESC
""").display()