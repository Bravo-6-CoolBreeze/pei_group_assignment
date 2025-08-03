# Databricks notebook source
spark.sql("""
CREATE CATALOG IF NOT EXISTS sales
COMMENT 'This catalog contains e-commerce sales data assets'
""")


# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS sales.bronze
COMMENT 'Schema to store all raw tables related to sales'
""")


# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS sales.silver
COMMENT 'Schema to store all sales related tables with cleaned and enriched data'
""")

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS sales.gold
COMMENT 'Schema to store all sales related aggregated data'
""")

# COMMAND ----------

spark.sql("""
CREATE VOLUME IF NOT EXISTS sales.bronze.raw_data_files
COMMENT 'Stores data files with sales related data'
""")

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/sales/bronze/raw_data_files/"))