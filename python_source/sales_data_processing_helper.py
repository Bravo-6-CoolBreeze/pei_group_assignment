# Databricks notebook source
from pyspark.sql.functions import col, regexp_replace, trim, when, length, to_date, round, year, sum
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, DateType
from pyspark.sql.utils import AnalysisException
import pandas as pd
import re
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def clean_column_name(column_name):
    """Clean column names by removing invalid characters and replacing space with underscore."""
    cleaned_name = re.sub(r"[,\;\{\}\(\)=]", "", column_name).lower()
    return re.sub(r"[\s]", "_", cleaned_name)

# COMMAND ----------

def write_df_to_table(df, trg_tbl_nm):
    """Write dataframe to Delta Table"""
    try:
        # Save to Delta table
        df.write.format("delta").mode("overwrite").saveAsTable(trg_tbl_nm)
        logger.info(f"Table {trg_tbl_nm} created successfully")
    except Exception as e:
        logger.error(f"Error loading table {trg_tbl_nm}: {str(e)}")
        raise

# COMMAND ----------

def read_raw_customers(file_path):
    """Read raw customers data from file"""
    try:
        pd_df = pd.read_excel(file_path, engine="openpyxl", dtype="str", na_filter=False)
        df = spark.createDataFrame(pd_df)

        # Clean column names
        for column in df.columns:
            df = df.withColumnRenamed(column, clean_column_name(column))
        
        return df
    except Exception as e:
        logger.error(f"Error reading customer file: {str(e)}")
        raise

# COMMAND ----------

def read_raw_orders(file_path):
    """Read raw orders data from file"""
    try:
        df = spark.read.option("multiLine", "true").option("primitivesAsString", "true").json(file_path)

        # Clean column names
        for column in df.columns:
            df = df.withColumnRenamed(column, clean_column_name(column))
        
        return df
    except Exception as e:
        logger.error(f"Error reading orders file: {str(e)}")
        raise

# COMMAND ----------

def read_raw_products(file_path):
    """Read raw products data from file"""
    try:
        product_schema = StructType([
                            StructField("product_id", StringType(), True),
                            StructField("category", StringType(), True),
                            StructField("sub_category", StringType(), True),
                            StructField("product_name", StringType(), True),
                            StructField("state", StringType(), True),
                            StructField("price_per_product", StringType(), True)
                        ]) 
        df = spark.read.option("quote", '"').option("escape", '"').option("mode", "FAILFAST").schema(product_schema).csv(file_path, header=True)

        # Clean column names
        for column in df.columns:
            df = df.withColumnRenamed(column, clean_column_name(column))
        
        return df
    except Exception as e:
        logger.error(f"Error reading products file: {str(e)}")
        raise

# COMMAND ----------

def clean_customers(df):
    """Clean customers raw data"""
    try:
        # Trim all columns
        for column in df.columns:
            df = df.withColumn(column, trim(col(column)))
        
        df = (
                df.withColumn("customer_name", 
                                    when(regexp_replace(col("customer_name"), r"[^A-Za-z]", "") != "", 
                                            regexp_replace(col("customer_name"), r"[^A-Za-z]", ""))
                                        .otherwise(None))
                    .withColumn("phone", 
                                    when(length(regexp_replace(col("phone"), r"\D", "")) >= 10,
                                            regexp_replace(col("phone"), r"\D", ""))
                                        .otherwise(None))
        )

        return df
    except Exception as e:
        logger.error(f"Error cleaning customer data: {str(e)}")
        raise

# COMMAND ----------

def clean_orders(df):
    """Clean orders raw data"""
    try:
        # Trim all columns
        for column in df.columns:
            df = df.withColumn(column, trim(col(column)))
        
        df = (
                df.withColumn("order_date", to_date(col("order_date"), "d/M/yyyy"))
                    .withColumn("ship_date", to_date(col("ship_date"), "d/M/yyyy"))
                    .withColumn("discount", col("discount").cast(DecimalType(10, 2)))
                    .withColumn("price", col("price").cast(DecimalType(10, 2)))
                    .withColumn("profit", col("profit").cast(DecimalType(10, 2)))
                    .withColumn("quantity", col("quantity").cast(IntegerType()))
            )
        
        return df
    except Exception as e:
        logger.error(f"Error cleaning orders data : {str(e)}")
        raise

# COMMAND ----------

def clean_products(df):
    """Clean products raw data"""
    try:
        # Trim all columns
        for column in df.columns:
            df = df.withColumn(column, trim(col(column)))
        
        df = df.withColumn("price_per_product", col("price_per_product").cast(DecimalType(10, 2)))

        return df
    except Exception as e:
        logger.error(f"Error cleaning products data: {str(e)}")
        raise

# COMMAND ----------

def create_enriched_orders_df(cleaned_customers_df, cleaned_orders_df, cleaned_products_df):
    """Create enriched orders master data"""
    try:
        enriched_orders_df = (
                cleaned_orders_df.alias("o").join(cleaned_customers_df.alias("c"), 
                                                    on = ["customer_id"], how = "inner")
                                .join(cleaned_products_df.alias("p"), 
                                        on = [(col("o.product_id") == col("p.product_id")), 
                                              (col("c.state") == col("p.state"))], 
                                        how = "inner")
                                .withColumn("profit", round(col("o.profit"), 2))
                                .select(
                                    col("c.customer_id"),
                                    col("c.customer_name"),
                                    col("c.country"),
                                    col("p.product_name"),
                                    col("p.sub_category"),
                                    col("p.category"),
                                    col("o.price"),
                                    col("o.quantity"),
                                    col("o.discount"),
                                    col("profit"),
                                    col("o.order_date"),
                                    col("o.ship_date"),
                                    col("o.ship_mode")
                                )
        )
        return enriched_orders_df
    except Exception as e:
        logger.error(f"Error creating enriched orders master dataset: {str(e)}")
        raise

# COMMAND ----------

def create_aggregated_profit_df(enriched_df):
    """Create aggregated profit dataset"""
    try:
        aggregated_df = (
        enriched_df.withColumn("year", year(col("order_date")))
                        .groupBy("year", "category", "sub_category", "customer_id", "customer_name")
                            .agg(round(sum(col("profit")), 2).alias("total_profit"))
                            .orderBy("year", "category", "sub_category", "customer_id")
                )

        return aggregated_df
    except Exception as e:
        logger.error(f"Error creating aggregated profit dataset: {str(e)}")
        raise

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