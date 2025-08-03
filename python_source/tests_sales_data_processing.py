# Databricks notebook source
# MAGIC %pip install --upgrade openpyxl

# COMMAND ----------

# MAGIC %run ./sales_data_processing_helper

# COMMAND ----------

from decimal import Decimal
from datetime import datetime

# COMMAND ----------

customer_data_file_path = "/Volumes/sales/bronze/raw_data_files/Customer.xlsx"
orders_data_file_path = "/Volumes/sales/bronze/raw_data_files/Orders.json"
products_data_file_path = "/Volumes/sales/bronze/raw_data_files/Products.csv"

# COMMAND ----------

# Test read_raw_customers function
def test_read_raw_customers():
    customers_df = read_raw_customers(customer_data_file_path)

    assert customers_df.count() == 793
    assert customers_df.dtypes == [("customer_id", "string"), ("customer_name", "string"), ("email", "string"),
                                    ("phone", "string"), ("address", "string"), ("segment", "string"), 
                                    ("country", "string"), ("city", "string"), ("state", "string"),
                                    ("postal_code", "string"), ("region", "string")]
    logger.info("test_read_raw_customers: PASSED")

# COMMAND ----------

# Test read_raw_orders function
def test_read_raw_orders():
    orders_df = read_raw_orders(orders_data_file_path)

    assert orders_df.count() == 9994
    assert orders_df.dtypes == [('customer_id', 'string'), ('discount', 'string'), ('order_date', 'string'),
                                ('order_id', 'string'), ('price', 'string'), ('product_id', 'string'),
                                ('profit', 'string'), ('quantity', 'string'), ('row_id', 'string'),
                                ('ship_date', 'string'), ('ship_mode', 'string')]
    logger.info("test_read_raw_orders: PASSED")

# COMMAND ----------

# Test read_raw_products function
def test_read_raw_products():
    products_df = read_raw_products(products_data_file_path)

    assert products_df.count() == 1851
    assert products_df.dtypes == [('product_id', 'string'), ('category', 'string'), ('sub_category', 'string'),
                                    ('product_name', 'string'), ('state', 'string'), ('price_per_product', 'string')]
    logger.info("test_read_raw_products: PASSED")

# COMMAND ----------

# Test clean_customers function
def test_clean_customers():
    # Create sample raw customers dataframe
    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("region", StringType(), True)
    ])
    data = [
        ("PT-19090", "Pete@#$ Takahito", "mikaylaarnold666@gmail.com", "786.638.6820", 
        "0236 Lane Squares\nPort Samantha, ME 15670", "Consumer", "United States", 
        "San Antonio", "Texas", "78207", "Central"),
        ("GH-14410", "Gary567 Hansen", "austindyer948@gmail.com", "001-542-415-0246x314", 
        "00347 Murphy Unions\nAshleyton, IA 29814", "Home Office", "United States", 
        "Chicago", "Illinois", "60653", "Central")
    ]
    df = spark.createDataFrame(data, customer_schema)

    cleaned_customers_df = clean_customers(df)
        
    assert cleaned_customers_df.count() == 2
    assert cleaned_customers_df.dtypes == [("customer_id", "string"), ("customer_name", "string"), ("email", "string"),
                                            ("phone", "string"), ("address", "string"), ("segment", "string"), 
                                            ("country", "string"), ("city", "string"), ("state", "string"),
                                            ("postal_code", "string"), ("region", "string")]
    assert (
        {cleaned_customers_df.collect()[0]["customer_name"], 
            cleaned_customers_df.collect()[1]["customer_name"]} == {"PeteTakahito", "GaryHansen"}
    )
    assert (
        {cleaned_customers_df.collect()[0]["phone"], 
            cleaned_customers_df.collect()[1]["phone"]} == {"7866386820", "0015424150246314"}
    )
    logger.info("test_clean_customers: PASSED")

# COMMAND ----------

# Test clean_orders function
def test_clean_orders():
    # Create sample raw orders dataframe
    orders_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("discount", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("price", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("profit", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("row_id", StringType(), True),
        StructField("ship_date", StringType(), True),
        StructField("ship_mode", StringType(), True)
    ])
    data = [
            ("CB-12025", "0", "27/11/2016", "CA-2016-117583", "79.95", "OFF-BI-10004233", "38.38", "5", "6", "30/11/2016", "First Class"),
            ("AB-10105", "0.7", "19/9/2017", "CA-2017-153822", "18.18", "OFF-BI-10001460", "-13.94", "4", "13", "25/9/2017", "Standard Class")
        ]
    df = spark.createDataFrame(data, orders_schema)

    cleaned_orders_df = clean_orders(df)
        
    assert cleaned_orders_df.count() == 2
    assert cleaned_orders_df.dtypes == [('customer_id', 'string'), ('discount', 'decimal(10,2)'), ('order_date', 'date'),
                                        ('order_id', 'string'), ('price', 'decimal(10,2)'), ('product_id', 'string'),
                                        ('profit', 'decimal(10,2)'), ('quantity', 'int'), ('row_id', 'string'),
                                        ('ship_date', 'date'), ('ship_mode', 'string')]
    assert (
        {cleaned_orders_df.collect()[0]["discount"], 
            cleaned_orders_df.collect()[1]["discount"]} == {Decimal("0.00"), Decimal("0.70")}
    )
    assert (
        {cleaned_orders_df.collect()[0]["order_date"], 
            cleaned_orders_df.collect()[1]["order_date"]} == {datetime.strptime("27/11/2016", "%d/%m/%Y").date(), 
                                                                datetime.strptime("19/9/2017", "%d/%m/%Y").date()}
    )
    assert (
        {cleaned_orders_df.collect()[0]["price"], 
            cleaned_orders_df.collect()[1]["price"]} == {Decimal("79.95"), Decimal("18.18")}
    )
    assert (
        {cleaned_orders_df.collect()[0]["profit"], 
            cleaned_orders_df.collect()[1]["profit"]} == {Decimal("38.38"), Decimal("-13.94")}
    )
    assert (
        {cleaned_orders_df.collect()[0]["quantity"], 
            cleaned_orders_df.collect()[1]["quantity"]} == {5, 4}
    )
    assert (
        {cleaned_orders_df.collect()[0]["ship_date"], 
            cleaned_orders_df.collect()[1]["ship_date"]} == {datetime.strptime("30/11/2016", "%d/%m/%Y").date(), 
                                                                datetime.strptime("25/9/2017", "%d/%m/%Y").date()}
    )
    logger.info("test_clean_orders: PASSED")

# COMMAND ----------

# Test clean_products function
def test_clean_products():
    # Create sample raw products dataframe
    products_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("price_per_product", StringType(), True)
    ])
    data = [
        ("OFF-BI-10004233", "Office Supplies", "Binders", 'GBC Pre-Punched Binding Paper, Plastic, White, 8-1/2" x 11"', "New Jersey", "15.99"),
        ("OFF-AP-10002118", "Office Supplies", "Appliances", '1.7 Cubic Foot Compact "Cube" Office Refrigerators', "Alabama", "208")
    ]
    df = spark.createDataFrame(data, products_schema)

    cleaned_products_df = clean_products(df)
    
    assert cleaned_products_df.count() == 2
    assert cleaned_products_df.dtypes == [('product_id', 'string'), ('category', 'string'), ('sub_category', 'string'),
                                            ('product_name', 'string'), ('state', 'string'), 
                                            ('price_per_product', 'decimal(10,2)')]
    assert (
            {cleaned_products_df.collect()[0]["price_per_product"], 
                cleaned_products_df.collect()[1]["price_per_product"]}  == {Decimal("15.99"), Decimal("208.00")}
    )
    logger.info("test_clean_products: PASSED")

# COMMAND ----------

# Test create_enriched_orders_df function
def test_create_enriched_orders_df():
    # Create sample cleaned data
    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("region", StringType(), True)
    ])
    customer_data = [
        ("CB-12025", "CassandraBrandow", "aimeejacobs870@gmail.com", "2489344344", 
        "USCGC Johnson FPO AE 24361", "Consumer", "United States", 
        "East Orange", "New Jersey", "7017", "East")
    ]
    cleaned_customers_df = spark.createDataFrame(customer_data, customer_schema)

    orders_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("discount", DecimalType(10, 2), True),
        StructField("order_date", DateType(), True),
        StructField("order_id", StringType(), True),
        StructField("price", DecimalType(10, 2), True),
        StructField("product_id", StringType(), True),
        StructField("profit", DecimalType(10, 2), True),
        StructField("quantity", IntegerType(), True),
        StructField("row_id", StringType(), True),
        StructField("ship_date", DateType(), True),
        StructField("ship_mode", StringType(), True)
    ])
    orders_data = [
            ("CB-12025", Decimal("0.00"), datetime.strptime("2016-11-27", "%Y-%m-%d").date(), "CA-2016-117583", Decimal("79.95"), "OFF-BI-10004233", Decimal("38.38"), 5, "6", datetime.strptime("2016-11-30", "%Y-%m-%d").date(), "First Class")
        ]
    cleaned_orders_df = spark.createDataFrame(orders_data, orders_schema)

    products_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("price_per_product", DecimalType(10, 2), True)
    ])
    products_data = [
        ("OFF-BI-10004233", "Office Supplies", "Binders", 'GBC Pre-Punched Binding Paper, Plastic, White, 8-1/2" x 11"', "New Jersey", Decimal("15.99"))
    ]
    cleaned_products_df = spark.createDataFrame(products_data, products_schema)
    enriched_master_orders_df = create_enriched_orders_df(cleaned_customers_df, cleaned_orders_df, cleaned_products_df)

    assert enriched_master_orders_df.count() == 1
    assert tuple(enriched_master_orders_df.collect()[0]) == ("CB-12025", "CassandraBrandow", "United States",
                                                                "GBC Pre-Punched Binding Paper, Plastic, White, 8-1/2\" x 11\"", "Binders", "Office Supplies", Decimal("79.95"),
                                                                5, Decimal("0.00"), Decimal("38.38"),
                                                                datetime.strptime("2016-11-27", "%Y-%m-%d").date(),
                                                                datetime.strptime("2016-11-30", "%Y-%m-%d").date(),
                                                                "First Class")
    logger.info("test_create_enriched_orders_df: PASSED")

# COMMAND ----------

# Test create_aggregated_profit_df function
def test_create_aggregated_profit_df():
    # Create sample enriched orders data
    enriched_data_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DecimalType(10, 2), True),
        StructField("quantity", IntegerType(), True),
        StructField("discount", DecimalType(10, 2), True),
        StructField("profit", DecimalType(11, 2), True),
        StructField("order_date", DateType(), True),
        StructField("ship_date", DateType(), True),
        StructField("ship_mode", StringType(), True)
    ])

    enriched_data = [
        ("LS-16945", "LindaSouthworth", "United States",
        "Hon 4700 Series Mobuis Mid-Back Task Chairs with Adjustable Arms", "Chairs",
        "Furniture", Decimal("854.35"), 3, Decimal("0.20"), Decimal("11.00"),
        datetime.strptime("2016-07-18", "%Y-%m-%d").date(),
        datetime.strptime("2016-07-24", "%Y-%m-%d").date(),
        "Standard Class"
        ),
        (
        "LS-16945", "LindaSouthworth", "United States",
        "Hon GuestStacker Chair", "Chairs", "Furniture",
        Decimal("544.01"), 3, Decimal("0.20"), Decimal("40.80"),
        datetime.strptime("2016-07-18", "%Y-%m-%d").date(),
        datetime.strptime("2016-07-24", "%Y-%m-%d").date(),
        "Standard Class"
        )
    ]

    enriched_data_df = spark.createDataFrame(enriched_data, enriched_data_schema)

    aggregated_profit_df = create_aggregated_profit_df(enriched_data_df)
    
    assert aggregated_profit_df.count() == 1
    assert (
        tuple(aggregated_profit_df.collect()[0]) == 
                                    (2016, "Furniture", "Chairs", "LS-16945", "LindaSouthworth", Decimal("51.80"))
    )
    logger.info("test_create_aggregated_profit_df: PASSED")

# COMMAND ----------

# Run all tests
def run_tests():
    tests = [
        test_read_raw_customers,
        test_read_raw_orders,
        test_read_raw_products,
        test_clean_customers,
        test_clean_orders,
        test_clean_products,
        test_create_enriched_orders_df,
        test_create_aggregated_profit_df
    ]
    for test in tests:
        try:
            test()
        except AssertionError as e:
            logger.error(f"{test.__name__}: FAILED - {str(e)}")
        except Exception as e:
            logger.error(f"{test.__name__}: ERROR - {str(e)}")

# COMMAND ----------

# Execute tests
run_tests()