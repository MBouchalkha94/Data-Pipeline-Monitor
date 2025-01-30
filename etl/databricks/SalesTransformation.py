# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------
# Configure Spark session
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# COMMAND ----------
def load_dimension_tables():
    """Load dimension tables from Azure SQL Database"""
    
    # Load dimension tables
    dim_customer = spark.read \
        .format("jdbc") \
        .option("url", dbutils.secrets.get("scope", "sql_connection_string")) \
        .option("dbtable", "dim_customer") \
        .load()
        
    dim_product = spark.read \
        .format("jdbc") \
        .option("url", dbutils.secrets.get("scope", "sql_connection_string")) \
        .option("dbtable", "dim_product") \
        .load()
        
    return dim_customer, dim_product

# COMMAND ----------
def transform_sales_data(sales_df, dim_customer, dim_product):
    """Transform sales data and join with dimension tables"""
    
    # Join with dimension tables
    enriched_sales = sales_df \
        .join(dim_customer, sales_df.customer_id == dim_customer.customer_id, "left") \
        .join(dim_product, sales_df.product_id == dim_product.product_id, "left")
    
    # Calculate additional metrics
    final_sales = enriched_sales \
        .withColumn("profit_margin", 
                   (col("total_amount") - col("unit_cost") * col("quantity")) / col("total_amount")) \
        .withColumn("processing_date", current_timestamp()) \
        .withColumn("year_month", date_format(col("date"), "yyyy-MM"))
    
    return final_sales

# COMMAND ----------
def write_to_delta_lake(df, table_name):
    """Write DataFrame to Delta Lake with merge strategy"""
    
    # Define the merge condition
    merge_condition = """
        target.date = source.date AND
        target.product_id = source.product_id AND
        target.customer_id = source.customer_id
    """
    
    # Create or get the Delta table
    if DeltaTable.isDeltaTable(spark, f"/delta/{table_name}"):
        delta_table = DeltaTable.forPath(spark, f"/delta/{table_name}")
        
        # Perform merge operation
        delta_table.alias("target") \
            .merge(df.alias("source"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        # Write the DataFrame as a new Delta table
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"/delta/{table_name}")

# COMMAND ----------
def main():
    # Get parameters
    date = dbutils.widgets.get("date")
    
    # Load dimension tables
    dim_customer, dim_product = load_dimension_tables()
    
    # Read sales data from staging
    sales_df = spark.read \
        .format("delta") \
        .load("/delta/staging/sales")
    
    # Transform sales data
    transformed_sales = transform_sales_data(sales_df, dim_customer, dim_product)
    
    # Write to Delta Lake
    write_to_delta_lake(transformed_sales, "sales_mart")
    
    # Create temp view for SQL queries
    transformed_sales.createOrReplaceTempView("sales_mart")
    
    # Calculate aggregations
    daily_sales = spark.sql("""
        SELECT 
            date,
            count(*) as transaction_count,
            sum(quantity) as total_quantity,
            sum(total_amount) as total_sales,
            avg(profit_margin) as avg_profit_margin
        FROM sales_mart
        GROUP BY date
        ORDER BY date
    """)
    
    # Write aggregations to Delta Lake
    write_to_delta_lake(daily_sales, "daily_sales_metrics")

# COMMAND ----------
# Run the main function
main()
