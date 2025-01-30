from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
import yaml

class IncrementalLoader:
    def __init__(self, config_path: str):
        """Initialize the incremental loader with configuration."""
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_path)
        self.spark = self._init_spark()
        self.blob_service = self._init_storage()

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from yaml file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}")
            raise

    def _init_spark(self) -> SparkSession:
        """Initialize Spark session with Delta Lake support."""
        return (SparkSession.builder
                .appName("IncrementalDataLoader")
                .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate())

    def _init_storage(self) -> BlobServiceClient:
        """Initialize Azure Blob Storage client."""
        credential = DefaultAzureCredential()
        return BlobServiceClient(
            account_url=f"https://{self.config['azure']['storage']['account_name']}.blob.core.windows.net",
            credential=credential
        )

    def _get_watermark(self, table_name: str) -> datetime:
        """Get the last processed watermark for a table."""
        try:
            container_client = self.blob_service.get_container_client(
                self.config['azure']['storage']['container_name']
            )
            blob_client = container_client.get_blob_client(f"watermarks/{table_name}.txt")
            
            watermark = blob_client.download_blob().readall().decode()
            return datetime.fromisoformat(watermark)
        except Exception:
            # If no watermark exists, return a default date
            return datetime(2000, 1, 1)

    def _update_watermark(self, table_name: str, watermark: datetime):
        """Update the watermark for a table."""
        try:
            container_client = self.blob_service.get_container_client(
                self.config['azure']['storage']['container_name']
            )
            blob_client = container_client.get_blob_client(f"watermarks/{table_name}.txt")
            
            blob_client.upload_blob(
                watermark.isoformat().encode(),
                overwrite=True
            )
        except Exception as e:
            self.logger.error(f"Error updating watermark: {str(e)}")
            raise

    def load_incremental_data(
        self,
        source_query: str,
        target_table: str,
        timestamp_column: str,
        merge_keys: List[str]
    ):
        """Load data incrementally using watermarks."""
        try:
            # Get current watermark
            last_watermark = self._get_watermark(target_table)
            current_watermark = datetime.now()

            # Read incremental data
            incremental_data = self.spark.sql(
                f"{source_query} WHERE {timestamp_column} > '{last_watermark.isoformat()}'"
            )

            if incremental_data.count() == 0:
                self.logger.info(f"No new data to process for {target_table}")
                return

            # Load target table
            target_path = f"/delta/{target_table}"
            if DeltaTable.isDeltaTable(self.spark, target_path):
                # Merge changes into existing table
                delta_table = DeltaTable.forPath(self.spark, target_path)
                
                # Prepare merge condition
                merge_condition = " AND ".join([
                    f"target.{key} = source.{key}" for key in merge_keys
                ])
                
                # Perform merge operation
                (delta_table.alias("target")
                 .merge(incremental_data.alias("source"), merge_condition)
                 .whenMatchedUpdateAll()
                 .whenNotMatchedInsertAll()
                 .execute())
            else:
                # Create new table
                incremental_data.write.format("delta").save(target_path)

            # Update watermark
            self._update_watermark(target_table, current_watermark)
            
            self.logger.info(
                f"Successfully processed {incremental_data.count()} records for {target_table}"
            )
            
        except Exception as e:
            self.logger.error(f"Error loading incremental data: {str(e)}")
            raise

    def load_sales_data(self):
        """Load sales data incrementally."""
        source_query = """
        SELECT 
            s.date,
            s.product_id,
            s.customer_id,
            s.quantity,
            s.unit_price,
            s.total_amount,
            s.discount_amount,
            p.category,
            p.subcategory,
            c.country,
            c.city
        FROM sales s
        JOIN dim_product p ON s.product_id = p.product_id
        JOIN dim_customer c ON s.customer_id = c.customer_id
        """
        
        self.load_incremental_data(
            source_query=source_query,
            target_table="sales_mart",
            timestamp_column="date",
            merge_keys=["date", "product_id", "customer_id"]
        )

    def load_inventory_data(self):
        """Load inventory data incrementally."""
        source_query = """
        SELECT 
            i.date,
            i.product_id,
            i.quantity_on_hand,
            i.quantity_reserved,
            i.quantity_available,
            p.category,
            p.subcategory,
            p.unit_price
        FROM inventory i
        JOIN dim_product p ON i.product_id = p.product_id
        """
        
        self.load_incremental_data(
            source_query=source_query,
            target_table="inventory_mart",
            timestamp_column="date",
            merge_keys=["date", "product_id"]
        )

    def process_incremental_load(self):
        """Process all incremental loads."""
        try:
            self.logger.info("Starting incremental data load process")
            
            # Load dimension tables first
            self.load_sales_data()
            self.load_inventory_data()
            
            self.logger.info("Completed incremental data load process")
            
        except Exception as e:
            self.logger.error(f"Error in incremental load process: {str(e)}")
            raise

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize loader
    loader = IncrementalLoader('config/config.yaml')
    
    # Process incremental loads
    loader.process_incremental_load()
