import os
from datetime import datetime
from typing import Dict, List

import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

class DataProcessor:
    def __init__(self, config: Dict):
        """Initialize the data processor with configuration."""
        self.config = config
        self.spark = self._init_spark()
        self.blob_service = self._init_storage()

    def _init_spark(self) -> SparkSession:
        """Initialize Spark session with Delta Lake support."""
        return (SparkSession.builder
                .appName("DataWarehouseModernization")
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

    def extract_data(self, source_path: str) -> pd.DataFrame:
        """Extract data from source system."""
        try:
            # Read data from source (example using CSV)
            df = pd.read_csv(source_path)
            print(f"Successfully extracted {len(df)} records from {source_path}")
            return df
        except Exception as e:
            print(f"Error extracting data: {str(e)}")
            raise

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformations to the data."""
        try:
            # Example transformations
            df['processed_date'] = datetime.now()
            df['year'] = pd.to_datetime(df['date']).dt.year
            df['month'] = pd.to_datetime(df['date']).dt.month
            
            # Add data quality checks
            if self.config['features']['enable_data_quality_checks']:
                self._validate_data(df)
                
            return df
        except Exception as e:
            print(f"Error transforming data: {str(e)}")
            raise

    def _validate_data(self, df: pd.DataFrame) -> None:
        """Perform data quality checks."""
        # Example validations
        assert not df.empty, "DataFrame is empty"
        assert df.duplicated().sum() == 0, "Duplicate records found"
        assert df.isnull().sum().sum() == 0, "Null values found"

    def load_data(self, df: pd.DataFrame, target_table: str) -> None:
        """Load data into Delta Lake format."""
        try:
            # Convert pandas DataFrame to Spark DataFrame
            spark_df = self.spark.createDataFrame(df)
            
            # Write to Delta Lake format
            spark_df.write.format("delta").mode("append").saveAsTable(target_table)
            
            print(f"Successfully loaded {len(df)} records to {target_table}")
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            raise

    def process_batch(self, source_path: str, target_table: str) -> None:
        """Process a batch of data through the ETL pipeline."""
        try:
            # Extract
            df = self.extract_data(source_path)
            
            # Transform
            df_transformed = self.transform_data(df)
            
            # Load
            self.load_data(df_transformed, target_table)
            
            print(f"Successfully processed batch from {source_path}")
        except Exception as e:
            print(f"Error processing batch: {str(e)}")
            raise

if __name__ == "__main__":
    # Example configuration
    config = {
        "azure": {
            "storage": {
                "account_name": os.getenv("AZURE_STORAGE_ACCOUNT")
            }
        },
        "features": {
            "enable_data_quality_checks": True
        }
    }
    
    # Initialize processor
    processor = DataProcessor(config)
    
    # Process a batch
    processor.process_batch(
        source_path="path/to/source/data.csv",
        target_table="processed_data"
    )
