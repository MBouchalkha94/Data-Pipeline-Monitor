import logging
from datetime import datetime, timedelta
import yaml
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

class RetentionManager:
    def __init__(self, config_path: str):
        """Initialize the retention manager with configuration."""
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_path)
        self.spark = self._init_spark()
        self.blob_service = self._init_storage()

    def _load_config(self, config_path: str) -> dict:
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
                .appName("RetentionManager")
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

    def cleanup_delta_tables(self):
        """Clean up old data from Delta tables based on retention policy."""
        try:
            for table_name, config in self.config['incremental_load']['tables'].items():
                retention_days = config.get('retention_days', 365)
                cutoff_date = datetime.now() - timedelta(days=retention_days)
                
                self.logger.info(f"Cleaning up {table_name} data older than {cutoff_date}")
                
                # Get Delta table
                delta_table = DeltaTable.forPath(
                    self.spark,
                    f"/delta/{table_name}"
                )
                
                # Delete old data
                delta_table.delete(f"date < '{cutoff_date.date()}'")
                
                # Optimize table
                delta_table.optimize().executeCompaction()
                
                # Vacuum old files (retain 7 days of history for time travel)
                delta_table.vacuum(168)  # 168 hours = 7 days
                
                self.logger.info(f"Completed cleanup of {table_name}")
                
        except Exception as e:
            self.logger.error(f"Error cleaning up Delta tables: {str(e)}")
            raise

    def cleanup_monitoring_data(self):
        """Clean up old monitoring data from blob storage."""
        try:
            container_client = self.blob_service.get_container_client("monitoring")
            retention_days = self.config['monitoring']['log_retention_days']
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            # List all blobs in monitoring container
            blobs = container_client.list_blobs()
            
            for blob in blobs:
                if blob.last_modified < cutoff_date:
                    self.logger.info(f"Deleting old monitoring data: {blob.name}")
                    container_client.delete_blob(blob.name)
                    
        except Exception as e:
            self.logger.error(f"Error cleaning up monitoring data: {str(e)}")
            raise

    def cleanup_alert_history(self):
        """Clean up old alert history from blob storage."""
        try:
            container_client = self.blob_service.get_container_client("monitoring")
            retention_days = self.config['monitoring']['log_retention_days']
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            # List alert history blobs
            blobs = container_client.list_blobs(name_starts_with="alert_history_")
            
            for blob in blobs:
                if blob.last_modified < cutoff_date:
                    self.logger.info(f"Deleting old alert history: {blob.name}")
                    container_client.delete_blob(blob.name)
                    
        except Exception as e:
            self.logger.error(f"Error cleaning up alert history: {str(e)}")
            raise

    def cleanup_pipeline_logs(self):
        """Clean up old pipeline execution logs."""
        try:
            retention_days = self.config['monitoring']['log_retention_days']
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            # Clean up local log files
            log_dir = "logs"
            if os.path.exists(log_dir):
                for filename in os.listdir(log_dir):
                    filepath = os.path.join(log_dir, filename)
                    if os.path.getmtime(filepath) < cutoff_date.timestamp():
                        self.logger.info(f"Deleting old log file: {filename}")
                        os.remove(filepath)
                        
        except Exception as e:
            self.logger.error(f"Error cleaning up pipeline logs: {str(e)}")
            raise

    def execute_retention_policy(self):
        """Execute all retention policies."""
        try:
            self.logger.info("Starting retention policy execution")
            
            # Clean up Delta tables
            self.cleanup_delta_tables()
            
            # Clean up monitoring data
            self.cleanup_monitoring_data()
            
            # Clean up alert history
            self.cleanup_alert_history()
            
            # Clean up pipeline logs
            self.cleanup_pipeline_logs()
            
            self.logger.info("Completed retention policy execution")
            
        except Exception as e:
            self.logger.error(f"Error executing retention policy: {str(e)}")
            raise

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize retention manager
    retention_manager = RetentionManager('config/config.yaml')
    
    # Execute retention policies
    retention_manager.execute_retention_policy()
