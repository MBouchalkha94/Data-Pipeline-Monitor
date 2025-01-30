import logging
from datetime import datetime
import yaml
import os
from data_processor import DataProcessor
from data_quality import DataQualityChecker
from incremental_loader import IncrementalLoader
from monitoring.alert_manager import AlertManager
from monitoring.pipeline_monitor import PipelineMonitor

class DataPipelineOrchestrator:
    def __init__(self, config_path: str):
        """Initialize the orchestrator with configuration."""
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_path)
        
        # Initialize components
        self.data_processor = DataProcessor(self.config)
        self.quality_checker = DataQualityChecker()
        self.incremental_loader = IncrementalLoader(config_path)
        self.alert_manager = AlertManager(config_path)
        self.pipeline_monitor = PipelineMonitor(
            self.config['azure']['subscription_id'],
            self.config['azure']['resource_group'],
            self.config['azure']['data_factory']['name']
        )

    def _load_config(self, config_path: str) -> dict:
        """Load configuration from yaml file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}")
            raise

    def process_data_pipeline(self):
        """Execute the complete data pipeline process."""
        try:
            self.logger.info("Starting data pipeline process")
            pipeline_start_time = datetime.now()
            
            # Step 1: Load incremental data
            if self.config['features']['enable_incremental_loading']:
                self.logger.info("Starting incremental data load")
                self.incremental_loader.process_incremental_load()

            # Step 2: Process data
            self.logger.info("Starting data processing")
            processed_sales = self.data_processor.process_batch(
                "sales_staging",
                "sales_processed"
            )
            processed_inventory = self.data_processor.process_batch(
                "inventory_staging",
                "inventory_processed"
            )

            # Step 3: Perform data quality checks
            if self.config['features']['enable_data_quality_checks']:
                self.logger.info("Performing data quality checks")
                sales_quality = self.quality_checker.validate_sales_data(processed_sales)
                inventory_quality = self.quality_checker.validate_inventory_data(processed_inventory)

                # Process quality results and send alerts if needed
                if self.config['features']['enable_alerts']:
                    self.alert_manager.process_data_quality_results(sales_quality)
                    self.alert_manager.process_data_quality_results(inventory_quality)

            # Step 4: Monitor pipeline performance
            pipeline_end_time = datetime.now()
            pipeline_metrics = self.pipeline_monitor.analyze_performance(days_back=1)
            
            if self.config['features']['enable_alerts']:
                self.alert_manager.process_pipeline_metrics(pipeline_metrics)

            # Step 5: Save monitoring data
            self.alert_manager.save_alert_history(
                f"https://{self.config['azure']['storage']['account_name']}.blob.core.windows.net",
                "monitoring"
            )

            self.logger.info("Data pipeline process completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in data pipeline process: {str(e)}")
            if self.config['features']['enable_alerts']:
                self.alert_manager._process_alert({
                    'level': 'ERROR',
                    'title': 'Pipeline Execution Failed',
                    'message': f"Error: {str(e)}"
                })
            raise

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('pipeline.log'),
            logging.StreamHandler()
        ]
    )

if __name__ == "__main__":
    # Set up logging
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # Get configuration path
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'config',
            'config.yaml'
        )

        # Initialize and run orchestrator
        orchestrator = DataPipelineOrchestrator(config_path)
        orchestrator.process_data_pipeline()

    except Exception as e:
        logger.error(f"Pipeline orchestration failed: {str(e)}")
        raise
