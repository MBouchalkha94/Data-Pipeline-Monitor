import logging
from datetime import datetime, timedelta
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.identity import DefaultAzureCredential
from azure.monitor.query import LogsQueryClient, MetricsQueryClient
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class PipelineMonitor:
    def __init__(self, subscription_id, resource_group, data_factory_name):
        """Initialize the pipeline monitor."""
        self.credential = DefaultAzureCredential()
        self.adf_client = DataFactoryManagementClient(
            credential=self.credential,
            subscription_id=subscription_id
        )
        self.logs_client = LogsQueryClient(credential=self.credential)
        self.metrics_client = MetricsQueryClient(credential=self.credential)
        
        self.resource_group = resource_group
        self.data_factory_name = data_factory_name
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def get_pipeline_runs(self, start_time, end_time):
        """Get pipeline runs within the specified time window."""
        try:
            runs = self.adf_client.pipeline_runs.query_by_factory(
                resource_group_name=self.resource_group,
                factory_name=self.data_factory_name,
                filter_parameters={
                    'lastUpdatedAfter': start_time.isoformat(),
                    'lastUpdatedBefore': end_time.isoformat()
                }
            )
            
            run_details = []
            for run in runs.value:
                run_details.append({
                    'run_id': run.run_id,
                    'pipeline_name': run.pipeline_name,
                    'status': run.status,
                    'start_time': run.run_start,
                    'end_time': run.run_end,
                    'duration_in_ms': run.duration_in_ms
                })
            
            return pd.DataFrame(run_details)
            
        except Exception as e:
            self.logger.error(f"Error getting pipeline runs: {str(e)}")
            raise

    def get_activity_runs(self, pipeline_name, run_id):
        """Get activity runs for a specific pipeline run."""
        try:
            activities = self.adf_client.activity_runs.query_by_pipeline_run(
                resource_group_name=self.resource_group,
                factory_name=self.data_factory_name,
                run_id=run_id,
                filter_parameters={}
            )
            
            activity_details = []
            for activity in activities.value:
                activity_details.append({
                    'activity_name': activity.activity_name,
                    'activity_type': activity.activity_type,
                    'status': activity.status,
                    'start_time': activity.activity_run_start,
                    'end_time': activity.activity_run_end,
                    'duration_in_ms': activity.duration_in_ms,
                    'error': activity.error.message if activity.error else None
                })
            
            return pd.DataFrame(activity_details)
            
        except Exception as e:
            self.logger.error(f"Error getting activity runs: {str(e)}")
            raise

    def get_pipeline_metrics(self, start_time, end_time):
        """Get pipeline performance metrics."""
        try:
            metrics_response = self.metrics_client.query_resource(
                resource_uri=f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}/providers/Microsoft.DataFactory/factories/{self.data_factory_name}",
                metric_names=["PipelineSucceededRuns", "PipelineFailedRuns", "ActivitySucceededRuns", "ActivityFailedRuns"],
                timespan=(start_time, end_time),
                granularity="PT1H"
            )
            
            metrics_data = []
            for metric in metrics_response.metrics:
                for time_series in metric.timeseries:
                    for data_point in time_series.data:
                        metrics_data.append({
                            'metric_name': metric.name,
                            'timestamp': data_point.timestamp,
                            'value': data_point.total
                        })
            
            return pd.DataFrame(metrics_data)
            
        except Exception as e:
            self.logger.error(f"Error getting pipeline metrics: {str(e)}")
            raise

    def analyze_performance(self, days_back=7):
        """Analyze pipeline performance over the specified time period."""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days_back)
        
        try:
            # Get pipeline runs
            pipeline_runs = self.get_pipeline_runs(start_time, end_time)
            
            # Calculate success rate
            total_runs = len(pipeline_runs)
            successful_runs = len(pipeline_runs[pipeline_runs['status'] == 'Succeeded'])
            success_rate = (successful_runs / total_runs) * 100 if total_runs > 0 else 0
            
            # Calculate average duration
            avg_duration = pipeline_runs['duration_in_ms'].mean() / 1000  # Convert to seconds
            
            # Get failed runs
            failed_runs = pipeline_runs[pipeline_runs['status'] == 'Failed']
            
            # Generate report
            report = {
                'period_start': start_time,
                'period_end': end_time,
                'total_runs': total_runs,
                'success_rate': success_rate,
                'avg_duration_seconds': avg_duration,
                'failed_runs': len(failed_runs),
                'failed_pipelines': failed_runs['pipeline_name'].unique().tolist()
            }
            
            return report
            
        except Exception as e:
            self.logger.error(f"Error analyzing performance: {str(e)}")
            raise

    def alert_on_failures(self, threshold_success_rate=95):
        """Check for pipeline failures and alert if success rate is below threshold."""
        try:
            # Get last 24 hours performance
            report = self.analyze_performance(days_back=1)
            
            if report['success_rate'] < threshold_success_rate:
                alert_message = f"""
                Pipeline Alert: Success rate below threshold
                Current Success Rate: {report['success_rate']:.2f}%
                Threshold: {threshold_success_rate}%
                Failed Pipelines: {', '.join(report['failed_pipelines'])}
                """
                
                self.logger.warning(alert_message)
                # Here you could add code to send alerts via email, Teams, etc.
                
            return report['success_rate'] >= threshold_success_rate
            
        except Exception as e:
            self.logger.error(f"Error in failure alerting: {str(e)}")
            raise

if __name__ == "__main__":
    # Load configuration from environment variables
    monitor = PipelineMonitor(
        subscription_id=os.getenv('AZURE_SUBSCRIPTION_ID'),
        resource_group=os.getenv('AZURE_RESOURCE_GROUP'),
        data_factory_name=os.getenv('AZURE_DATA_FACTORY_NAME')
    )
    
    # Analyze performance
    performance_report = monitor.analyze_performance()
    print("Performance Report:", performance_report)
    
    # Check for alerts
    monitor.alert_on_failures()
