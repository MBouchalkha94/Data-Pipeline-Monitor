import logging
from datetime import datetime
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import yaml
import os

class AlertManager:
    def __init__(self, config_path):
        """Initialize the alert manager with configuration."""
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_path)
        self.alert_history = []

    def _load_config(self, config_path):
        """Load configuration from yaml file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}")
            raise

    def _send_email_alert(self, subject, body):
        """Send email alert using SMTP."""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.config['alerts']['email']['sender']
            msg['To'] = self.config['alerts']['email']['recipients']
            msg['Subject'] = subject

            msg.attach(MIMEText(body, 'html'))

            with smtplib.SMTP(self.config['alerts']['email']['smtp_server']) as server:
                server.starttls()
                server.login(
                    self.config['alerts']['email']['username'],
                    self.config['alerts']['email']['password']
                )
                server.send_message(msg)

            self.logger.info(f"Email alert sent: {subject}")
        except Exception as e:
            self.logger.error(f"Error sending email alert: {str(e)}")

    def _send_teams_alert(self, title, message):
        """Send alert to Microsoft Teams webhook."""
        try:
            payload = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "0076D7",
                "summary": title,
                "sections": [{
                    "activityTitle": title,
                    "activitySubtitle": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "text": message
                }]
            }

            response = requests.post(
                self.config['alerts']['teams']['webhook_url'],
                json=payload
            )
            response.raise_for_status()
            self.logger.info(f"Teams alert sent: {title}")
        except Exception as e:
            self.logger.error(f"Error sending Teams alert: {str(e)}")

    def process_data_quality_results(self, results):
        """Process data quality check results and trigger alerts if needed."""
        alerts = []

        # Check schema validation
        if not results['checks']['schema']['schema_valid']:
            alerts.append({
                'level': 'ERROR',
                'title': 'Schema Validation Failed',
                'message': f"Table: {results['table']}\n" +
                          f"Missing columns: {results['checks']['schema']['missing_columns']}\n" +
                          f"Type mismatches: {results['checks']['schema']['type_mismatches']}"
            })

        # Check for nulls
        if results['checks']['nulls']['has_nulls']:
            alerts.append({
                'level': 'WARNING',
                'title': 'Null Values Detected',
                'message': f"Table: {results['table']}\n" +
                          f"Null counts: {results['checks']['nulls']['null_counts']}"
            })

        # Check value ranges
        if not results['checks']['value_ranges']['in_range']:
            alerts.append({
                'level': 'WARNING',
                'title': 'Value Range Violations',
                'message': f"Table: {results['table']}\n" +
                          f"Violations: {results['checks']['value_ranges']['violations']}"
            })

        # Process alerts
        for alert in alerts:
            self._process_alert(alert)

    def process_pipeline_metrics(self, metrics):
        """Process pipeline monitoring metrics and trigger alerts if needed."""
        alerts = []

        # Check pipeline success rate
        if metrics['success_rate'] < self.config['alerts']['thresholds']['pipeline_success_rate']:
            alerts.append({
                'level': 'ERROR',
                'title': 'Low Pipeline Success Rate',
                'message': f"Current success rate: {metrics['success_rate']}%\n" +
                          f"Failed pipelines: {metrics['failed_pipelines']}"
            })

        # Check processing time
        if metrics['avg_duration_seconds'] > self.config['alerts']['thresholds']['max_processing_time']:
            alerts.append({
                'level': 'WARNING',
                'title': 'Pipeline Processing Time Alert',
                'message': f"Average duration: {metrics['avg_duration_seconds']} seconds\n" +
                          f"Threshold: {self.config['alerts']['thresholds']['max_processing_time']} seconds"
            })

        # Process alerts
        for alert in alerts:
            self._process_alert(alert)

    def _process_alert(self, alert):
        """Process and distribute alerts based on configuration."""
        # Add to alert history
        self.alert_history.append({
            'timestamp': datetime.now().isoformat(),
            'level': alert['level'],
            'title': alert['title'],
            'message': alert['message']
        })

        # Format alert message
        html_message = f"""
        <h2>{alert['title']}</h2>
        <p><strong>Level:</strong> {alert['level']}</p>
        <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <pre>{alert['message']}</pre>
        """

        # Send alerts based on configuration
        if self.config['alerts']['email']['enabled']:
            self._send_email_alert(alert['title'], html_message)

        if self.config['alerts']['teams']['enabled']:
            self._send_teams_alert(alert['title'], alert['message'])

    def save_alert_history(self, storage_account_url, container_name):
        """Save alert history to Azure Blob Storage."""
        try:
            credential = DefaultAzureCredential()
            blob_service_client = BlobServiceClient(
                account_url=storage_account_url,
                credential=credential
            )
            container_client = blob_service_client.get_container_client(container_name)

            # Create blob name with timestamp
            blob_name = f"alert_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            blob_client = container_client.get_blob_client(blob_name)

            # Upload alert history
            blob_client.upload_blob(
                json.dumps(self.alert_history, indent=2),
                overwrite=True
            )

            self.logger.info(f"Alert history saved to blob: {blob_name}")
        except Exception as e:
            self.logger.error(f"Error saving alert history: {str(e)}")

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Initialize alert manager
    alert_manager = AlertManager('config/config.yaml')
    
    # Example data quality results
    quality_results = {
        'table': 'sales',
        'checks': {
            'schema': {
                'schema_valid': False,
                'missing_columns': ['customer_id'],
                'type_mismatches': [{'column': 'quantity', 'expected': 'int', 'actual': 'string'}]
            },
            'nulls': {
                'has_nulls': True,
                'null_counts': {'product_id': 5}
            },
            'value_ranges': {
                'in_range': False,
                'violations': {'quantity_below_min': 3}
            }
        }
    }
    
    # Process results and send alerts
    alert_manager.process_data_quality_results(quality_results)
    
    # Save alert history
    alert_manager.save_alert_history(
        "https://youraccount.blob.core.windows.net",
        "monitoring"
    )
