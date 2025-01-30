import logging
from datetime import datetime
import json
from typing import Dict, List, Optional
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import yaml
from dataclasses import dataclass, asdict
import hashlib

@dataclass
class ColumnMetadata:
    name: str
    data_type: str
    description: Optional[str] = None
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_reference: Optional[str] = None
    business_name: Optional[str] = None
    sensitivity_level: Optional[str] = None
    validation_rules: Optional[List[Dict]] = None
    last_modified: str = datetime.now().isoformat()

@dataclass
class TableMetadata:
    name: str
    schema: str
    description: Optional[str] = None
    owner: Optional[str] = None
    columns: List[ColumnMetadata] = None
    tags: List[str] = None
    retention_period_days: Optional[int] = None
    update_frequency: Optional[str] = None
    last_modified: str = datetime.now().isoformat()
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    data_classification: Optional[str] = None
    source_system: Optional[str] = None

class MetadataManager:
    def __init__(self, config_path: str):
        """Initialize the metadata manager."""
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_path)
        self.blob_service = self._init_storage()
        self.metadata_cache = {}

    def _load_config(self, config_path: str) -> dict:
        """Load configuration from yaml file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}")
            raise

    def _init_storage(self) -> BlobServiceClient:
        """Initialize Azure Blob Storage client."""
        credential = DefaultAzureCredential()
        return BlobServiceClient(
            account_url=f"https://{self.config['azure']['storage']['account_name']}.blob.core.windows.net",
            credential=credential
        )

    def register_table(self, table_metadata: TableMetadata):
        """Register or update table metadata."""
        try:
            # Convert metadata to dictionary
            metadata_dict = asdict(table_metadata)
            
            # Generate unique ID for the table
            table_id = f"{table_metadata.schema}.{table_metadata.name}"
            
            # Add to cache
            self.metadata_cache[table_id] = metadata_dict
            
            # Save to blob storage
            self._save_metadata(table_id, metadata_dict)
            
            self.logger.info(f"Registered metadata for table: {table_id}")
            
        except Exception as e:
            self.logger.error(f"Error registering table metadata: {str(e)}")
            raise

    def _save_metadata(self, table_id: str, metadata: Dict):
        """Save metadata to blob storage."""
        try:
            container_client = self.blob_service.get_container_client("metadata")
            blob_client = container_client.get_blob_client(f"tables/{table_id}.json")
            
            # Add version and timestamp
            metadata['_metadata'] = {
                'version': '1.0',
                'last_updated': datetime.now().isoformat(),
                'checksum': hashlib.md5(json.dumps(metadata).encode()).hexdigest()
            }
            
            # Save to blob storage
            blob_client.upload_blob(
                json.dumps(metadata, indent=2),
                overwrite=True
            )
            
        except Exception as e:
            self.logger.error(f"Error saving metadata: {str(e)}")
            raise

    def get_table_metadata(self, schema: str, table_name: str) -> TableMetadata:
        """Retrieve table metadata."""
        try:
            table_id = f"{schema}.{table_name}"
            
            # Check cache first
            if table_id in self.metadata_cache:
                return TableMetadata(**self.metadata_cache[table_id])
            
            # If not in cache, load from blob storage
            container_client = self.blob_service.get_container_client("metadata")
            blob_client = container_client.get_blob_client(f"tables/{table_id}.json")
            
            metadata_json = blob_client.download_blob().readall()
            metadata_dict = json.loads(metadata_json)
            
            # Update cache
            self.metadata_cache[table_id] = metadata_dict
            
            return TableMetadata(**metadata_dict)
            
        except Exception as e:
            self.logger.error(f"Error retrieving table metadata: {str(e)}")
            raise

    def update_table_stats(self, schema: str, table_name: str, 
                          row_count: int, size_bytes: int):
        """Update table statistics."""
        try:
            table_id = f"{schema}.{table_name}"
            metadata = self.get_table_metadata(schema, table_name)
            
            # Update statistics
            metadata.row_count = row_count
            metadata.size_bytes = size_bytes
            metadata.last_modified = datetime.now().isoformat()
            
            # Save updated metadata
            self.register_table(metadata)
            
        except Exception as e:
            self.logger.error(f"Error updating table stats: {str(e)}")
            raise

    def generate_metadata_report(self) -> pd.DataFrame:
        """Generate a report of all registered tables and their metadata."""
        try:
            container_client = self.blob_service.get_container_client("metadata")
            
            # List all table metadata files
            metadata_files = container_client.list_blobs(name_starts_with="tables/")
            
            table_info = []
            for blob in metadata_files:
                metadata_json = container_client.get_blob_client(blob).download_blob().readall()
                metadata = json.loads(metadata_json)
                
                # Extract key information
                table_info.append({
                    'schema': metadata['schema'],
                    'table_name': metadata['name'],
                    'description': metadata.get('description'),
                    'owner': metadata.get('owner'),
                    'row_count': metadata.get('row_count'),
                    'size_bytes': metadata.get('size_bytes'),
                    'last_modified': metadata.get('last_modified'),
                    'column_count': len(metadata.get('columns', [])),
                    'data_classification': metadata.get('data_classification'),
                    'retention_period_days': metadata.get('retention_period_days')
                })
            
            return pd.DataFrame(table_info)
            
        except Exception as e:
            self.logger.error(f"Error generating metadata report: {str(e)}")
            raise

    def validate_sensitive_data_handling(self) -> Dict:
        """Validate handling of sensitive data across all tables."""
        try:
            validation_results = {
                'compliant_tables': [],
                'non_compliant_tables': [],
                'issues': []
            }
            
            # Get all table metadata
            container_client = self.blob_service.get_container_client("metadata")
            metadata_files = container_client.list_blobs(name_starts_with="tables/")
            
            for blob in metadata_files:
                metadata_json = container_client.get_blob_client(blob).download_blob().readall()
                metadata = json.loads(metadata_json)
                table_id = f"{metadata['schema']}.{metadata['name']}"
                
                # Check for sensitive columns
                sensitive_columns = [
                    col for col in metadata.get('columns', [])
                    if col.get('sensitivity_level') in ['HIGH', 'MEDIUM']
                ]
                
                if sensitive_columns:
                    # Validate requirements for tables with sensitive data
                    issues = []
                    
                    if not metadata.get('data_classification'):
                        issues.append("Missing data classification")
                    
                    if not metadata.get('retention_period_days'):
                        issues.append("Missing retention period")
                    
                    if issues:
                        validation_results['non_compliant_tables'].append(table_id)
                        validation_results['issues'].append({
                            'table_id': table_id,
                            'issues': issues
                        })
                    else:
                        validation_results['compliant_tables'].append(table_id)
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Error validating sensitive data handling: {str(e)}")
            raise

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize metadata manager
    manager = MetadataManager('config/config.yaml')
    
    # Example: Register table metadata
    columns = [
        ColumnMetadata(
            name="customer_id",
            data_type="VARCHAR(50)",
            description="Unique identifier for customer",
            is_nullable=False,
            is_primary_key=True
        ),
        ColumnMetadata(
            name="email",
            data_type="VARCHAR(255)",
            description="Customer email address",
            sensitivity_level="HIGH"
        )
    ]
    
    table_metadata = TableMetadata(
        name="customers",
        schema="dbo",
        description="Customer master data",
        owner="data_team",
        columns=columns,
        tags=["customer", "master_data"],
        data_classification="CONFIDENTIAL"
    )
    
    manager.register_table(table_metadata)
    
    # Generate report
    report = manager.generate_metadata_report()
    print(report)
    
    # Validate sensitive data handling
    validation_results = manager.validate_sensitive_data_handling()
    print("\nSensitive Data Handling Validation:")
    print(json.dumps(validation_results, indent=2))
