import logging
from datetime import datetime
import json
from typing import Dict, List, Optional
import networkx as nx
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import yaml
import pandas as pd

class DataLineageTracker:
    def __init__(self, config_path: str):
        """Initialize the data lineage tracker."""
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_path)
        self.lineage_graph = nx.DiGraph()
        self.blob_service = self._init_storage()

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

    def add_data_source(self, source_id: str, metadata: Dict):
        """Add a data source node to the lineage graph."""
        self.lineage_graph.add_node(
            source_id,
            node_type='source',
            metadata=metadata,
            timestamp=datetime.now().isoformat()
        )

    def add_transformation(self, 
                         transform_id: str, 
                         inputs: List[str], 
                         outputs: List[str], 
                         metadata: Dict):
        """Add a transformation node and its relationships."""
        # Add transformation node
        self.lineage_graph.add_node(
            transform_id,
            node_type='transformation',
            metadata=metadata,
            timestamp=datetime.now().isoformat()
        )
        
        # Add edges from inputs to transformation
        for input_id in inputs:
            self.lineage_graph.add_edge(
                input_id,
                transform_id,
                edge_type='input',
                timestamp=datetime.now().isoformat()
            )
        
        # Add edges from transformation to outputs
        for output_id in outputs:
            self.lineage_graph.add_edge(
                transform_id,
                output_id,
                edge_type='output',
                timestamp=datetime.now().isoformat()
            )

    def track_pipeline_execution(self, 
                               pipeline_id: str, 
                               steps: List[Dict]):
        """Track the execution of a data pipeline."""
        try:
            # Add pipeline node
            self.lineage_graph.add_node(
                pipeline_id,
                node_type='pipeline',
                metadata={
                    'start_time': datetime.now().isoformat(),
                    'status': 'running'
                }
            )
            
            # Track each step
            for step in steps:
                step_id = f"{pipeline_id}_{step['name']}"
                self.lineage_graph.add_node(
                    step_id,
                    node_type='pipeline_step',
                    metadata=step
                )
                self.lineage_graph.add_edge(
                    pipeline_id,
                    step_id,
                    edge_type='contains'
                )
                
            # Update pipeline status
            self.lineage_graph.nodes[pipeline_id]['metadata']['status'] = 'completed'
            self.lineage_graph.nodes[pipeline_id]['metadata']['end_time'] = \
                datetime.now().isoformat()
                
        except Exception as e:
            self.logger.error(f"Error tracking pipeline execution: {str(e)}")
            if pipeline_id in self.lineage_graph:
                self.lineage_graph.nodes[pipeline_id]['metadata']['status'] = 'failed'
                self.lineage_graph.nodes[pipeline_id]['metadata']['error'] = str(e)
            raise

    def track_data_quality_check(self, 
                               source_id: str, 
                               check_results: Dict):
        """Track data quality check results."""
        check_id = f"quality_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Add quality check node
        self.lineage_graph.add_node(
            check_id,
            node_type='quality_check',
            metadata=check_results,
            timestamp=datetime.now().isoformat()
        )
        
        # Link to source
        self.lineage_graph.add_edge(
            source_id,
            check_id,
            edge_type='quality_check'
        )

    def get_data_lineage(self, node_id: str) -> Dict:
        """Get the complete lineage for a specific node."""
        try:
            # Get upstream nodes (sources)
            upstream = nx.ancestors(self.lineage_graph, node_id)
            
            # Get downstream nodes (targets)
            downstream = nx.descendants(self.lineage_graph, node_id)
            
            # Create subgraph with all related nodes
            relevant_nodes = upstream.union(downstream).union({node_id})
            lineage_subgraph = self.lineage_graph.subgraph(relevant_nodes)
            
            return {
                'node_id': node_id,
                'upstream_nodes': list(upstream),
                'downstream_nodes': list(downstream),
                'graph': nx.node_link_data(lineage_subgraph)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting data lineage: {str(e)}")
            raise

    def export_lineage_graph(self, format: str = 'json') -> str:
        """Export the lineage graph in various formats."""
        try:
            if format == 'json':
                return nx.node_link_data(self.lineage_graph)
            elif format == 'dot':
                return nx.drawing.nx_pydot.to_pydot(self.lineage_graph).to_string()
            else:
                raise ValueError(f"Unsupported format: {format}")
                
        except Exception as e:
            self.logger.error(f"Error exporting lineage graph: {str(e)}")
            raise

    def save_lineage_history(self):
        """Save the current lineage graph to blob storage."""
        try:
            container_client = self.blob_service.get_container_client("governance")
            
            # Create blob name with timestamp
            blob_name = f"lineage_graph_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            blob_client = container_client.get_blob_client(blob_name)
            
            # Export graph and save to blob
            graph_data = self.export_lineage_graph(format='json')
            blob_client.upload_blob(
                json.dumps(graph_data, indent=2),
                overwrite=True
            )
            
            self.logger.info(f"Lineage graph saved to blob: {blob_name}")
            
        except Exception as e:
            self.logger.error(f"Error saving lineage history: {str(e)}")
            raise

    def generate_lineage_report(self) -> pd.DataFrame:
        """Generate a report of data lineage relationships."""
        try:
            relationships = []
            
            # Iterate through all edges in the graph
            for source, target, data in self.lineage_graph.edges(data=True):
                source_node = self.lineage_graph.nodes[source]
                target_node = self.lineage_graph.nodes[target]
                
                relationships.append({
                    'source_id': source,
                    'source_type': source_node.get('node_type'),
                    'target_id': target,
                    'target_type': target_node.get('node_type'),
                    'relationship_type': data.get('edge_type'),
                    'timestamp': data.get('timestamp')
                })
            
            return pd.DataFrame(relationships)
            
        except Exception as e:
            self.logger.error(f"Error generating lineage report: {str(e)}")
            raise

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize lineage tracker
    tracker = DataLineageTracker('config/config.yaml')
    
    # Example usage
    tracker.add_data_source(
        'sales_raw',
        {'format': 'csv', 'location': 'azure_blob_storage'}
    )
    
    tracker.add_transformation(
        'sales_transform_1',
        ['sales_raw'],
        ['sales_processed'],
        {'transformation_type': 'clean_and_validate'}
    )
    
    # Save lineage history
    tracker.save_lineage_history()
    
    # Generate report
    report = tracker.generate_lineage_report()
    print(report)
