from datetime import datetime
import pandas as pd
import numpy as np
from great_expectations.dataset import PandasDataset
from typing import Dict, List, Optional
import logging
import json

class DataQualityChecker:
    def __init__(self):
        """Initialize the data quality checker."""
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Define expected schemas
        self.schemas = {
            'sales': {
                'date': 'datetime64[ns]',
                'product_id': 'str',
                'customer_id': 'str',
                'quantity': 'int64',
                'unit_price': 'float64',
                'total_amount': 'float64',
                'discount_amount': 'float64'
            },
            'inventory': {
                'date': 'datetime64[ns]',
                'product_id': 'str',
                'quantity_on_hand': 'int64',
                'quantity_reserved': 'int64',
                'quantity_available': 'int64'
            }
        }

    def validate_schema(self, df: pd.DataFrame, table_name: str) -> Dict:
        """Validate that the DataFrame matches the expected schema."""
        expected_schema = self.schemas.get(table_name)
        if not expected_schema:
            raise ValueError(f"No schema defined for table {table_name}")
        
        results = {
            'schema_valid': True,
            'missing_columns': [],
            'type_mismatches': []
        }
        
        # Check for missing columns
        for col, dtype in expected_schema.items():
            if col not in df.columns:
                results['schema_valid'] = False
                results['missing_columns'].append(col)
            elif str(df[col].dtype) != dtype:
                results['schema_valid'] = False
                results['type_mismatches'].append({
                    'column': col,
                    'expected_type': dtype,
                    'actual_type': str(df[col].dtype)
                })
        
        return results

    def check_nulls(self, df: pd.DataFrame, required_columns: List[str]) -> Dict:
        """Check for null values in required columns."""
        results = {
            'has_nulls': False,
            'null_counts': {}
        }
        
        for col in required_columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                results['has_nulls'] = True
                results['null_counts'][col] = null_count
        
        return results

    def check_value_ranges(self, df: pd.DataFrame, rules: Dict) -> Dict:
        """Check if values fall within expected ranges."""
        results = {
            'in_range': True,
            'violations': {}
        }
        
        for col, rule in rules.items():
            if 'min' in rule:
                mask = df[col] < rule['min']
                violations = df[mask][col].count()
                if violations > 0:
                    results['in_range'] = False
                    results['violations'][f"{col}_below_min"] = violations
            
            if 'max' in rule:
                mask = df[col] > rule['max']
                violations = df[mask][col].count()
                if violations > 0:
                    results['in_range'] = False
                    results['violations'][f"{col}_above_max"] = violations
        
        return results

    def check_referential_integrity(
        self, 
        df: pd.DataFrame, 
        ref_df: pd.DataFrame, 
        key_column: str
    ) -> Dict:
        """Check referential integrity between datasets."""
        results = {
            'integrity_maintained': True,
            'orphaned_records': 0,
            'orphaned_keys': []
        }
        
        # Find keys in df that don't exist in ref_df
        orphaned_keys = set(df[key_column]) - set(ref_df[key_column])
        if orphaned_keys:
            results['integrity_maintained'] = False
            results['orphaned_records'] = len(orphaned_keys)
            results['orphaned_keys'] = list(orphaned_keys)
        
        return results

    def check_duplicates(self, df: pd.DataFrame, key_columns: List[str]) -> Dict:
        """Check for duplicate records based on key columns."""
        duplicates = df.duplicated(subset=key_columns, keep='first')
        duplicate_records = df[duplicates]
        
        return {
            'has_duplicates': len(duplicate_records) > 0,
            'duplicate_count': len(duplicate_records),
            'duplicate_records': duplicate_records.to_dict('records') if len(duplicate_records) > 0 else []
        }

    def validate_sales_data(self, sales_df: pd.DataFrame) -> Dict:
        """Validate sales data with specific business rules."""
        results = {
            'timestamp': datetime.now().isoformat(),
            'table': 'sales',
            'record_count': len(sales_df),
            'checks': {}
        }
        
        # Schema validation
        results['checks']['schema'] = self.validate_schema(sales_df, 'sales')
        
        # Null checks
        results['checks']['nulls'] = self.check_nulls(
            sales_df, 
            ['date', 'product_id', 'customer_id', 'quantity', 'unit_price']
        )
        
        # Value range checks
        range_rules = {
            'quantity': {'min': 1, 'max': 1000},
            'unit_price': {'min': 0},
            'total_amount': {'min': 0},
            'discount_amount': {'min': 0}
        }
        results['checks']['value_ranges'] = self.check_value_ranges(sales_df, range_rules)
        
        # Duplicate checks
        results['checks']['duplicates'] = self.check_duplicates(
            sales_df,
            ['date', 'product_id', 'customer_id']
        )
        
        # Business rule: total_amount should equal quantity * unit_price * (1 - discount_rate)
        sales_df['calculated_total'] = (
            sales_df['quantity'] * 
            sales_df['unit_price'] * 
            (1 - sales_df['discount_amount']/(sales_df['quantity'] * sales_df['unit_price']))
        )
        amount_mismatch = ~np.isclose(
            sales_df['total_amount'], 
            sales_df['calculated_total'], 
            rtol=1e-05
        )
        results['checks']['amount_calculation'] = {
            'has_mismatches': amount_mismatch.any(),
            'mismatch_count': amount_mismatch.sum()
        }
        
        return results

    def validate_inventory_data(self, inventory_df: pd.DataFrame) -> Dict:
        """Validate inventory data with specific business rules."""
        results = {
            'timestamp': datetime.now().isoformat(),
            'table': 'inventory',
            'record_count': len(inventory_df),
            'checks': {}
        }
        
        # Schema validation
        results['checks']['schema'] = self.validate_schema(inventory_df, 'inventory')
        
        # Null checks
        results['checks']['nulls'] = self.check_nulls(
            inventory_df,
            ['date', 'product_id', 'quantity_on_hand', 'quantity_reserved']
        )
        
        # Value range checks
        range_rules = {
            'quantity_on_hand': {'min': 0},
            'quantity_reserved': {'min': 0},
            'quantity_available': {'min': 0}
        }
        results['checks']['value_ranges'] = self.check_value_ranges(
            inventory_df, 
            range_rules
        )
        
        # Duplicate checks
        results['checks']['duplicates'] = self.check_duplicates(
            inventory_df,
            ['date', 'product_id']
        )
        
        # Business rule: quantity_available should equal quantity_on_hand - quantity_reserved
        inventory_df['calculated_available'] = (
            inventory_df['quantity_on_hand'] - inventory_df['quantity_reserved']
        )
        quantity_mismatch = (
            inventory_df['quantity_available'] != inventory_df['calculated_available']
        )
        results['checks']['quantity_calculation'] = {
            'has_mismatches': quantity_mismatch.any(),
            'mismatch_count': quantity_mismatch.sum()
        }
        
        return results

    def save_validation_results(self, results: Dict, output_path: str):
        """Save validation results to a JSON file."""
        try:
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2)
            self.logger.info(f"Validation results saved to {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving validation results: {str(e)}")
            raise

if __name__ == "__main__":
    # Example usage
    checker = DataQualityChecker()
    
    # Generate sample data using the data generator
    from data_generator import DataGenerator
    generator = DataGenerator()
    
    # Generate sample sales data
    end_date = datetime.now()
    start_date = end_date - pd.Timedelta(days=30)
    sales_df = generator.generate_sales_data(start_date, end_date, 1000)
    
    # Generate sample inventory data
    inventory_df = generator.generate_inventory_data(end_date)
    
    # Validate data
    sales_results = checker.validate_sales_data(sales_df)
    inventory_results = checker.validate_inventory_data(inventory_df)
    
    # Save results
    checker.save_validation_results(
        sales_results,
        'data_quality_results_sales.json'
    )
    checker.save_validation_results(
        inventory_results,
        'data_quality_results_inventory.json'
    )
