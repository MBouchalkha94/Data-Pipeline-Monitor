import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etl.python.data_quality import DataQualityChecker
from etl.python.data_generator import DataGenerator

class TestDataQuality(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.checker = DataQualityChecker()
        cls.generator = DataGenerator()
        
        # Generate sample data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        cls.sales_df = cls.generator.generate_sales_data(start_date, end_date, 100)
        cls.inventory_df = cls.generator.generate_inventory_data(end_date)

    def test_schema_validation(self):
        """Test schema validation functionality."""
        # Test valid schema
        results = self.checker.validate_schema(self.sales_df, 'sales')
        self.assertTrue(results['schema_valid'])
        self.assertEqual(len(results['missing_columns']), 0)
        self.assertEqual(len(results['type_mismatches']), 0)
        
        # Test invalid schema
        invalid_df = self.sales_df.copy()
        invalid_df['quantity'] = invalid_df['quantity'].astype(str)
        results = self.checker.validate_schema(invalid_df, 'sales')
        self.assertFalse(results['schema_valid'])
        self.assertTrue(any(mismatch['column'] == 'quantity' 
                          for mismatch in results['type_mismatches']))

    def test_null_checks(self):
        """Test null value detection."""
        # Test data without nulls
        results = self.checker.check_nulls(
            self.sales_df,
            ['date', 'product_id', 'customer_id']
        )
        self.assertFalse(results['has_nulls'])
        
        # Test data with nulls
        df_with_nulls = self.sales_df.copy()
        df_with_nulls.loc[0, 'product_id'] = None
        results = self.checker.check_nulls(
            df_with_nulls,
            ['date', 'product_id', 'customer_id']
        )
        self.assertTrue(results['has_nulls'])
        self.assertEqual(results['null_counts']['product_id'], 1)

    def test_value_ranges(self):
        """Test value range validation."""
        rules = {
            'quantity': {'min': 1, 'max': 1000},
            'unit_price': {'min': 0}
        }
        
        # Test valid ranges
        results = self.checker.check_value_ranges(self.sales_df, rules)
        self.assertTrue(results['in_range'])
        
        # Test invalid ranges
        invalid_df = self.sales_df.copy()
        invalid_df.loc[0, 'quantity'] = 0  # Below min
        results = self.checker.check_value_ranges(invalid_df, rules)
        self.assertFalse(results['in_range'])
        self.assertEqual(results['violations']['quantity_below_min'], 1)

    def test_duplicate_detection(self):
        """Test duplicate record detection."""
        # Test data without duplicates
        results = self.checker.check_duplicates(
            self.sales_df,
            ['date', 'product_id', 'customer_id']
        )
        self.assertFalse(results['has_duplicates'])
        
        # Test data with duplicates
        df_with_duplicates = pd.concat([self.sales_df, 
                                      self.sales_df.iloc[[0]]])
        results = self.checker.check_duplicates(
            df_with_duplicates,
            ['date', 'product_id', 'customer_id']
        )
        self.assertTrue(results['has_duplicates'])
        self.assertEqual(results['duplicate_count'], 1)

    def test_sales_validation(self):
        """Test complete sales data validation."""
        results = self.checker.validate_sales_data(self.sales_df)
        
        # Check overall structure
        self.assertIn('timestamp', results)
        self.assertIn('table', results)
        self.assertIn('record_count', results)
        self.assertIn('checks', results)
        
        # Check specific validations
        checks = results['checks']
        self.assertTrue(checks['schema']['schema_valid'])
        self.assertFalse(checks['nulls']['has_nulls'])
        self.assertTrue(checks['value_ranges']['in_range'])
        self.assertFalse(checks['duplicates']['has_duplicates'])

    def test_inventory_validation(self):
        """Test complete inventory data validation."""
        results = self.checker.validate_inventory_data(self.inventory_df)
        
        # Check overall structure
        self.assertIn('timestamp', results)
        self.assertIn('table', results)
        self.assertIn('record_count', results)
        self.assertIn('checks', results)
        
        # Check specific validations
        checks = results['checks']
        self.assertTrue(checks['schema']['schema_valid'])
        self.assertFalse(checks['nulls']['has_nulls'])
        self.assertTrue(checks['value_ranges']['in_range'])
        self.assertFalse(checks['duplicates']['has_duplicates'])
        
        # Check quantity calculation
        self.assertFalse(checks['quantity_calculation']['has_mismatches'])

    def test_edge_cases(self):
        """Test handling of edge cases."""
        # Test empty DataFrame
        empty_df = pd.DataFrame(columns=self.sales_df.columns)
        results = self.checker.validate_sales_data(empty_df)
        self.assertEqual(results['record_count'], 0)
        
        # Test single row DataFrame
        single_row_df = self.sales_df.iloc[[0]]
        results = self.checker.validate_sales_data(single_row_df)
        self.assertEqual(results['record_count'], 1)
        
        # Test very large values
        large_values_df = self.sales_df.copy()
        large_values_df.loc[0, 'quantity'] = 1e9
        results = self.checker.validate_sales_data(large_values_df)
        self.assertFalse(results['checks']['value_ranges']['in_range'])

if __name__ == '__main__':
    unittest.main()
