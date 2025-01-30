import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

class DataGenerator:
    def __init__(self):
        self.products = self._generate_products()
        self.customers = self._generate_customers()

    def _generate_products(self):
        categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports']
        subcategories = {
            'Electronics': ['Phones', 'Laptops', 'Tablets', 'Accessories'],
            'Clothing': ['Shirts', 'Pants', 'Dresses', 'Shoes'],
            'Home & Garden': ['Furniture', 'Decor', 'Kitchen', 'Garden'],
            'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Children'],
            'Sports': ['Equipment', 'Clothing', 'Accessories', 'Nutrition']
        }

        products = []
        for i in range(100):
            category = random.choice(categories)
            products.append({
                'product_id': f'P{i:03d}',
                'product_name': f'Product {i}',
                'category': category,
                'subcategory': random.choice(subcategories[category]),
                'unit_price': round(random.uniform(10, 1000), 2)
            })
        return pd.DataFrame(products)

    def _generate_customers(self):
        countries = ['USA', 'UK', 'Canada', 'Australia', 'Germany']
        cities = {
            'USA': ['New York', 'Los Angeles', 'Chicago', 'Houston'],
            'UK': ['London', 'Manchester', 'Birmingham', 'Liverpool'],
            'Canada': ['Toronto', 'Vancouver', 'Montreal', 'Calgary'],
            'Australia': ['Sydney', 'Melbourne', 'Brisbane', 'Perth'],
            'Germany': ['Berlin', 'Munich', 'Hamburg', 'Frankfurt']
        }

        customers = []
        for i in range(1000):
            country = random.choice(countries)
            customers.append({
                'customer_id': f'C{i:04d}',
                'customer_name': f'Customer {i}',
                'email': f'customer{i}@example.com',
                'country': country,
                'city': random.choice(cities[country])
            })
        return pd.DataFrame(customers)

    def generate_sales_data(self, start_date, end_date, num_transactions):
        """Generate sample sales transactions."""
        dates = pd.date_range(start=start_date, end=end_date, freq='H')
        
        transactions = []
        for _ in range(num_transactions):
            product = self.products.sample().iloc[0]
            customer = self.customers.sample().iloc[0]
            date = random.choice(dates)
            
            quantity = random.randint(1, 10)
            unit_price = product['unit_price']
            discount_rate = random.choice([0, 0, 0, 0.1, 0.2])  # 60% no discount
            
            transactions.append({
                'date': date,
                'product_id': product['product_id'],
                'customer_id': customer['customer_id'],
                'quantity': quantity,
                'unit_price': unit_price,
                'total_amount': quantity * unit_price * (1 - discount_rate),
                'discount_amount': quantity * unit_price * discount_rate
            })
        
        return pd.DataFrame(transactions)

    def generate_inventory_data(self, date):
        """Generate inventory snapshot for all products."""
        inventory = []
        for _, product in self.products.iterrows():
            quantity_on_hand = random.randint(50, 500)
            quantity_reserved = random.randint(0, min(50, quantity_on_hand))
            
            inventory.append({
                'date': date,
                'product_id': product['product_id'],
                'quantity_on_hand': quantity_on_hand,
                'quantity_reserved': quantity_reserved,
                'quantity_available': quantity_on_hand - quantity_reserved
            })
        
        return pd.DataFrame(inventory)

if __name__ == "__main__":
    # Example usage
    generator = DataGenerator()
    
    # Generate one month of sales data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    sales_data = generator.generate_sales_data(start_date, end_date, 1000)
    
    # Generate current inventory snapshot
    inventory_data = generator.generate_inventory_data(end_date)
    
    # Save to CSV files
    sales_data.to_csv('sample_sales_data.csv', index=False)
    inventory_data.to_csv('sample_inventory_data.csv', index=False)
    generator.products.to_csv('sample_products.csv', index=False)
    generator.customers.to_csv('sample_customers.csv', index=False)
