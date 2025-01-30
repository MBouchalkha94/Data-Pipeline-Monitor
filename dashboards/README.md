# Power BI Dashboards

This directory contains Power BI dashboard templates for the Data Warehouse Modernization project.

## Dashboard Structure

### 1. Sales Analytics Dashboard (sales_analytics.pbix)
- **Sales Overview**
  - Daily/Monthly/Yearly Sales Trends
  - Sales by Product Category
  - Sales by Region
  - Top Customers
  
- **Product Performance**
  - Product Sales Rankings
  - Category Performance
  - Profit Margins Analysis
  
- **Customer Insights**
  - Customer Segmentation
  - Purchase Patterns
  - Customer Lifetime Value

### 2. Inventory Management Dashboard (inventory_management.pbix)
- **Inventory Overview**
  - Current Stock Levels
  - Stock Movement Trends
  - Low Stock Alerts
  
- **Stock Analysis**
  - Stock Turnover Ratio
  - Days of Inventory
  - Reorder Point Analysis

### 3. Data Quality Dashboard (data_quality.pbix)
- **Quality Metrics**
  - Data Completeness
  - Data Accuracy
  - Data Freshness
  
- **Issue Tracking**
  - Quality Check Results
  - Error Trends
  - Resolution Status

## Setup Instructions

1. Open the .pbix file in Power BI Desktop
2. Configure the data source connection to your Azure SQL Database:
   - Server: your-server.database.windows.net
   - Database: your-database
   - Authentication: Azure Active Directory

3. Set up the refresh schedule in Power BI Service:
   - Daily refresh for sales and inventory data
   - Hourly refresh for data quality metrics

## Data Model

The dashboards use the following data model:

```
Fact Tables:
- FactSales
- FactInventory
- FactDataQuality

Dimension Tables:
- DimDate
- DimProduct
- DimCustomer
- DimLocation
```

## Customization

To customize the dashboards:

1. Use the template as a starting point
2. Modify visuals based on specific requirements
3. Add or remove pages as needed
4. Adjust refresh schedules based on data update frequency

## Best Practices

1. Always use the latest version of Power BI Desktop
2. Implement row-level security if required
3. Use bookmarks for different view configurations
4. Optimize data model for better performance
5. Document any custom measures or calculations
