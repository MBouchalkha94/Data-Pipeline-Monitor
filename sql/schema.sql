-- Dimension Tables
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    date_value DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    quarter INT NOT NULL,
    is_weekend BIT NOT NULL,
    is_holiday BIT NOT NULL
);

CREATE TABLE dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    country VARCHAR(50),
    city VARCHAR(50),
    effective_date DATETIME NOT NULL,
    end_date DATETIME,
    is_current BIT NOT NULL
);

CREATE TABLE dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    subcategory VARCHAR(50),
    unit_price DECIMAL(10,2),
    effective_date DATETIME NOT NULL,
    end_date DATETIME,
    is_current BIT NOT NULL
);

-- Fact Tables
CREATE TABLE fact_sales (
    sales_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_key INT FOREIGN KEY REFERENCES dim_date(date_key),
    customer_key INT FOREIGN KEY REFERENCES dim_customer(customer_key),
    product_key INT FOREIGN KEY REFERENCES dim_product(product_key),
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2),
    created_at DATETIME DEFAULT GETUTCDATE(),
    updated_at DATETIME DEFAULT GETUTCDATE()
);

CREATE TABLE fact_inventory (
    inventory_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_key INT FOREIGN KEY REFERENCES dim_date(date_key),
    product_key INT FOREIGN KEY REFERENCES dim_product(product_key),
    quantity_on_hand INT NOT NULL,
    quantity_reserved INT NOT NULL,
    quantity_available INT NOT NULL,
    created_at DATETIME DEFAULT GETUTCDATE(),
    updated_at DATETIME DEFAULT GETUTCDATE()
);

-- Indexes
CREATE NONCLUSTERED INDEX IX_fact_sales_date ON fact_sales(date_key);
CREATE NONCLUSTERED INDEX IX_fact_sales_customer ON fact_sales(customer_key);
CREATE NONCLUSTERED INDEX IX_fact_sales_product ON fact_sales(product_key);

CREATE NONCLUSTERED INDEX IX_fact_inventory_date ON fact_inventory(date_key);
CREATE NONCLUSTERED INDEX IX_fact_inventory_product ON fact_inventory(product_key);
