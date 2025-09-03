-- Wait for SQL Server to be ready
WAITFOR DELAY '00:00:05';
GO

-- Create source database for demo
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'IcebergDemoSource')
BEGIN
    CREATE DATABASE IcebergDemoSource;
END
GO

-- Create target database for backload testing
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'IcebergDemoTarget')
BEGIN
    CREATE DATABASE IcebergDemoTarget;
END
GO

USE IcebergDemoSource;
GO

-- Create schema for better organization
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'sales')
BEGIN
    EXEC('CREATE SCHEMA sales');
END
GO

-- 1. Customer Dimension Table (demonstrating various string and date types)
CREATE TABLE sales.customers (
    customer_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_code NVARCHAR(20) NOT NULL UNIQUE,
    first_name NVARCHAR(100) NOT NULL,
    last_name NVARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(20),
    birth_date DATE,
    registration_date DATETIME2(7) NOT NULL DEFAULT SYSDATETIME(),
    last_login_date DATETIME2(3),
    credit_score FLOAT,
    credit_limit DECIMAL(19,4),
    is_active BIT NOT NULL DEFAULT 1,
    customer_segment NVARCHAR(50),
    notes NVARCHAR(MAX),
    profile_image VARBINARY(MAX),
    metadata_json NVARCHAR(MAX)
);
GO

-- 2. Product Catalog Table (demonstrating MONEY and various numeric types)
CREATE TABLE sales.products (
    product_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    sku VARCHAR(50) NOT NULL UNIQUE,
    product_name NVARCHAR(255) NOT NULL,
    category NVARCHAR(100) NOT NULL,
    subcategory NVARCHAR(100),
    unit_price MONEY NOT NULL,
    cost DECIMAL(19,4) NOT NULL,
    weight_kg FLOAT,
    dimensions_length_cm DECIMAL(10,2),
    dimensions_width_cm DECIMAL(10,2),
    dimensions_height_cm DECIMAL(10,2),
    is_available BIT NOT NULL DEFAULT 1,
    discontinued BIT DEFAULT 0,
    created_date DATETIME2(7) DEFAULT SYSDATETIME(),
    modified_date DATETIME2(7) DEFAULT SYSDATETIME(),
    supplier_id INT,
    reorder_level INT,
    quantity_per_unit SMALLINT,
    product_image VARBINARY(MAX)
);
GO

-- 3. Sales Transactions Table (high precision monetary and temporal data)
CREATE TABLE sales.transactions (
    transaction_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    transaction_date DATETIME2(7) NOT NULL,
    customer_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(19,4) NOT NULL,
    discount_percent DECIMAL(5,2) DEFAULT 0,
    tax_rate DECIMAL(5,4) NOT NULL,
    tax_amount DECIMAL(19,4) NOT NULL,
    total_amount DECIMAL(19,4) NOT NULL,
    payment_method NVARCHAR(50),
    currency_code CHAR(3) DEFAULT 'USD',
    exchange_rate DECIMAL(10,6) DEFAULT 1.000000,
    order_status NVARCHAR(20) NOT NULL DEFAULT 'PENDING',
    shipped_date DATETIME2(3),
    delivered_date DATE,
    warehouse_id SMALLINT,
    sales_rep_id INT,
    commission_rate DECIMAL(5,4),
    notes NVARCHAR(MAX),
    created_timestamp DATETIME2(7) DEFAULT SYSDATETIME(),
    modified_timestamp DATETIME2(7) DEFAULT SYSDATETIME(),
    FOREIGN KEY (customer_id) REFERENCES sales.customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES sales.products(product_id)
);
GO

-- 4. Order Details Table (for demonstrating complex relationships)
CREATE TABLE sales.order_details (
    order_detail_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    transaction_id BIGINT NOT NULL,
    line_item_number SMALLINT NOT NULL,
    product_variant VARCHAR(100),
    special_instructions NVARCHAR(500),
    gift_wrap BIT DEFAULT 0,
    gift_message NVARCHAR(500),
    warranty_months TINYINT,
    extended_warranty_price MONEY,
    FOREIGN KEY (transaction_id) REFERENCES sales.transactions(transaction_id),
    UNIQUE(transaction_id, line_item_number)
);
GO

-- 5. Inventory Snapshots Table (for time-series data)
CREATE TABLE sales.inventory_snapshots (
    snapshot_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    product_id BIGINT NOT NULL,
    warehouse_id SMALLINT NOT NULL,
    quantity_on_hand INT NOT NULL,
    quantity_reserved INT DEFAULT 0,
    quantity_available INT,
    reorder_point INT,
    reorder_quantity INT,
    last_counted_date DATETIME2(0),
    adjustment_reason NVARCHAR(200),
    FOREIGN KEY (product_id) REFERENCES sales.products(product_id),
    INDEX IX_snapshot_date (snapshot_date),
    INDEX IX_product_warehouse (product_id, warehouse_id)
);
GO

-- Insert test data
-- Customers (1000 rows)
DECLARE @i INT = 1;
WHILE @i <= 1000
BEGIN
    INSERT INTO sales.customers (
        customer_code, first_name, last_name, email, phone,
        birth_date, credit_score, credit_limit, customer_segment,
        notes, metadata_json
    ) VALUES (
        CONCAT('CUST', RIGHT('00000' + CAST(@i AS VARCHAR), 5)),
        CONCAT('FirstName', @i),
        CONCAT('LastName', @i),
        CONCAT('customer', @i, '@example.com'),
        CONCAT('555-', RIGHT('0000' + CAST(@i AS VARCHAR), 4)),
        DATEADD(DAY, -(@i * 30), '2000-01-01'),
        300 + (@i % 500),
        CAST(1000 + (@i * 100) AS DECIMAL(19,4)),
        CASE 
            WHEN @i % 4 = 0 THEN 'Premium'
            WHEN @i % 4 = 1 THEN 'Standard'
            WHEN @i % 4 = 2 THEN 'Basic'
            ELSE 'Enterprise'
        END,
        CASE WHEN @i % 10 = 0 THEN 'VIP customer with special requirements' ELSE NULL END,
        CASE WHEN @i % 5 = 0 THEN '{"preferences": {"newsletter": true, "sms": false}}' ELSE NULL END
    );
    SET @i = @i + 1;
END
GO

-- Products (500 rows)
DECLARE @j INT = 1;
WHILE @j <= 500
BEGIN
    INSERT INTO sales.products (
        sku, product_name, category, subcategory,
        unit_price, cost, weight_kg,
        dimensions_length_cm, dimensions_width_cm, dimensions_height_cm,
        supplier_id, reorder_level, quantity_per_unit
    ) VALUES (
        CONCAT('SKU', RIGHT('00000' + CAST(@j AS VARCHAR), 5)),
        CONCAT('Product ', @j),
        CASE 
            WHEN @j % 5 = 0 THEN 'Electronics'
            WHEN @j % 5 = 1 THEN 'Clothing'
            WHEN @j % 5 = 2 THEN 'Home'
            WHEN @j % 5 = 3 THEN 'Sports'
            ELSE 'Books'
        END,
        CONCAT('Sub', @j % 10),
        CAST(9.99 + (@j * 0.5) AS MONEY),
        CAST(5.00 + (@j * 0.3) AS DECIMAL(19,4)),
        0.1 + (@j * 0.01),
        10 + (@j % 50),
        5 + (@j % 30),
        2 + (@j % 20),
        1 + (@j % 20),
        10 + (@j % 50),
        1 + (@j % 10)
    );
    SET @j = @j + 1;
END
GO

-- Sales Transactions (10000 rows)
DECLARE @k INT = 1;
DECLARE @customer_count INT = 1000;
DECLARE @product_count INT = 500;
WHILE @k <= 10000
BEGIN
    DECLARE @cust_id INT = 1 + (@k % @customer_count);
    DECLARE @prod_id INT = 1 + (@k % @product_count);
    DECLARE @qty INT = 1 + (@k % 10);
    DECLARE @price DECIMAL(19,4) = 10.00 + (@k % 1000);
    DECLARE @discount DECIMAL(5,2) = CASE WHEN @k % 10 = 0 THEN 10.00 ELSE 0 END;
    DECLARE @tax_rate DECIMAL(5,4) = 0.0825;
    DECLARE @subtotal DECIMAL(19,4) = @qty * @price * (1 - @discount/100);
    DECLARE @tax DECIMAL(19,4) = @subtotal * @tax_rate;
    
    INSERT INTO sales.transactions (
        transaction_date, customer_id, product_id, quantity,
        unit_price, discount_percent, tax_rate, tax_amount, total_amount,
        payment_method, order_status, warehouse_id, sales_rep_id, commission_rate
    ) VALUES (
        DATEADD(MINUTE, -@k, SYSDATETIME()),
        @cust_id,
        @prod_id,
        @qty,
        @price,
        @discount,
        @tax_rate,
        @tax,
        @subtotal + @tax,
        CASE 
            WHEN @k % 4 = 0 THEN 'Credit Card'
            WHEN @k % 4 = 1 THEN 'Debit Card'
            WHEN @k % 4 = 2 THEN 'PayPal'
            ELSE 'Wire Transfer'
        END,
        CASE 
            WHEN @k % 5 = 0 THEN 'COMPLETED'
            WHEN @k % 5 = 1 THEN 'SHIPPED'
            WHEN @k % 5 = 2 THEN 'PROCESSING'
            WHEN @k % 5 = 3 THEN 'PENDING'
            ELSE 'CANCELLED'
        END,
        1 + (@k % 5),
        1 + (@k % 20),
        CASE WHEN @k % 3 = 0 THEN 0.0250 ELSE 0.0150 END
    );
    SET @k = @k + 1;
END
GO

-- Order Details (5000 rows for some transactions)
DECLARE @m INT = 1;
WHILE @m <= 5000
BEGIN
    INSERT INTO sales.order_details (
        transaction_id, line_item_number, product_variant,
        gift_wrap, warranty_months, extended_warranty_price
    ) VALUES (
        @m,
        1,
        CASE WHEN @m % 3 = 0 THEN CONCAT('Color: ', CASE @m % 5 WHEN 0 THEN 'Red' WHEN 1 THEN 'Blue' WHEN 2 THEN 'Green' WHEN 3 THEN 'Black' ELSE 'White' END) ELSE NULL END,
        CASE WHEN @m % 20 = 0 THEN 1 ELSE 0 END,
        CASE WHEN @m % 10 = 0 THEN 12 WHEN @m % 10 = 1 THEN 24 ELSE NULL END,
        CASE WHEN @m % 10 <= 1 THEN CAST(29.99 AS MONEY) ELSE NULL END
    );
    SET @m = @m + 1;
END
GO

-- Inventory Snapshots (2000 rows)
DECLARE @n INT = 1;
WHILE @n <= 2000
BEGIN
    INSERT INTO sales.inventory_snapshots (
        snapshot_date, product_id, warehouse_id, quantity_on_hand,
        quantity_reserved, quantity_available, reorder_point, reorder_quantity
    ) VALUES (
        DATEADD(DAY, -(@n % 30), CAST(GETDATE() AS DATE)),
        1 + (@n % 500),
        1 + (@n % 5),
        100 + (@n % 500),
        CASE WHEN @n % 10 = 0 THEN 10 + (@n % 20) ELSE 0 END,
        100 + (@n % 500) - CASE WHEN @n % 10 = 0 THEN 10 + (@n % 20) ELSE 0 END,
        20 + (@n % 30),
        50 + (@n % 100)
    );
    SET @n = @n + 1;
END
GO

-- Create the same schema in the target database for backload testing
USE IcebergDemoTarget;
GO

-- Create schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'sales')
BEGIN
    EXEC('CREATE SCHEMA sales');
END
GO

-- Create empty tables with same structure (for backload testing)
-- We'll create the same tables but without data
-- This allows us to test the backload functionality

-- Note: The DDL is identical to source, just no data inserted
CREATE TABLE sales.customers (
    customer_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_code NVARCHAR(20) NOT NULL UNIQUE,
    first_name NVARCHAR(100) NOT NULL,
    last_name NVARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(20),
    birth_date DATE,
    registration_date DATETIME2(7) NOT NULL DEFAULT SYSDATETIME(),
    last_login_date DATETIME2(3),
    credit_score FLOAT,
    credit_limit DECIMAL(19,4),
    is_active BIT NOT NULL DEFAULT 1,
    customer_segment NVARCHAR(50),
    notes NVARCHAR(MAX),
    profile_image VARBINARY(MAX),
    metadata_json NVARCHAR(MAX)
);
GO

CREATE TABLE sales.products (
    product_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    sku VARCHAR(50) NOT NULL UNIQUE,
    product_name NVARCHAR(255) NOT NULL,
    category NVARCHAR(100) NOT NULL,
    subcategory NVARCHAR(100),
    unit_price MONEY NOT NULL,
    cost DECIMAL(19,4) NOT NULL,
    weight_kg FLOAT,
    dimensions_length_cm DECIMAL(10,2),
    dimensions_width_cm DECIMAL(10,2),
    dimensions_height_cm DECIMAL(10,2),
    is_available BIT NOT NULL DEFAULT 1,
    discontinued BIT DEFAULT 0,
    created_date DATETIME2(7) DEFAULT SYSDATETIME(),
    modified_date DATETIME2(7) DEFAULT SYSDATETIME(),
    supplier_id INT,
    reorder_level INT,
    quantity_per_unit SMALLINT,
    product_image VARBINARY(MAX)
);
GO

CREATE TABLE sales.transactions (
    transaction_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    transaction_date DATETIME2(7) NOT NULL,
    customer_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(19,4) NOT NULL,
    discount_percent DECIMAL(5,2) DEFAULT 0,
    tax_rate DECIMAL(5,4) NOT NULL,
    tax_amount DECIMAL(19,4) NOT NULL,
    total_amount DECIMAL(19,4) NOT NULL,
    payment_method NVARCHAR(50),
    currency_code CHAR(3) DEFAULT 'USD',
    exchange_rate DECIMAL(10,6) DEFAULT 1.000000,
    order_status NVARCHAR(20) NOT NULL DEFAULT 'PENDING',
    shipped_date DATETIME2(3),
    delivered_date DATE,
    warehouse_id SMALLINT,
    sales_rep_id INT,
    commission_rate DECIMAL(5,4),
    notes NVARCHAR(MAX),
    created_timestamp DATETIME2(7) DEFAULT SYSDATETIME(),
    modified_timestamp DATETIME2(7) DEFAULT SYSDATETIME(),
    FOREIGN KEY (customer_id) REFERENCES sales.customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES sales.products(product_id)
);
GO

CREATE TABLE sales.order_details (
    order_detail_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    transaction_id BIGINT NOT NULL,
    line_item_number SMALLINT NOT NULL,
    product_variant VARCHAR(100),
    special_instructions NVARCHAR(500),
    gift_wrap BIT DEFAULT 0,
    gift_message NVARCHAR(500),
    warranty_months TINYINT,
    extended_warranty_price MONEY,
    FOREIGN KEY (transaction_id) REFERENCES sales.transactions(transaction_id),
    UNIQUE(transaction_id, line_item_number)
);
GO

CREATE TABLE sales.inventory_snapshots (
    snapshot_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    product_id BIGINT NOT NULL,
    warehouse_id SMALLINT NOT NULL,
    quantity_on_hand INT NOT NULL,
    quantity_reserved INT DEFAULT 0,
    quantity_available INT,
    reorder_point INT,
    reorder_quantity INT,
    last_counted_date DATETIME2(0),
    adjustment_reason NVARCHAR(200),
    FOREIGN KEY (product_id) REFERENCES sales.products(product_id),
    INDEX IX_snapshot_date (snapshot_date),
    INDEX IX_product_warehouse (product_id, warehouse_id)
);
GO

PRINT 'Databases and tables created successfully!';
PRINT 'Source database has test data loaded.';
PRINT 'Target database has empty tables ready for backload testing.';
GO