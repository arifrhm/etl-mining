-- Step 1: Create OLTP_DB and define the schema
IF DB_ID('OLTP_DB') IS NULL
    CREATE DATABASE OLTP_DB;
GO

-- Switch to OLTP_DB for defining the transactional schema
USE OLTP_DB;
GO

-- Create Customers table
IF OBJECT_ID('dbo.Customers', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Customers (
        CustomerID INT IDENTITY(1,1) PRIMARY KEY,
        FirstName NVARCHAR(50) NOT NULL,
        LastName NVARCHAR(50) NOT NULL,
        Email NVARCHAR(100) NOT NULL,
        JoinDate DATE NOT NULL
    );
END;
GO

-- Create Products table
IF OBJECT_ID('dbo.Products', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Products (
        ProductID INT IDENTITY(1,1) PRIMARY KEY,
        ProductName NVARCHAR(100) NOT NULL,
        Price DECIMAL(10,2) NOT NULL
    );
END;
GO

-- Create Orders table
IF OBJECT_ID('dbo.Orders', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Orders (
        OrderID INT IDENTITY(1,1) PRIMARY KEY,
        CustomerID INT NOT NULL,
        OrderDate DATE NOT NULL,
        Status NVARCHAR(20) NOT NULL,
        FOREIGN KEY (CustomerID) REFERENCES dbo.Customers(CustomerID)
    );
END;
GO

-- Create OrderDetails table
IF OBJECT_ID('dbo.OrderDetails', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.OrderDetails (
        OrderDetailID INT IDENTITY(1,1) PRIMARY KEY,
        OrderID INT NOT NULL,
        ProductID INT NOT NULL,
        Quantity INT NOT NULL,
        Price DECIMAL(10,2) NOT NULL,
        FOREIGN KEY (OrderID) REFERENCES dbo.Orders(OrderID),
        FOREIGN KEY (ProductID) REFERENCES dbo.Products(ProductID)
    );
END;
GO

-- Create Sales table
IF OBJECT_ID('dbo.Sales', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Sales (
        SaleID INT IDENTITY(1,1) PRIMARY KEY,
        OrderID INT NOT NULL,
        SaleDate DATE NOT NULL,
        TotalAmount DECIMAL(12,2) NOT NULL,
        FOREIGN KEY (OrderID) REFERENCES dbo.Orders(OrderID)
    );
END;
GO

-- Step 2: Create OLAP_DB and define the stored procedure for analysis
IF DB_ID('OLAP_DB') IS NULL
    CREATE DATABASE OLAP_DB;
GO

-- Switch to OLAP_DB for creating the reporting stored procedure
USE OLAP_DB;
GO

-- Create Customers table
IF OBJECT_ID('dbo.Customers', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Customers (
        CustomerID INT IDENTITY(1,1) PRIMARY KEY,
        FirstName NVARCHAR(50) NOT NULL,
        LastName NVARCHAR(50) NOT NULL,
        Email NVARCHAR(100) NOT NULL,
        JoinDate DATE NOT NULL
    );
END;
GO

-- Create Products table
IF OBJECT_ID('dbo.Products', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Products (
        ProductID INT IDENTITY(1,1) PRIMARY KEY,
        ProductName NVARCHAR(100) NOT NULL,
        Price DECIMAL(10,2) NOT NULL
    );
END;
GO

-- Create Orders table
IF OBJECT_ID('dbo.Orders', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Orders (
        OrderID INT IDENTITY(1,1) PRIMARY KEY,
        CustomerID INT NOT NULL,
        OrderDate DATE NOT NULL,
        Status NVARCHAR(20) NOT NULL,
        FOREIGN KEY (CustomerID) REFERENCES dbo.Customers(CustomerID)
    );
END;
GO

-- Create OrderDetails table
IF OBJECT_ID('dbo.OrderDetails', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.OrderDetails (
        OrderDetailID INT IDENTITY(1,1) PRIMARY KEY,
        OrderID INT NOT NULL,
        ProductID INT NOT NULL,
        Quantity INT NOT NULL,
        Price DECIMAL(10,2) NOT NULL,
        FOREIGN KEY (OrderID) REFERENCES dbo.Orders(OrderID),
        FOREIGN KEY (ProductID) REFERENCES dbo.Products(ProductID)
    );
END;
GO

-- Create Sales table
IF OBJECT_ID('dbo.Sales', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Sales (
        SaleID INT IDENTITY(1,1) PRIMARY KEY,
        OrderID INT NOT NULL,
        SaleDate DATE NOT NULL,
        TotalAmount DECIMAL(12,2) NOT NULL,
        FOREIGN KEY (OrderID) REFERENCES dbo.Orders(OrderID)
    );
END;
GO

-- Drop the procedure if it already exists
IF OBJECT_ID('usp_GetSalesSummary', 'P') IS NOT NULL
    DROP PROCEDURE usp_GetSalesSummary;
GO

-- Create the stored procedure to aggregate sales data
CREATE PROCEDURE usp_GetSalesSummary
AS
BEGIN
    SELECT 
        p.ProductName,
        SUM(od.Quantity) AS TotalQuantity,
        SUM(od.Price * od.Quantity) AS TotalSales
    FROM 
        OLTP_DB.dbo.Sales s
    JOIN 
        OLTP_DB.dbo.OrderDetails od ON s.OrderID = od.OrderID
    JOIN 
        OLTP_DB.dbo.Products p ON od.ProductID = p.ProductID
    GROUP BY 
        p.ProductName
    ORDER BY 
        TotalSales DESC;
END;
GO
