CREATE DATABASE AnalisisVentas;

USE AnalisisVentas;

CREATE TABLE customers (
    CustomerID INT PRIMARY KEY,
    FirstName VARCHAR(255),
    LastName VARCHAR(255),
    Email VARCHAR(255),
    Phone VARCHAR(255),
    City VARCHAR(255),
    Country VARCHAR(255)
);

CREATE TABLE products (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(255),
    Category VARCHAR(255),
    Price DECIMAL(10, 2),
    Stock INT
);

CREATE TABLE orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderDate DATE,
    Status VARCHAR(50),
    FOREIGN KEY (CustomerID) REFERENCES customers(CustomerID)
);

CREATE TABLE order_details (
    OrderID INT,
    ProductID INT,
    Quantity INT,
    TotalPrice DECIMAL(10, 2),
    PRIMARY KEY (OrderID, ProductID),
    FOREIGN KEY (OrderID) REFERENCES orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES products(ProductID)
);

CREATE TABLE FuenteDeDatos (
    SourceID INT PRIMARY KEY IDENTITY(1,1),
    SourceName NVARCHAR(100) NOT NULL,
    SourceDescription NVARCHAR(200),
    SourceType NVARCHAR(50),
    ConnectionInfo NVARCHAR(200)
);

SELECT COUNT(*) AS TotalCustomers FROM customers;
SELECT COUNT(*) AS TotalProducts FROM products;
SELECT COUNT(*) AS TotalOrders FROM orders;
SELECT COUNT(*) AS TotalOrderDetails FROM order_details;
SELECT COUNT(*) AS TotalFuenteDeDatos FROM FuenteDeDatos;

SELECT TOP 10 * FROM customers;
SELECT TOP 10 * FROM products;
SELECT TOP 10 * FROM orders;
SELECT TOP 10 * FROM order_details;
SELECT TOP 10 * FROM FuenteDeDatos;