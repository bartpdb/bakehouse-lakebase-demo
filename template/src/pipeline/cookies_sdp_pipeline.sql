-- Databricks notebook source
-- DBTITLE 0,Pipeline Overview
-- MAGIC %md
-- MAGIC # Cookies Analytics Pipeline - Medallion Architecture with Lakeflow SDP
-- MAGIC
-- MAGIC This pipeline demonstrates how analytical data can be transformed and enriched to power both analytics and operational applications through Lakebase.
-- MAGIC
-- MAGIC **Pipeline Architecture:**
-- MAGIC - **Bronze Layer**: Raw data ingestion from source systems (Delta Share) + operational data from Lakebase
-- MAGIC - **Silver Layer**: Cleaned and standardized data with quality checks
-- MAGIC - **Gold Layer**: Business-level aggregations and customer insights with ML predictions
-- MAGIC
-- MAGIC **Key Features:**
-- MAGIC - Dual-source bronze layer: Delta Share (historical) + Lakebase (real-time operational)
-- MAGIC - Data cleaning following Medallion Architecture with data quality checks
-- MAGIC - Customer Lifetime Value (CLV) calculation
-- MAGIC - Churn prediction using Unity Catalog ML models

-- COMMAND ----------

-- DBTITLE 0,Bronze Layer
-- MAGIC %md
-- MAGIC ## Bronze Layer - Raw Data Ingestion
-- MAGIC
-- MAGIC The bronze layer references raw data from the Cookies Dataset (Delta Share) as temporary views,
-- MAGIC combined with real-time operational data from Lakebase for transactions.

-- COMMAND ----------

-- DBTITLE 0,Bronze: Customers
CREATE TEMPORARY VIEW bronze_customers
COMMENT "Raw customer data from source system"
AS SELECT
  *
FROM samples.bakehouse.sales_customer

-- COMMAND ----------

-- DBTITLE 0,Bronze: Franchises
CREATE TEMPORARY VIEW bronze_franchises
COMMENT "Raw franchise location data"
AS SELECT
  *
FROM samples.bakehouse.sales_franchises

-- COMMAND ----------

-- DBTITLE 0,Bronze: Transactions (Delta Share + Lakebase)
CREATE TEMPORARY VIEW bronze_transactions
COMMENT "Transaction data from Delta Share (historical) combined with new operational transactions from Lakebase"
AS
SELECT * FROM samples.bakehouse.sales_transactions
UNION ALL
SELECT
  transactionid AS transactionID,
  customerid AS customerID,
  franchiseid AS franchiseID,
  datetime AS dateTime,
  product,
  quantity,
  unitprice AS unitPrice,
  totalprice AS totalPrice,
  payment_method AS paymentMethod,
  cardnumber AS cardNumber
FROM lakebase_operational.public.transactions
WHERE datetime > (SELECT COALESCE(MAX(dateTime), '1970-01-01') FROM samples.bakehouse.sales_transactions)

-- COMMAND ----------

-- DBTITLE 0,Bronze: Supplies
CREATE TEMPORARY VIEW bronze_supplies
COMMENT "Raw supplier data"
AS SELECT
  *
FROM samples.bakehouse.sales_suppliers

-- COMMAND ----------

-- DBTITLE 0,Silver Layer
-- MAGIC %md
-- MAGIC ## Silver Layer - Cleaned and Standardized Data
-- MAGIC
-- MAGIC The silver layer applies data quality rules, standardizes formats, and ensures data consistency.

-- COMMAND ----------

-- DBTITLE 0,Silver: Customers (Cleaned)
CREATE OR REFRESH MATERIALIZED VIEW silver_customers (
  CONSTRAINT valid_customer_id EXPECT (customerID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_email EXPECT (email_address IS NOT NULL AND email_address LIKE '%@%') ON VIOLATION DROP ROW
)
COMMENT "Cleaned customer data with standardized formats and quality checks"
AS SELECT
  customerID,
  TRIM(LOWER(email_address)) as email_address,
  INITCAP(TRIM(first_name)) as first_name,
  INITCAP(TRIM(last_name)) as last_name,
  CONCAT(INITCAP(TRIM(first_name)), ' ', INITCAP(TRIM(last_name))) as full_name,
  phone_number,
  TRIM(address) as address,
  TRIM(city) as city,
  TRIM(state) as state,
  TRIM(country) as country,
  TRIM(continent) as continent,
  postal_zip_code,
  CASE
    WHEN TRIM(LOWER(gender)) IN ('m', 'male') THEN 'Male'
    WHEN TRIM(LOWER(gender)) IN ('f', 'female') THEN 'Female'
    ELSE 'Other'
  END as gender
FROM bronze_customers

-- COMMAND ----------

-- DBTITLE 0,Silver: Franchises (Cleaned)
CREATE OR REFRESH MATERIALIZED VIEW silver_franchises (
  CONSTRAINT valid_franchise_id EXPECT (franchiseID IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleaned franchise location data"
AS SELECT
  franchiseID,
  INITCAP(TRIM(name)) as franchise_name,
  TRIM(city) as city,
  TRIM(district) as district,
  TRIM(zipcode) as zipcode,
  TRIM(country) as country,
  TRIM(size) as size,
  longitude,
  latitude,
  supplierID
FROM bronze_franchises

-- COMMAND ----------

-- DBTITLE 0,Silver: Transactions (Enriched)
CREATE OR REFRESH MATERIALIZED VIEW silver_transactions (
  CONSTRAINT valid_transaction EXPECT (transactionID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount EXPECT (totalPrice > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_date EXPECT (dateTime IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleaned transaction data including real-time orders from Lakebase"
AS SELECT
  transactionID,
  customerID,
  franchiseID,
  dateTime,
  DATE(dateTime) as transaction_date,
  TRIM(product) as product,
  quantity,
  unitPrice,
  totalPrice,
  TRIM(paymentMethod) as payment_method,
  cardNumber,
  YEAR(dateTime) as transaction_year,
  MONTH(dateTime) as transaction_month,
  DAYOFWEEK(dateTime) as day_of_week,
  HOUR(dateTime) as hour_of_day
FROM bronze_transactions
WHERE totalPrice > 0

-- COMMAND ----------

-- DBTITLE 0,Silver: Supplies (Cleaned)
CREATE OR REFRESH MATERIALIZED VIEW silver_supplies (
  CONSTRAINT valid_supply_id EXPECT (supplierID IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleaned supplies and supplier data"
AS SELECT
  supplierID,
  TRIM(name) as supplier_name,
  TRIM(ingredient) as ingredient,
  TRIM(continent) as continent,
  TRIM(city) as city,
  TRIM(district) as district,
  TRIM(size) as size,
  longitude,
  latitude,
  TRIM(LOWER(approved)) as approved
FROM bronze_supplies

-- COMMAND ----------

-- DBTITLE 0,Gold Layer
-- MAGIC %md
-- MAGIC ## Gold Layer - Customer Insights & Analytics
-- MAGIC
-- MAGIC The gold layer provides business-ready tables optimized for both analytical queries and operational applications.

-- COMMAND ----------

-- DBTITLE 0,Gold: Customer Insights with ML Predictions
CREATE OR REFRESH MATERIALIZED VIEW gold_customer_insights
COMMENT "Comprehensive customer insights combining transactional data with ML-powered churn predictions"
AS
WITH customer_transactions AS (
  SELECT
    customerID,
    COUNT(DISTINCT transactionID) as total_transactions,
    SUM(totalPrice) as total_spent,
    ROUND(AVG(totalPrice), 2) as avg_order_value,
    MIN(dateTime) as first_purchase_date,
    MAX(dateTime) as last_purchase_date,
    DATEDIFF(CURRENT_DATE(), MAX(DATE(dateTime))) as days_since_last_purchase,
    COUNT(DISTINCT franchiseID) as unique_franchises_visited,
    COUNT(DISTINCT DATE(dateTime)) as unique_shopping_days,
    SUM(quantity) as total_items_purchased
  FROM silver_transactions
  GROUP BY customerID
),
customer_segmentations AS (
  SELECT
    customerID,
    CASE
      WHEN ct.total_spent >= PERCENTILE(ct.total_spent, 0.75) OVER() THEN 'High Value'
      WHEN ct.total_spent >= PERCENTILE(ct.total_spent, 0.25) OVER() THEN 'Medium Value'
      WHEN ct.total_spent > 0 THEN 'Low Value'
      ELSE 'No Purchases'
    END as customer_value_segment,
    CASE
      WHEN ct.unique_franchises_visited >= PERCENTILE(ct.unique_franchises_visited, 0.75) OVER() THEN 'High Connection'
      WHEN ct.unique_franchises_visited >= PERCENTILE(ct.unique_franchises_visited, 0.25) OVER() THEN 'Medium Connection'
      WHEN ct.unique_franchises_visited > 0 THEN 'Low Connection'
      ELSE 'No Purchases'
    END as customer_connection_segment
  FROM customer_transactions ct
)
SELECT
  c.customerID,
  c.full_name,
  c.first_name,
  c.last_name,
  c.email_address,
  c.phone_number,
  c.city,
  c.state,
  c.country,
  c.continent,
  c.gender,
  COALESCE(ct.total_transactions, 0) as total_transactions,
  COALESCE(ct.total_spent, 0) as total_spent,
  COALESCE(ct.avg_order_value, 0) as avg_order_value,
  COALESCE(ct.days_since_last_purchase, 999) as days_since_last_purchase,
  COALESCE(ct.unique_franchises_visited, 0) as unique_franchises_visited,
  COALESCE(ct.total_items_purchased, 0) as total_items_purchased,
  COALESCE(cs.customer_value_segment, 0) as customer_value_segment,
  COALESCE(cs.customer_connection_segment, 0) as customer_connection_segment,
  ct.first_purchase_date,
  ct.last_purchase_date,
  AI_QUERY(
      'cookies-churn-predictor',
      request => struct(total_transactions, total_spent, avg_order_value, unique_franchises_visited, total_items_purchased),
      returnType => 'FLOAT'
  ) as churn_probability
FROM silver_customers c
LEFT JOIN customer_transactions ct ON c.customerID = ct.customerID
LEFT JOIN customer_segmentations cs ON c.customerID = cs.customerID

-- COMMAND ----------

-- DBTITLE 0,Gold: Franchise Performance Summary
CREATE OR REFRESH MATERIALIZED VIEW gold_franchise_performance
COMMENT "Franchise-level performance metrics for operational dashboards"
AS
SELECT
  f.franchiseID,
  f.franchise_name,
  f.city,
  f.district,
  f.country,
  f.size,
  f.longitude,
  f.latitude,
  s.supplier_name,
  s.approved as supplier_approved,
  COUNT(DISTINCT t.transactionID) as total_transactions,
  COUNT(DISTINCT t.customerID) as unique_customers,
  SUM(t.totalPrice) as total_revenue,
  AVG(t.totalPrice) as avg_transaction_value,
  SUM(t.quantity) as total_items_sold,
  COUNT(DISTINCT CASE
    WHEN t.transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
    THEN t.transactionID
  END) as transactions_last_30d,
  SUM(CASE
    WHEN t.transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
    THEN t.totalPrice
    ELSE 0
  END) as revenue_last_30d,
  COUNT(DISTINCT CASE
    WHEN t.transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
    THEN t.customerID
  END) as unique_customers_last_30d,
  CURRENT_TIMESTAMP() as metric_generated_at
FROM silver_franchises f
LEFT JOIN silver_supplies s ON f.supplierID = s.supplierID
LEFT JOIN silver_transactions t ON f.franchiseID = t.franchiseID
GROUP BY
  f.franchiseID,
  f.franchise_name,
  f.city,
  f.district,
  f.country,
  f.size,
  f.longitude,
  f.latitude,
  s.supplier_name,
  s.approved

-- COMMAND ----------

-- DBTITLE 0,Gold: Product Analytics
CREATE OR REFRESH MATERIALIZED VIEW gold_product_analytics
COMMENT "Product-level performance metrics showing top sellers and trends"
AS
SELECT
  t.product,
  COUNT(DISTINCT t.transactionID) as total_transactions,
  SUM(t.quantity) as total_quantity_sold,
  SUM(t.totalPrice) as total_revenue,
  AVG(t.unitPrice) as avg_unit_price,
  COUNT(DISTINCT t.customerID) as unique_customers,
  COUNT(DISTINCT t.franchiseID) as franchises_selling,
  SUM(CASE
    WHEN t.transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
    THEN t.quantity
    ELSE 0
  END) as quantity_sold_last_30d,
  SUM(CASE
    WHEN t.transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
    THEN t.totalPrice
    ELSE 0
  END) as revenue_last_30d,
  CURRENT_TIMESTAMP() as metric_generated_at
FROM silver_transactions t
GROUP BY t.product
