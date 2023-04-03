-- Databricks notebook source
-- this will be a database/schema generated for an individual pipeline
use dlt_onboarding_1678976061961w7wdx60m;

-- COMMAND ----------

-- DBTITLE 1,Sales by State, $
SELECT state, SUM(ordered_products_explode.price) as sales
FROM (
  SELECT state, EXPLODE(ordered_products) as ordered_products_explode
  FROM sales_orders_cleaned 
  )
GROUP BY state

-- COMMAND ----------

-- DBTITLE 1,Sales by Loyalty Segment
SELECT loyalty_segment, SUM(ordered_products_explode.price) as sales
FROM (
  SELECT loyalty_segment, EXPLODE(ordered_products) as ordered_products_explode
  FROM sales_orders_cleaned 
  )
GROUP BY loyalty_segment

-- COMMAND ----------

-- DBTITLE 1,Total items sold by State (optional)
SELECT state, SUM(ordered_products_explode.qty) as quantity
FROM (
  SELECT state, EXPLODE(ordered_products) as ordered_products_explode
  FROM sales_orders_cleaned 
  )
GROUP BY state

-- COMMAND ----------

-- DBTITLE 1,Sales in Chicago vs Los Angeles
SELECT * from sales_order_in_la
UNION ALL
SELECT * from sales_order_in_chicago
