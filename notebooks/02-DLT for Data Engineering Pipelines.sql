-- Databricks notebook source
-- This simple pipeline will leverage DLT as a managed dataflow service. There are a few key advantages of using DLT for production; first is the ability to generate a visual dag showcasing dependencies between different tbales. Second is the use of expectations to ensure data quality. Third is the ability to seamlessly toggle between dev and prod mode. The first raw table we are working with is customer data that currently exists in cloud storage. 

CREATE INCREMENTAL LIVE TABLE customers
COMMENT "The customers buying finished products, ingested from /databricks-datasets."
TBLPROPERTIES ("quality" = "mapping")
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/customers/", "csv")

-- COMMAND ----------

-- The second raw table is sales order info that also exists in cloud storage. This is an incremental table, meaning that it will leverage autoloader under the hood to update as new files are added to the specified directory in cloud storage. This table also includes a comment, which is helpful for data discovery purposes. 

CREATE INCREMENTAL LIVE TABLE sales_orders_raw
COMMENT "The raw sales orders, ingested from /databricks-datasets."
TBLPROPERTIES ("quality" = "bronze")
AS
--SELECT * FROM cloud_files("/databricks-datasets/retail-org/sales_stream/sales_stream.json/", "json", map("cloudFiles.inferColumnTypes", "true"))
SELECT * FROM cloud_files("/databricks-datasets/retail-org/sales_orders/", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- This table is a cleaned or silver table that is the result of a join between the two raw tables above. This table also contains a constraint-- a constraint is basically a way to specifiy some data quality standard, and to indicate outcomes of what happens if a row doesn't meet the standard. In this case, the row would be dropped from the target dataset. You could also configure constraints to retain the row or to halt the entire pipeline if a constraint is not met. 

CREATE LIVE TABLE sales_orders_cleaned(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
)
PARTITIONED BY (order_datetime)
COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
TBLPROPERTIES ("quality" = "silver")
AS
SELECT f.customer_id, f.customer_name, f.number_of_line_items, f.order_datetime, f.order_number, f.ordered_products,
       c.state, c.city, c.lon, c.lat, c.units_purchased, c.loyalty_segment
  FROM LIVE.sales_orders_raw f
    LEFT JOIN LIVE.customers c
      ON c.customer_id = f.customer_id
     AND c.customer_name = f.customer_name

-- COMMAND ----------

-- This table is a business level aggregate or gold table. In this case, it is a simple slice of the cleaned data which only focuses on sales that took place in Los Angeles.

CREATE LIVE TABLE sales_order_in_la
COMMENT "Sales orders in LA."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT * FROM LIVE.sales_orders_cleaned WHERE city = 'Los Angeles'

-- COMMAND ----------

-- This table is a business level aggregate or gold table. In this case, it is a simple slice of the cleaned data which only focuses on sales that took place in Chicago.

CREATE LIVE TABLE sales_order_in_chicago
COMMENT "Sales orders in Chicago."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT * FROM LIVE.sales_orders_cleaned WHERE city = 'Chicago'
