# Databricks notebook source
# MAGIC %md
# MAGIC # 04: Dashboard & SQL Queries
# MAGIC 
# MAGIC This notebook demonstrates how to query the Delta tables using Spark SQL. We register our Delta tables as SQL views, making them accessible for business analysts to run queries, build dashboards, and set up alerts.
# MAGIC 
# MAGIC **School Kid Analogy:**
# MAGIC > The **Dashboard** is like placing your clean, sorted toy collection on a display shelf! Anyone who walks into your room can look at the shelf, count the blue legos, and see the big red warning sign without having to dig through your toy boxes.
# MAGIC 
# MAGIC **Client/PM Pitch:**
# MAGIC > By exposing clean Delta tables as SQL views, we bridge the gap between Data Engineering and Business Intelligence. Analysts can query real-time data using standard SQL without needing to know PySpark. These tables can directly power BI tools like **Databricks SQL Dashboards**, Tableau, or PowerBI, providing live updates to decision-makers with sub-second query performance.

# COMMAND ----------

# MAGIC %run ./00_Setup_Config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Delta Tables as SQL Views
# MAGIC This registers the physical Delta file locations as temporary tables in Spark's catalog.

# COMMAND ----------

# Register Bronze
spark.read.format("delta").load(bronze_path).createOrReplaceTempView("iot_bronze_view")

# Register Silver
spark.read.format("delta").load(silver_path).createOrReplaceTempView("iot_silver_view")

# Register Gold
spark.read.format("delta").load(gold_path).createOrReplaceTempView("iot_gold_view")

print("All Delta tables registered as temporary SQL views!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1: Raw Message Auditing (Bronze)
# MAGIC Let's count how many raw JSON files and records have been ingested.
# MAGIC We'll write this in SQL. In Databricks, starting a cell with `%sql` lets you run native SQL queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   _input_file_name,
# MAGIC   COUNT(*) as record_count,
# MAGIC   MIN(_ingested_time) as ingestion_start,
# MAGIC   MAX(_ingested_time) as ingestion_end
# MAGIC FROM iot_bronze_view
# MAGIC GROUP BY _input_file_name
# MAGIC ORDER BY ingestion_end DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2: Clean Telemetry Overview (Silver)
# MAGIC Let's view the average temperature and humidity of our sensors, grouped by their device ID and reported status.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   device_id,
# MAGIC   status,
# MAGIC   COUNT(*) as total_readings,
# MAGIC   ROUND(AVG(temperature), 2) as avg_temp,
# MAGIC   ROUND(AVG(humidity), 2) as avg_humidity
# MAGIC FROM iot_silver_view
# MAGIC GROUP BY device_id, status
# MAGIC ORDER BY device_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3: Real-Time Anomaly Detection (Gold)
# MAGIC Let's query our Gold table to find out if there are any devices running dangerously hot.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   device_id,
# MAGIC   window_start,
# MAGIC   window_end,
# MAGIC   avg_temperature,
# MAGIC   max_temperature,
# MAGIC   alert_status
# MAGIC FROM iot_gold_view
# MAGIC WHERE alert_status = 'CRITICAL'
# MAGIC ORDER BY window_start DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to Build a Databricks SQL Dashboard
# MAGIC To turn these queries into a visual dashboard:
# MAGIC 1. In Databricks, click on **Dashboards** in the left sidebar (or click the **"Preview Dashboards"** button at the top right of the notebook).
# MAGIC 2. Click **Create Dashboard**.
# MAGIC 3. Add widgets based on these SQL queries:
# MAGIC    * **Bar Chart**: Show the number of CRITICAL alerts by `device_id`.
# MAGIC    * **Line Chart**: Plot `avg_temperature` over time (`window_start`).
# MAGIC    * **KPI Counter**: Show total records ingested in the last hour.
