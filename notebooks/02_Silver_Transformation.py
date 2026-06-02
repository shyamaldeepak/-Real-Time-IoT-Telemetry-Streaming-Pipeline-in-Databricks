# Databricks notebook source
# MAGIC %md
# MAGIC # 02: Silver Transformation (Clean & Enrich)
# MAGIC 
# MAGIC This notebook reads the raw data from the **Bronze** Delta table, cleans it up (e.g., parsing timestamps, filtering out missing values), and writes it into a clean **Silver** Delta table.
# MAGIC 
# MAGIC **School Kid Analogy:**
# MAGIC > The **Silver Layer** is like sorting and washing your toys. We open the Bronze Box, wash off the dirt, throw away any broken toys (corrupted files), and dry them so they are neat and clean in the Silver Box.
# MAGIC 
# MAGIC **Client/PM Pitch:**
# MAGIC > The **Silver Layer** represents our "Single Source of Truth." In this layer, we apply data quality checks, schema compliance, and cast datatypes. Filtering bad data here prevents down-stream BI dashboards or machine learning models from breaking. Since Delta Lake supports **ACID transactions**, we guarantee that even in a streaming environment, our data is never left in a partially-written, corrupt state.

# COMMAND ----------

# MAGIC %run ./00_Setup_Config

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, round

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read from Bronze Delta Table
# MAGIC We read from our Bronze table as a live data stream.

# COMMAND ----------

bronze_stream = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean and Transform
# MAGIC Here we clean the data:
# MAGIC 1. Parse string timestamp to a formal Timestamp Datatype.
# MAGIC 2. Filter out rows that do not have a valid `device_id` or `timestamp`.
# MAGIC 3. Filter out records where humidity is null (demonstrating data quality cleaning).
# MAGIC 4. Ensure temperature is rounded nicely.

# COMMAND ----------

silver_stream = (
    bronze_stream
    # Parse ISO 8601 string timestamp to Timestamp type
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    # Filter out corrupted records
    .filter(col("device_id").isNotNull())
    .filter(col("timestamp").isNotNull())
    .filter(col("humidity").isNotNull())  # Exclude test corrupted values
    # Round temperature for precision consistency
    .withColumn("temperature", round(col("temperature"), 1))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Silver Delta Table
# MAGIC Write the stream to a Delta Lake format directory.

# COMMAND ----------

# Start the stream and write as a Silver Delta Table
query = (
    silver_stream.writeStream
    .format("delta")
    .option("checkpointLocation", silver_checkpoint)
    .outputMode("append")
    .start(silver_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Silver Data
# MAGIC Let's view some of the cleaned records currently stored in the Silver Delta table.

# COMMAND ----------

try:
    display(spark.read.format("delta").load(silver_path).limit(10))
except Exception as e:
    print("Wait for the stream to process a few files, then rerun this cell!")
