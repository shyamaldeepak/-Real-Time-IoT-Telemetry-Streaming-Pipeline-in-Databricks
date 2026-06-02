# Databricks notebook source
# MAGIC %md
# MAGIC # 01: Bronze Ingestion (Raw Stream Ingestion)
# MAGIC 
# MAGIC This notebook uses Databricks **Auto Loader** (`cloudFiles`) to ingest raw JSON files from our landing zone directory and save them into a raw Delta table (the **Bronze** layer).
# MAGIC 
# MAGIC **School Kid Analogy:**
# MAGIC > Think of the **Bronze Box** as our giant toy box. As soon as new toys (JSON files) arrive, a helper robot (Auto Loader) immediately picks them up and throws them into the box without sorting or cleaning them. This way, we never lose any toys!
# MAGIC 
# MAGIC **Client/PM Pitch:**
# MAGIC > **Auto Loader** provides low-latency, cost-effective ingestion of files from cloud storage (like AWS S3 or Azure ADLS). It tracks newly arrived files incrementally using file notification or directory listing queueing, meaning we never have to run expensive bucket scans. This reduces cloud costs by up to 90% for active streams.

# COMMAND ----------

# MAGIC %run ./00_Setup_Config

# COMMAND ----------

from pyspark.sql.functions import input_file_name, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Stream Using Auto Loader
# MAGIC Read files from the landing directory in real time. We append metadata about the file itself.

# COMMAND ----------

# Configure Auto Loader (cloudFiles)
raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path) # Stores schema information automatically
    .schema(iot_schema)                               # Enforce our predefined IoT schema
    .load(landing_zone_path)
)

# Add metadata columns (useful for auditing and debugging)
bronze_stream = (
    raw_stream
    .withColumn("_input_file_name", input_file_name())
    .withColumn("_ingested_time", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Bronze Delta Table
# MAGIC Write the stream to a Delta Lake format directory.

# COMMAND ----------

# Start the stream and write as a Delta Table
query = (
    bronze_stream.writeStream
    .format("delta")
    .option("checkpointLocation", bronze_checkpoint)  # Saves stream state so we can resume if stopped
    .outputMode("append")
    .start(bronze_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Ingested Data
# MAGIC Let's view some of the raw records currently stored in the Bronze Delta table.

# COMMAND ----------

# Read the delta table as a static dataframe
try:
    display(spark.read.format("delta").load(bronze_path).limit(10))
except Exception as e:
    print("Wait for the stream to process a few files, then rerun this cell!")
