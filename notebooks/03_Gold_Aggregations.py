# Databricks notebook source
# MAGIC %md
# MAGIC # 03: Gold Aggregations (Real-Time Aggregates & Alerts)
# MAGIC 
# MAGIC This notebook reads the clean telemetry from the **Silver** Delta table, calculates rolling averages in time windows (using PySpark event-time windowing and watermarks), and flags anomalies. It saves these high-value business insights into the **Gold** Delta table.
# MAGIC 
# MAGIC **School Kid Analogy:**
# MAGIC > The **Gold Layer** is where we make something cool out of the toys. We group similar toys together in neat little bundles (like grouping lego blocks by color in 5-minute blocks) and count them. If we find a toy that is dangerously hot, we put a red flag on it so everybody knows!
# MAGIC 
# MAGIC **Client/PM Pitch:**
# MAGIC > The **Gold Layer** contains aggregate data optimized for business intelligence, reporting, and dashboarding. Running streaming aggregations with **watermarking** ensures that late-arriving data is handled automatically. Computing these averages continuously means downstream BI queries don't have to scan billions of rows, reducing dashboard load times to milliseconds and saving query compute costs.

# COMMAND ----------

# MAGIC %run ./00_Setup_Config

# COMMAND ----------

from pyspark.sql.functions import col, window, avg, max, round, when

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read from Silver Delta Table
# MAGIC We read from our Silver table as a live data stream.

# COMMAND ----------

silver_stream = (
    spark.readStream
    .format("delta")
    .load(silver_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Windowed Aggregation & Alert Logic
# MAGIC We use:
# MAGIC 1. **Watermarking**: Tells Spark how long to wait for late-arriving data (e.g., 10 minutes).
# MAGIC 2. **Sliding Windows**: A 5-minute window that recalculates every 1 minute.
# MAGIC 3. **Anomaly Alerts**: If the maximum temperature in that window exceeds 80°C, flag it as "CRITICAL".

# COMMAND ----------

gold_stream = (
    silver_stream
    # Define watermark (max allowed delay for late data is 10 minutes)
    .withWatermark("timestamp", "10 minutes")
    # Group by device and a 5-minute sliding window (moving every 1 minute)
    .groupBy(
        col("device_id"),
        window(col("timestamp"), "5 minutes", "1 minute")
    )
    # Calculate aggregation values
    .agg(
        round(avg("temperature"), 1).alias("avg_temperature"),
        round(avg("humidity"), 1).alias("avg_humidity"),
        max("temperature").alias("max_temperature")
    )
    # Flag overheat anomalies
    .withColumn(
        "alert_status", 
        when(col("max_temperature") > 80.0, "CRITICAL").otherwise("NORMAL")
    )
    # Flatten the window struct to normal columns for easy Delta table storage
    .select(
        col("device_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temperature"),
        col("avg_humidity"),
        col("max_temperature"),
        col("alert_status")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Gold Delta Table
# MAGIC We write the aggregated stream to our Gold directory. Since we are using watermarking, Spark knows when a window is closed and can safely write the final aggregates.

# COMMAND ----------

query = (
    gold_stream.writeStream
    .format("delta")
    .option("checkpointLocation", gold_checkpoint)
    .outputMode("append") # Append mode is supported for windowed streaming with watermarks
    .start(gold_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Gold Data
# MAGIC Let's view the resulting aggregated records and check if any anomalies are flagged.

# COMMAND ----------

try:
    display(spark.read.format("delta").load(gold_path).limit(10))
except Exception as e:
    print("Wait for the stream to process a few files, then rerun this cell!")
