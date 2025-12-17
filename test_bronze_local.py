#!/usr/bin/env python3
"""
Test script to run Bronze pipeline locally.
This version uses simpler paths and avoids the mkdir issue.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    current_timestamp,
    desc,
    input_file_name,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
import os

print("="*60)
print("Bronze Pipeline Test - Local Execution")
print("="*60)

# Paths
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))  # Script is in project root
INPUT_PATH = os.path.join(PROJECT_ROOT, "data/sensor_data")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "output_test/delta/bronze")
CHECKPOINT_PATH = os.path.join(PROJECT_ROOT, "checkpoints_test/bronze")

print(f"✓ Project root: {PROJECT_ROOT}")
print(f"✓ Input path: {INPUT_PATH}")
print(f"✓ Output path: {OUTPUT_PATH}")
print(f"✓ Checkpoint path: {CHECKPOINT_PATH}")

# Create output directories
os.makedirs(OUTPUT_PATH, exist_ok=True)
os.makedirs(CHECKPOINT_PATH, exist_ok=True)
print("✓ Directories created")

# JARs
jar1 = os.path.join(PROJECT_ROOT, "jars/delta-spark_2.12-3.2.1.jar")
jar2 = os.path.join(PROJECT_ROOT, "jars/delta-storage-3.2.1.jar")
jar_path = f"{jar1},{jar2}"

print(f"✓ JARs configured")

# Create Spark session
print("\nCreating Spark session...")
spark = SparkSession.builder \
    .appName("Bronze_Test") \
    .master("local[2]") \
    .config("spark.jars", jar_path) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(f"✓ Spark {spark.version} session created\n")

# Schema
sensor_schema = StructType([
    StructField("timestamp", StringType(), False),
    StructField("device_id", StringType(), False),
    StructField("building", StringType(), False),
    StructField("floor", IntegerType(), False),
    StructField("type", StringType(), False),
    StructField("value", DoubleType(), False),
    StructField("unit", StringType(), False)
])

# Read streaming
print("Reading JSON stream...")
json_stream = spark.readStream \
    .schema(sensor_schema) \
    .option("maxFilesPerTrigger", 10) \
    .json(INPUT_PATH)

print("✓ Stream configured\n")

# Transformations
bronze_stream = json_stream \
    .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", input_file_name()) \
    .withColumn(
        "anomaly_detected",
        when((col("type") == "co2") & (col("value") > 1000), True)
        .when((col("type") == "temperature") & ((col("value") < 15) | (col("value") > 30)), True)
        .when((col("type") == "humidity") & ((col("value") < 20) | (col("value") > 80)), True)
        .otherwise(False)
    ) \
    .select(
        "device_id", "building", "floor", "type", "value", "unit",
        "event_timestamp", "ingestion_timestamp", "anomaly_detected", "source_file"
    )

print("✓ Transformations configured\n")

# Write to Delta
print("Starting streaming query...")
print("Processing will run for 20 seconds...\n")

query = bronze_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="5 seconds") \
    .start(OUTPUT_PATH)

# Run for 20 seconds
import time
time.sleep(20)

# Check progress
if query.recentProgress:
    total_rows = 0
    for p in query.recentProgress:
        total_rows += p.get('numInputRows', 0)
    print(f"📊 Processed {total_rows} records in {len(query.recentProgress)} batches")
else:
    print("⚠️  No batches processed yet")

# Stop
query.stop()
print("\n✓ Query stopped")

# Verify output
bronze_df = spark.read.format("delta").load(OUTPUT_PATH)
count = bronze_df.count()
print(f"\n{'='*60}")
print(f"✅ SUCCESS: {count} records written to Delta Lake")
print(f"{'='*60}")

# Show sample
print("\nSample data:")
bronze_df.select("device_id", "building", "type", "value", "anomaly_detected").show(5)

# Stop Spark
spark.stop()
print("\n✓ Spark session stopped")
print("✅ Test completed successfully!")
