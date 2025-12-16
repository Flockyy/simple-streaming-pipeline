#!/usr/bin/env python3
"""
Test script to run Silver pipeline locally.
This version uses simpler paths and works with local Kafka.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import time

print("="*60)
print("Silver Pipeline Test - Local Execution")
print("="*60)

# Paths
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "output_test/delta/silver")
CHECKPOINT_PATH = os.path.join(PROJECT_ROOT, "checkpoints_test/silver")

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "iot_sensors"

print(f"✓ Project root: {PROJECT_ROOT}")
print(f"✓ Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"✓ Topic: {KAFKA_TOPIC}")
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
print("\nCreating Spark session with Kafka support...")
spark = SparkSession.builder \
    .appName("Silver_Test") \
    .master("local[2]") \
    .config("spark.jars", jar_path) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
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

# Read from Kafka
print("Connecting to Kafka...")
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

print("✓ Kafka stream configured\n")

# Parse JSON
parsed_stream = kafka_stream \
    .select(
        from_json(col("value").cast("string"), sensor_schema).alias("data"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp")
    ) \
    .select("data.*", "kafka_partition", "kafka_offset", "kafka_timestamp")

print("✓ JSON parsing configured\n")

# Silver transformations
silver_stream = parsed_stream \
    .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn(
        "comfort_index",
        when(col("type") == "temperature", 
             when(col("value") < 18, "cold")
             .when((col("value") >= 18) & (col("value") <= 24), "comfortable")
             .otherwise("acceptable")
        ).otherwise("N/A")
    ) \
    .withColumn(
        "air_quality",
        when(col("type") == "co2",
             when(col("value") < 600, "excellent")
             .when((col("value") >= 600) & (col("value") <= 800), "good")
             .when((col("value") > 800) & (col("value") <= 1000), "fair")
             .otherwise("poor")
        ).otherwise("N/A")
    ) \
    .withColumn(
        "anomaly_detected",
        when((col("type") == "co2") & (col("value") > 1000), True)
        .when((col("type") == "temperature") & ((col("value") < 15) | (col("value") > 30)), True)
        .when((col("type") == "humidity") & ((col("value") < 20) | (col("value") > 80)), True)
        .otherwise(False)
    ) \
    .select(
        "device_id", "building", "floor", "type", "value", "unit",
        "event_timestamp", "processing_time", "comfort_index", "air_quality",
        "anomaly_detected", "kafka_partition", "kafka_offset", "kafka_timestamp"
    )

print("✓ Silver transformations configured\n")

# Write to Delta
print("Starting streaming query...")
print("Processing will run for 25 seconds...\n")

query = silver_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="5 seconds") \
    .start(OUTPUT_PATH)

# Run for 25 seconds
time.sleep(25)

# Check progress
if query.recentProgress:
    total_rows = 0
    for p in query.recentProgress:
        total_rows += p.get('numInputRows', 0)
    print(f"📊 Processed {total_rows} Kafka messages in {len(query.recentProgress)} batches")
else:
    print("⚠️  No batches processed yet")

# Stop
query.stop()
print("\n✓ Query stopped")

# Verify output
silver_df = spark.read.format("delta").load(OUTPUT_PATH)
count = silver_df.count()
print(f"\n{'='*60}")
print(f"✅ SUCCESS: {count} records written to Delta Lake Silver")
print(f"{'='*60}")

# Show sample
print("\nSample enriched data:")
silver_df.select("device_id", "building", "type", "value", "comfort_index", "air_quality", "anomaly_detected") \
    .show(5, truncate=False)

# Statistics
print("\n📈 Enrichment Statistics:")
print("\nComfort Index distribution:")
silver_df.filter(col("type") == "temperature") \
    .groupBy("comfort_index").count() \
    .orderBy("comfort_index") \
    .show()

print("Air Quality distribution:")
silver_df.filter(col("type") == "co2") \
    .groupBy("air_quality").count() \
    .orderBy("air_quality") \
    .show()

anomalies = silver_df.filter(col("anomaly_detected") == True).count()
print(f"⚠️  Total anomalies detected: {anomalies}")

# Stop Spark
spark.stop()
print("\n✓ Spark session stopped")
print("✅ Test completed successfully!")
