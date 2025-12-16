"""
Bronze Pipeline: JSON Files to Delta Lake
Streams JSON sensor data files to Delta Lake Bronze layer
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration from environment variables
INPUT_PATH = os.getenv("INPUT_PATH", "data/sensor_data")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "output/delta/bronze/sensor_data")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "checkpoints/bronze")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[*]")

print("="*60)
print("BRONZE PIPELINE - JSON TO DELTA LAKE")
print("="*60)
print(f"Input Path: {INPUT_PATH}")
print(f"Output Path: {OUTPUT_PATH}")
print(f"Checkpoint Path: {CHECKPOINT_PATH}")
print(f"Spark Master: {SPARK_MASTER_URL}")
print("="*60 + "\n")

# Create Spark Session with Delta Lake support
# Support both Docker (/app/jars) and local (jars/) paths
if os.path.exists("/app/jars/delta-spark_2.12-3.2.1.jar"):
    jar_path = "/app/jars/delta-spark_2.12-3.2.1.jar,/app/jars/delta-storage-3.2.1.jar"
else:
    jar_path = os.path.abspath("jars/delta-spark_2.12-3.2.1.jar") + "," + os.path.abspath("jars/delta-storage-3.2.1.jar")

spark = SparkSession.builder \
    .appName("Bronze_Pipeline_JSON_to_Delta") \
    .master(SPARK_MASTER_URL) \
    .config("spark.jars", jar_path) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .getOrCreate()

# Disable symlink resolution for WSL2 Docker compatibility
spark.sparkContext._jsc.hadoopConfiguration().set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
spark.sparkContext.setLogLevel("WARN")
print(f"✓ Spark Session created (version {spark.version})\n")

# Define schema for sensor data
sensor_schema = StructType([
    StructField("timestamp", StringType(), False),
    StructField("device_id", StringType(), False),
    StructField("building", StringType(), False),
    StructField("floor", IntegerType(), False),
    StructField("type", StringType(), False),
    StructField("value", DoubleType(), False),
    StructField("unit", StringType(), False)
])

# Read streaming JSON files
print("📖 Setting up JSON file stream...")
# Use explicit file:// URI for Docker (WSL2 compatibility)
input_path = INPUT_PATH
json_stream = spark.readStream \
    .schema(sensor_schema) \
    .option("maxFilesPerTrigger", 5) \
    .json(input_path)

print("✓ Stream configured\n")

# Bronze transformations
print("🔧 Applying Bronze transformations...")
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
    .withColumn(
        "data_quality",
        when(
            col("device_id").isNotNull() & 
            col("building").isNotNull() & 
            col("value").isNotNull(),
            "complete"
        ).otherwise("incomplete")
    ) \
    .select(
        col("device_id"),
        col("building"),
        col("floor"),
        col("type"),
        col("value"),
        col("unit"),
        col("event_timestamp"),
        col("ingestion_timestamp"),
        col("anomaly_detected"),
        col("data_quality"),
        col("source_file")
    )

print("✓ Transformations configured\n")

# Write to Delta Lake
print("💾 Starting Delta Lake write stream...")
query = bronze_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="10 seconds") \
    .start(OUTPUT_PATH)

print(f"✓ Streaming query started (ID: {query.id})")
print(f"✓ Status: {query.status}")
print("\n" + "="*60)
print("🔄 PIPELINE IS RUNNING - Processing sensor data...")
print("   Press Ctrl+C to stop")
print("="*60 + "\n")

try:
    # Wait for termination
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n⏹️  Stopping pipeline...")
    query.stop()
    spark.stop()
    print("✓ Pipeline stopped gracefully")
    sys.exit(0)
