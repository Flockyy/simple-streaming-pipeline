"""
Silver Pipeline: Kafka to Delta Lake
Consumes sensor data from Kafka, applies transformations, and writes to Delta Lake Silver layer
"""
import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
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

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot_sensors")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "output/delta/silver/sensor_data")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "checkpoints/silver")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[*]")

print("="*60)
print("SILVER PIPELINE - KAFKA TO DELTA LAKE")
print("="*60)
print(f"Kafka Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Kafka Topic: {KAFKA_TOPIC}")
print(f"Output Path: {OUTPUT_PATH}")
print(f"Checkpoint Path: {CHECKPOINT_PATH}")
print(f"Spark Master: {SPARK_MASTER_URL}")
print("="*60 + "\n")

# Wait for Kafka to be ready
print("Waiting for Kafka to be ready...")
time.sleep(15)

# Create Spark Session with Delta Lake and Kafka support
# Support both Docker (/app/jars) and local (jars/) paths
if os.path.exists("/app/jars/delta-spark_2.12-3.2.1.jar"):
    jar_path = "/app/jars/delta-spark_2.12-3.2.1.jar,/app/jars/delta-storage-3.2.1.jar"
    kafka_jars = "/app/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/app/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,/app/jars/kafka-clients-3.5.1.jar,/app/jars/commons-pool2-2.12.0.jar"
    jar_path = jar_path + "," + kafka_jars
    spark_builder = SparkSession.builder.config("spark.jars", jar_path)  # type: ignore[attr-defined]
else:
    jar_path = os.path.abspath("jars/delta-spark_2.12-3.2.1.jar") + "," + os.path.abspath("jars/delta-storage-3.2.1.jar")
    # Local: use packages for Kafka (Maven download)
    spark_builder = SparkSession.builder.config("spark.jars", jar_path).config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")  # type: ignore[attr-defined]

spark = spark_builder \
    .appName("Silver_Pipeline_Kafka_to_Delta") \
    .master(SPARK_MASTER_URL) \
    .config("spark.jars", jar_path) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .getOrCreate()  # type: ignore[attr-defined]

# Disable symlink resolution for WSL2 Docker compatibility
spark.sparkContext._jsc.hadoopConfiguration().set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
spark.sparkContext.setLogLevel("WARN")
print(f"Spark Session created (version {spark.version})\n")

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

# Read from Kafka stream
print("Connecting to Kafka stream...")
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 100) \
    .option("failOnDataLoss", "false") \
    .load()

print("✓ Kafka stream configured\n")

# Silver transformations
print("Applying Silver transformations...")
silver_stream = kafka_stream \
    .select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), sensor_schema).alias("data")
    ) \
    .select(
        col("data.*"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp")
    ) \
    .filter(col("device_id").isNotNull()) \
    .filter(col("building").isNotNull()) \
    .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn(
        "comfort_index",
        when(
            (col("type") == "temperature") & (col("value").between(20, 24)),
            "comfortable"
        ).when(
            (col("type") == "temperature") & (col("value").between(18, 26)),
            "acceptable"
        ).otherwise("N/A")
    ) \
    .withColumn(
        "air_quality",
        when((col("type") == "co2") & (col("value") <= 600), "excellent")
        .when((col("type") == "co2") & (col("value") <= 800), "good")
        .when((col("type") == "co2") & (col("value") <= 1000), "fair")
        .when((col("type") == "co2"), "poor")
        .otherwise("N/A")
    ) \
    .withColumn(
        "anomaly_detected",
        when((col("type") == "co2") & (col("value") > 1000), True)
        .when((col("type") == "temperature") & ((col("value") < 15) | (col("value") > 30)), True)
        .when((col("type") == "humidity") & ((col("value") < 20) | (col("value") > 80)), True)
        .otherwise(False)
    ) \
    .withColumn(
        "data_quality_flag",
        when(
            col("device_id").isNotNull() & 
            col("value").isNotNull() & 
            col("building").isNotNull(),
            "complete"
        ).otherwise("partial")
    ) \
    .select(
        col("device_id"),
        col("building"),
        col("floor"),
        col("type"),
        col("value"),
        col("unit"),
        col("event_timestamp"),
        col("anomaly_detected"),
        col("comfort_index"),
        col("air_quality"),
        col("data_quality_flag"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("kafka_timestamp"),
        col("processing_time")
    )

print("Transformations configured\n")

# Write to Delta Lake
print("Starting Delta Lake write stream...")
query = silver_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="5 seconds") \
    .start(OUTPUT_PATH)

print(f"Streaming query started (ID: {query.id})")
print(f"Status: {query.status}")
print("\n" + "="*60)
print("PIPELINE IS RUNNING - Processing Kafka messages...")
print("   Press Ctrl+C to stop")
print("="*60 + "\n")

try:
    # Wait for termination
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping pipeline...")
    query.stop()
    spark.stop()
    print("Pipeline stopped gracefully")
    sys.exit(0)
