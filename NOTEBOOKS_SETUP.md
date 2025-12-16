# 📓 Jupyter Notebooks Setup Guide

## Overview
This project includes two Jupyter notebooks demonstrating streaming pipelines using PySpark and Delta Lake.

## Prerequisites
✅ Already configured in your environment:
- Python 3.12.3 with virtual environment (`.venv`)
- PySpark 3.5.3
- Delta Lake 3.2.1
- Jupyter 7.5.1
- Jupyter kernel: "Streaming Pipeline (PySpark 3.5.3)"

## Notebooks

### 1. Bronze Pipeline: `2.1_pipeline_json_to_delta.ipynb`
**Purpose**: Stream JSON sensor files → Delta Lake Bronze layer

**Requirements**:
- ✅ Works 100% locally (no external services needed)
- Input data: `data/sensor_data/*.json` (100 files provided)

**What it does**:
- Reads JSON files using Spark Structured Streaming
- Validates and enriches data with metadata
- Detects anomalies (CO₂, temperature, humidity)
- Writes to Delta Lake with ACID transactions

**To run**:
1. Open `2.1_pipeline_json_to_delta.ipynb` in VS Code
2. Select kernel: "Streaming Pipeline (PySpark 3.5.3)"
3. Run all cells (Ctrl+Shift+Enter)
4. Output: `output/delta/bronze/sensor_data/`

---

### 2. Silver Pipeline: `2.2_pipeline_kafka_to_delta.ipynb`
**Purpose**: Consume Kafka messages → Delta Lake Silver layer with enrichments

**Requirements**:
- ⚠️ Requires Kafka running on `localhost:9092`
- Docker Compose recommended for Kafka setup

**What it does**:
- Consumes messages from Kafka topic `iot_sensors`
- Enriches data with business logic:
  - Comfort Index (temperature classification)
  - Air Quality (CO₂ levels)
- Tracks Kafka metadata (partition, offset, timestamp)
- Writes enriched data to Delta Lake Silver

**To run**:

#### Option A: With Docker (Recommended)
```bash
# Start Kafka + Producer
cd ~/wsl-projects/simple-streaming-pipeline
docker-compose up -d zookeeper kafka kafka-producer

# Wait 15 seconds for Kafka to be ready
sleep 15

# Open notebook and run all cells
# Output: output/delta/silver/sensor_data/

# Stop services when done
docker-compose down
```

#### Option B: Local Kafka (Advanced)
If you have Kafka installed locally:
1. Start Zookeeper and Kafka
2. Update `KAFKA_BOOTSTRAP_SERVERS` in notebook if needed
3. Run a producer to send messages to `iot_sensors` topic
4. Execute notebook cells

---

## Quick Start (Both Notebooks)

### 1. Activate Environment
```bash
cd ~/wsl-projects/simple-streaming-pipeline
source .venv/bin/activate
```

### 2. Launch Jupyter
```bash
# Option 1: From VS Code
# Just open the .ipynb files and select the kernel

# Option 2: From command line
jupyter notebook
```

### 3. Run Bronze Pipeline (Always works locally)
- Open `2.1_pipeline_json_to_delta.ipynb`
- Select kernel: "Streaming Pipeline (PySpark 3.5.3)"
- Run All Cells
- ✅ Should process 500 records from 100 JSON files

### 4. Run Silver Pipeline (Needs Kafka)
- Start Kafka: `docker-compose up -d zookeeper kafka kafka-producer`
- Wait 15 seconds
- Open `2.2_pipeline_kafka_to_delta.ipynb`
- Select kernel: "Streaming Pipeline (PySpark 3.5.3)"
- Run All Cells
- ✅ Should process ~3500+ Kafka messages

---

## Verification

### Check Bronze Output
```bash
ls -la output/delta/bronze/sensor_data/
# Should see Parquet files and _delta_log/
```

### Check Silver Output (after Kafka run)
```bash
ls -la output/delta/silver/sensor_data/
# Should see Parquet files and _delta_log/
```

### View Delta History
Run in notebook:
```python
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "output/delta/bronze/sensor_data")
delta_table.history().show()
```

---

## Troubleshooting

### Bronze Pipeline Issues
**Problem**: `FileNotFoundException` on data/sensor_data
**Solution**: Verify you're in the project root directory
```bash
pwd  # Should be ~/wsl-projects/simple-streaming-pipeline
ls data/sensor_data/  # Should show sensor_data_*.json files
```

**Problem**: JAR files not found
**Solution**: Check JARs exist
```bash
ls -lh jars/
# Should show:
# delta-spark_2.12-3.2.1.jar (5.9M)
# delta-storage-3.2.1.jar (25K)
```

### Silver Pipeline Issues
**Problem**: `Kafka cluster not available`
**Solution**: 
```bash
# Check Docker services
docker ps | grep kafka
# Should show: zookeeper, kafka, kafka-producer

# Check Kafka logs
docker logs kafka
```

**Problem**: No messages in Kafka
**Solution**: Verify producer is running
```bash
docker logs kafka-producer | tail -20
# Should show messages being sent
```

**Problem**: `kafka-python` import errors
**Solution**: Already handled - use Docker producer instead

---

## File Locations
```
~/wsl-projects/simple-streaming-pipeline/
├── 2.1_pipeline_json_to_delta.ipynb    # Bronze pipeline
├── 2.2_pipeline_kafka_to_delta.ipynb   # Silver pipeline
├── data/sensor_data/                    # Input JSON files (100 files)
├── output/delta/bronze/                 # Bronze Delta tables
├── output/delta/silver/                 # Silver Delta tables
├── checkpoints/bronze/                  # Streaming checkpoints
├── checkpoints/silver/                  # Streaming checkpoints
├── jars/                                # Delta Lake JARs
├── .venv/                               # Virtual environment
└── docker-compose.yml                   # Kafka infrastructure
```

---

## Key Concepts Demonstrated

### Bronze Notebook
- ✅ File-based streaming source
- ✅ Schema definition and validation
- ✅ Anomaly detection
- ✅ Delta Lake ACID transactions
- ✅ Checkpointing for fault tolerance
- ✅ Delta versioning and time travel

### Silver Notebook
- ✅ Kafka streaming source
- ✅ JSON deserialization
- ✅ Business enrichments (comfort, air quality)
- ✅ Kafka metadata tracking
- ✅ Processing latency monitoring
- ✅ Exactly-once semantics

---

## Next Steps
After successfully running both notebooks:
1. Explore Delta Lake features (time travel, schema evolution)
2. Modify business logic (different thresholds, new enrichments)
3. Add a Gold layer with aggregations
4. Implement real-time dashboards
5. Scale with actual Kafka cluster

---

## Support
- **Local execution**: Bronze pipeline works out-of-the-box
- **Kafka requirement**: Silver pipeline needs Kafka (Docker recommended)
- **Tested**: Both notebooks validated with successful execution
- **Environment**: Native WSL2 filesystem required (not Windows mounts)
