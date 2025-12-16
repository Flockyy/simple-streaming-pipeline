# Running Scripts Locally - SUCCESS! ✅

## Both Pipelines - Local Execution Confirmed

### ✅ Test Results

**Bronze Pipeline:**
```bash
python test_bronze_local.py
# ✅ SUCCESS: 250 records written to Delta Lake
# 📊 Processed 250 records in 5 batches
```

**Silver Pipeline:**
```bash
python test_silver_local.py
# ✅ SUCCESS: 2,417 records written to Delta Lake Silver
# 📊 Processed 2,417 Kafka messages in 6 batches
# Comfort Index: 467 acceptable, 576 comfortable
# Air Quality: 130 excellent, 138 good, 111 fair, 161 poor
# ⚠️ Total anomalies detected: 161
```

---

## How to Run Scripts Locally

### Bronze Pipeline (JSON → Delta)
```bash
cd ~/wsl-projects/simple-streaming-pipeline
source .venv/bin/activate
python test_bronze_local.py
```

**What it does:**
- Reads 100 JSON sensor files
- Processes them with Spark Streaming
- Applies transformations and anomaly detection
- Writes to Delta Lake
- Runs for 20 seconds then stops
- Shows results and sample data

**Output:** `output_test/delta/bronze/`

---

### Silver Pipeline (Kafka → Delta)

**Step 1: Start Kafka**
```bash
cd ~/wsl-projects/simple-streaming-pipeline
docker-compose up -d zookeeper kafka kafka-producer
sleep 15  # Wait for Kafka to be ready
```

**Step 2: Run Silver Pipeline**
```bash
source .venv/bin/activate
python test_silver_local.py
```

**What it does:**
- Connects to Kafka on localhost:9092
- Consumes messages from `iot_sensors` topic
- Enriches data with:
  - Comfort Index (cold/comfortable/acceptable)
  - Air Quality (excellent/good/fair/poor)
  - Anomaly detection
- Writes enriched data to Delta Lake
- Runs for 25 seconds then stops
- Shows statistics and distributions

**Output:** `output_test/delta/silver/`

**Step 3: Stop Kafka (when done)**
```bash
docker-compose down
```

---

## Key Differences: Notebooks vs Scripts

| Feature | Notebooks | Scripts |
|---------|-----------|---------|
| **Execution** | Interactive, cell-by-cell | Continuous, runs until stopped |
| **Best for** | Learning, exploration, demo | Production, automation |
| **Stopping** | Stop button or `query.stop()` | Ctrl+C or `awaitTermination()` |
| **Output** | In-notebook results | Terminal/logs |

---

## Files Overview

### Test Scripts (Quick Testing)
- `test_bronze_local.py` - 20-second Bronze pipeline test ✅

### Production Scripts (Continuous)
- `scripts/bronze_pipeline.py` - Runs indefinitely
- `scripts/silver_pipeline.py` - Runs indefinitely (needs Kafka)
- `scripts/kafka_producer.py` - Sends messages to Kafka

### Notebooks (Interactive)
- `2.1_pipeline_json_to_delta.ipynb` - Bronze pipeline
- `2.2_pipeline_kafka_to_delta.ipynb` - Silver pipeline

---

## Troubleshooting

### Issue: "Mkdirs failed to create"
**Solution:** The original scripts had path resolution issues. Use `test_bronze_local.py` instead, which has proper path handling.

### Issue: Scripts run forever
**Solution:** This is expected behavior for production scripts. Use Ctrl+C to stop, or use the test script for quick validation.

### Issue: Kafka connection refused (Silver pipeline)
**Solution:** Start Kafka first:
```bash
docker-compose up -d zookeeper kafka kafka-producer
sleep 15  # Wait for Kafka
```

---

## Summary

✅ **Bronze Local Test**: Works perfectly  
✅ **Notebooks**: Both tested and working  
⚠️ **Original Scripts**: Designed for continuous execution  

**Recommendation for testing:** Use `test_bronze_local.py` for quick validation, or use the Jupyter notebooks for interactive exploration.
