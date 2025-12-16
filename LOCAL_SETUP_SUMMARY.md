# Local Setup Summary - Simple Streaming Pipeline

## ✅ Environment Configuration Complete

### Installed Software
- **Java**: OpenJDK 17.0.17
- **Python**: 3.12.3 (via uv venv)
- **PySpark**: 3.5.3
- **Delta Lake**: 3.2.1
- **Kafka Python**: 2.0.2
- **Jupyter**: 7.5.1 with IPyKernel

### JARs Downloaded
- `delta-spark_2.12-3.2.1.jar` (5.9M)
- `delta-storage-3.2.1.jar` (25K)

### Environment Variables
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

### Python Virtual Environment
```bash
source .venv/bin/activate
```

### Jupyter Kernel
- Name: `streaming-pipeline`
- Display: "Streaming Pipeline (PySpark 3.5.3)"
- Location: `~/.local/share/jupyter/kernels/streaming-pipeline/`

## ✅ Bronze Pipeline Tested

### Test Results
- **Status**: ✅ Working
- **Records Processed**: 425
- **Anomalies Detected**: 27
- **Output**: `output/delta/bronze/sensor_data/`
- **Parquet Files**: 15 files created

### Sample Data
```
device_id       | building | type        | value | anomaly_detected
sensor-temp-003 | B        | temperature | 27.8  | false
sensor-co2-020  | A        | co2         | 645.0 | false
sensor-hum-002  | A        | humidity    | 41.3  | false
```

### Anomaly Detection
- CO2 > 1000 ppm
- Temperature < 15°C or > 30°C
- Humidity < 20% or > 80%

## 📂 Directory Structure
```
simple-streaming-pipeline/
├── data/
│   └── sensor_data/          # 100 JSON sensor files
├── jars/
│   ├── delta-spark_2.12-3.2.1.jar
│   └── delta-storage-3.2.1.jar
├── scripts/
│   ├── bronze_pipeline.py    # JSON → Delta Bronze (TESTED ✅)
│   ├── silver_pipeline.py    # Kafka → Delta Silver  
│   └── kafka_producer.py     # Kafka producer
├── output/
│   └── delta/
│       ├── bronze/           # Bronze Delta tables
│       └── silver/           # Silver Delta tables
├── checkpoints/
│   ├── bronze/               # Bronze checkpoints
│   └── silver/               # Silver checkpoints
├── .venv/                    # Python virtual environment
├── docker-compose.yml        # Docker orchestration
├── Dockerfile                # Spark + Delta + Kafka image
├── requirements.txt          # Python dependencies
└── test_local_setup.py       # Environment test script
```

## 🚀 Usage

### Run Bronze Pipeline (Tested ✅)
```bash
source .venv/bin/activate
python scripts/bronze_pipeline.py
```

### Run Silver Pipeline (Requires Kafka)
```bash
# Start Kafka first (Docker or local)
docker-compose up -d zookeeper kafka

# Run Silver pipeline
source .venv/bin/activate
python scripts/silver_pipeline.py
```

### Run Kafka Producer
```bash
source .venv/bin/activate
python scripts/kafka_producer.py
```

### Launch Jupyter Notebook
```bash
source .venv/bin/activate
jupyter notebook

# Select kernel: "Streaming Pipeline (PySpark 3.5.3)"
```

### Query Delta Lake Data
```python
from pyspark.sql import SparkSession
import os

jar_path = os.path.abspath('jars/delta-spark_2.12-3.2.1.jar') + ',' + \
           os.path.abspath('jars/delta-storage-3.2.1.jar')

spark = SparkSession.builder \
    .appName('QueryDelta') \
    .config('spark.jars', jar_path) \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()

# Query Bronze
bronze_df = spark.read.format('delta').load('output/delta/bronze/sensor_data')
bronze_df.show()

# Query Silver (when available)
silver_df = spark.read.format('delta').load('output/delta/silver/sensor_data')
silver_df.show()
```

## 🐳 Docker Alternative

### Build and Run with Docker
```bash
# Build images
docker-compose build

# Start all services
docker-compose up -d

# Scale Spark workers
docker-compose up -d --scale spark-worker=3

# Check logs
docker-compose logs -f bronze-pipeline
docker-compose logs -f silver-pipeline

# Stop everything
docker-compose down
```

### Docker Services
- **Zookeeper**: Kafka coordination (port 2181)
- **Kafka**: Message broker (port 9092)
- **Spark Master**: Cluster manager (port 8080)
- **Spark Worker(s)**: Processing nodes (port 8081+)
- **Bronze Pipeline**: JSON → Delta Lake
- **Silver Pipeline**: Kafka → Delta Lake
- **Kafka Producer**: Sends sensor data to Kafka

## 📊 Data Flow

### Bronze Layer (Tested ✅)
```
JSON Files → Spark Streaming → Transformations → Delta Lake
                                  ├─ Type conversion
                                  ├─ Timestamp parsing
                                  ├─ Anomaly detection
                                  └─ Data quality flags
```

### Silver Layer (Requires Kafka)
```
Kafka Topic → Spark Streaming → Enrichment → Delta Lake
                                  ├─ Comfort index
                                  ├─ Air quality
                                  ├─ Anomaly detection
                                  └─ Kafka metadata
```

## 🔍 Verification Commands

### Check Java
```bash
java -version
# Should show: openjdk version "17.0.17"
```

### Check Python Packages
```bash
source .venv/bin/activate
python -c "import pyspark; print(pyspark.__version__)"
# Should show: 3.5.3
```

### Check Delta Lake
```bash
ls -lh jars/
# Should show both JAR files
```

### Check Bronze Output
```bash
ls -lh output/delta/bronze/sensor_data/*.parquet | wc -l
# Should show multiple Parquet files
```

### Test Environment
```bash
source .venv/bin/activate
python test_local_setup.py
# Should show: "ALL TESTS PASSED"
```

## 🎯 Next Steps

1. **For Kafka Testing**: Start Kafka with Docker
   ```bash
   docker-compose up -d zookeeper kafka
   ```

2. **Test Silver Pipeline**: Requires running Kafka
   ```bash
   python scripts/kafka_producer.py  # Terminal 1
   python scripts/silver_pipeline.py  # Terminal 2
   ```

3. **Create Notebooks**: Convert scripts to Jupyter notebooks for interactive analysis

4. **Add Gold Layer**: Create aggregated analytics tables

5. **Monitoring**: Add Prometheus/Grafana for metrics

## ⚠️ Troubleshooting

### Issue: Module not found
```bash
# Ensure venv is activated
source .venv/bin/activate
```

### Issue: Java not found
```bash
# Check JAVA_HOME
echo $JAVA_HOME
# Should show: /usr/lib/jvm/java-17-openjdk-amd64

# Source zshrc if needed
source ~/.zshrc
```

### Issue: Delta Lake errors
```bash
# Verify JARs exist
ls -lh jars/

# Check JAR path in scripts
# Should use: os.path.abspath('jars/...')
```

### Issue: Port already in use (Spark UI)
```bash
# Spark will automatically try next port
# 4040 → 4041 → 4042...
```

## 📝 Notes

- Bronze pipeline tested and working ✅
- Silver pipeline requires Kafka to be running
- All paths updated for local execution (no Docker /app/ paths)
- Environment is clean and reproducible
- Both Docker and local execution supported

---

**Status**: Environment ready for local development! 🚀
**Bronze Pipeline**: Tested and validated ✅
**Silver Pipeline**: Ready to test with Kafka
