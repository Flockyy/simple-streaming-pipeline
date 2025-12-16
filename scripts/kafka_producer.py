"""
Kafka Producer: Reads sensor data files and sends to Kafka
"""
import os
import sys
import json
import glob
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot_sensors")
DATA_PATH = os.getenv("DATA_PATH", "data/sensor_data")
INTERVAL = float(os.getenv("INTERVAL", "0.1"))
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "0")) if os.getenv("MAX_MESSAGES") else None

print("="*60)
print("KAFKA PRODUCER - IoT SENSOR DATA")
print("="*60)
print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Kafka Topic: {KAFKA_TOPIC}")
print(f"Data Path: {DATA_PATH}")
print(f"Interval: {INTERVAL}s")
print(f"Max Messages: {MAX_MESSAGES if MAX_MESSAGES else 'unlimited'}")
print("="*60 + "\n")

# Wait for Kafka to be ready
print("⏳ Waiting for Kafka to be ready...")
time.sleep(10)

# Create Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3,
        max_in_flight_requests_per_connection=1
    )
    print("✓ Connected to Kafka broker\n")
except Exception as e:
    print(f"✗ Failed to connect to Kafka: {e}")
    sys.exit(1)

# Get all JSON files
json_files = sorted(glob.glob(f"{DATA_PATH}/*.json"))
print(f"📁 Found {len(json_files)} JSON files\n")

if not json_files:
    print("✗ No JSON files found!")
    sys.exit(1)

print("🚀 Starting to send messages to Kafka...\n")

messages_sent = 0
errors = 0

try:
    for json_file in json_files:
        with open(json_file, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    
                    # Send to Kafka
                    future = producer.send(KAFKA_TOPIC, value=data)
                    record_metadata = future.get(timeout=10)
                    
                    messages_sent += 1
                    
                    if messages_sent % 50 == 0:
                        print(f"📤 Sent {messages_sent} messages "
                              f"(device: {data.get('device_id', 'N/A')}, "
                              f"building: {data.get('building', 'N/A')})")
                    
                    # Check max messages limit
                    if MAX_MESSAGES and messages_sent >= MAX_MESSAGES:
                        print(f"\n✓ Reached limit of {MAX_MESSAGES} messages")
                        break
                    
                    time.sleep(INTERVAL)
                    
                except json.JSONDecodeError as e:
                    errors += 1
                    print(f"✗ JSON decode error: {e}")
                except KafkaError as e:
                    errors += 1
                    print(f"✗ Kafka send error: {e}")
        
        if MAX_MESSAGES and messages_sent >= MAX_MESSAGES:
            break

except KeyboardInterrupt:
    print("\n⏹️  Interrupted by user")
finally:
    producer.flush()
    producer.close()
    
    print("\n" + "="*60)
    print("📊 SUMMARY")
    print("="*60)
    print(f"Messages sent: {messages_sent}")
    print(f"Errors: {errors}")
    print("="*60)
    
    sys.exit(0)
