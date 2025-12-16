# Simple Streaming Pipeline with Docker

Pipeline de streaming temps réel avec Apache Spark, Kafka et Delta Lake, entièrement containerisé avec Docker.

## 📋 Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ Sensor Data  │────▶│    Kafka     │────▶│    Silver    │
│  (JSON)      │     │   Producer   │     │   Pipeline   │
└──────────────┘     └──────────────┘     └──────────────┘
                             │                     │
                             ▼                     ▼
                     ┌──────────────┐     ┌──────────────┐
                     │    Kafka     │     │  Delta Lake  │
                     │   Broker     │     │   (Silver)   │
                     └──────────────┘     └──────────────┘

┌──────────────┐                         ┌──────────────┐
│ Sensor Data  │────────────────────────▶│    Bronze    │
│  (JSON)      │                         │   Pipeline   │
└──────────────┘                         └──────────────┘
                                                 │
                                                 ▼
                                         ┌──────────────┐
                                         │  Delta Lake  │
                                         │   (Bronze)   │
                                         └──────────────┘
```

## 🚀 Quick Start

### Prérequis

- Docker Engine 20.10+
- Docker Compose 2.0+
- 8 GB RAM minimum
- 10 GB espace disque libre

### Installation et Démarrage

1. **Cloner le projet**
```bash
git clone https://github.com/Flockyy/simple-streaming-pipeline.git
cd simple-streaming-pipeline
```

2. **Créer les répertoires nécessaires**
```bash
mkdir -p output checkpoints
```

3. **Builder les images Docker**
```bash
docker-compose build
```

4. **Démarrer tous les services**
```bash
docker-compose up -d
```

5. **Vérifier le statut**
```bash
docker-compose ps
```

## 📦 Services

### Infrastructure
- **Zookeeper** (port 2181) - Coordination Kafka
- **Kafka** (port 9092) - Message broker
- **Spark Master** (port 8080) - Web UI Spark
- **Spark Worker** (port 8081) - Worker Web UI

### Pipelines
- **bronze-pipeline** - JSON → Delta Lake Bronze
- **silver-pipeline** - Kafka → Delta Lake Silver
- **kafka-producer** - Envoie données vers Kafka

## 🔧 Configuration

### Variables d'Environnement

#### Bronze Pipeline
```bash
SPARK_MASTER_URL=spark://spark-master:7077
INPUT_PATH=/app/data/sensor_data
OUTPUT_PATH=/app/output/delta/bronze/sensor_data
CHECKPOINT_PATH=/app/checkpoints/bronze
```

#### Silver Pipeline
```bash
SPARK_MASTER_URL=spark://spark-master:7077
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=iot_sensors
OUTPUT_PATH=/app/output/delta/silver/sensor_data
CHECKPOINT_PATH=/app/checkpoints/silver
```

#### Kafka Producer
```bash
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=iot_sensors
DATA_PATH=/app/data/sensor_data
INTERVAL=0.1  # Secondes entre messages
MAX_MESSAGES=500  # Limite (0 = unlimited)
```

## 📊 Monitoring

### Web UIs

- **Spark Master**: http://localhost:8080
- **Spark Worker**: http://localhost:8081

### Logs

```bash
# Tous les services
docker-compose logs -f

# Service spécifique
docker-compose logs -f bronze-pipeline
docker-compose logs -f silver-pipeline
docker-compose logs -f kafka-producer

# Kafka
docker-compose logs -f kafka
```

### Vérifier les données traitées

```bash
# Bronze Delta Lake
docker-compose exec bronze-pipeline ls -lh /app/output/delta/bronze/sensor_data/

# Silver Delta Lake
docker-compose exec silver-pipeline ls -lh /app/output/delta/silver/sensor_data/
```

## 🛠️ Commandes Utiles

### Gestion des Services

```bash
# Démarrer tous les services
docker-compose up -d

# Arrêter tous les services
docker-compose down

# Redémarrer un service
docker-compose restart bronze-pipeline

# Arrêter un service spécifique
docker-compose stop silver-pipeline

# Voir les logs en temps réel
docker-compose logs -f
```

### Kafka Management

```bash
# Lister les topics
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Voir les détails d'un topic
docker-compose exec kafka kafka-topics --describe --topic iot_sensors --bootstrap-server localhost:9092

# Consommer des messages (debug)
docker-compose exec kafka kafka-console-consumer --topic iot_sensors --from-beginning --bootstrap-server localhost:9092 --max-messages 10
```

### Nettoyage

```bash
# Supprimer tous les containers et volumes
docker-compose down -v

# Nettoyer les données (attention: perte de données!)
rm -rf output/ checkpoints/

# Rebuild complet
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d
```

## 📁 Structure du Projet

```
simple-streaming-pipeline/
├── docker-compose.yml          # Orchestration des services
├── Dockerfile                  # Image Spark avec Delta/Kafka
├── requirements.txt            # Dépendances Python
├── scripts/
│   ├── bronze_pipeline.py     # Pipeline JSON → Delta Bronze
│   ├── silver_pipeline.py     # Pipeline Kafka → Delta Silver
│   └── kafka_producer.py      # Producteur Kafka
├── data/
│   └── sensor_data/           # Fichiers JSON des capteurs
├── output/                    # Delta Lake tables (créé automatiquement)
│   └── delta/
│       ├── bronze/
│       └── silver/
└── checkpoints/               # Spark checkpoints (créé automatiquement)
    ├── bronze/
    └── silver/
```

## 🔍 Données des Capteurs

### Format JSON
```json
{
  "timestamp": "2024-01-15T10:30:00",
  "device_id": "sensor_001",
  "building": "A",
  "floor": 2,
  "type": "temperature",
  "value": 22.5,
  "unit": "°C"
}
```

### Types de Capteurs
- **temperature** - Température (°C)
- **humidity** - Humidité (%)
- **co2** - Niveau CO2 (ppm)
- **energy_consumption** - Consommation énergétique (kWh)

## 🏗️ Couches de Données

### Bronze Layer
- **Source**: Fichiers JSON bruts
- **Format**: Delta Lake
- **Transformations**:
  - Parsing JSON
  - Conversion types
  - Détection anomalies basique
  - Horodatage ingestion
- **Schéma**:
  ```
  device_id, building, floor, type, value, unit,
  event_timestamp, ingestion_timestamp,
  anomaly_detected, data_quality, source_file
  ```

### Silver Layer
- **Source**: Messages Kafka
- **Format**: Delta Lake
- **Transformations**:
  - Enrichissement métadonnées Kafka
  - Calcul `comfort_index` (température)
  - Calcul `air_quality` (CO2)
  - Détection anomalies avancée
  - Flags qualité de données
- **Schéma**:
  ```
  device_id, building, floor, type, value, unit,
  event_timestamp, processing_time, anomaly_detected,
  comfort_index, air_quality, data_quality_flag,
  kafka_partition, kafka_offset, kafka_timestamp
  ```

## 🧪 Tests et Validation

### 1. Vérifier que Kafka fonctionne

```bash
# Créer un message de test
docker-compose exec kafka kafka-console-producer --topic iot_sensors --bootstrap-server localhost:9092
# Taper un message JSON et Ctrl+C

# Consommer pour vérifier
docker-compose exec kafka kafka-console-consumer --topic iot_sensors --from-beginning --bootstrap-server localhost:9092 --max-messages 1
```

### 2. Vérifier les pipelines

```bash
# Statut des queries Spark (dans les logs)
docker-compose logs bronze-pipeline | grep "Query ID"
docker-compose logs silver-pipeline | grep "Query ID"
```

### 3. Interroger les données Delta Lake

```bash
# Entrer dans le container Spark
docker-compose exec spark-master bash

# Lancer PySpark
pyspark --jars /app/jars/delta-spark_2.12-3.2.1.jar,/app/jars/delta-storage-3.2.1.jar \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# Dans PySpark
from delta.tables import DeltaTable

# Bronze
bronze_df = spark.read.format("delta").load("/app/output/delta/bronze/sensor_data")
bronze_df.count()
bronze_df.show(5)

# Silver
silver_df = spark.read.format("delta").load("/app/output/delta/silver/sensor_data")
silver_df.count()
silver_df.show(5)

# Anomalies
silver_df.filter("anomaly_detected = true").show()

# Statistiques
silver_df.groupBy("building", "type").count().show()
```

## 🐛 Troubleshooting

### Kafka ne démarre pas
```bash
# Vérifier les logs
docker-compose logs kafka

# Redémarrer Zookeeper et Kafka
docker-compose restart zookeeper kafka
```

### Pipeline bloqué
```bash
# Vérifier les logs
docker-compose logs bronze-pipeline

# Nettoyer les checkpoints et redémarrer
docker-compose down
rm -rf checkpoints/bronze/* checkpoints/silver/*
docker-compose up -d
```

### Espace disque insuffisant
```bash
# Voir l'utilisation
docker system df

# Nettoyer images/volumes inutilisés
docker system prune -a --volumes
```

### Pas de données dans Delta Lake
```bash
# Vérifier que les fichiers JSON existent
docker-compose exec bronze-pipeline ls -lh /app/data/sensor_data/

# Vérifier les logs du producteur Kafka
docker-compose logs kafka-producer

# Vérifier que Kafka reçoit des messages
docker-compose exec kafka kafka-console-consumer --topic iot_sensors --from-beginning --bootstrap-server localhost:9092 --max-messages 5
```

## 🔒 Production Considerations

### Sécurité
- Activer l'authentification Kafka (SASL/SSL)
- Configurer des secrets pour les credentials
- Utiliser des réseaux Docker isolés
- Scanner les images pour vulnérabilités

### Performance
- Augmenter resources Spark (workers, mémoire, cores)
- Tuning Kafka (partitions, replication factor)
- Optimiser Delta Lake (compaction, Z-ordering)
- Monitorer avec Prometheus/Grafana

### Haute Disponibilité
- Multiple Kafka brokers (réplication)
- Multiple Spark workers
- Backup des données Delta Lake
- Health checks et restart policies

## 📚 Ressources

- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Docker Compose Reference](https://docs.docker.com/compose/)

## 📝 License

MIT License

## 👥 Auteur

Créé pour le projet de streaming temps réel avec capteurs IoT.
