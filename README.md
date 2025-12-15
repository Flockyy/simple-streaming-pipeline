# Simple Streaming Pipeline - SmartTech IoT

Pipeline de streaming avec Apache Spark Structured Streaming, Apache Kafka et Delta Lake pour le traitement de données IoT en temps réel.

## 📋 Prérequis

- Python 3.9+
- uv (gestionnaire de packages et d'environnements Python)
- Apache Kafka (pour le notebook 2.2)
- Java 11+ (pour Spark)

## 🚀 Installation

### 1. Créer l'environnement virtuel avec uv

```bash
# Créer et activer l'environnement
uv venv
source .venv/bin/activate  # Linux/Mac
# ou .venv\Scripts\activate  # Windows

# Installer les dépendances
uv pip install pyspark delta-spark kafka-python jupyter notebook ipykernel
```

### 2. Configuration Jupyter

```bash
# Enregistrer le kernel Jupyter
python -m ipykernel install --user --name=streaming-pipeline --display-name="Python (Streaming Pipeline)"
```

### 3. Lancer Jupyter

```bash
jupyter notebook
# ou
jupyter lab
```

## 📚 Notebooks

### 2.1 - Pipeline JSON → Delta Lake (Bronze)
**Fichier**: `2.1_pipeline_json_to_delta.ipynb`

Pipeline simple démontrant :
- Lecture de flux JSON en streaming
- Transformations basiques (nettoyage, filtrage)
- Écriture dans Delta Lake (couche Bronze)
- Checkpointing pour tolérance aux pannes
- Monitoring du streaming

**Exécution** : Aucune dépendance externe (génère ses propres données de test)

### 2.2 - Pipeline Kafka → Delta Lake (Silver)
**Fichier**: `2.2_pipeline_kafka_to_delta.ipynb`

Pipeline avancé avec message broker :
- Simulation de capteurs IoT produisant vers Kafka
- Consommation des messages Kafka
- Transformations avancées et enrichissement
- Écriture dans Delta Lake (couche Silver)
- Analyse des offsets, partitions, consumer groups

**Prérequis** : Apache Kafka en cours d'exécution

## 🔧 Configuration Kafka (pour notebook 2.2)

### Installation Kafka

#### Avec Homebrew (Mac)
```bash
brew install kafka
brew services start zookeeper
brew services start kafka
```

#### Avec Docker
```bash
# Démarrer Kafka avec Docker Compose
cat > docker-compose.yml <<EOF
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
EOF

docker-compose up -d
```

#### Installation manuelle
```bash
# Télécharger Kafka
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka_2.13-3.6.0.tgz
cd kafka_2.13-3.6.0

# Démarrer Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties &

# Démarrer Kafka
bin/kafka-server-start.sh config/server.properties &
```

### Créer le topic Kafka

```bash
# Avec installation locale
bin/kafka-topics.sh --create \
  --topic iot_sensors \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Vérifier la création
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

## 🏗️ Architecture

```
┌─────────────────┐
│  Capteurs IoT   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   JSON Files    │──────┐
│   ou Kafka      │      │
└─────────────────┘      │
                         │
                         ▼
              ┌──────────────────────┐
              │  Spark Structured    │
              │     Streaming        │
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │    Transformations   │
              │  (Nettoyage, Enrich) │
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │   Delta Lake         │
              │  (Bronze/Silver)     │
              └──────────────────────┘
```

## 📊 Concepts Couverts

### Spark Structured Streaming
- Streaming structuré (tables infinies)
- Modes de sortie (append, update, complete)
- Checkpointing et tolérance aux pannes
- Triggers (processingTime, once, continuous)
- Fenêtres temporelles et watermarks

### Apache Kafka
- Topics, partitions, offsets
- Consumer groups
- Garanties de livraison (exactly-once)
- Rôle du message broker dans une architecture temps réel

### Delta Lake
- Transactions ACID
- Versioning et time travel
- Schema evolution
- Architecture Médaillon (Bronze/Silver/Gold)

## 🔍 Dépannage

### Erreur Java
```bash
# Vérifier Java
java -version
# Doit être Java 11 ou supérieur

# Définir JAVA_HOME si nécessaire
export JAVA_HOME=$(/usr/libexec/java_home -v 11)  # Mac
```

### Erreur Kafka Connection
```bash
# Vérifier que Kafka est en cours d'exécution
nc -zv localhost 9092

# Vérifier les logs Kafka
tail -f /tmp/kafka-logs/server.log
```

### Problèmes de performance Spark
```bash
# Augmenter la mémoire driver
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
export SPARK_DRIVER_MEMORY=4g
```

## 📝 Structure du Projet

```
simple-streaming-pipeline/
├── 2.1_pipeline_json_to_delta.ipynb    # Pipeline JSON → Bronze
├── 2.2_pipeline_kafka_to_delta.ipynb   # Pipeline Kafka → Silver
├── pyproject.toml                       # Configuration uv/pip
├── README.md                            # Ce fichier
└── .venv/                               # Environnement virtuel (créé par uv)
```

## 📚 Ressources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Architecture Médaillon](https://www.databricks.com/glossary/medallion-architecture)

## 📄 License

Ce projet est destiné à des fins pédagogiques dans le cadre de la formation SmartTech IoT.
