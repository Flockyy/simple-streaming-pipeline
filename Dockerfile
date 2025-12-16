# Use official Python image and install Spark manually
FROM python:3.11-slim

USER root

# Install Java 17 and other dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    procps \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Download and install Spark
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create app directories
RUN mkdir -p /app/data /app/output /app/checkpoints /app/scripts /app/jars

# Download Delta Lake JARs
RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
    -O /app/jars/delta-spark_2.12-3.2.1.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar \
    -O /app/jars/delta-storage-3.2.1.jar

# Download Kafka connector JAR
RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    -O /app/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    -O /app/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar \
    -O /app/jars/kafka-clients-3.5.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
    -O /app/jars/commons-pool2-2.12.0.jar

# Copy requirements and install Python packages
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application scripts
COPY scripts/ /app/scripts/

# Set working directory
WORKDIR /app

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV SPARK_CLASSPATH=/app/jars/*

# Default command (can be overridden in docker-compose)
CMD ["bash"]
