ARG FLINK_VERSION=2.2.0
FROM flink:${FLINK_VERSION}-scala_2.12-java17

USER root

ARG FLINK_VERSION=2.2.0
ARG KAFKA_CONNECTOR_VERSION=5.0.0-2.2
ARG KAFKA_CLIENTS_VERSION=4.2.0

# Install Python and system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       python3 \
       python3-pip \
       python3-dev \
       build-essential \
       curl \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Configure Python and install PyFlink and Couchbase SDK
RUN ln -sf /usr/bin/python3 /usr/bin/python \
    && python3 -m pip install --no-cache-dir --break-system-packages \
       apache-flink==${FLINK_VERSION} \
       --only-binary=couchbase \
       couchbase==4.3.2

# Install Flink Kafka connector
RUN curl -fsSL \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
    -o "/opt/flink/lib/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"

# Install Kafka clients dependency
RUN curl -fsSL \
    "https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
    -o "/opt/flink/lib/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"

# Install Azure filesystem plugin
RUN mkdir -p /opt/flink/plugins/azure-fs-hadoop \
    && test -f "/opt/flink/opt/flink-azure-fs-hadoop-${FLINK_VERSION}.jar" \
    && cp "/opt/flink/opt/flink-azure-fs-hadoop-${FLINK_VERSION}.jar" \
       /opt/flink/plugins/azure-fs-hadoop/

# Prepare Flink directories and OpenShift-compatible permissions
RUN mkdir -p /opt/flink/usrlib \
    && chgrp -R 0 /opt/flink \
    && chmod -R g=u /opt/flink

# Copy the PyFlink application
COPY --chown=flink:flink src/equipment_agregate.py \
    /opt/flink/usrlib/equipment_agregate.py

# Python configuration
ENV PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3
ENV PATH="/opt/flink/bin:${PATH}"

USER flink