# Suposiciones y variables de laboratorio

## Confirmado por el usuario
- Azure Storage Account: `flinkstorage911`.
- Docker Hub namespace: `xlen911`.
- Docker image repository: `xlen911/application-mode`.
- Contenedor Azure HA: `openshift-ha` (ya creado).

## Valores simulados para la PoC
- Kafka Strimzi namespace: `kafka`.
- Kafka cluster name: `lab-kafka`.
- Kafka bootstrap Service: `lab-kafka-kafka-bootstrap.kafka.svc:9092`.
- Kafka topic: `equipment-events`.
- Consumer group: `equipment-agregate-1l`.
- Couchbase sink: `console` inicialmente, sin conexión externa.
- Azure checkpoint container: `flink-checkpoints`.
- Azure savepoint container: `flink-savepoints`.
- Azure path base: `flink/application-mode`.

Los valores de Kafka son convenciones de ejemplo para un cluster Strimzi. No se instala ni se simula un broker real mediante este repositorio. Hay que validar el Service y el tópico en el cluster antes de esperar consumo de mensajes. El código usa salida a consola hasta configurar Couchbase.

## Secretos que no deben versionarse
- Azure Shared Key en `azure-secret-st-flinkdata`, claves `azurestorageaccountname` y `azurestorageaccountkey`.
- Docker Hub PAT como GitHub Actions secret `DOCKERHUB_TOKEN`.
- Couchbase username/password en `couchbase-credentials`, solo si se habilita ese sink.

No se inventan ni se incluyen claves reales en Git.
