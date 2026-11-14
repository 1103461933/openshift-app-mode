# Kafka / Strimzi

`equipment-topic.yaml` es un ejemplo de KafkaTopic Strimzi.

Antes de aplicarlo:
1. Confirma que Strimzi Operator y el recurso Kafka están instalados.
2. Cambia `metadata.namespace` al namespace real de Kafka.
3. Cambia `strimzi.io/cluster` al nombre real del recurso Kafka.
4. Actualiza `kafka.bootstrapServers` en los valores Helm.

El ejemplo usa listener interno PLAINTEXT para el laboratorio. Si el listener requiere TLS/SASL, añade la configuración correspondiente.
