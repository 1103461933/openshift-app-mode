# Arquitectura de la PoC

```text
OpenShift SNO
|
+-- openshift-gitops
|   +-- root Application
|       +-- namespaces
|       +-- Flink Operator Application
|
+-- flink-operator
|   +-- Flink Kubernetes Operator 1.16.1
|       +-- watches openshift-bridge-1l
|       +-- watches openshift-bridge-2l
|
+-- openshift-bridge-1l
|   +-- Secret azure-secret-st-flinkdata
|   +-- FlinkDeployment equipment-agregate
|       +-- JobManager leader
|       +-- JobManager standby
|       +-- TaskManagers allocated by Native Kubernetes
|
+-- openshift-bridge-2l
    +-- reserved for later expansion
```

## Data flow

```text
Kafka (Strimzi)
   |
   v
PyFlink equipment-agregate
   |-- keyed state (RocksDB)
   |-- Azure openshift-ha
   |-- Azure flink-checkpoints
   |-- Azure flink-savepoints
   +-- Couchbase (optional) / console (default)
```

## HA scopes

1. Flink job HA: `high-availability.type: kubernetes`, durable `high-availability.storageDir`, two JobManager replicas.
2. Operator HA: independent feature using operator leader election. This PoC uses one operator replica.

## SNO limitation

Both JobManagers may be scheduled on the same node. The PoC validates pod/JobManager recovery, not node-level fault tolerance.
