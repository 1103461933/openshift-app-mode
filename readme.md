# OpenShift Flink Application Mode

GitOps-managed Apache Flink Application Mode deployment on OpenShift.

## Infrastructure

- OpenShift SNO: sno-flink-6k8pd
- OpenShift GitOps / Argo CD
- Apache Flink Kubernetes Operator
- Kafka
- Couchbase
- Azure Blob Storage

## Tenants

| Tenant | Namespace | Job |
|---|---|---|
| 1L | openshift-bridge-1l | equipment-agregate |
| 2L | openshift-bridge-2l | equipment-agregate |

## Architecture

- One shared Flink Kubernetes Operator.
- One independent Flink Application cluster per tenant.
- One equipment-agregate job per cluster.
- Shared Kafka, Couchbase and Azure Storage.
