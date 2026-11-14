$ErrorActionPreference = "Stop"

oc get namespace openshift-bridge-1l,openshift-bridge-2l
if ($LASTEXITCODE -ne 0) { throw "Run 01-create-namespaces.ps1 first." }

oc get pods -n cert-manager
if ($LASTEXITCODE -ne 0) {
    throw "cert-manager must be installed before the Flink Operator webhook."
}

helm dependency build .\platform\flink-operator
if ($LASTEXITCODE -ne 0) { throw "helm dependency build failed." }

helm upgrade --install flink-kubernetes-operator `
    .\platform\flink-operator `
    --namespace flink-operator `
    --create-namespace `
    --wait `
    --timeout 10m

if ($LASTEXITCODE -ne 0) { throw "Flink Operator installation failed." }

oc get pods -n flink-operator
oc get crd flinkdeployments.flink.apache.org
