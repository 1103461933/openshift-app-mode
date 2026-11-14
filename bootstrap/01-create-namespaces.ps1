$ErrorActionPreference = "Stop"

Write-Host "Checking OpenShift login..."
oc whoami
if ($LASTEXITCODE -ne 0) { throw "oc is not authenticated." }

Write-Host "Creating lab namespaces..."
oc apply -f .\clusters\sno-flink-6k8pd\namespaces.yaml
if ($LASTEXITCODE -ne 0) { throw "Failed to create namespaces." }

oc get namespace openshift-bridge-1l,openshift-bridge-2l
