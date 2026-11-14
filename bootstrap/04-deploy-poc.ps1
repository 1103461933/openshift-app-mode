param(
    [string]$StorageAccountName = "flinkstorage911",
    [string]$ImageTag = "v1.1.0",
    [string]$Namespace = "openshift-bridge-1l"
)

$ErrorActionPreference = "Stop"

if ($Namespace -ne "openshift-bridge-1l") {
    throw "This PoC intentionally deploys only in openshift-bridge-1l."
}

oc get crd flinkdeployments.flink.apache.org
if ($LASTEXITCODE -ne 0) { throw "Install the Flink Operator first." }

oc get secret azure-secret-st-flinkdata -n $Namespace
if ($LASTEXITCODE -ne 0) { throw "Create the Azure Secret first." }

helm dependency build .\tenants\1l\equipment-agregate
if ($LASTEXITCODE -ne 0) { throw "Could not build tenant chart dependency." }

$tempManifest = Join-Path $env:TEMP ("flink-poc-" + [guid]::NewGuid().ToString("N") + ".yaml")
try {
    $manifest = & helm template equipment-agregate .\tenants\1l\equipment-agregate `
        --namespace $Namespace `
        --set "flink-application.image.tag=$ImageTag" `
        --set "flink-application.azure.storageAccountName=$StorageAccountName"

    if ($LASTEXITCODE -ne 0) { throw "Helm template failed." }
    $manifest | Set-Content -Path $tempManifest -Encoding utf8

    oc apply -f $tempManifest
    if ($LASTEXITCODE -ne 0) { throw "FlinkDeployment apply failed." }
}
finally {
    if (Test-Path $tempManifest) { Remove-Item $tempManifest -Force }
}

oc get flinkdeployments -n $Namespace
