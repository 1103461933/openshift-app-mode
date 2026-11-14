param(
    [string]$StorageAccountName = "flinkstorage911",
    [string[]]$Containers = @("openshift-ha", "flink-checkpoints", "flink-savepoints")
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw "Azure CLI (az) is required. Install it and run az login first."
}

$account = az storage account show --name $StorageAccountName --query name -o tsv
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($account)) {
    throw "Could not access Storage Account '$StorageAccountName'. Check subscription and permissions."
}

foreach ($container in $Containers) {
    Write-Host "Ensuring private container '$container' exists in '$StorageAccountName'..."
    az storage container create `
        --account-name $StorageAccountName `
        --name $container `
        --auth-mode login `
        --public-access off `
        --only-show-errors `
        --output none
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create/check container '$container'. Verify Storage Blob Data Contributor permissions."
    }
}

Write-Host "Azure containers are ready."
