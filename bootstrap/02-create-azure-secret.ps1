param(
    [string[]]$Namespaces = @("openshift-bridge-1l"),
    [string]$StorageAccountName = "flinkstorage911"
)

$ErrorActionPreference = "Stop"
$accountName = $StorageAccountName
Write-Host "Using Azure Storage account: $accountName"
$accountKeySecure = Read-Host "Azure Storage account Shared Key" -AsSecureString
$accountKey = [System.Net.NetworkCredential]::new("", $accountKeySecure).Password

if ([string]::IsNullOrWhiteSpace($accountName) -or [string]::IsNullOrWhiteSpace($accountKey)) {
    throw "Storage account name and key are required."
}

$tempDir = Join-Path $env:TEMP ("flink-azure-secret-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tempDir | Out-Null

try {
    $accountNameFile = Join-Path $tempDir "account-name.txt"
    $accountKeyFile = Join-Path $tempDir "account-key.txt"
    [System.IO.File]::WriteAllText($accountNameFile, $accountName)
    [System.IO.File]::WriteAllText($accountKeyFile, $accountKey)

    foreach ($namespace in $Namespaces) {
        Write-Host "Creating/updating azure-secret-st-flinkdata in $namespace ..."
        $yaml = & oc create secret generic azure-secret-st-flinkdata `
            --namespace $namespace `
            --from-file="azurestorageaccountname=$accountNameFile" `
            --from-file="azurestorageaccountkey=$accountKeyFile" `
            --dry-run=client -o yaml

        if ($LASTEXITCODE -ne 0) { throw "Could not generate Secret for $namespace." }
        $yaml | & oc apply -f -
        if ($LASTEXITCODE -ne 0) { throw "Could not apply Secret in $namespace." }
    }
    Write-Host "Azure Secret created. Secret values were not printed."
}
finally {
    if (Test-Path $tempDir) { Remove-Item -Path $tempDir -Recurse -Force }
    $accountKey = $null
    $accountKeySecure = $null
}
