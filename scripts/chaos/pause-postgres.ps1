param(
    [int]$PauseSeconds = 15,
    [string]$ComposeFile = "deploy/docker-compose/docker-compose.yml"
)

Write-Host "Pausing postgres for $PauseSeconds seconds..."
docker compose -f $ComposeFile pause postgres

if ($LASTEXITCODE -ne 0) {
    throw "Failed to pause postgres."
}

try {
    Start-Sleep -Seconds $PauseSeconds
}
finally {
    docker compose -f $ComposeFile unpause postgres

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to unpause postgres."
    }
}

Write-Host "Postgres resumed. Verify ingest rejection behavior and processor recovery."
