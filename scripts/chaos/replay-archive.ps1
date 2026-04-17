param(
    [int]$EventCount = 25,
    [int]$WaitTimeoutSeconds = 90,
    [string]$ComposeFile = "deploy/docker-compose/docker-compose.yml",
    [string]$IngestEndpoint = "http://localhost:8080/api/v1/events",
    [string]$ReplayEndpoint = "http://localhost:8080/api/v1/admin/replay",
    [string]$OverviewEndpoint = "http://localhost:8081/api/v1/metrics/overview",
    [string]$PrometheusEndpoint = "http://localhost:9090",
    [string]$BearerToken = "",
    [switch]$PauseComposeSimulator = $true,
    [string]$OutputPath = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path "$PSScriptRoot/../.."
$artifactDir = Join-Path $repoRoot "artifacts/failure-drills"
$defaultBearerToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWRtaW4iLCJpc3MiOiJwdWxzZXN0cmVhbS1sb2NhbCIsInN1YiI6ImxvY2FsLWFkbWluIiwiYXVkIjpbInB1bHNlc3RyZWFtLWxvY2FsIl0sImV4cCI6MjA5MTI3ODUwNiwiaWF0IjoxNzc1OTE4NTA2fQ.q__74RIQsNpC5AfZY6yr-6dwTAP8gybP2_4kB5NQ-Vs"

New-Item -ItemType Directory -Force $artifactDir | Out-Null

if ([string]::IsNullOrWhiteSpace($BearerToken)) {
    $BearerToken = $env:PULSESTREAM_BEARER_TOKEN
}
if ([string]::IsNullOrWhiteSpace($BearerToken)) {
    $BearerToken = $defaultBearerToken
}

function Get-AuthHeaders {
    param([string]$Token)

    if ([string]::IsNullOrWhiteSpace($Token)) {
        return @{}
    }

    return @{ Authorization = "Bearer $Token" }
}

function Wait-HttpReady {
    param(
        [string]$Url,
        [hashtable]$Headers = @{},
        [int]$TimeoutSeconds = 20
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            if ($Headers.Count -gt 0) {
                Invoke-WebRequest -Uri $Url -Headers $Headers -UseBasicParsing | Out-Null
            }
            else {
                Invoke-WebRequest -Uri $Url -UseBasicParsing | Out-Null
            }
            return $true
        }
        catch {
            Start-Sleep -Milliseconds 250
        }
    }

    return $false
}

function Test-ContainerExists {
    param([string]$Name)

    $names = @(docker ps -a --format "{{.Names}}")
    return ($names -contains $Name)
}

function Stop-ComposeSimulator {
    param([string]$ComposeFilePath)

    try {
        docker compose -f $ComposeFilePath stop producer-simulator | Out-Null
    }
    catch {
        Write-Warning "Unable to stop compose producer-simulator."
    }
}

function Start-ComposeSimulator {
    param([string]$ComposeFilePath)

    try {
        docker compose -f $ComposeFilePath up -d producer-simulator | Out-Null
    }
    catch {
        Write-Warning "Unable to restart compose producer-simulator."
    }
}

function Get-ComposeServiceContainerIds {
    param(
        [string]$ComposeFilePath,
        [string]$Service
    )

    $ids = @(docker compose -f $ComposeFilePath ps -q $Service 2>$null)
    return @($ids | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}

function Get-PrimaryProcessorContainerId {
    param([string]$ComposeFilePath)

    $containerIds = @(Get-ComposeServiceContainerIds -ComposeFilePath $ComposeFilePath -Service "stream-processor")
    if ($containerIds.Count -eq 0) {
        throw "stream-processor service is not running"
    }

    return $containerIds[0]
}

function Get-ContainerEnvMap {
    param([string]$ContainerId)

    $envMap = @{}
    $inspection = docker inspect $ContainerId | ConvertFrom-Json
    foreach ($line in @($inspection[0].Config.Env)) {
        if ([string]::IsNullOrWhiteSpace($line) -or -not $line.Contains("=")) {
            continue
        }
        $parts = $line.Split("=", 2)
        $envMap[$parts[0]] = $parts[1]
    }

    return $envMap
}

function Get-ContainerNetworkName {
    param([string]$ContainerId)

    $inspection = docker inspect $ContainerId | ConvertFrom-Json
    $networkNames = @($inspection[0].NetworkSettings.Networks.PSObject.Properties.Name)
    if ($networkNames.Count -eq 0) {
        return ""
    }

    return $networkNames[0]
}

function Start-ReplayVerifierProcessor {
    param(
        [string]$ComposeFilePath,
        [string]$RunId
    )

    $mainProcessor = Get-PrimaryProcessorContainerId -ComposeFilePath $ComposeFilePath
    $envMap = Get-ContainerEnvMap -ContainerId $mainProcessor
    $networkName = Get-ContainerNetworkName -ContainerId $mainProcessor
    if ([string]::IsNullOrWhiteSpace($networkName)) {
        throw "unable to resolve stream-processor network"
    }

    $groupId = "pulsestream-replay-drill-$RunId"
    $instanceId = "replay-drill-$RunId"
    $containerName = "pulsestream-replay-drill-$RunId"

    if (Test-ContainerExists -Name $containerName) {
        docker rm -f $containerName | Out-Null
    }

    $dockerArgs = @(
        "run",
        "-d",
        "--rm",
        "--name", $containerName,
        "--network", $networkName,
        "-e", "POSTGRES_URL=$($envMap["POSTGRES_URL"])",
        "-e", "POSTGRES_ADMIN_URL=$($envMap["POSTGRES_ADMIN_URL"])",
        "-e", "KAFKA_BROKERS=$($envMap["KAFKA_BROKERS"])",
        "-e", "KAFKA_TOPIC=$($envMap["KAFKA_TOPIC"])",
        "-e", "KAFKA_DLQ_TOPIC=$($envMap["KAFKA_DLQ_TOPIC"])",
        "-e", "KAFKA_GROUP_ID=$groupId",
        "-e", "SERVICE_INSTANCE_ID=$instanceId",
        "-e", "PROCESSOR_LISTEN_ADDR=:8082",
        "-e", "PROCESSOR_SNAPSHOT_INTERVAL=1s",
        "-e", "OTEL_TRACES_EXPORTER=none",
        "docker-compose-stream-processor"
    )

    $containerId = (& docker @dockerArgs).Trim()
    if ([string]::IsNullOrWhiteSpace($containerId)) {
        throw "failed to start replay verifier processor"
    }

    $deadline = (Get-Date).AddSeconds(20)
    while ((Get-Date) -lt $deadline) {
        try {
            docker exec $containerName wget -qO- http://localhost:8082/healthz | Out-Null
            return [ordered]@{
                container_name = $containerName
                container_id   = $containerId
                group_id       = $groupId
                instance_id    = $instanceId
            }
        }
        catch {
            Start-Sleep -Milliseconds 500
        }
    }

    throw "replay verifier processor did not become healthy"
}

function Stop-ReplayVerifierProcessor {
    param([string]$ContainerName)

    if ([string]::IsNullOrWhiteSpace($ContainerName)) {
        return
    }

    docker rm -f $ContainerName | Out-Null
}

function Get-ContainerMetricValue {
    param(
        [string]$ContainerName,
        [string]$MetricName
    )

    $metrics = docker exec $ContainerName wget -qO- http://localhost:8082/metrics
    if ($LASTEXITCODE -ne 0) {
        throw "failed to read metrics from $ContainerName"
    }

    foreach ($line in @($metrics -split "`n")) {
        $trimmed = $line.Trim()
        if ($trimmed.StartsWith("#")) {
            continue
        }
        if ($trimmed -match "^$([regex]::Escape($MetricName))\s+([0-9eE+\.-]+)$") {
            return [double]$Matches[1]
        }
    }

    return 0.0
}

function Wait-ContainerMetricAtLeast {
    param(
        [string]$ContainerName,
        [string]$MetricName,
        [double]$ExpectedMinimum,
        [int]$TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $value = Get-ContainerMetricValue -ContainerName $ContainerName -MetricName $MetricName
            if ($value -ge $ExpectedMinimum) {
                return $value
            }
        }
        catch {
        }
        Start-Sleep -Milliseconds 500
    }

    return (Get-ContainerMetricValue -ContainerName $ContainerName -MetricName $MetricName)
}

function Get-PrometheusInstantValue {
    param(
        [string]$BaseUrl,
        [string]$Query
    )

    $encodedQuery = [System.Uri]::EscapeDataString($Query)
    $response = Invoke-RestMethod -Uri "$BaseUrl/api/v1/query?query=$encodedQuery"
    if ($response.status -ne "success") {
        throw "Prometheus query failed: $Query"
    }

    $results = @($response.data.result)
    if ($results.Count -eq 0) {
        return 0.0
    }

    return [double]$results[0].value[1]
}

function Wait-PrometheusMetricDelta {
    param(
        [string]$BaseUrl,
        [string]$Query,
        [double]$Before,
        [double]$ExpectedDelta,
        [int]$TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $value = Get-PrometheusInstantValue -BaseUrl $BaseUrl -Query $Query
            if (($value - $Before) -ge $ExpectedDelta) {
                return $value
            }
        }
        catch {
        }
        Start-Sleep -Milliseconds 500
    }

    return (Get-PrometheusInstantValue -BaseUrl $BaseUrl -Query $Query)
}

function Escape-SqlLiteral {
    param([string]$Value)

    return $Value.Replace("'", "''")
}

function Invoke-PostgresScalar {
    param([string]$Sql)

    $output = docker exec docker-compose-postgres-1 psql -U postgres -d pulsestream -t -A -c $Sql
    if ($LASTEXITCODE -ne 0) {
        throw "postgres query failed: $Sql"
    }

    $line = @($output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Last 1)
    if ($line.Count -eq 0) {
        return 0
    }

    return [int64]("$($line[0])".Trim())
}

function Invoke-PostgresNonQuery {
    param([string]$Sql)

    docker exec docker-compose-postgres-1 psql -U postgres -d pulsestream -v ON_ERROR_STOP=1 -c $Sql | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "postgres command failed: $Sql"
    }
}

function Wait-PostgresScalarAtLeast {
    param(
        [string]$Sql,
        [int64]$ExpectedMinimum,
        [int]$TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $value = Invoke-PostgresScalar -Sql $Sql
            if ($value -ge $ExpectedMinimum) {
                return $value
            }
        }
        catch {
        }
        Start-Sleep -Milliseconds 500
    }

    return (Invoke-PostgresScalar -Sql $Sql)
}

function Post-TelemetryEvent {
    param(
        [string]$Url,
        [hashtable]$Headers,
        [string]$TenantId,
        [string]$SourceId,
        [string]$EventId,
        [int64]$Sequence
    )

    $event = [ordered]@{
        schema_version = 1
        event_id       = $EventId
        tenant_id      = $TenantId
        source_id      = $SourceId
        event_type     = "telemetry"
        timestamp      = (Get-Date).ToUniversalTime().ToString("o")
        value          = 42.0 + ($Sequence % 10)
        status         = "ok"
        region         = "replay-drill"
        sequence       = $Sequence
    }
    $body = $event | ConvertTo-Json -Compress
    Invoke-RestMethod -Method Post -Uri $Url -Headers $Headers -ContentType "application/json" -Body $body -TimeoutSec 10 | Out-Null
}

$headers = Get-AuthHeaders -Token $BearerToken
$runId = (Get-Date).ToUniversalTime().ToString("yyyyMMddHHmmss")
$tenantId = "replay_tenant_$runId"
$sourceId = "replay_source_001"
$archiveDate = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd")
$drillStartedAt = Get-Date
$verifier = $null
$simulatorStopped = $false
$verifierLogPath = Join-Path $artifactDir "replay-archive-$runId.verifier.log"

try {
    if ($PauseComposeSimulator) {
        Stop-ComposeSimulator -ComposeFilePath $ComposeFile
        $simulatorStopped = $true
    }

    if (-not (Wait-HttpReady -Url "$PrometheusEndpoint/-/ready" -TimeoutSeconds 30)) {
        throw "Prometheus did not become ready."
    }
    if (-not (Wait-HttpReady -Url $OverviewEndpoint -Headers $headers -TimeoutSeconds 30)) {
        throw "Query service overview endpoint did not become ready."
    }
    if (-not (Wait-HttpReady -Url "http://localhost:8080/healthz" -TimeoutSeconds 30)) {
        throw "Ingest service did not become ready."
    }

    $verifier = Start-ReplayVerifierProcessor -ComposeFilePath $ComposeFile -RunId $runId
    Start-Sleep -Seconds 2

    $processedSql = "select count(*) from processed_events where tenant_id = '$(Escape-SqlLiteral -Value $tenantId)'"
    $sourceEventsSql = "select coalesce(sum(events_count), 0) from source_metrics where tenant_id = '$(Escape-SqlLiteral -Value $tenantId)'"

    $acceptedBefore = Get-PrometheusInstantValue -BaseUrl $PrometheusEndpoint -Query "sum(pulsestream_ingest_accepted_total)"
    $replayedMetricBefore = Get-PrometheusInstantValue -BaseUrl $PrometheusEndpoint -Query "sum(pulsestream_ingest_replayed_total)"

    for ($i = 1; $i -le $EventCount; $i++) {
        Post-TelemetryEvent `
            -Url $IngestEndpoint `
            -Headers $headers `
            -TenantId $tenantId `
            -SourceId $sourceId `
            -EventId "$tenantId-$sourceId-$i" `
            -Sequence $i
    }

    $processedBeforeReplay = Wait-PostgresScalarAtLeast -Sql $processedSql -ExpectedMinimum $EventCount -TimeoutSeconds $WaitTimeoutSeconds
    $sourceEventsBeforeReplay = Invoke-PostgresScalar -Sql $sourceEventsSql
    $verifierProcessedBeforeReplay = Get-ContainerMetricValue -ContainerName $verifier.container_name -MetricName "pulsestream_processor_processed_total"
    $verifierDuplicateBeforeReplay = Get-ContainerMetricValue -ContainerName $verifier.container_name -MetricName "pulsestream_processor_duplicate_total"

    $replayBody = [ordered]@{
        start_date = $archiveDate
        end_date   = $archiveDate
        tenant_id  = $tenantId
        limit      = $EventCount
    } | ConvertTo-Json -Compress
    $replayStartedAt = Get-Date
    $replayResponse = Invoke-RestMethod -Method Post -Uri $ReplayEndpoint -Headers $headers -ContentType "application/json" -Body $replayBody -TimeoutSec 60
    $replayFinishedAt = Get-Date

    $replayedCount = [int64]$replayResponse.replay.replayed
    $replayedMetricAfter = Wait-PrometheusMetricDelta -BaseUrl $PrometheusEndpoint -Query "sum(pulsestream_ingest_replayed_total)" -Before $replayedMetricBefore -ExpectedDelta $replayedCount -TimeoutSeconds 30
    $verifierDuplicateAfterReplay = Wait-ContainerMetricAtLeast -ContainerName $verifier.container_name -MetricName "pulsestream_processor_duplicate_total" -ExpectedMinimum ($verifierDuplicateBeforeReplay + $replayedCount) -TimeoutSeconds $WaitTimeoutSeconds
    $verifierProcessedAfterReplay = Get-ContainerMetricValue -ContainerName $verifier.container_name -MetricName "pulsestream_processor_processed_total"
    Start-Sleep -Seconds 2
    $processedAfterReplay = Invoke-PostgresScalar -Sql $processedSql
    $sourceEventsAfterReplay = Invoke-PostgresScalar -Sql $sourceEventsSql

    $escapedTenantID = Escape-SqlLiteral -Value $tenantId
    Invoke-PostgresNonQuery -Sql "delete from source_metrics where tenant_id = '$escapedTenantID'; delete from tenant_metrics where tenant_id = '$escapedTenantID'; delete from processed_events where tenant_id = '$escapedTenantID';"
    $processedAfterReset = Invoke-PostgresScalar -Sql $processedSql
    $sourceEventsAfterReset = Invoke-PostgresScalar -Sql $sourceEventsSql

    $verifierProcessedBeforeRebuild = Get-ContainerMetricValue -ContainerName $verifier.container_name -MetricName "pulsestream_processor_processed_total"
    $verifierDuplicateBeforeRebuild = Get-ContainerMetricValue -ContainerName $verifier.container_name -MetricName "pulsestream_processor_duplicate_total"
    $rebuildReplayStartedAt = Get-Date
    $rebuildReplayResponse = Invoke-RestMethod -Method Post -Uri $ReplayEndpoint -Headers $headers -ContentType "application/json" -Body $replayBody -TimeoutSec 60
    $rebuildReplayFinishedAt = Get-Date
    $rebuildReplayedCount = [int64]$rebuildReplayResponse.replay.replayed
    $replayedMetricAfterRebuild = Wait-PrometheusMetricDelta -BaseUrl $PrometheusEndpoint -Query "sum(pulsestream_ingest_replayed_total)" -Before $replayedMetricAfter -ExpectedDelta $rebuildReplayedCount -TimeoutSeconds 30
    $verifierProcessedAfterRebuild = Wait-ContainerMetricAtLeast -ContainerName $verifier.container_name -MetricName "pulsestream_processor_processed_total" -ExpectedMinimum ($verifierProcessedBeforeRebuild + $rebuildReplayedCount) -TimeoutSeconds $WaitTimeoutSeconds
    $verifierDuplicateAfterRebuild = Get-ContainerMetricValue -ContainerName $verifier.container_name -MetricName "pulsestream_processor_duplicate_total"
    Start-Sleep -Seconds 2
    $processedAfterRebuild = Invoke-PostgresScalar -Sql $processedSql
    $sourceEventsAfterRebuild = Invoke-PostgresScalar -Sql $sourceEventsSql
    $acceptedAfter = Get-PrometheusInstantValue -BaseUrl $PrometheusEndpoint -Query "sum(pulsestream_ingest_accepted_total)"

    docker logs $verifier.container_name 1> $verifierLogPath 2>&1

    $report = [ordered]@{
        started_at_utc                         = $drillStartedAt.ToUniversalTime().ToString("o")
        completed_at_utc                       = (Get-Date).ToUniversalTime().ToString("o")
        tenant_id                              = $tenantId
        source_id                              = $sourceId
        archive_date                           = $archiveDate
        event_count                            = $EventCount
        replay_limit                           = $EventCount
        paused_compose_simulator               = [bool]$PauseComposeSimulator
        verifier_group_id                      = $verifier.group_id
        verifier_instance_id                   = $verifier.instance_id
        accepted_metric_delta                  = [int64][Math]::Round($acceptedAfter - $acceptedBefore, 0)
        replay_metric_delta                    = [int64][Math]::Round($replayedMetricAfterRebuild - $replayedMetricBefore, 0)
        replay_duration_seconds                = [Math]::Round(($replayFinishedAt - $replayStartedAt).TotalSeconds, 3)
        replay_response_files_read             = [int]$replayResponse.replay.files_read
        replay_response_scanned                = [int64]$replayResponse.replay.scanned
        replay_response_skipped                = [int64]$replayResponse.replay.skipped
        replay_response_replayed               = $replayedCount
        processed_events_before_replay         = $processedBeforeReplay
        processed_events_after_replay          = $processedAfterReplay
        source_metric_events_before_replay     = $sourceEventsBeforeReplay
        source_metric_events_after_replay      = $sourceEventsAfterReplay
        source_metric_overcount_delta          = $sourceEventsAfterReplay - $sourceEventsBeforeReplay
        verifier_processed_before_replay       = [int64][Math]::Round($verifierProcessedBeforeReplay, 0)
        verifier_processed_after_replay        = [int64][Math]::Round($verifierProcessedAfterReplay, 0)
        verifier_processed_delta_after_replay  = [int64][Math]::Round($verifierProcessedAfterReplay - $verifierProcessedBeforeReplay, 0)
        verifier_duplicate_before_replay       = [int64][Math]::Round($verifierDuplicateBeforeReplay, 0)
        verifier_duplicate_after_replay        = [int64][Math]::Round($verifierDuplicateAfterReplay, 0)
        verifier_duplicate_delta_after_replay  = [int64][Math]::Round($verifierDuplicateAfterReplay - $verifierDuplicateBeforeReplay, 0)
        duplicate_safe                         = (($sourceEventsAfterReplay - $sourceEventsBeforeReplay) -eq 0 -and ($processedAfterReplay - $processedBeforeReplay) -eq 0 -and ($verifierDuplicateAfterReplay - $verifierDuplicateBeforeReplay) -ge $replayedCount)
        processed_events_after_reset           = $processedAfterReset
        source_metric_events_after_reset       = $sourceEventsAfterReset
        rebuild_replay_duration_seconds        = [Math]::Round(($rebuildReplayFinishedAt - $rebuildReplayStartedAt).TotalSeconds, 3)
        rebuild_replay_response_scanned        = [int64]$rebuildReplayResponse.replay.scanned
        rebuild_replay_response_skipped        = [int64]$rebuildReplayResponse.replay.skipped
        rebuild_replay_response_replayed       = $rebuildReplayedCount
        processed_events_after_rebuild         = $processedAfterRebuild
        source_metric_events_after_rebuild     = $sourceEventsAfterRebuild
        verifier_processed_before_rebuild      = [int64][Math]::Round($verifierProcessedBeforeRebuild, 0)
        verifier_processed_after_rebuild       = [int64][Math]::Round($verifierProcessedAfterRebuild, 0)
        verifier_processed_delta_after_rebuild = [int64][Math]::Round($verifierProcessedAfterRebuild - $verifierProcessedBeforeRebuild, 0)
        verifier_duplicate_before_rebuild      = [int64][Math]::Round($verifierDuplicateBeforeRebuild, 0)
        verifier_duplicate_after_rebuild       = [int64][Math]::Round($verifierDuplicateAfterRebuild, 0)
        verifier_duplicate_delta_after_rebuild = [int64][Math]::Round($verifierDuplicateAfterRebuild - $verifierDuplicateBeforeRebuild, 0)
        rebuild_restored                       = ($processedAfterReset -eq 0 -and $sourceEventsAfterReset -eq 0 -and $processedAfterRebuild -eq $EventCount -and $sourceEventsAfterRebuild -eq $EventCount)
        rebuild_processed_by_verifier          = (($verifierProcessedAfterRebuild - $verifierProcessedBeforeRebuild) -ge $rebuildReplayedCount)
        verifier_log                           = $verifierLogPath
    }

    if ([string]::IsNullOrWhiteSpace($OutputPath)) {
        $OutputPath = Join-Path $artifactDir "replay-archive-$runId.json"
    }

    $report | ConvertTo-Json -Depth 8 | Set-Content -Path $OutputPath

    Write-Host ""
    Write-Host "Replay drill report written to $OutputPath"
    Write-Host ("Tenant                         : {0}" -f $tenantId)
    Write-Host ("Replay response replayed        : {0}" -f $report.replay_response_replayed)
    Write-Host ("Verifier duplicate delta        : {0}" -f $report.verifier_duplicate_delta_after_replay)
    Write-Host ("Source metric overcount delta   : {0}" -f $report.source_metric_overcount_delta)
    Write-Host ("Duplicate safe                  : {0}" -f $report.duplicate_safe)
    Write-Host ("Rebuild response replayed       : {0}" -f $report.rebuild_replay_response_replayed)
    Write-Host ("Rebuild processed delta         : {0}" -f $report.verifier_processed_delta_after_rebuild)
    Write-Host ("Rebuild restored                : {0}" -f $report.rebuild_restored)
    Write-Host ("Rebuild processed by verifier   : {0}" -f $report.rebuild_processed_by_verifier)

    $evidenceScript = Join-Path $repoRoot "scripts/evidence/update-evidence.ps1"
    if (Test-Path $evidenceScript) {
        try {
            & $evidenceScript | Out-Null
        }
        catch {
            Write-Warning "Replay drill completed, but evidence summary refresh failed: $($_.Exception.Message)"
        }
    }
}
finally {
    if ($null -ne $verifier) {
        try {
            if (-not (Test-Path $verifierLogPath) -and (Test-ContainerExists -Name $verifier.container_name)) {
                docker logs $verifier.container_name 1> $verifierLogPath 2>&1
            }
        }
        catch {
        }
        Stop-ReplayVerifierProcessor -ContainerName $verifier.container_name
    }
    if ($simulatorStopped) {
        Start-ComposeSimulator -ComposeFilePath $ComposeFile
    }
}
