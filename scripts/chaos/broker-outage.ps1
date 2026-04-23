param(
    [int]$Rate = 800,
    [int]$BatchSize = 25,
    [int]$WarmupSeconds = 5,
    [int]$DurationSeconds = 40,
    [int]$OutageAfterSeconds = 8,
    [int]$OutageSeconds = 12,
    [int]$BrokerRecoveryTimeoutSeconds = 180,
    [int]$SampleIntervalMilliseconds = 1000,
    [int]$MaxInFlight = 1000,
    [int]$ProcessorReplicas = 1,
    [string]$ComposeFile = "deploy/docker-compose/docker-compose.yml",
    [string]$BearerToken = "",
    [string]$PrometheusEndpoint = "http://localhost:9090",
    [string]$OutputPath = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path "$PSScriptRoot/../.."
$artifactDir = Join-Path $repoRoot "artifacts/failure-drills"
$benchmarkContainerName = "pulsestream-broker-outage-producer"
$defaultBearerToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWRtaW4iLCJpc3MiOiJwdWxzZXN0cmVhbS1sb2NhbCIsInN1YiI6ImxvY2FsLWFkbWluIiwiYXVkIjpbInB1bHNlc3RyZWFtLWxvY2FsIl0sImV4cCI6MjA5MTI3ODUwNiwiaWF0IjoxNzc1OTE4NTA2fQ.q__74RIQsNpC5AfZY6yr-6dwTAP8gybP2_4kB5NQ-Vs"

New-Item -ItemType Directory -Force $artifactDir | Out-Null

if ([string]::IsNullOrWhiteSpace($BearerToken)) {
    $BearerToken = $env:PULSESTREAM_BEARER_TOKEN
}
if ([string]::IsNullOrWhiteSpace($BearerToken)) {
    $BearerToken = $defaultBearerToken
}

$composeIngestEndpoint = "http://ingest-service:8080/api/v1/events"
if ($BatchSize -gt 1 -and $composeIngestEndpoint.TrimEnd("/") -ieq "http://ingest-service:8080/api/v1/events") {
    $composeIngestEndpoint = "http://ingest-service:8080/api/v1/events/batch"
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

function Wait-ContainerHTTPReady {
    param(
        [string]$ContainerName,
        [string]$Url = "http://localhost:8083/healthz",
        [int]$TimeoutSeconds = 20
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            docker exec $ContainerName wget -qO- $Url | Out-Null
            if ($LASTEXITCODE -eq 0) {
                return $true
            }
        }
        catch {
        }

        Start-Sleep -Milliseconds 250
    }

    return $false
}

function Get-AuthHeaders {
    param([string]$Token)

    if ([string]::IsNullOrWhiteSpace($Token)) {
        return @{}
    }

    return @{ Authorization = "Bearer $Token" }
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

function Get-ComposeServiceContainerIds {
    param(
        [string]$ComposeFilePath,
        [string]$Service
    )

    $ids = @(docker compose -f $ComposeFilePath ps -q $Service 2>$null)
    return @($ids | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}

function Get-ContainerHealthStatus {
    param([string]$ContainerId)

    $status = docker inspect -f "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $ContainerId 2>$null
    if ($LASTEXITCODE -ne 0) {
        return ""
    }

    return "$status".Trim()
}

function Wait-ComposeServiceReplicas {
    param(
        [string]$ComposeFilePath,
        [string]$Service,
        [int]$ExpectedCount,
        [int]$TimeoutSeconds = 60
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $containerIds = @(Get-ComposeServiceContainerIds -ComposeFilePath $ComposeFilePath -Service $Service)
        if ($containerIds.Count -eq $ExpectedCount) {
            $allHealthy = $true
            foreach ($containerId in $containerIds) {
                $status = Get-ContainerHealthStatus -ContainerId $containerId
                if ($status -ne "healthy" -and $status -ne "running") {
                    $allHealthy = $false
                    break
                }
            }

            if ($allHealthy) {
                return $containerIds
            }
        }

        Start-Sleep -Milliseconds 500
    }

    return @()
}

function Test-ComposeServiceReady {
    param(
        [string]$ComposeFilePath,
        [string]$Service
    )

    $containerIds = @(Get-ComposeServiceContainerIds -ComposeFilePath $ComposeFilePath -Service $Service)
    if ($containerIds.Count -eq 0) {
        return $false
    }

    foreach ($containerId in $containerIds) {
        $status = Get-ContainerHealthStatus -ContainerId $containerId
        if ($status -ne "healthy" -and $status -ne "running") {
            return $false
        }
    }

    return $true
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

function Wait-PrometheusQueryExactValue {
    param(
        [string]$BaseUrl,
        [string]$Query,
        [double]$ExpectedValue,
        [int]$TimeoutSeconds = 45
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $value = Get-PrometheusInstantValue -BaseUrl $BaseUrl -Query $Query
            if ($value -eq $ExpectedValue) {
                return $true
            }
        }
        catch {
        }

        Start-Sleep -Milliseconds 500
    }

    return $false
}

function Get-Snapshot {
    param(
        [string]$PrometheusBaseUrl,
        [string]$Token
    )

    $headers = Get-AuthHeaders -Token $Token
    if ($headers.Count -gt 0) {
        $overview = Invoke-RestMethod -Uri "http://localhost:8081/api/v1/metrics/overview" -Headers $headers
    }
    else {
        $overview = Invoke-RestMethod -Uri "http://localhost:8081/api/v1/metrics/overview"
    }

    return [pscustomobject]@{
        timestamp_utc            = (Get-Date).ToUniversalTime()
        archived_total           = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_ingest_archived_total)")
        accepted_total           = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_ingest_accepted_total)")
        rejected_total           = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_ingest_rejected_total)")
        publish_failed_total     = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query 'sum(pulsestream_ingest_rejected_total{reason="publish_failed"})')
        backpressure_total       = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query 'sum(pulsestream_ingest_rejected_total{reason="backpressure"})')
        processed_total          = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_processed_total)")
        consumer_lag             = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_consumer_lag)")
        processor_replicas       = [int](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query 'count(up{job="stream-processor"})')
        processing_p95_ms        = [double]$overview.processing_p95_ms
        processing_p99_ms        = [double]$overview.processing_p99_ms
    }
}

function Get-MonotonicDelta {
    param([double[]]$Values)

    if ($null -eq $Values -or $Values.Count -lt 2) {
        return 0
    }

    $delta = 0.0
    for ($i = 1; $i -lt $Values.Count; $i++) {
        if ($Values[$i] -ge $Values[$i - 1]) {
            $delta += ($Values[$i] - $Values[$i - 1])
        }
        else {
            $delta += $Values[$i]
        }
    }

    return [int64][Math]::Round($delta, 0)
}

$resumeSimulator = $false
$restoreProcessorReplicas = $null
$samples = @()
$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$stdoutPath = Join-Path $artifactDir "broker-outage-$runId.stdout.log"
$stderrPath = Join-Path $artifactDir "broker-outage-$runId.stderr.log"
$originalProcessorReplicas = @(Get-ComposeServiceContainerIds -ComposeFilePath $ComposeFile -Service "stream-processor").Count
$brokerOutageStartedAt = $null
$brokerOutageEndedAt = $null
$brokerReadyAt = $null
$brokerRecoveryTimedOut = $false

try {
    if (@(Get-ComposeServiceContainerIds -ComposeFilePath $ComposeFile -Service "producer-simulator").Count -gt 0) {
        Stop-ComposeSimulator -ComposeFilePath $ComposeFile
        if ($LASTEXITCODE -eq 0) {
            $resumeSimulator = $true
        }
    }

    if ($ProcessorReplicas -gt 0 -and $ProcessorReplicas -ne $originalProcessorReplicas) {
        Write-Host "Scaling stream-processor to $ProcessorReplicas replicas before the drill..."
        $restoreProcessorReplicas = $originalProcessorReplicas
        docker compose -f $ComposeFile up -d --no-deps --scale stream-processor=$ProcessorReplicas stream-processor | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to scale stream-processor."
        }
    }

    $processorContainers = @(Wait-ComposeServiceReplicas -ComposeFilePath $ComposeFile -Service "stream-processor" -ExpectedCount $ProcessorReplicas -TimeoutSeconds 90)
    if ($processorContainers.Count -ne $ProcessorReplicas) {
        throw "Timed out waiting for $ProcessorReplicas stream-processor replicas."
    }

    Stop-ComposeSimulator -ComposeFilePath $ComposeFile

    if (-not (Wait-HttpReady -Url "$PrometheusEndpoint/-/ready" -TimeoutSeconds 30)) {
        throw "Prometheus did not become ready."
    }
    if (-not (Wait-HttpReady -Url "http://localhost:8081/api/v1/metrics/overview" -Headers (Get-AuthHeaders -Token $BearerToken) -TimeoutSeconds 30)) {
        throw "Query service overview endpoint did not become ready."
    }
    if (-not (Wait-PrometheusQueryExactValue -BaseUrl $PrometheusEndpoint -Query 'count(up{job="stream-processor"})' -ExpectedValue $ProcessorReplicas -TimeoutSeconds 60)) {
        throw "Prometheus did not discover the expected number of stream-processor replicas."
    }

    if (Test-ContainerExists -Name $benchmarkContainerName) {
        docker rm -f $benchmarkContainerName | Out-Null
    }
    docker compose -f $ComposeFile run -d `
        --name $benchmarkContainerName `
        --no-deps `
        -e SIM_INGEST_ENDPOINT=$composeIngestEndpoint `
        -e SIM_BEARER_TOKEN=$BearerToken `
        -e SIM_RATE_PER_SEC=$Rate `
        -e SIM_BATCH_SIZE=$BatchSize `
        -e SIM_TENANT_COUNT=5 `
        -e SIM_SOURCES_PER_TENANT=25 `
        -e SIM_MAX_IN_FLIGHT=$MaxInFlight `
        -e SIM_DUPLICATE_EVERY=0 `
        -e SIM_MALFORMED_EVERY=0 `
        -e SIM_BURST_EVERY=0s `
        -e SIM_BURST_SIZE=0 `
        -e HTTP_ACCESS_LOG_ENABLED=false `
        -e OTEL_TRACES_EXPORTER=none `
        producer-simulator | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to start the broker-outage benchmark producer."
    }

    if (-not (Wait-ContainerHTTPReady -ContainerName $benchmarkContainerName -TimeoutSeconds 20)) {
        throw "Benchmark producer did not become ready."
    }

    Write-Host "Warming up load for $WarmupSeconds seconds..."
    Start-Sleep -Seconds $WarmupSeconds

    $loopStart = Get-Date
    $deadline = $loopStart.AddSeconds($DurationSeconds)
    $brokerStopped = $false
    $brokerRestarted = $false

    while ((Get-Date) -lt $deadline) {
        $now = Get-Date

        if (-not $brokerStopped -and ($now - $loopStart).TotalSeconds -ge $OutageAfterSeconds) {
            $brokerStopped = $true
            $brokerOutageStartedAt = $now.ToUniversalTime()
            Write-Host "Stopping Kafka during active load..."
            docker compose -f $ComposeFile stop kafka | Out-Null
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to stop kafka."
            }
        }

        if ($brokerStopped -and -not $brokerRestarted -and ($now - $brokerOutageStartedAt.ToLocalTime()).TotalSeconds -ge $OutageSeconds) {
            $brokerRestarted = $true
            $brokerOutageEndedAt = (Get-Date).ToUniversalTime()
            Write-Host "Starting Kafka after the outage window..."
            docker compose -f $ComposeFile up -d kafka | Out-Null
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to restart kafka."
            }
        }

        if ($brokerRestarted -and $null -eq $brokerReadyAt -and -not $brokerRecoveryTimedOut) {
            if (Test-ComposeServiceReady -ComposeFilePath $ComposeFile -Service "kafka") {
                $brokerReadyAt = (Get-Date).ToUniversalTime()
                Write-Host "Kafka reported healthy after the outage window."
            }
            elseif (($now - $brokerOutageEndedAt.ToLocalTime()).TotalSeconds -ge $BrokerRecoveryTimeoutSeconds) {
                $brokerRecoveryTimedOut = $true
                Write-Warning "Kafka did not become healthy within $BrokerRecoveryTimeoutSeconds seconds; continuing so the drill still emits an artifact."
            }
        }

        $samples += Get-Snapshot -PrometheusBaseUrl $PrometheusEndpoint -Token $BearerToken
        Start-Sleep -Milliseconds $SampleIntervalMilliseconds
    }
}
finally {
    try {
        if (Test-ContainerExists -Name $benchmarkContainerName) {
            docker logs $benchmarkContainerName 1> $stdoutPath 2> $stderrPath
        }
    }
    catch {
        Write-Warning "Failed to capture broker-outage producer logs."
    }

    try {
        if (Test-ContainerExists -Name $benchmarkContainerName) {
            docker rm -f $benchmarkContainerName | Out-Null
        }
    }
    catch {
        Write-Warning "Failed to remove the broker-outage producer container."
    }

    try {
        $kafkaContainers = @(Get-ComposeServiceContainerIds -ComposeFilePath $ComposeFile -Service "kafka")
        if ($kafkaContainers.Count -eq 0) {
            docker compose -f $ComposeFile up -d kafka | Out-Null
        }
    }
    catch {
        Write-Warning "Failed to ensure kafka is running after the drill."
    }

    if ($null -ne $restoreProcessorReplicas -and $restoreProcessorReplicas -ge 0) {
        try {
            docker compose -f $ComposeFile up -d --no-deps --scale stream-processor=$restoreProcessorReplicas stream-processor | Out-Null
        }
        catch {
            Write-Warning "Failed to restore the original stream-processor replica count."
        }
    }

    if ($resumeSimulator) {
        try {
            docker compose -f $ComposeFile up -d producer-simulator | Out-Null
        }
        catch {
            Write-Warning "Failed to restart the steady-state producer-simulator."
        }
    }
}

if ($samples.Count -eq 0) {
    throw "No samples were captured during the broker outage drill."
}

$peakLag = ($samples | Measure-Object -Property consumer_lag -Maximum).Maximum
$peakP95 = ($samples | Measure-Object -Property processing_p95_ms -Maximum).Maximum
$peakP99 = ($samples | Measure-Object -Property processing_p99_ms -Maximum).Maximum
$peakReplicas = ($samples | Measure-Object -Property processor_replicas -Maximum).Maximum
$finalSample = $samples[-1]
$archivedDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.archived_total })
$acceptedDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.accepted_total })
$rejectedDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.rejected_total })
$publishFailedDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.publish_failed_total })
$backpressureDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.backpressure_total })
$processedDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.processed_total })
$archivedNotAcceptedDelta = $archivedDelta - $acceptedDelta
$archiveAccountingGap = $archivedDelta - $acceptedDelta - $publishFailedDelta

$acceptedRecoveredAt = $null
if ($null -ne $brokerReadyAt) {
    $acceptedRecoveryBaseline = $finalSample.accepted_total
    $preRecoverySamples = @($samples | Where-Object { $_.timestamp_utc -le $brokerReadyAt })
    if ($preRecoverySamples.Count -gt 0) {
        $acceptedRecoveryBaseline = $preRecoverySamples[-1].accepted_total
    }
    $acceptedRecoveredSample = $samples | Where-Object {
        $_.timestamp_utc -gt $brokerReadyAt -and $_.accepted_total -gt $acceptedRecoveryBaseline
    } | Select-Object -First 1
    if ($null -ne $acceptedRecoveredSample) {
        $acceptedRecoveredAt = $acceptedRecoveredSample.timestamp_utc
    }
}

$report = [ordered]@{
    started_at_utc                = $samples[0].timestamp_utc
    completed_at_utc              = $finalSample.timestamp_utc
    rate_target_eps               = $Rate
    batch_size                    = $BatchSize
    ingest_endpoint               = $composeIngestEndpoint
    warmup_seconds                = $WarmupSeconds
    duration_seconds              = $DurationSeconds
    outage_after_seconds          = $OutageAfterSeconds
    outage_seconds                = $OutageSeconds
    broker_recovery_timeout_seconds = $BrokerRecoveryTimeoutSeconds
    broker_outage_started_at_utc  = $brokerOutageStartedAt
    broker_outage_ended_at_utc    = $brokerOutageEndedAt
    broker_ready_at_utc           = $brokerReadyAt
    broker_recovery_timed_out     = $brokerRecoveryTimedOut
    accepted_recovered_at_utc     = $acceptedRecoveredAt
    accepted_recovered_within_window = ($null -ne $acceptedRecoveredAt)
    accepted_recovery_seconds     = if ($null -ne $acceptedRecoveredAt -and $null -ne $brokerReadyAt) { [Math]::Round(($acceptedRecoveredAt - $brokerReadyAt).TotalSeconds, 2) } else { $null }
    processor_replicas_requested  = $ProcessorReplicas
    peak_processor_replicas       = [int]$peakReplicas
    archived_total_delta          = $archivedDelta
    accepted_total_delta          = $acceptedDelta
    rejected_total_delta          = $rejectedDelta
    publish_failed_total_delta    = $publishFailedDelta
    backpressure_total_delta      = $backpressureDelta
    processed_total_delta         = $processedDelta
    archived_not_accepted_delta   = $archivedNotAcceptedDelta
    archive_accounting_gap        = $archiveAccountingGap
    peak_consumer_lag             = [int64]$peakLag
    final_consumer_lag            = $finalSample.consumer_lag
    peak_processing_p95_ms        = [double]$peakP95
    peak_processing_p99_ms        = [double]$peakP99
    sample_count                  = $samples.Count
    producer_stdout_log           = $stdoutPath
    producer_stderr_log           = $stderrPath
}

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path $artifactDir "broker-outage-$runId.json"
}

$report | ConvertTo-Json -Depth 8 | Set-Content -Path $OutputPath

Write-Host ""
Write-Host "Failure drill report written to $OutputPath"
Write-Host ("Archived delta        : {0}" -f $report.archived_total_delta)
Write-Host ("Accepted delta        : {0}" -f $report.accepted_total_delta)
Write-Host ("Batch size            : {0}" -f $report.batch_size)
Write-Host ("Publish failed delta  : {0}" -f $report.publish_failed_total_delta)
Write-Host ("Backpressure delta    : {0}" -f $report.backpressure_total_delta)
Write-Host ("Accounting gap        : {0}" -f $report.archive_accounting_gap)
Write-Host ("Peak lag              : {0}" -f $report.peak_consumer_lag)
Write-Host ("Recovered in window   : {0}" -f $report.accepted_recovered_within_window)

$evidenceScript = Join-Path $repoRoot "scripts/evidence/update-evidence.ps1"
if (Test-Path $evidenceScript) {
    try {
        & $evidenceScript | Out-Null
    }
    catch {
        Write-Warning "Broker outage drill completed, but evidence summary refresh failed: $($_.Exception.Message)"
    }
}
