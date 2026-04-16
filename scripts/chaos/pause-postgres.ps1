param(
    [int]$Rate = 800,
    [int]$WarmupSeconds = 5,
    [int]$DurationSeconds = 60,
    [int]$PauseAfterSeconds = 8,
    [int]$PauseSeconds = 12,
    [int]$PostgresRecoveryTimeoutSeconds = 90,
    [int]$SampleIntervalMilliseconds = 1000,
    [int]$MaxInFlight = 1000,
    [int]$ProcessorReplicas = 1,
    [string]$ComposeFile = "deploy/docker-compose/docker-compose.yml",
    [string]$BearerToken = "",
    [string]$PrometheusEndpoint = "http://localhost:9090",
    [string]$OverviewEndpoint = "http://localhost:8081/api/v1/metrics/overview",
    [string]$OutputPath = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path "$PSScriptRoot/../.."
$artifactDir = Join-Path $repoRoot "artifacts/failure-drills"
$producerContainerName = "pulsestream-postgres-pause-producer"
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

function Get-OverviewProbe {
    param(
        [string]$Url,
        [string]$Token
    )

    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $headers = Get-AuthHeaders -Token $Token
        if ($headers.Count -gt 0) {
            $overview = Invoke-RestMethod -Uri $Url -Headers $headers -TimeoutSec 3
        }
        else {
            $overview = Invoke-RestMethod -Uri $Url -TimeoutSec 3
        }
        $stopwatch.Stop()

        return [pscustomobject]@{
            ok                    = $true
            status                = "ok"
            latency_ms            = [Math]::Round($stopwatch.Elapsed.TotalMilliseconds, 2)
            processing_p95_ms     = [double]$overview.processing_p95_ms
            processing_p99_ms     = [double]$overview.processing_p99_ms
            processor_instances   = [int]$overview.processor_instances
            processor_lag         = [int64]$overview.processor_lag
            processor_inflight    = [int64]$overview.processor_inflight
        }
    }
    catch {
        $stopwatch.Stop()
        return [pscustomobject]@{
            ok                    = $false
            status                = $_.Exception.Message
            latency_ms            = [Math]::Round($stopwatch.Elapsed.TotalMilliseconds, 2)
            processing_p95_ms     = 0.0
            processing_p99_ms     = 0.0
            processor_instances   = 0
            processor_lag         = 0
            processor_inflight    = 0
        }
    }
}

function Get-Snapshot {
    param(
        [string]$PrometheusBaseUrl,
        [string]$OverviewUrl,
        [string]$Token
    )

    $overview = Get-OverviewProbe -Url $OverviewUrl -Token $Token

    return [pscustomobject]@{
        timestamp_utc          = (Get-Date).ToUniversalTime()
        overview_ok            = [bool]$overview.ok
        overview_status        = [string]$overview.status
        overview_latency_ms    = [double]$overview.latency_ms
        archived_total         = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_ingest_archived_total)")
        accepted_total         = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_ingest_accepted_total)")
        rejected_total         = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_ingest_rejected_total)")
        publish_failed_total   = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query 'sum(pulsestream_ingest_rejected_total{reason="publish_failed"})')
        backpressure_total     = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query 'sum(pulsestream_ingest_rejected_total{reason="backpressure"})')
        processed_total        = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_processed_total)")
        fetch_errors_total     = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_fetch_errors_total)")
        commit_retries_total   = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_commit_retries_total)")
        consumer_lag           = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_consumer_lag)")
        processor_inflight     = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_inflight_messages)")
        ingest_inflight        = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_ingest_inflight_requests)")
        processor_replicas     = [int](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query 'count(up{job="stream-processor"})')
        processing_p95_ms      = [double]$overview.processing_p95_ms
        processing_p99_ms      = [double]$overview.processing_p99_ms
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
$stdoutPath = Join-Path $artifactDir "pause-postgres-$runId.stdout.log"
$stderrPath = Join-Path $artifactDir "pause-postgres-$runId.stderr.log"
$originalProcessorReplicas = @(Get-ComposeServiceContainerIds -ComposeFilePath $ComposeFile -Service "stream-processor").Count
$postgresPausedAt = $null
$postgresResumedAt = $null
$postgresReadyAt = $null
$postgresRecoveryTimedOut = $false

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
        docker compose -f $ComposeFile up -d --scale stream-processor=$ProcessorReplicas stream-processor prometheus | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to scale stream-processor."
        }
    }

    $processorContainers = @(Wait-ComposeServiceReplicas -ComposeFilePath $ComposeFile -Service "stream-processor" -ExpectedCount $ProcessorReplicas -TimeoutSeconds 90)
    if ($processorContainers.Count -ne $ProcessorReplicas) {
        throw "Timed out waiting for $ProcessorReplicas stream-processor replicas."
    }

    if (-not (Wait-HttpReady -Url "$PrometheusEndpoint/-/ready" -TimeoutSeconds 30)) {
        throw "Prometheus did not become ready."
    }
    if (-not (Wait-HttpReady -Url $OverviewEndpoint -Headers (Get-AuthHeaders -Token $BearerToken) -TimeoutSeconds 30)) {
        throw "Query service overview endpoint did not become ready."
    }
    if (-not (Wait-PrometheusQueryExactValue -BaseUrl $PrometheusEndpoint -Query 'count(up{job="stream-processor"})' -ExpectedValue $ProcessorReplicas -TimeoutSeconds 60)) {
        throw "Prometheus did not discover the expected number of stream-processor replicas."
    }

    if (Test-ContainerExists -Name $producerContainerName) {
        docker rm -f $producerContainerName | Out-Null
    }
    docker compose -f $ComposeFile run -d `
        --name $producerContainerName `
        --no-deps `
        --service-ports `
        -e SIM_INGEST_ENDPOINT=http://ingest-service:8080/api/v1/events `
        -e SIM_BEARER_TOKEN=$BearerToken `
        -e SIM_RATE_PER_SEC=$Rate `
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
        throw "Failed to start the postgres-pause benchmark producer."
    }

    if (-not (Wait-HttpReady -Url "http://localhost:8083/healthz" -TimeoutSeconds 20)) {
        throw "Benchmark producer did not become ready."
    }

    Write-Host "Warming up load for $WarmupSeconds seconds..."
    Start-Sleep -Seconds $WarmupSeconds

    $loopStart = Get-Date
    $deadline = $loopStart.AddSeconds($DurationSeconds)
    $postgresPaused = $false
    $postgresUnpaused = $false

    while ((Get-Date) -lt $deadline) {
        $now = Get-Date

        if (-not $postgresPaused -and ($now - $loopStart).TotalSeconds -ge $PauseAfterSeconds) {
            $postgresPaused = $true
            $postgresPausedAt = $now.ToUniversalTime()
            Write-Host "Pausing Postgres during active load..."
            docker compose -f $ComposeFile pause postgres | Out-Null
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to pause postgres."
            }
        }

        if ($postgresPaused -and -not $postgresUnpaused -and ($now - $postgresPausedAt.ToLocalTime()).TotalSeconds -ge $PauseSeconds) {
            $postgresUnpaused = $true
            $postgresResumedAt = (Get-Date).ToUniversalTime()
            Write-Host "Unpausing Postgres after the pause window..."
            docker compose -f $ComposeFile unpause postgres | Out-Null
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to unpause postgres."
            }
        }

        if ($postgresUnpaused -and $null -eq $postgresReadyAt -and -not $postgresRecoveryTimedOut) {
            if (Test-ComposeServiceReady -ComposeFilePath $ComposeFile -Service "postgres") {
                $postgresReadyAt = (Get-Date).ToUniversalTime()
                Write-Host "Postgres reported healthy after the pause window."
            }
            elseif (($now - $postgresResumedAt.ToLocalTime()).TotalSeconds -ge $PostgresRecoveryTimeoutSeconds) {
                $postgresRecoveryTimedOut = $true
                Write-Warning "Postgres did not become healthy within $PostgresRecoveryTimeoutSeconds seconds; continuing so the drill still emits an artifact."
            }
        }

        $samples += Get-Snapshot -PrometheusBaseUrl $PrometheusEndpoint -OverviewUrl $OverviewEndpoint -Token $BearerToken
        Start-Sleep -Milliseconds $SampleIntervalMilliseconds
    }
}
finally {
    try {
        if (Test-ContainerExists -Name $producerContainerName) {
            docker logs $producerContainerName 1> $stdoutPath 2> $stderrPath
        }
    }
    catch {
        Write-Warning "Failed to capture postgres-pause producer logs."
    }

    try {
        if (Test-ContainerExists -Name $producerContainerName) {
            docker rm -f $producerContainerName | Out-Null
        }
    }
    catch {
        Write-Warning "Failed to remove the postgres-pause producer container."
    }

    try {
        $postgresContainers = @(Get-ComposeServiceContainerIds -ComposeFilePath $ComposeFile -Service "postgres")
        if ($postgresContainers.Count -gt 0) {
            foreach ($postgresContainer in $postgresContainers) {
                $state = docker inspect -f "{{.State.Status}}" $postgresContainer 2>$null
                if ("$state".Trim() -eq "paused") {
                    docker compose -f $ComposeFile unpause postgres | Out-Null
                    break
                }
            }
        }
        else {
            docker compose -f $ComposeFile up -d postgres | Out-Null
        }
    }
    catch {
        Write-Warning "Failed to ensure postgres is running after the drill."
    }

    if ($null -ne $restoreProcessorReplicas -and $restoreProcessorReplicas -ge 0) {
        try {
            docker compose -f $ComposeFile up -d --scale stream-processor=$restoreProcessorReplicas stream-processor prometheus | Out-Null
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
    throw "No samples were captured during the postgres pause drill."
}

$peakLag = ($samples | Measure-Object -Property consumer_lag -Maximum).Maximum
$peakProcessorInFlight = ($samples | Measure-Object -Property processor_inflight -Maximum).Maximum
$peakIngestInFlight = ($samples | Measure-Object -Property ingest_inflight -Maximum).Maximum
$peakOverviewLatency = ($samples | Measure-Object -Property overview_latency_ms -Maximum).Maximum
$peakP95 = ($samples | Measure-Object -Property processing_p95_ms -Maximum).Maximum
$peakP99 = ($samples | Measure-Object -Property processing_p99_ms -Maximum).Maximum
$peakReplicas = ($samples | Measure-Object -Property processor_replicas -Maximum).Maximum
$overviewFailureCount = @($samples | Where-Object { -not $_.overview_ok }).Count
$finalSample = $samples[-1]

$archivedDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.archived_total })
$acceptedDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.accepted_total })
$rejectedDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.rejected_total })
$publishFailedDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.publish_failed_total })
$backpressureDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.backpressure_total })
$processedDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.processed_total })
$fetchErrorsDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.fetch_errors_total })
$commitRetriesDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.commit_retries_total })

$processedRecoveredAt = $null
if ($null -ne $postgresReadyAt) {
    $processedRecoveryBaseline = $finalSample.processed_total
    $preRecoverySamples = @($samples | Where-Object { $_.timestamp_utc -le $postgresReadyAt })
    if ($preRecoverySamples.Count -gt 0) {
        $processedRecoveryBaseline = $preRecoverySamples[-1].processed_total
    }
    $processedRecoveredSample = $samples | Where-Object {
        $_.timestamp_utc -gt $postgresReadyAt -and $_.processed_total -gt $processedRecoveryBaseline
    } | Select-Object -First 1
    if ($null -ne $processedRecoveredSample) {
        $processedRecoveredAt = $processedRecoveredSample.timestamp_utc
    }
}

$report = [ordered]@{
    started_at_utc                  = $samples[0].timestamp_utc
    completed_at_utc                = $finalSample.timestamp_utc
    rate_target_eps                 = $Rate
    warmup_seconds                  = $WarmupSeconds
    duration_seconds                = $DurationSeconds
    pause_after_seconds             = $PauseAfterSeconds
    pause_seconds                   = $PauseSeconds
    postgres_recovery_timeout_seconds = $PostgresRecoveryTimeoutSeconds
    postgres_paused_at_utc          = $postgresPausedAt
    postgres_resumed_at_utc         = $postgresResumedAt
    postgres_ready_at_utc           = $postgresReadyAt
    postgres_recovery_timed_out     = $postgresRecoveryTimedOut
    processed_recovered_at_utc      = $processedRecoveredAt
    processed_recovered_within_window = ($null -ne $processedRecoveredAt)
    processed_recovery_seconds      = if ($null -ne $processedRecoveredAt -and $null -ne $postgresReadyAt) { [Math]::Round(($processedRecoveredAt - $postgresReadyAt).TotalSeconds, 2) } else { $null }
    processor_replicas_requested    = $ProcessorReplicas
    peak_processor_replicas         = [int]$peakReplicas
    archived_total_delta            = $archivedDelta
    accepted_total_delta            = $acceptedDelta
    rejected_total_delta            = $rejectedDelta
    publish_failed_total_delta      = $publishFailedDelta
    backpressure_total_delta        = $backpressureDelta
    processed_total_delta           = $processedDelta
    fetch_errors_total_delta        = $fetchErrorsDelta
    commit_retries_total_delta      = $commitRetriesDelta
    peak_consumer_lag               = [int64]$peakLag
    final_consumer_lag              = $finalSample.consumer_lag
    peak_processor_inflight         = [int64]$peakProcessorInFlight
    peak_ingest_inflight            = [int64]$peakIngestInFlight
    overview_failure_count          = $overviewFailureCount
    peak_overview_latency_ms        = [double]$peakOverviewLatency
    peak_processing_p95_ms          = [double]$peakP95
    peak_processing_p99_ms          = [double]$peakP99
    sample_count                    = $samples.Count
    producer_stdout_log             = $stdoutPath
    producer_stderr_log             = $stderrPath
}

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path $artifactDir "pause-postgres-$runId.json"
}

$report | ConvertTo-Json -Depth 8 | Set-Content -Path $OutputPath

Write-Host ""
Write-Host "Failure drill report written to $OutputPath"
Write-Host ("Accepted delta          : {0}" -f $report.accepted_total_delta)
Write-Host ("Processed delta         : {0}" -f $report.processed_total_delta)
Write-Host ("Backpressure delta      : {0}" -f $report.backpressure_total_delta)
Write-Host ("Overview failures       : {0}" -f $report.overview_failure_count)
Write-Host ("Peak processor in-flight: {0}" -f $report.peak_processor_inflight)
Write-Host ("Peak lag                : {0}" -f $report.peak_consumer_lag)
Write-Host ("Processed recovered     : {0}" -f $report.processed_recovered_within_window)
