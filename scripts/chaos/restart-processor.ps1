param(
    [int]$Rate = 1000,
    [int]$WarmupSeconds = 5,
    [int]$DurationSeconds = 30,
    [int]$RestartAfterSeconds = 8,
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
$benchmarkContainerName = "pulsestream-chaos-producer"
$defaultBearerToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWRtaW4iLCJpc3MiOiJwdWxzZXN0cmVhbS1sb2NhbCIsInN1YiI6ImxvY2FsLWFkbWluIiwiYXVkIjpbInB1bHNlc3RyZWFtLWxvY2FsIl0sImV4cCI6MjA5MTI3ODUwNiwiaWF0IjoxNzc1OTE4NTA2fQ.q__74RIQsNpC5AfZY6yr-6dwTAP8gybP2_4kB5NQ-Vs"

New-Item -ItemType Directory -Force $artifactDir | Out-Null

if ([string]::IsNullOrWhiteSpace($BearerToken)) {
    $BearerToken = $env:PULSESTREAM_BEARER_TOKEN
}
if ([string]::IsNullOrWhiteSpace($BearerToken)) {
    $BearerToken = $defaultBearerToken
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

function Wait-PrometheusQueryValue {
    param(
        [string]$BaseUrl,
        [string]$Query,
        [double]$ExpectedMinimum,
        [int]$TimeoutSeconds = 45
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $value = Get-PrometheusInstantValue -BaseUrl $BaseUrl -Query $Query
            if ($value -ge $ExpectedMinimum) {
                return $true
            }
        }
        catch {
        }

        Start-Sleep -Milliseconds 500
    }

    return $false
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
        timestamp_utc      = (Get-Date).ToUniversalTime()
        accepted_total     = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_ingest_accepted_total)")
        rejected_total     = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_ingest_rejected_total)")
        processed_total    = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_processed_total)")
        duplicate_total    = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_duplicate_total)")
        consumer_lag       = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_consumer_lag)")
        replica_count      = [int](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query 'count(up{job="stream-processor"})')
        processing_p95_ms  = [double]$overview.processing_p95_ms
        processing_p99_ms  = [double]$overview.processing_p99_ms
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
$restartTargetContainer = $null
$restartTriggeredAt = $null
$restartCompletedAt = $null
$samples = @()
$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$stdoutPath = Join-Path $artifactDir "restart-processor-$runId.stdout.log"
$stderrPath = Join-Path $artifactDir "restart-processor-$runId.stderr.log"
$originalProcessorReplicas = @(Get-ComposeServiceContainerIds -ComposeFilePath $ComposeFile -Service "stream-processor").Count

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
    $restartTargetContainer = $processorContainers[0]

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
        throw "Failed to start the chaos benchmark producer."
    }

    if (-not (Wait-HttpReady -Url "http://localhost:8083/healthz" -TimeoutSeconds 20)) {
        throw "Benchmark producer did not become ready."
    }

    Write-Host "Warming up load for $WarmupSeconds seconds..."
    Start-Sleep -Seconds $WarmupSeconds

    $loopStart = Get-Date
    $deadline = $loopStart.AddSeconds($DurationSeconds)

    while ((Get-Date) -lt $deadline) {
        $now = Get-Date
        if ($null -eq $restartTriggeredAt -and ($now - $loopStart).TotalSeconds -ge $RestartAfterSeconds) {
            $restartTriggeredAt = $now.ToUniversalTime()
            Write-Host "Restarting stream-processor replica $restartTargetContainer during active load..."
            docker restart $restartTargetContainer | Out-Null
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to restart stream-processor replica $restartTargetContainer."
            }

            $restartWaitDeadline = (Get-Date).AddSeconds(45)
            while ((Get-Date) -lt $restartWaitDeadline) {
                $status = Get-ContainerHealthStatus -ContainerId $restartTargetContainer
                if ($status -eq "healthy" -or $status -eq "running") {
                    break
                }
                Start-Sleep -Milliseconds 500
            }

            $restartCompletedAt = (Get-Date).ToUniversalTime()
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
        Write-Warning "Failed to capture chaos producer logs."
    }

    try {
        if (Test-ContainerExists -Name $benchmarkContainerName) {
            docker rm -f $benchmarkContainerName | Out-Null
        }
    }
    catch {
        Write-Warning "Failed to remove the chaos producer container."
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
    throw "No samples were captured during the restart drill."
}

$preRestartSample = $samples[0]
if ($null -ne $restartTriggeredAt) {
    $beforeRestart = @($samples | Where-Object { $_.timestamp_utc -lt $restartTriggeredAt })
    if ($beforeRestart.Count -gt 0) {
        $preRestartSample = $beforeRestart[-1]
    }
}

$peakLag = ($samples | Measure-Object -Property consumer_lag -Maximum).Maximum
$peakP95 = ($samples | Measure-Object -Property processing_p95_ms -Maximum).Maximum
$peakP99 = ($samples | Measure-Object -Property processing_p99_ms -Maximum).Maximum
$peakReplicas = ($samples | Measure-Object -Property replica_count -Maximum).Maximum
$finalSample = $samples[-1]
$acceptedDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.accepted_total })
$processedDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.processed_total })
$duplicateDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.duplicate_total })
$rejectedDelta = Get-MonotonicDelta -Values @($samples | ForEach-Object { [double]$_.rejected_total })

$lagRecoveredAt = $null
if ($null -ne $restartTriggeredAt) {
    $lagRecoveredSample = $samples | Where-Object {
        $_.timestamp_utc -gt $restartTriggeredAt -and $_.consumer_lag -le 100
    } | Select-Object -First 1
    if ($null -ne $lagRecoveredSample) {
        $lagRecoveredAt = $lagRecoveredSample.timestamp_utc
    }
}

$report = [ordered]@{
    started_at_utc              = $samples[0].timestamp_utc
    completed_at_utc            = $finalSample.timestamp_utc
    rate_target_eps             = $Rate
    warmup_seconds              = $WarmupSeconds
    duration_seconds            = $DurationSeconds
    restart_after_seconds       = $RestartAfterSeconds
    restart_target_container    = $restartTargetContainer
    processor_replicas_requested = $ProcessorReplicas
    peak_processor_replicas     = [int]$peakReplicas
    restart_triggered_at_utc    = $restartTriggeredAt
    restart_completed_at_utc    = $restartCompletedAt
    lag_recovered_at_utc        = $lagRecoveredAt
    lag_recovered_within_window = ($null -ne $lagRecoveredAt)
    recovery_seconds            = if ($null -ne $lagRecoveredAt) { [Math]::Round(($lagRecoveredAt - $restartTriggeredAt).TotalSeconds, 2) } else { $null }
    accepted_total_delta        = $acceptedDelta
    processed_total_delta       = $processedDelta
    duplicate_total_delta       = $duplicateDelta
    rejected_total_delta        = $rejectedDelta
    pre_restart_consumer_lag    = $preRestartSample.consumer_lag
    final_consumer_lag          = $finalSample.consumer_lag
    peak_consumer_lag           = [int64]$peakLag
    peak_processing_p95_ms      = [double]$peakP95
    peak_processing_p99_ms      = [double]$peakP99
    sample_count                = $samples.Count
    producer_stdout_log         = $stdoutPath
    producer_stderr_log         = $stderrPath
}

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path $artifactDir "restart-processor-$runId.json"
}

$report | ConvertTo-Json -Depth 8 | Set-Content -Path $OutputPath

Write-Host ""
Write-Host "Failure drill report written to $OutputPath"
Write-Host ("Restart target      : {0}" -f $report.restart_target_container)
Write-Host ("Processor replicas  : {0}" -f $report.peak_processor_replicas)
Write-Host ("Peak lag            : {0}" -f $report.peak_consumer_lag)
Write-Host ("Final lag           : {0}" -f $report.final_consumer_lag)
Write-Host ("Processed delta     : {0}" -f $report.processed_total_delta)
Write-Host ("Accepted delta      : {0}" -f $report.accepted_total_delta)
Write-Host ("Recovered in window : {0}" -f $report.lag_recovered_within_window)
