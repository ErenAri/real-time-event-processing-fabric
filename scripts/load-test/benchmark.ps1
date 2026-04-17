param(
    [ValidateSet("compose", "local")]
    [string]$ProducerMode = "compose",
    [int]$Rate = 5000,
    [int]$ProducerCount = 1,
    [int]$ProducerPortStart = 18083,
    [int]$DurationSeconds = 60,
    [int]$WarmupSeconds = 5,
    [int]$SampleIntervalMilliseconds = 1000,
    [int]$TenantCount = 5,
    [int]$SourcesPerTenant = 25,
    [int]$MaxInFlight = 0,
    [int64]$DuplicateEvery = 0,
    [int64]$MalformedEvery = 0,
    [string]$BurstEvery = "0s",
    [int]$BurstSize = 0,
    [int]$ProcessorReplicas = 1,
    [string]$Endpoint = "http://localhost:8080/api/v1/events",
    [string]$ComposeIngestEndpoint = "http://ingest-service:8080/api/v1/events",
    [string]$OverviewEndpoint = "http://localhost:8081/api/v1/metrics/overview",
    [string]$BearerToken = "",
    [string]$PrometheusEndpoint = "http://localhost:9090",
    [string]$SimulatorHealthEndpoint = "http://localhost:8083/healthz",
    [string]$ComposeFile = "deploy/docker-compose/docker-compose.yml",
    [switch]$PauseComposeSimulator = $true,
    [string]$OutputPath = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path "$PSScriptRoot/../.."
$binDir = Join-Path $repoRoot "bin"
$artifactDir = Join-Path $repoRoot "artifacts/benchmarks"
$binaryPath = Join-Path $binDir "producer-simulator-bench.exe"
$benchmarkContainerName = "pulsestream-benchmark-producer"
$defaultBearerToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWRtaW4iLCJpc3MiOiJwdWxzZXN0cmVhbS1sb2NhbCIsInN1YiI6ImxvY2FsLWFkbWluIiwiYXVkIjpbInB1bHNlc3RyZWFtLWxvY2FsIl0sImV4cCI6MjA5MTI3ODUwNiwiaWF0IjoxNzc1OTE4NTA2fQ.q__74RIQsNpC5AfZY6yr-6dwTAP8gybP2_4kB5NQ-Vs"

New-Item -ItemType Directory -Force $binDir, $artifactDir | Out-Null

if ([string]::IsNullOrWhiteSpace($BearerToken)) {
    $BearerToken = $env:PULSESTREAM_BEARER_TOKEN
}
if ([string]::IsNullOrWhiteSpace($BearerToken)) {
    $BearerToken = $defaultBearerToken
}

if ($ProducerCount -lt 1) {
    throw "ProducerCount must be greater than or equal to 1."
}

function Get-ProducerRate {
    param([int]$Index)

    $baseRate = [Math]::Floor($Rate / $ProducerCount)
    $remainder = $Rate % $ProducerCount
    if ($Index -le $remainder) {
        return [int]($baseRate + 1)
    }
    return [int]$baseRate
}

function Get-BenchmarkContainerName {
    param([int]$Index)

    if ($ProducerCount -eq 1) {
        return $benchmarkContainerName
    }
    return "$benchmarkContainerName-$Index"
}

function Get-ProducerStdoutPath {
    param([int]$Index)

    if ($ProducerCount -eq 1) {
        return Join-Path $artifactDir "producer-benchmark-$runId.stdout.log"
    }
    return Join-Path $artifactDir "producer-benchmark-$runId-$Index.stdout.log"
}

function Get-ProducerStderrPath {
    param([int]$Index)

    if ($ProducerCount -eq 1) {
        return Join-Path $artifactDir "producer-benchmark-$runId.stderr.log"
    }
    return Join-Path $artifactDir "producer-benchmark-$runId-$Index.stderr.log"
}

function Get-Percentile {
    param(
        [double[]]$Values,
        [double]$Percentile
    )

    if ($null -eq $Values -or $Values.Count -eq 0) {
        return 0
    }

    $sorted = $Values | Sort-Object
    $index = [Math]::Ceiling(($Percentile / 100.0) * $sorted.Count) - 1
    if ($index -lt 0) {
        $index = 0
    }
    if ($index -ge $sorted.Count) {
        $index = $sorted.Count - 1
    }

    return [double]$sorted[$index]
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

function Stop-BenchmarkContainers {
    param([object[]]$ProducerRuns)

    foreach ($producer in @($ProducerRuns)) {
        if ($producer.mode -ne "compose") {
            continue
        }
        try {
            if (Test-ContainerExists -Name $producer.name) {
                docker rm -f $producer.name | Out-Null
            }
        }
        catch {
            Write-Warning "Failed to remove benchmark producer container $($producer.name)."
        }
    }
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

function Get-OverviewSample {
    param(
        [string]$Url,
        [string]$Token
    )

    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $headers = Get-AuthHeaders -Token $Token
    if ($headers.Count -gt 0) {
        $overview = Invoke-RestMethod -Uri $Url -Headers $headers
    }
    else {
        $overview = Invoke-RestMethod -Uri $Url
    }
    $stopwatch.Stop()

    return [pscustomobject]@{
        timestamp_utc    = (Get-Date).ToUniversalTime()
        query_latency_ms = [Math]::Round($stopwatch.Elapsed.TotalMilliseconds, 2)
        overview         = $overview
    }
}

function Get-ServiceMetricsSnapshot {
    param([string]$PrometheusBaseUrl)

    return [pscustomobject]@{
        timestamp_utc = (Get-Date).ToUniversalTime()
        ingest = [pscustomobject]@{
            accepted_total = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_ingest_accepted_total)")
            rejected_total = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_ingest_rejected_total)")
        }
        processor = [pscustomobject]@{
            processed_total   = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_processed_total)")
            duplicate_total   = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_duplicate_total)")
            consumer_lag      = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_consumer_lag)")
            replica_count     = [int](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query 'count(up{job="stream-processor"})')
            active_partitions = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_active_partitions)")
            inflight_messages = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_inflight_messages)")
        }
        simulator = [pscustomobject]@{
            sent_total   = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_simulator_sent_total)")
            failed_total = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_simulator_failed_total)")
        }
    }
}

function Get-BenchmarkSample {
    param(
        [string]$OverviewUrl,
        [string]$PrometheusBaseUrl,
        [string]$Token
    )

    $overviewSample = Get-OverviewSample -Url $OverviewUrl -Token $Token

    return [pscustomobject]@{
        timestamp_utc       = $overviewSample.timestamp_utc
        query_latency_ms    = $overviewSample.query_latency_ms
        overview            = $overviewSample.overview
        consumer_lag        = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_consumer_lag)")
        processor_replicas  = [int](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query 'count(up{job="stream-processor"})')
        active_partitions   = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_active_partitions)")
        inflight_messages   = [int64](Get-PrometheusInstantValue -BaseUrl $PrometheusBaseUrl -Query "sum(pulsestream_processor_inflight_messages)")
    }
}

function Save-Environment {
    return @{
        SIM_RATE_PER_SEC         = $env:SIM_RATE_PER_SEC
        SIM_TENANT_COUNT         = $env:SIM_TENANT_COUNT
        SIM_SOURCES_PER_TENANT   = $env:SIM_SOURCES_PER_TENANT
        SIM_MAX_IN_FLIGHT        = $env:SIM_MAX_IN_FLIGHT
        SIM_DUPLICATE_EVERY      = $env:SIM_DUPLICATE_EVERY
        SIM_MALFORMED_EVERY      = $env:SIM_MALFORMED_EVERY
        SIM_BURST_EVERY          = $env:SIM_BURST_EVERY
        SIM_BURST_SIZE           = $env:SIM_BURST_SIZE
        SIM_PRODUCER_ID          = $env:SIM_PRODUCER_ID
        SIM_SEED                 = $env:SIM_SEED
        SIMULATOR_LISTEN_ADDR    = $env:SIMULATOR_LISTEN_ADDR
        SIM_INGEST_ENDPOINT      = $env:SIM_INGEST_ENDPOINT
        SIM_BEARER_TOKEN         = $env:SIM_BEARER_TOKEN
        HTTP_ACCESS_LOG_ENABLED  = $env:HTTP_ACCESS_LOG_ENABLED
        OTEL_TRACES_EXPORTER     = $env:OTEL_TRACES_EXPORTER
        OTEL_TRACES_SAMPLE_RATIO = $env:OTEL_TRACES_SAMPLE_RATIO
    }
}

function Restore-Environment {
    param([hashtable]$Saved)

    foreach ($entry in $Saved.GetEnumerator()) {
        if ($null -eq $entry.Value) {
            Remove-Item "Env:$($entry.Key)" -ErrorAction SilentlyContinue
        }
        else {
            Set-Item "Env:$($entry.Key)" $entry.Value
        }
    }
}

function Get-HardwareSummary {
    $logicalCpu = 0
    $memoryGiB = 0
    $osCaption = "unknown"

    try {
        $cpu = Get-CimInstance Win32_Processor | Measure-Object -Property NumberOfLogicalProcessors -Sum
        $logicalCpu = [int]$cpu.Sum
    }
    catch {
        $logicalCpu = 0
    }

    try {
        $computerSystem = Get-CimInstance Win32_ComputerSystem
        $memoryGiB = [Math]::Round(($computerSystem.TotalPhysicalMemory / 1GB), 2)
    }
    catch {
        $memoryGiB = 0
    }

    try {
        $osCaption = (Get-CimInstance Win32_OperatingSystem).Caption
    }
    catch {
        $osCaption = "unknown"
    }

    return [pscustomobject]@{
        machine_name      = $env:COMPUTERNAME
        os                = $osCaption
        logical_cpu_count = $logicalCpu
        total_memory_gib  = $memoryGiB
    }
}

if ($MaxInFlight -le 0) {
    $defaultProducerRate = [Math]::Max(1, [Math]::Ceiling($Rate / [double]$ProducerCount))
    $MaxInFlight = [Math]::Min([Math]::Max($defaultProducerRate, 64), 2048)
}

$savedEnvironment = Save-Environment
$loadProcesses = @()
$producerRuns = @()
$resumeSimulator = $false
$restoreProcessorReplicas = $null
$producerStdoutPath = $null
$producerStderrPath = $null
$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$originalProcessorReplicas = @(Get-ComposeServiceContainerIds -ComposeFilePath $ComposeFile -Service "stream-processor").Count

if ($PauseComposeSimulator) {
    if (@(Get-ComposeServiceContainerIds -ComposeFilePath $ComposeFile -Service "producer-simulator").Count -gt 0) {
        Stop-ComposeSimulator -ComposeFilePath $ComposeFile
        if ($LASTEXITCODE -eq 0) {
            $resumeSimulator = $true
        }
    }
}

$samples = @()
$startSample = $null
$endSample = $null
$startMetrics = $null
$endMetrics = $null
$reportPath = $OutputPath
$producerStdoutPath = Get-ProducerStdoutPath -Index 1
$producerStderrPath = Get-ProducerStderrPath -Index 1

try {
    if ($ProcessorReplicas -gt 0 -and $ProcessorReplicas -ne $originalProcessorReplicas) {
        Write-Host "Scaling stream-processor to $ProcessorReplicas replicas..."
        $restoreProcessorReplicas = $originalProcessorReplicas
        docker compose -f $ComposeFile up -d --scale stream-processor=$ProcessorReplicas stream-processor prometheus | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to scale stream-processor."
        }

        $processorContainers = @(Wait-ComposeServiceReplicas -ComposeFilePath $ComposeFile -Service "stream-processor" -ExpectedCount $ProcessorReplicas -TimeoutSeconds 90)
        if ($processorContainers.Count -ne $ProcessorReplicas) {
            throw "Timed out waiting for $ProcessorReplicas stream-processor replicas."
        }
    }

    if ($PauseComposeSimulator) {
        Stop-ComposeSimulator -ComposeFilePath $ComposeFile
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

    if ($ProducerMode -eq "local") {
        Write-Host "Building benchmark producer binary..."
        go build -o $binaryPath ./services/producer-simulator
        if ($LASTEXITCODE -ne 0) {
            Restore-Environment $savedEnvironment
            throw "Failed to build benchmark producer binary."
        }

        Write-Host "Starting $ProducerCount local benchmark producer(s) against $Endpoint at $Rate total events/sec for $DurationSeconds seconds..."
        for ($i = 1; $i -le $ProducerCount; $i++) {
            $producerRate = Get-ProducerRate -Index $i
            $stdoutPath = Get-ProducerStdoutPath -Index $i
            $stderrPath = Get-ProducerStderrPath -Index $i
            $producerID = "bench-$runId-$i"
            $healthUrl = $SimulatorHealthEndpoint

            $env:SIM_RATE_PER_SEC = "$producerRate"
            $env:SIM_TENANT_COUNT = "$TenantCount"
            $env:SIM_SOURCES_PER_TENANT = "$SourcesPerTenant"
            $env:SIM_MAX_IN_FLIGHT = "$MaxInFlight"
            $env:SIM_DUPLICATE_EVERY = "$DuplicateEvery"
            $env:SIM_MALFORMED_EVERY = "$MalformedEvery"
            $env:SIM_BURST_EVERY = $BurstEvery
            $env:SIM_BURST_SIZE = "$BurstSize"
            $env:SIM_PRODUCER_ID = $producerID
            $env:SIM_SEED = "$([int64]42 + $i)"
            $env:SIM_INGEST_ENDPOINT = $Endpoint
            $env:SIM_BEARER_TOKEN = $BearerToken
            $env:HTTP_ACCESS_LOG_ENABLED = "false"
            $env:OTEL_TRACES_EXPORTER = "none"

            if ($ProducerCount -gt 1) {
                $listenPort = $ProducerPortStart + $i - 1
                $env:SIMULATOR_LISTEN_ADDR = ":$listenPort"
                $healthUrl = "http://localhost:$listenPort/healthz"
            }
            else {
                Remove-Item "Env:SIMULATOR_LISTEN_ADDR" -ErrorAction SilentlyContinue
            }

            $process = Start-Process `
                -FilePath $binaryPath `
                -WorkingDirectory $repoRoot `
                -PassThru `
                -WindowStyle Hidden `
                -RedirectStandardOutput $stdoutPath `
                -RedirectStandardError $stderrPath

            $loadProcesses += $process
            $producerRuns += [pscustomobject]@{
                mode        = "local"
                name        = $producerID
                rate        = $producerRate
                producer_id = $producerID
                health_url  = $healthUrl
                stdout_log  = $stdoutPath
                stderr_log  = $stderrPath
            }
        }
    }
    else {
        Write-Host "Starting $ProducerCount compose benchmark producer(s) against $ComposeIngestEndpoint at $Rate total events/sec for $DurationSeconds seconds..."
        for ($i = 1; $i -le $ProducerCount; $i++) {
            $containerName = Get-BenchmarkContainerName -Index $i
            $producerRate = Get-ProducerRate -Index $i
            $stdoutPath = Get-ProducerStdoutPath -Index $i
            $stderrPath = Get-ProducerStderrPath -Index $i
            $producerID = "bench-$runId-$i"

            if (Test-ContainerExists -Name $containerName) {
                docker rm -f $containerName | Out-Null
            }

            $composeArgs = @(
                "compose", "-f", $ComposeFile,
                "run", "-d",
                "--name", $containerName,
                "--no-deps"
            )
            if ($ProducerCount -eq 1) {
                $composeArgs += "--service-ports"
            }
            $composeArgs += @(
                "-e", "SIM_INGEST_ENDPOINT=$ComposeIngestEndpoint",
                "-e", "SIM_BEARER_TOKEN=$BearerToken",
                "-e", "SIM_RATE_PER_SEC=$producerRate",
                "-e", "SIM_TENANT_COUNT=$TenantCount",
                "-e", "SIM_SOURCES_PER_TENANT=$SourcesPerTenant",
                "-e", "SIM_MAX_IN_FLIGHT=$MaxInFlight",
                "-e", "SIM_DUPLICATE_EVERY=$DuplicateEvery",
                "-e", "SIM_MALFORMED_EVERY=$MalformedEvery",
                "-e", "SIM_BURST_EVERY=$BurstEvery",
                "-e", "SIM_BURST_SIZE=$BurstSize",
                "-e", "SIM_PRODUCER_ID=$producerID",
                "-e", "SIM_SEED=$([int64]42 + $i)",
                "-e", "HTTP_ACCESS_LOG_ENABLED=false",
                "-e", "OTEL_TRACES_EXPORTER=none",
                "producer-simulator"
            )

            & docker @composeArgs | Out-Null
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to start compose benchmark producer container $containerName."
            }

            $producerRuns += [pscustomobject]@{
                mode        = "compose"
                name        = $containerName
                rate        = $producerRate
                producer_id = $producerID
                health_url  = "container:http://localhost:8083/healthz"
                stdout_log  = $stdoutPath
                stderr_log  = $stderrPath
            }
        }
    }

    foreach ($producer in @($producerRuns)) {
        if ($producer.mode -eq "compose" -and $ProducerCount -gt 1) {
            if (-not (Wait-ContainerHTTPReady -ContainerName $producer.name -TimeoutSeconds 20)) {
                throw "Benchmark producer $($producer.name) did not become ready inside its container."
            }
        }
        elseif (-not (Wait-HttpReady -Url $producer.health_url -TimeoutSeconds 20)) {
            throw "Benchmark producer $($producer.name) did not become ready on $($producer.health_url)."
        }
    }

    if ($WarmupSeconds -gt 0) {
        Write-Host "Warming up for $WarmupSeconds seconds..."
        Start-Sleep -Seconds $WarmupSeconds
    }

    $startMetrics = Get-ServiceMetricsSnapshot -PrometheusBaseUrl $PrometheusEndpoint
    $startSample = Get-BenchmarkSample -OverviewUrl $OverviewEndpoint -PrometheusBaseUrl $PrometheusEndpoint -Token $BearerToken

    $deadline = (Get-Date).AddSeconds($DurationSeconds)
    while ((Get-Date) -lt $deadline) {
        $samples += Get-BenchmarkSample -OverviewUrl $OverviewEndpoint -PrometheusBaseUrl $PrometheusEndpoint -Token $BearerToken
        Start-Sleep -Milliseconds $SampleIntervalMilliseconds
    }

    Start-Sleep -Seconds 2
    $endMetrics = Get-ServiceMetricsSnapshot -PrometheusBaseUrl $PrometheusEndpoint
    $endSample = Get-BenchmarkSample -OverviewUrl $OverviewEndpoint -PrometheusBaseUrl $PrometheusEndpoint -Token $BearerToken
}
finally {
    if ($ProducerMode -eq "local") {
        foreach ($process in @($loadProcesses)) {
            if ($null -ne $process -and -not $process.HasExited) {
                Stop-Process -Id $process.Id -Force
            }
        }
        Restore-Environment $savedEnvironment
    }
    else {
        foreach ($producer in @($producerRuns)) {
            try {
                if (Test-ContainerExists -Name $producer.name) {
                    docker logs $producer.name 1> $producer.stdout_log 2> $producer.stderr_log
                }
            }
            catch {
                Write-Warning "Failed to capture benchmark container logs for $($producer.name)."
            }
        }
        Stop-BenchmarkContainers -ProducerRuns $producerRuns
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
            Write-Warning "Failed to restart compose producer-simulator."
        }
    }
}

if ($null -eq $startSample -or $null -eq $endSample -or $null -eq $startMetrics -or $null -eq $endMetrics) {
    throw "Benchmark did not capture the required samples."
}

$measuredSeconds = [Math]::Round(($endSample.timestamp_utc - $startSample.timestamp_utc).TotalSeconds, 2)
$acceptedDelta = [int64]$endMetrics.ingest.accepted_total - [int64]$startMetrics.ingest.accepted_total
$processedDelta = [int64]$endMetrics.processor.processed_total - [int64]$startMetrics.processor.processed_total
$duplicateDelta = [int64]$endMetrics.processor.duplicate_total - [int64]$startMetrics.processor.duplicate_total
$rejectedDelta = [int64]$endMetrics.ingest.rejected_total - [int64]$startMetrics.ingest.rejected_total
$producerSentDelta = [int64]$endMetrics.simulator.sent_total - [int64]$startMetrics.simulator.sent_total
$producerFailedDelta = [int64]$endMetrics.simulator.failed_total - [int64]$startMetrics.simulator.failed_total

$queryLatencies = @($samples | ForEach-Object { [double]$_.query_latency_ms })
$peakLag = 0
$peakP50 = 0
$peakP95 = 0
$peakP99 = 0
$peakProcessorReplicas = 0
$peakActivePartitions = 0
$peakInFlight = 0

foreach ($sample in $samples) {
    if ([int64]$sample.consumer_lag -gt $peakLag) {
        $peakLag = [int64]$sample.consumer_lag
    }
    if ([double]$sample.overview.processing_p50_ms -gt $peakP50) {
        $peakP50 = [double]$sample.overview.processing_p50_ms
    }
    if ([double]$sample.overview.processing_p95_ms -gt $peakP95) {
        $peakP95 = [double]$sample.overview.processing_p95_ms
    }
    if ([double]$sample.overview.processing_p99_ms -gt $peakP99) {
        $peakP99 = [double]$sample.overview.processing_p99_ms
    }
    if ([int]$sample.processor_replicas -gt $peakProcessorReplicas) {
        $peakProcessorReplicas = [int]$sample.processor_replicas
    }
    if ([int64]$sample.active_partitions -gt $peakActivePartitions) {
        $peakActivePartitions = [int64]$sample.active_partitions
    }
    if ([int64]$sample.inflight_messages -gt $peakInFlight) {
        $peakInFlight = [int64]$sample.inflight_messages
    }
}

$acceptedEPS = [Math]::Round(($acceptedDelta / [Math]::Max($measuredSeconds, 1)), 2)
$processedEPS = [Math]::Round(($processedDelta / [Math]::Max($measuredSeconds, 1)), 2)
$rejectedEPS = [Math]::Round(($rejectedDelta / [Math]::Max($measuredSeconds, 1)), 2)
$producerSentEPS = [Math]::Round(($producerSentDelta / [Math]::Max($measuredSeconds, 1)), 2)
$producerFailedEPS = [Math]::Round(($producerFailedDelta / [Math]::Max($measuredSeconds, 1)), 2)
$offeredToAcceptedRatio = 0
if ($producerSentDelta -gt 0) {
    $offeredToAcceptedRatio = [Math]::Round(($acceptedDelta / [double]$producerSentDelta), 4)
}

$report = [ordered]@{
    producer_mode               = $ProducerMode
    producer_count              = $ProducerCount
    started_at_utc              = $startSample.timestamp_utc
    completed_at_utc            = $endSample.timestamp_utc
    measured_duration_seconds   = $measuredSeconds
    rate_target_eps             = $Rate
    warmup_seconds              = $WarmupSeconds
    sample_interval_ms          = $SampleIntervalMilliseconds
    tenant_count                = $TenantCount
    sources_per_tenant          = $SourcesPerTenant
    max_in_flight_per_producer  = $MaxInFlight
    duplicate_every             = $DuplicateEvery
    malformed_every             = $MalformedEvery
    burst_every                 = $BurstEvery
    burst_size                  = $BurstSize
    processor_replicas_requested = $ProcessorReplicas
    processor_replicas_observed = $endMetrics.processor.replica_count
    peak_processor_replicas     = $peakProcessorReplicas
    peak_active_partitions      = $peakActivePartitions
    peak_inflight_messages      = $peakInFlight
    producer_sent_total_delta   = $producerSentDelta
    producer_failed_total_delta = $producerFailedDelta
    accepted_total_delta        = $acceptedDelta
    processed_total_delta       = $processedDelta
    duplicate_total_delta       = $duplicateDelta
    rejected_total_delta        = $rejectedDelta
    producer_sent_eps           = $producerSentEPS
    producer_failed_eps         = $producerFailedEPS
    accepted_eps                = $acceptedEPS
    processed_eps               = $processedEPS
    rejected_eps                = $rejectedEPS
    offered_to_accepted_ratio   = $offeredToAcceptedRatio
    peak_consumer_lag           = $peakLag
    peak_processing_p50_ms      = $peakP50
    peak_processing_p95_ms      = $peakP95
    peak_processing_p99_ms      = $peakP99
    query_latency_p50_ms        = [Math]::Round((Get-Percentile -Values $queryLatencies -Percentile 50), 2)
    query_latency_p95_ms        = [Math]::Round((Get-Percentile -Values $queryLatencies -Percentile 95), 2)
    query_latency_p99_ms        = [Math]::Round((Get-Percentile -Values $queryLatencies -Percentile 99), 2)
    sample_count                = $samples.Count
    producer_stdout_log         = $producerStdoutPath
    producer_stderr_log         = $producerStderrPath
    producer_runs               = @($producerRuns | ForEach-Object {
        [ordered]@{
            name        = $_.name
            mode        = $_.mode
            rate        = $_.rate
            producer_id = $_.producer_id
            stdout_log  = $_.stdout_log
            stderr_log  = $_.stderr_log
        }
    })
    machine                     = Get-HardwareSummary
}

if ([string]::IsNullOrWhiteSpace($reportPath)) {
    $reportPath = Join-Path $artifactDir "benchmark-$runId.json"
}

$report | ConvertTo-Json -Depth 8 | Set-Content -Path $reportPath

Write-Host ""
Write-Host "Benchmark report written to $reportPath"
Write-Host ("Producer Mode       : {0}" -f $report.producer_mode)
Write-Host ("Producer Count      : {0}" -f $report.producer_count)
Write-Host ("Processor Replicas  : {0}" -f $report.processor_replicas_observed)
Write-Host ("Producer EPS        : {0}" -f $report.producer_sent_eps)
Write-Host ("Accepted EPS        : {0}" -f $report.accepted_eps)
Write-Host ("Processed EPS       : {0}" -f $report.processed_eps)
Write-Host ("Peak lag            : {0}" -f $report.peak_consumer_lag)
Write-Host ("Peak partitions     : {0}" -f $report.peak_active_partitions)
Write-Host ("Peak in-flight      : {0}" -f $report.peak_inflight_messages)
Write-Host ("Peak P95 ms         : {0}" -f $report.peak_processing_p95_ms)
Write-Host ("Peak P99 ms         : {0}" -f $report.peak_processing_p99_ms)
Write-Host ("Query P95 ms        : {0}" -f $report.query_latency_p95_ms)
Write-Host ("Markdown row        : | {0} | {1} | {2}s | {3} | {4} | {5} | {6} | {7} | {8} producers, mode {9}, replicas {10}, max_in_flight/producer {11} |" -f `
    (Get-Date -Format "yyyy-MM-dd"), `
    $Rate, `
    [Math]::Round($measuredSeconds, 0), `
    $report.accepted_eps, `
    $report.processed_eps, `
    $report.peak_processing_p95_ms, `
    $report.peak_processing_p99_ms, `
    $report.peak_consumer_lag, `
    $report.producer_count, `
    $report.producer_mode, `
    $report.processor_replicas_observed, `
    $report.max_in_flight_per_producer)

$evidenceScript = Join-Path $repoRoot "scripts/evidence/update-evidence.ps1"
if (Test-Path $evidenceScript) {
    try {
        & $evidenceScript | Out-Null
    }
    catch {
        Write-Warning "Benchmark completed, but evidence summary refresh failed: $($_.Exception.Message)"
    }
}
