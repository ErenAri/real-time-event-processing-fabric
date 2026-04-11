param(
    [string]$ComposeFile = "deploy/docker-compose/docker-compose.yml",
    [string]$BootstrapServer = "kafka:9092",
    [string]$Topic = "pulsestream.events",
    [string]$Payload = '{"schema_version":1',
    [string]$OverviewEndpoint = "http://localhost:8081/api/v1/metrics/overview",
    [string]$BearerToken = "",
    [switch]$PauseComposeSimulator = $true,
    [int]$DrainTimeoutSeconds = 60,
    [int]$MaxLagBeforeInject = 0,
    [int]$TimeoutSeconds = 15,
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

function Get-Overview {
    param(
        [string]$Url,
        [hashtable]$Headers
    )

    return Invoke-RestMethod -Method Get -Uri $Url -Headers $Headers
}

function ConvertTo-SingleQuotedShellLiteral {
    param([string]$Value)

    return $Value.Replace("'", "'""'""'")
}

function Stop-ComposeSimulator {
    param([string]$ComposeFilePath)

    docker compose -f $ComposeFilePath stop producer-simulator | Out-Null
}

function Start-ComposeSimulator {
    param([string]$ComposeFilePath)

    docker compose -f $ComposeFilePath start producer-simulator | Out-Null
}

$headers = Get-AuthHeaders -Token $BearerToken
$simulatorStopped = $false

try {
    if ($PauseComposeSimulator) {
        Stop-ComposeSimulator -ComposeFilePath $ComposeFile
        $simulatorStopped = $true
    }

    $drainDeadline = (Get-Date).AddSeconds($DrainTimeoutSeconds)
    do {
        $before = Get-Overview -Url $OverviewEndpoint -Headers $headers
        if ([int64]$before.consumer_lag -le [int64]$MaxLagBeforeInject) {
            break
        }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $drainDeadline)

    if ([int64]$before.consumer_lag -gt [int64]$MaxLagBeforeInject) {
        throw "consumer lag did not drain to $MaxLagBeforeInject within $DrainTimeoutSeconds seconds"
    }

    $startedAt = Get-Date

    $shellPayload = ConvertTo-SingleQuotedShellLiteral -Value $Payload
    $shellTopic = ConvertTo-SingleQuotedShellLiteral -Value $Topic
    $shellBootstrap = ConvertTo-SingleQuotedShellLiteral -Value $BootstrapServer
    $publishCommand = "printf '%s\n' '$shellPayload' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server '$shellBootstrap' --topic '$shellTopic' >/dev/null"
    docker compose -f $ComposeFile exec -T kafka sh -lc $publishCommand | Out-Null

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $after = $null
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Milliseconds 500
        $candidate = Get-Overview -Url $OverviewEndpoint -Headers $headers
        if ([int64]$candidate.dead_letter_total -gt [int64]$before.dead_letter_total) {
            $after = $candidate
            break
        }
    }

    if ($null -eq $after) {
        throw "dead-letter total did not increase within $TimeoutSeconds seconds"
    }

    $result = [ordered]@{
        started_at = $startedAt.ToUniversalTime().ToString("o")
        finished_at = (Get-Date).ToUniversalTime().ToString("o")
        bootstrap_server = $BootstrapServer
        topic = $Topic
        payload = $Payload
        paused_compose_simulator = [bool]$PauseComposeSimulator
        consumer_lag_before = [int64]$before.consumer_lag
        dead_letter_total_before = [int64]$before.dead_letter_total
        dead_letter_total_after = [int64]$after.dead_letter_total
        dead_letter_delta = [int64]$after.dead_letter_total - [int64]$before.dead_letter_total
        consumer_lag_after = [int64]$after.consumer_lag
        processor_instances_after = [int]$after.processor_instances
    }

    if ([string]::IsNullOrWhiteSpace($OutputPath)) {
        $timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")
        $OutputPath = Join-Path $artifactDir "inject-poison-message-$timestamp.json"
    }

    $result | ConvertTo-Json -Depth 6 | Set-Content -Encoding utf8 $OutputPath
    Write-Host "Poison-message drill artifact written to $OutputPath"
    Write-Host ($result | ConvertTo-Json -Depth 6)
}
finally {
    if ($simulatorStopped) {
        Start-ComposeSimulator -ComposeFilePath $ComposeFile
    }
}
