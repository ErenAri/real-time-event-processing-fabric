param(
    [string]$ComposeFile = "deploy/docker-compose/docker-compose.yml",
    [string]$BootstrapServer = "kafka:9092",
    [string]$Topic = "pulsestream.events",
    [string]$Payload = '{"schema_version":1',
    [string]$OverviewEndpoint = "http://localhost:8081/api/v1/metrics/overview",
    [string]$BearerToken = "",
    [switch]$PauseComposeSimulator = $true,
    [int]$ReadyTimeoutSeconds = 10,
    [int]$TimeoutSeconds = 20,
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

function ConvertTo-Base64Utf8 {
    param([string]$Value)

    return [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Value))
}

function Stop-ComposeSimulator {
    param([string]$ComposeFilePath)

    docker compose -f $ComposeFilePath stop producer-simulator | Out-Null
}

function Start-ComposeSimulator {
    param([string]$ComposeFilePath)

    docker compose -f $ComposeFilePath start producer-simulator | Out-Null
}

function Get-PrimaryProcessorContainerId {
    param([string]$ComposeFilePath)

    $containerIds = @(docker compose -f $ComposeFilePath ps -q stream-processor)
    $containerIds = @($containerIds | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($containerIds.Count -eq 0) {
        throw "stream-processor service is not running"
    }

    return $containerIds[0]
}

function Get-ContainerEnvMap {
    param([string]$ContainerId)

    $envMap = @{}
    $inspection = docker inspect $ContainerId | ConvertFrom-Json
    $lines = @($inspection[0].Config.Env)
    foreach ($line in $lines) {
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

function Start-DrillProcessor {
    param(
        [string]$ComposeFilePath,
        [string]$TopicName,
        [int]$ReadySeconds
    )

    $mainProcessor = Get-PrimaryProcessorContainerId -ComposeFilePath $ComposeFilePath
    $envMap = Get-ContainerEnvMap -ContainerId $mainProcessor
    $networkName = Get-ContainerNetworkName -ContainerId $mainProcessor
    if ([string]::IsNullOrWhiteSpace($networkName)) {
        throw "unable to resolve stream-processor network"
    }

    $timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddHHmmss")
    $groupId = "pulsestream-dlq-drill-$timestamp"
    $instanceId = "dlq-drill-$timestamp"
    $containerName = "pulsestream-dlq-drill-$timestamp"

    $dockerArgs = @(
        "run",
        "-d",
        "--rm",
        "--name", $containerName,
        "--network", $networkName,
        "-e", "POSTGRES_URL=$($envMap["POSTGRES_URL"])",
        "-e", "POSTGRES_ADMIN_URL=$($envMap["POSTGRES_ADMIN_URL"])",
        "-e", "KAFKA_BROKERS=$($envMap["KAFKA_BROKERS"])",
        "-e", "KAFKA_TOPIC=$TopicName",
        "-e", "KAFKA_DLQ_TOPIC=$($envMap["KAFKA_DLQ_TOPIC"])",
        "-e", "KAFKA_GROUP_ID=$groupId",
        "-e", "SERVICE_INSTANCE_ID=$instanceId",
        "-e", "PROCESSOR_LISTEN_ADDR=:8082",
        "docker-compose-stream-processor"
    )

    $containerId = (& docker @dockerArgs).Trim()
    if ([string]::IsNullOrWhiteSpace($containerId)) {
        throw "failed to start drill processor container"
    }

    $deadline = (Get-Date).AddSeconds($ReadySeconds)
    do {
        $status = (docker inspect -f "{{.State.Status}}" $containerName 2>$null).Trim()
        if ($status -eq "running") {
            Start-Sleep -Seconds 2
            return [ordered]@{
                container_name = $containerName
                container_id = $containerId
                group_id = $groupId
                instance_id = $instanceId
            }
        }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $deadline)

    throw "drill processor container did not become ready"
}

function Stop-DrillProcessor {
    param([string]$ContainerName)

    if ([string]::IsNullOrWhiteSpace($ContainerName)) {
        return
    }

    docker rm -f $ContainerName 2>$null | Out-Null
}

$headers = Get-AuthHeaders -Token $BearerToken
$simulatorStopped = $false
$drillProcessor = $null

try {
    if ($PauseComposeSimulator) {
        Stop-ComposeSimulator -ComposeFilePath $ComposeFile
        $simulatorStopped = $true
    }

    $before = Get-Overview -Url $OverviewEndpoint -Headers $headers
    $drillProcessor = Start-DrillProcessor -ComposeFilePath $ComposeFile -TopicName $Topic -ReadySeconds $ReadyTimeoutSeconds
    $startedAt = Get-Date

    $payloadBase64 = ConvertTo-Base64Utf8 -Value $Payload
    $shellTopic = ConvertTo-SingleQuotedShellLiteral -Value $Topic
    $shellBootstrap = ConvertTo-SingleQuotedShellLiteral -Value $BootstrapServer
    $publishCommand = "printf '%s' '$payloadBase64' | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server '$shellBootstrap' --topic '$shellTopic' >/dev/null"
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
        drill_processor_group_id = $drillProcessor.group_id
        drill_processor_instance_id = $drillProcessor.instance_id
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

    $evidenceScript = Join-Path $repoRoot "scripts/evidence/update-evidence.ps1"
    if (Test-Path $evidenceScript) {
        try {
            & $evidenceScript | Out-Null
        }
        catch {
            Write-Warning "Poison-message drill completed, but evidence summary refresh failed: $($_.Exception.Message)"
        }
    }
}
finally {
    if ($null -ne $drillProcessor) {
        Stop-DrillProcessor -ContainerName $drillProcessor.container_name
    }
    if ($simulatorStopped) {
        Start-ComposeSimulator -ComposeFilePath $ComposeFile
    }
}
