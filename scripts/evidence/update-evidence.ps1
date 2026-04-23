param(
    [string]$ArtifactRoot = "",
    [string]$OutputPath = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path "$PSScriptRoot/../.."

if ([string]::IsNullOrWhiteSpace($ArtifactRoot)) {
    $ArtifactRoot = Join-Path $repoRoot "artifacts"
}
if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path $ArtifactRoot "evidence/latest.json"
}

$benchmarkDir = Join-Path $ArtifactRoot "benchmarks"
$failureDir = Join-Path $ArtifactRoot "failure-drills"
$outputDir = Split-Path -Parent $OutputPath
New-Item -ItemType Directory -Force $outputDir | Out-Null

function Get-RelativeArtifactPath {
    param([string]$Path)

    $rootPath = (Resolve-Path $repoRoot).Path.TrimEnd("\", "/")
    $targetPath = (Resolve-Path $Path).Path
    if ($targetPath.StartsWith($rootPath, [System.StringComparison]::OrdinalIgnoreCase)) {
        $relativePath = $targetPath.Substring($rootPath.Length).TrimStart("\", "/")
    }
    else {
        $relativePath = $targetPath
    }
    return $relativePath.Replace("\", "/")
}

function Get-LatestJson {
    param(
        [string]$Directory,
        [string]$Pattern
    )

    if (-not (Test-Path $Directory)) {
        return $null
    }

    return Get-ChildItem -Path $Directory -Filter $Pattern -File |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
}

function Read-JsonArtifact {
    param([System.IO.FileInfo]$File)

    if ($null -eq $File) {
        return $null
    }
    return Get-Content -Raw $File.FullName | ConvertFrom-Json
}

function Get-PropertyValue {
    param(
        [object]$Object,
        [string]$Name,
        [object]$Default = $null
    )

    if ($null -eq $Object) {
        return $Default
    }
    if ($Object.PSObject.Properties.Name -notcontains $Name) {
        return $Default
    }
    $value = $Object.$Name
    if ($null -eq $value) {
        return $Default
    }
    return $value
}

function Get-FirstPropertyValue {
    param(
        [object]$Object,
        [string[]]$Names,
        [object]$Default = ""
    )

    foreach ($name in $Names) {
        $value = Get-PropertyValue -Object $Object -Name $name -Default $null
        if ($null -ne $value) {
            return $value
        }
    }
    return $Default
}

function Get-NumberValue {
    param(
        [object]$Object,
        [string]$Name,
        [double]$Default = 0
    )

    $value = Get-PropertyValue -Object $Object -Name $Name -Default $Default
    try {
        return [double]$value
    }
    catch {
        return $Default
    }
}

function Format-EvidenceNumber {
    param(
        [object]$Value,
        [int]$Digits = 0
    )

    try {
        $number = [double]$Value
        return $number.ToString("N$Digits", [System.Globalization.CultureInfo]::InvariantCulture)
    }
    catch {
        return "$Value"
    }
}

function Format-EvidenceTimestamp {
    param([object]$Value)

    if ($null -eq $Value) {
        return ""
    }
    try {
        if ($Value -is [datetime]) {
            return $Value.ToUniversalTime().ToString("o")
        }
        $parsed = [datetime]::Parse("$Value", [System.Globalization.CultureInfo]::InvariantCulture)
        return $parsed.ToUniversalTime().ToString("o")
    }
    catch {
        return "$Value"
    }
}

function New-Metric {
    param(
        [string]$Label,
        [object]$Value,
        [string]$Unit = "",
        [string]$Tone = ""
    )

    $metric = [ordered]@{
        label = $Label
        value = "$Value"
    }
    if (-not [string]::IsNullOrWhiteSpace($Unit)) {
        $metric.unit = $Unit
    }
    if (-not [string]::IsNullOrWhiteSpace($Tone)) {
        $metric.tone = $Tone
    }
    return $metric
}

function New-Gate {
    param(
        [string]$Name,
        [double]$Target,
        [double]$Observed,
        [string]$Unit,
        [bool]$Passed
    )

    return [ordered]@{
        name = $Name
        status = if ($Passed) { "pass" } else { "fail" }
        target = $Target
        observed = $Observed
        unit = $Unit
    }
}

function New-BenchmarkEvidence {
    $file = Get-LatestJson -Directory $benchmarkDir -Pattern "benchmark-*.json"
    $report = Read-JsonArtifact -File $file
    if ($null -eq $file -or $null -eq $report) {
        return $null
    }

    $target = Get-NumberValue -Object $report -Name "rate_target_eps"
    $accepted = Get-NumberValue -Object $report -Name "accepted_eps"
    $processed = Get-NumberValue -Object $report -Name "processed_eps"
    $peakLag = [int64](Get-NumberValue -Object $report -Name "peak_consumer_lag")
    $drainSeconds = if ($null -eq (Get-PropertyValue -Object $report -Name "post_load_drain_seconds" -Default $null)) {
        999999
    }
    else {
        Get-NumberValue -Object $report -Name "post_load_drain_seconds"
    }
    $replicas = [int](Get-NumberValue -Object $report -Name "processor_replicas_observed")
    $producerCount = [int](Get-NumberValue -Object $report -Name "producer_count" -Default 1)
    $batchSize = [int](Get-NumberValue -Object $report -Name "batch_size" -Default 1)
    $queryP95 = Get-NumberValue -Object $report -Name "query_latency_p95_ms"
    $gaps = @()
    $gates = @(
        New-Gate "processed_eps_2000" 2000 $processed "eps" ($processed -ge 2000)
        New-Gate "processed_eps_5000" 5000 $processed "eps" ($processed -ge 5000)
        New-Gate "query_p95_250ms" 250 $queryP95 "ms" ($queryP95 -le 250)
        New-Gate "post_load_drain_30s" 30 $drainSeconds "s" ($drainSeconds -le 30)
    )

    if ($target -gt 0 -and $accepted -lt ($target * 0.8)) {
        $gaps += "Accepted throughput is below 80 percent of target; producer or ingest path is still the bottleneck."
    }
    if ($processed -lt 2000) {
        $gaps += "Processed throughput is below the next credibility gate of 2,000 eps."
    }
    elseif ($processed -lt 5000) {
        $gaps += "Processed throughput cleared the 2,000 eps gate but remains below the MVP target of 5,000 eps."
    }
    if ($peakLag -gt 50000) {
        $gaps += "Peak consumer lag exceeded 50,000 messages; drain capacity needs more work before claiming strong sustained throughput."
    }
    if ($drainSeconds -gt 30) {
        $gaps += "Post-load drain exceeded 30 seconds; processor capacity does not yet clear backlog quickly."
    }
    if ($gaps.Count -eq 0) {
        $gaps += "No benchmark gate failed in the latest artifact; repeat on clean state before making a broader capacity claim."
    }

    return [ordered]@{
        artifact = Get-RelativeArtifactPath -Path $file.FullName
        started_at_utc = Format-EvidenceTimestamp (Get-PropertyValue -Object $report -Name 'started_at_utc' -Default '')
        completed_at_utc = Format-EvidenceTimestamp (Get-PropertyValue -Object $report -Name 'completed_at_utc' -Default '')
        target_eps = $target
        accepted_eps = $accepted
        processed_eps = $processed
        query_p95_ms = $queryP95
        peak_lag = $peakLag
        post_load_drain_seconds = $drainSeconds
        producer_count = $producerCount
        processor_replicas = $replicas
        batch_size = $batchSize
        summary = "Accepted $(Format-EvidenceNumber $accepted 1) eps and processed $(Format-EvidenceNumber $processed 1) eps against a $(Format-EvidenceNumber $target 0) eps target using $producerCount producer(s), $replicas processor replica(s), and batch size $batchSize."
        gaps = $gaps
        gates = $gates
    }
}

function New-DrillEvidence {
    param(
        [string]$ScenarioID,
        [string]$Title,
        [string]$Pattern
    )

    $file = Get-LatestJson -Directory $failureDir -Pattern $Pattern
    $report = Read-JsonArtifact -File $file
    if ($null -eq $file -or $null -eq $report) {
        return [ordered]@{
            scenario_id = $ScenarioID
            title = $Title
            status = "missing"
            artifact = ""
            started_at_utc = ""
            completed_at_utc = ""
            result = "No artifact found for this drill."
            operator_note = "Run the matching script under scripts/chaos to generate evidence."
            remaining_gap = "Evidence is not yet reproducible for this scenario."
            metrics = @()
        }
    }

    $startedAt = Format-EvidenceTimestamp (Get-FirstPropertyValue -Object $report -Names @('started_at_utc', 'started_at') -Default '')
    $completedAt = Format-EvidenceTimestamp (Get-FirstPropertyValue -Object $report -Names @('completed_at_utc', 'finished_at') -Default '')
    $artifact = Get-RelativeArtifactPath -Path $file.FullName

    switch ($ScenarioID) {
        "restart-processor" {
            $recovered = [bool](Get-PropertyValue -Object $report -Name "lag_recovered_within_window" -Default $false)
            $recoverySeconds = Get-PropertyValue -Object $report -Name "recovery_seconds" -Default $null
            $peakLag = [int64](Get-NumberValue -Object $report -Name "peak_consumer_lag")
            $finalLag = [int64](Get-NumberValue -Object $report -Name "final_consumer_lag")
            return [ordered]@{
                scenario_id = $ScenarioID
                title = $Title
                status = if ($recovered) { "verified" } else { "degraded" }
                artifact = $artifact
                started_at_utc = $startedAt
                completed_at_utc = $completedAt
                result = if ($recovered) { "Lag recovered in $recoverySeconds seconds." } else { "Processor restarted, but lag did not drain inside the observation window." }
                operator_note = "Consumer group recovery is observable through lag, processed delta, and replica count."
                remaining_gap = if ($recovered) { "Repeat at higher load and replica counts." } else { "Increase drain capacity or extend the observation window before claiming fast recovery." }
                metrics = @(
                    New-Metric "accepted delta" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "accepted_total_delta") 0)
                    New-Metric "processed delta" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "processed_total_delta") 0)
                    New-Metric "peak lag" (Format-EvidenceNumber $peakLag 0)
                    New-Metric "final lag" (Format-EvidenceNumber $finalLag 0)
                )
            }
        }
        "broker-outage" {
            $accountingGap = [int64](Get-NumberValue -Object $report -Name "archive_accounting_gap")
            $recovered = [bool](Get-PropertyValue -Object $report -Name "accepted_recovered_within_window" -Default $false)
            return [ordered]@{
                scenario_id = $ScenarioID
                title = $Title
                status = if ($accountingGap -eq 0 -and $recovered) { "verified" } else { "degraded" }
                artifact = $artifact
                started_at_utc = $startedAt
                completed_at_utc = $completedAt
                result = "Archive accounting gap $accountingGap; accepted traffic recovered: $recovered."
                operator_note = "Publish failures and backpressure are explicit, and the processor should remain live while Kafka is unavailable."
                remaining_gap = "Client-side timeout counts still require log parsing before they are first-class evidence."
                metrics = @(
                    New-Metric "accepted delta" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "accepted_total_delta") 0)
                    New-Metric "publish failed" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "publish_failed_total_delta") 0)
                    New-Metric "backpressure" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "backpressure_total_delta") 0)
                    New-Metric "peak lag" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "peak_consumer_lag") 0)
                )
            }
        }
        "pause-postgres" {
            $recovered = [bool](Get-PropertyValue -Object $report -Name "processed_recovered_within_window" -Default $false)
            $recoverySeconds = Get-PropertyValue -Object $report -Name "processed_recovery_seconds" -Default $null
            return [ordered]@{
                scenario_id = $ScenarioID
                title = $Title
                status = if ($recovered) { "verified" } else { "degraded" }
                artifact = $artifact
                started_at_utc = $startedAt
                completed_at_utc = $completedAt
                result = if ($recovered) { "Processor progress recovered in $recoverySeconds seconds after Postgres became ready." } else { "Processor progress did not recover inside the observation window." }
                operator_note = "The drill captures write-path degradation, read-path failure, and recovery timing."
                remaining_gap = "Final lag and producer timeouts still define the overload cleanup work."
                metrics = @(
                    New-Metric "processed delta" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "processed_total_delta") 0)
                    New-Metric "overview failures" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "overview_failure_count") 0)
                    New-Metric "peak API latency" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "peak_overview_latency_ms") 2) "ms"
                    New-Metric "final lag" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "final_consumer_lag") 0)
                )
            }
        }
        "replay-archive" {
            $duplicateSafe = [bool](Get-PropertyValue -Object $report -Name "duplicate_safe" -Default $false)
            $rebuildRestored = [bool](Get-PropertyValue -Object $report -Name "rebuild_restored" -Default $false)
            $rebuildProcessedByVerifier = [bool](Get-PropertyValue -Object $report -Name "rebuild_processed_by_verifier" -Default $false)
            return [ordered]@{
                scenario_id = $ScenarioID
                title = $Title
                status = if ($duplicateSafe -and $rebuildRestored -and $rebuildProcessedByVerifier) { "verified" } else { "degraded" }
                artifact = $artifact
                started_at_utc = $startedAt
                completed_at_utc = $completedAt
                result = "Duplicate-safe: $duplicateSafe; rebuild restored: $rebuildRestored; verifier observed rebuild: $rebuildProcessedByVerifier."
                operator_note = "Replay republishes archive records through the same Kafka and processor path."
                remaining_gap = "Repeat replay drills against the tenant/hour indexed archive layout under larger data volumes."
                metrics = @(
                    New-Metric "replayed duplicates" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "replay_response_replayed") 0)
                    New-Metric "duplicate delta" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "verifier_duplicate_delta_after_replay") 0)
                    New-Metric "overcount delta" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "source_metric_overcount_delta") 0)
                    New-Metric "rebuild replayed" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "rebuild_replay_response_replayed") 0)
                    New-Metric "rebuild verifier delta" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "verifier_processed_delta_after_rebuild") 0)
                )
            }
        }
        "poison-message" {
            $deadLetterDelta = [int64](Get-NumberValue -Object $report -Name "dead_letter_delta")
            return [ordered]@{
                scenario_id = $ScenarioID
                title = $Title
                status = if ($deadLetterDelta -gt 0) { "verified" } else { "degraded" }
                artifact = $artifact
                started_at_utc = $startedAt
                completed_at_utc = $completedAt
                result = "Dead-letter delta $deadLetterDelta."
                operator_note = "A poison Kafka record should be isolated into the DLQ without blocking the processor loop."
                remaining_gap = "Add dashboard-level drill trigger only after the evidence endpoint remains stable."
                metrics = @(
                    New-Metric "DLQ delta" (Format-EvidenceNumber $deadLetterDelta 0)
                    New-Metric "lag before" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "consumer_lag_before") 0)
                    New-Metric "lag after" (Format-EvidenceNumber (Get-NumberValue -Object $report -Name "consumer_lag_after") 0)
                )
            }
        }
    }
}

$failureDrills = @(
    New-DrillEvidence -ScenarioID "restart-processor" -Title "Processor restart" -Pattern "restart-processor-*.json"
    New-DrillEvidence -ScenarioID "broker-outage" -Title "Broker outage" -Pattern "broker-outage-*.json"
    New-DrillEvidence -ScenarioID "pause-postgres" -Title "Postgres pause" -Pattern "pause-postgres-*.json"
    New-DrillEvidence -ScenarioID "replay-archive" -Title "Replay rebuild" -Pattern "replay-archive-*.json"
    New-DrillEvidence -ScenarioID "poison-message" -Title "Poison message" -Pattern "inject-poison-message-*.json"
)

$summary = [ordered]@{
    schema_version = 1
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    status = "available"
    artifact_root = (Get-RelativeArtifactPath -Path $ArtifactRoot)
    benchmark = New-BenchmarkEvidence
    failure_drills = $failureDrills
}

$summary | ConvertTo-Json -Depth 12 | Set-Content -Path $OutputPath -Encoding utf8

Write-Host "Evidence summary written to $OutputPath"
