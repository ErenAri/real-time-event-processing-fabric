param(
    [int]$Rate = 2000,
    [int]$DurationSeconds = 60,
    [int]$WarmupSeconds = 10,
    [int]$ProcessorReplicas = 3,
    [int]$ProducerCount = 4,
    [int]$BatchSize = 25,
    [int]$MaxInFlight = 768,
    [int]$TenantCount = 50,
    [int]$SourcesPerTenant = 200,
    [string]$ComposeFile = "deploy/docker-compose/docker-compose.yml",
    [switch]$ResetVolumes,
    [switch]$NoBuild,
    [switch]$AllowGateFailure
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path "$PSScriptRoot/../.."
$benchmarkScript = Join-Path $repoRoot "scripts/load-test/benchmark.ps1"
$evidenceScript = Join-Path $repoRoot "scripts/evidence/update-evidence.ps1"
$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$reportPath = Join-Path $repoRoot "artifacts/benchmarks/benchmark-performance-gate-$runId.json"

function Invoke-Checked {
    param(
        [string]$FilePath,
        [string[]]$Arguments
    )

    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $LASTEXITCODE`: $FilePath $($Arguments -join ' ')"
    }
}

function Wait-HttpReady {
    param(
        [string]$Url,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 5 | Out-Null
            return $true
        }
        catch {
            Start-Sleep -Milliseconds 500
        }
    }

    return $false
}

function Invoke-ComposeUpWithRetry {
    param([string[]]$Arguments)

    $maxAttempts = 2
    for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
        & docker @Arguments
        if ($LASTEXITCODE -eq 0) {
            return
        }
        if ($attempt -eq $maxAttempts) {
            throw "Command failed with exit code $LASTEXITCODE`: docker $($Arguments -join ' ')"
        }

        Write-Warning "Docker Compose startup failed; waiting 30 seconds before retry $($attempt + 1)/$maxAttempts."
        Start-Sleep -Seconds 30
    }
}

Push-Location $repoRoot
try {
    if ($ResetVolumes) {
        Write-Host "Resetting Compose stack and named volumes..."
        Invoke-Checked "docker" @("compose", "-f", $ComposeFile, "down", "--volumes", "--remove-orphans")
    }

    $upArgs = @("compose", "-f", $ComposeFile, "up", "-d")
    if (-not $NoBuild) {
        $upArgs += "--build"
    }
    Write-Host "Starting local stack..."
    Invoke-ComposeUpWithRetry $upArgs

    foreach ($probe in @("http://localhost:8080/healthz", "http://localhost:8081/healthz", "http://localhost:9090/-/ready")) {
        if (-not (Wait-HttpReady -Url $probe -TimeoutSeconds 120)) {
            throw "Timed out waiting for $probe."
        }
    }

    Write-Host "Running performance gate: target=$Rate eps, producers=$ProducerCount, processors=$ProcessorReplicas..."
    & $benchmarkScript `
        -Rate $Rate `
        -DurationSeconds $DurationSeconds `
        -WarmupSeconds $WarmupSeconds `
        -ProcessorReplicas $ProcessorReplicas `
        -ProducerCount $ProducerCount `
        -BatchSize $BatchSize `
        -MaxInFlight $MaxInFlight `
        -TenantCount $TenantCount `
        -SourcesPerTenant $SourcesPerTenant `
        -OutputPath $reportPath
    if ($LASTEXITCODE -ne 0) {
        throw "Benchmark harness failed with exit code $LASTEXITCODE."
    }

    $report = Get-Content -Raw $reportPath | ConvertFrom-Json
    $report | Add-Member -Force -NotePropertyName gate_workflow -NotePropertyValue ([ordered]@{
        script = "scripts/load-test/run-performance-gate.ps1"
        reset_volumes = [bool]$ResetVolumes
        no_build = [bool]$NoBuild
        rate_eps = $Rate
        duration_seconds = $DurationSeconds
        warmup_seconds = $WarmupSeconds
        processor_replicas = $ProcessorReplicas
        producer_count = $ProducerCount
        max_in_flight_per_producer = $MaxInFlight
        batch_size = $BatchSize
    })
    $report | ConvertTo-Json -Depth 10 | Set-Content -Path $reportPath

    if (Test-Path $evidenceScript) {
        & $evidenceScript | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Evidence summary refresh failed."
        }
    }

    Invoke-Checked "cmd" @("/c", "npm", "run", "evidence:validate")

    Write-Host ""
    Write-Host "Performance gate report: $reportPath"
    foreach ($gate in @($report.gates)) {
        Write-Host ("{0}: {1} observed={2} target={3}{4}" -f $gate.name, $gate.status, $gate.observed, $gate.target, $gate.unit)
    }

    $failedGates = @($report.gates | Where-Object { $_.status -ne "pass" })
    if ($failedGates.Count -gt 0 -and -not $AllowGateFailure) {
        throw "Performance gate failed: $(@($failedGates | ForEach-Object { $_.name }) -join ', ')"
    }
}
finally {
    Pop-Location
}
