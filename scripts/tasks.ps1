<#
.SYNOPSIS
    Windows PowerShell helper script for project lifecycle tasks (equivalent to Makefile).

.EXAMPLE
    .\scripts\tasks.ps1 lint
    .\scripts\tasks.ps1 test
    .\scripts\tasks.ps1 format
#>

param (
    [Parameter(Position = 0, Mandatory = $true)]
    [ValidateSet("lint", "format", "test", "up", "down", "dashboard", "backfill", "clean", "demo", "help")]
    [string]$Task
)

$ErrorActionPreference = "Stop"

# Auto-detect virtualenv executable paths
$PythonExe = "python"
$RuffExe = "ruff"
$PytestExe = "pytest"
$StreamlitExe = "streamlit"

if (Test-Path ".\.venv\Scripts\python.exe") {
    $PythonExe = ".\.venv\Scripts\python.exe"
    $RuffExe = ".\.venv\Scripts\ruff.exe"
    $PytestExe = ".\.venv\Scripts\pytest.exe"
    $StreamlitExe = ".\.venv\Scripts\streamlit.exe"
}

switch ($Task) {
    "lint" {
        Write-Host "--> Running Ruff Lint..." -ForegroundColor Cyan
        & $RuffExe check .
        Write-Host "--> Running Ruff Format Check..." -ForegroundColor Cyan
        & $RuffExe format --check .
    }
    "format" {
        Write-Host "--> Fixing and Formatting code with Ruff..." -ForegroundColor Cyan
        & $RuffExe check --fix .
        & $RuffExe format .
    }
    "test" {
        Write-Host "--> Running Pytest..." -ForegroundColor Cyan
        & $PytestExe
    }
    "up" {
        Write-Host "--> Starting Docker Compose services..." -ForegroundColor Cyan
        docker compose up -d
    }
    "down" {
        Write-Host "--> Stopping Docker Compose services..." -ForegroundColor Cyan
        docker compose down
    }
    "dashboard" {
        Write-Host "--> Starting Streamlit Dashboard..." -ForegroundColor Cyan
        $env:PYTHONPATH = (Get-Location).Path
        & $StreamlitExe run dashboard/app.py --server.address localhost
    }
    "backfill" {
        Write-Host "--> Running Backfill..." -ForegroundColor Cyan
        & $PythonExe -m nse_pipeline.cli backfill
    }
    "clean" {
        Write-Host "--> Cleaning cache directories..." -ForegroundColor Cyan
        Get-ChildItem -Path . -Recurse -Include "__pycache__", ".pytest_cache", ".ruff_cache" -Directory | Remove-Item -Recurse -Force
        Write-Host "Clean complete." -ForegroundColor Green
    }
    "demo" {
        Write-Host "--> [1/4] Starting PostgreSQL Docker Compose services..." -ForegroundColor Cyan
        docker compose up -d
        Write-Host "--> Waiting for PostgreSQL container to become healthy..." -ForegroundColor Cyan
        $maxRetries = 30
        $retry = 0
        $healthy = $false
        while ($retry -lt $maxRetries) {
            $status = (docker inspect --format='{{.State.Health.Status}}' nse_postgres 2>$null)
            if ($status -eq "healthy") {
                $healthy = $true
                break
            }
            Start-Sleep -Seconds 1
            $retry++
        }
        if (-not $healthy) {
            Write-Host "Warning: Container health check timeout; proceeding with connectivity check." -ForegroundColor Yellow
        } else {
            Write-Host "PostgreSQL is healthy." -ForegroundColor Green
        }

        Write-Host "--> [2/4] Initializing database schema & dimensional tables..." -ForegroundColor Cyan
        & $PythonExe -m nse_pipeline.cli db init

        Write-Host "--> [3/4] Running daily pipeline for latest warehouse trading date (idempotent skip check)..." -ForegroundColor Cyan
        & $PythonExe -m nse_pipeline.cli daily --date 2025-03-28 -q

        Write-Host "--> [4/4] Demo complete! To launch the Streamlit dashboard, run:" -ForegroundColor Green
        Write-Host "      .\scripts\tasks.ps1 dashboard" -ForegroundColor Yellow
        Write-Host "      (or: streamlit run dashboard/app.py --server.address 127.0.0.1)" -ForegroundColor DarkGray
    }
    "help" {
        Write-Host "Usage: .\scripts\tasks.ps1 [lint|format|test|up|down|dashboard|backfill|clean|demo]" -ForegroundColor Yellow
    }
}
