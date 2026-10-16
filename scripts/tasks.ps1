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
    [ValidateSet("lint", "format", "test", "up", "down", "dashboard", "backfill", "clean", "help")]
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
        & $StreamlitExe run dashboard/app.py
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
    "help" {
        Write-Host "Usage: .\scripts\tasks.ps1 [lint|format|test|up|down|dashboard|backfill|clean]" -ForegroundColor Yellow
    }
}
