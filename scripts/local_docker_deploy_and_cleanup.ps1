# One-shot: build + up + logs + down, then remove local runtime files (logs/exports) and .env if we created it.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error "docker not on PATH. Install Docker Desktop and ensure 'docker compose' works, then re-run this script."
}

$createdEnv = $false
if (-not (Test-Path (Join-Path $root ".env"))) {
    Copy-Item (Join-Path $root ".env.example") (Join-Path $root ".env")
    $createdEnv = $true
    Write-Host "Created .env from .env.example (dry-run keys; replace for real trading)."
}

docker compose build
docker compose up -d
Start-Sleep -Seconds 5
docker compose logs bot --tail 40
docker compose down

foreach ($dir in @("logs", "exports")) {
    $p = Join-Path $root $dir
    if (Test-Path $p) {
        Get-ChildItem $p -Force -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    }
}

if ($createdEnv) {
    Remove-Item (Join-Path $root ".env") -Force
    Write-Host "Removed temporary .env (was copied from .env.example only for this run)."
}

Write-Host "Cleared contents of logs/ and exports/. Done."
