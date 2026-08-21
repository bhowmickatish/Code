param(
    [int]$Count = 3
)

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

Write-Host "Starting $Count consumers in group '$env:KAFKA_GROUP_ID' (default: order-consumer-group)..."

$jobs = @()
for ($i = 1; $i -le $Count; $i++) {
    $id = "consumer-$i"
    $jobs += Start-Process -FilePath "go" -ArgumentList "run", "./cmd/consumer", "-id", $id -PassThru -NoNewWindow
    Write-Host "  started $id (pid $($jobs[-1].Id))"
}

Write-Host ""
Write-Host "Press Ctrl+C to stop all consumers."
try {
    $jobs | Wait-Process
} finally {
    $jobs | ForEach-Object {
        if (-not $_.HasExited) {
            Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
        }
    }
}
