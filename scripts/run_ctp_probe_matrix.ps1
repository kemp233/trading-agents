param(
    [string]$Password = $env:CTP_PASSWORD
)

$ErrorActionPreference = 'Stop'

if (-not $Password) {
    $securePassword = Read-Host 'Enter CTP password' -AsSecureString
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassword)
    try {
        $Password = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot 'venv\Scripts\python.exe'
$probe = Join-Path $repoRoot 'scripts\ctp_probe_runner.py'
$logDir = Join-Path $repoRoot 'logs\ctp_probe'

if (-not (Test-Path $python)) {
    throw "Python not found: $python"
}

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$tests = @(
    @{ Label = 'env2_7x24_prod'; Front = 'tcp://182.254.243.31:40001'; Production = 'true' },
    @{ Label = 'env1_trade_10201_prod'; Front = 'tcp://180.168.146.187:10201'; Production = 'true' },
    @{ Label = 'env1_trade_10202_prod'; Front = 'tcp://180.168.146.187:10202'; Production = 'true' },
    @{ Label = 'env1_trade_10201_test'; Front = 'tcp://180.168.146.187:10201'; Production = 'false' },
    @{ Label = 'env1_trade_10202_test'; Front = 'tcp://180.168.146.187:10202'; Production = 'false' }
)

$previousPassword = $env:CTP_PASSWORD
$env:CTP_PASSWORD = $Password

try {
    foreach ($test in $tests) {
        Write-Host ''
        Write-Host "=== Running $($test.Label) ==="
        & $python $probe `
            --front $test.Front `
            --production-mode $test.Production `
            --label $test.Label `
            --output-dir $logDir
        Write-Host "ExitCode=$LASTEXITCODE"
    }
}
finally {
    $env:CTP_PASSWORD = $previousPassword
}

Write-Host ''
Write-Host "Logs saved under: $logDir"
