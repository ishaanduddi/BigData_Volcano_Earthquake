$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
& "$root\.venv\Scripts\python.exe" "$root\scripts\start_hadoop.py"
exit $LASTEXITCODE
