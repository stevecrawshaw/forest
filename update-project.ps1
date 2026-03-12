param(
    [Parameter(Mandatory=$true)]
    [string]$Name,
    
    [Parameter(Mandatory=$true)]
    [string]$Description
)
# USAGE .\update-project.ps1 -Name "my-awesome-project" -Description "This is my awesome project"
# Set error action preference to stop on errors
$ErrorActionPreference = "Stop"

try {
    # Get the path to pyproject.toml in the same directory as the script
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $tomlPath = Join-Path $scriptDir "pyproject.toml"
    
    # Check if pyproject.toml exists
    if (-not (Test-Path $tomlPath)) {
        Write-Error "pyproject.toml not found at: $tomlPath"
        exit 1
    }
    
    # Read the file content
    $content = Get-Content $tomlPath -Raw
    
    # Update the name field
    $content = $content -replace '(?m)^name\s*=\s*"[^"]*"', "name = `"$Name`""
    
    # Update the description field
    $content = $content -replace '(?m)^description\s*=\s*"[^"]*"', "description = `"$Description`""
    
    # Write the updated content back to the file
    Set-Content $tomlPath -Value $content -NoNewline
    
    Write-Host "Successfully updated pyproject.toml:" -ForegroundColor Green
    Write-Host "  Name: $Name" -ForegroundColor White
    Write-Host "  Description: $Description" -ForegroundColor White
    
    exit 0
}
catch {
    Write-Error "Failed to update pyproject.toml: $($_.Exception.Message)"
    exit 1
}
