param(
    [string]$TestResultsPath = "TestResults",
    [string]$XmlFileName = "TestResults.xml",
    [string]$HtmlFileName = "TestResultsCustom.html"
)

# Run dotnet tests
Write-Host "Running dotnet tests..." -ForegroundColor Cyan
dotnet test --logger "xunit;logfilename=$XmlFileName"

if ($LASTEXITCODE -ne 0) {
    Write-Host "Tests failed or encountered an error (Exit Code: $LASTEXITCODE)" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "Tests completed successfully" -ForegroundColor Green

# Generate custom HTML report
Write-Host "Generating custom HTML report..." -ForegroundColor Cyan
& .\scripts\Generate-TestReport.ps1 -XmlPath "$TestResultsPath/$XmlFileName" -OutputPath "$TestResultsPath/$HtmlFileName"

Write-Host "Test report generation completed!" -ForegroundColor Green
Write-Host "HTML report available at: $TestResultsPath/$HtmlFileName" -ForegroundColor Yellow
