param(
    [string]$XmlPath = "TestResults/TestResults.xml",
    [string]$OutputPath = "TestResults/TestResultsCustom.html"
)

# Load XML (read as UTF-8 raw to preserve Unicode characters)
[xml](Get-Content -Path $XmlPath -Encoding UTF8 -Raw) | Out-Null
[xml]$xml = [xml](Get-Content -Path $XmlPath -Encoding UTF8 -Raw)

# Group tests by class
$testsByClass = @{}

# Get all test elements from xUnit XML format
$allTests = $xml.assemblies.assembly.collection.test

foreach ($test in $allTests) {
    $className = $test.type
    
    if (-not $testsByClass.ContainsKey($className)) {
        $testsByClass[$className] = @()
    }
    $testsByClass[$className] += $test
}

# Calculate statistics from assembly level
$assembly = $xml.assemblies.assembly
$totalTests = [int]$assembly.total
$passedTests = [int]$assembly.passed
$failedTests = [int]$assembly.failed
$skippedTests = [int]$assembly.skipped
$passPercentage = if ($totalTests -gt 0) { [math]::Round(($passedTests / $totalTests) * 100, 2) } else { 0 }

$html = @"
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8" />
    <title>Test Results Report</title>
    <style>
        * { font-family: Arial, sans-serif; }
        body { margin: 20px; background-color: #f5f5f5; }
        .header { background-color: #2c3e50; color: white; padding: 20px; border-radius: 5px; margin-bottom: 20px; }
        .header h1 { margin: 0; }
        .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px; }
        .summary-block { background-color: white; padding: 15px; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .summary-block .label { font-size: 12px; color: #7f8c8d; text-transform: uppercase; }
        .summary-block .value { font-size: 28px; font-weight: bold; margin-top: 5px; }
        .summary-block.passed .value { color: #27ae60; }
        .summary-block.failed .value { color: #e74c3c; }
        .summary-block.skipped .value { color: #f39c12; }
        .test-class { background-color: white; margin-bottom: 20px; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); overflow: hidden; }
        .class-header { background-color: #34495e; color: white; padding: 15px; cursor: pointer; user-select: none; }
        .class-header:hover { background-color: #2c3e50; }
        .class-header h2 { margin: 0; font-size: 18px; }
        .class-header .stats { font-size: 12px; margin-top: 5px; opacity: 0.8; }
        .toggle-icon { float: right; font-size: 16px; }
        .class-body { padding: 0; }
        .test-item { padding: 12px 15px; border-bottom: 1px solid #ecf0f1; display: flex; align-items: center; }
        .test-item:last-child { border-bottom: none; }
        .test-item.passed { border-left: 4px solid #27ae60; background-color: #f0fdf4; }
        .test-item.failed { border-left: 4px solid #e74c3c; background-color: #fef2f2; }
        .test-item.skipped { border-left: 4px solid #f39c12; background-color: #fffbf0; }
        .test-icon { font-weight: bold; margin-right: 10px; font-size: 16px; }
        .test-icon.passed { color: #27ae60; }
        .test-icon.failed { color: #e74c3c; }
        .test-icon.skipped { color: #f39c12; }
        .test-name { flex: 1; word-break: break-word; }
        .test-duration { color: #7f8c8d; font-size: 12px; margin-left: 10px; white-space: nowrap; }
        .error-message { color: #e74c3c; font-size: 12px; margin-top: 5px; padding: 10px; background-color: #ffe8e8; border-radius: 3px; }
    </style>
    <script>
        function toggleClass(element) {
            const body = element.nextElementSibling;
            const icon = element.querySelector('.toggle-icon');
            if (body.style.display === 'none') {
                body.style.display = 'block';
                icon.textContent = '▼';
            } else {
                body.style.display = 'none';
                icon.textContent = '▶';
            }
        }
    </script>
</head>
<body>
    <div class="header">
        <h1>Test Results Report</h1>
        <p>Generated on $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')</p>
    </div>
    
    <div class="summary">
        <div class="summary-block">
            <div class="label">Total Tests</div>
            <div class="value">$totalTests</div>
        </div>
        <div class="summary-block passed">
            <div class="label">Passed</div>
            <div class="value">$passedTests</div>
        </div>
        <div class="summary-block failed">
            <div class="label">Failed</div>
            <div class="value">$failedTests</div>
        </div>
        <div class="summary-block skipped">
            <div class="label">Skipped</div>
            <div class="value">$skippedTests</div>
        </div>
        <div class="summary-block">
            <div class="label">Pass Rate</div>
            <div class="value">$passPercentage%</div>
        </div>
    </div>
"@

# Add test classes
foreach ($className in $testsByClass.Keys | Sort-Object) {
    $tests = $testsByClass[$className]
    $classPassed = ($tests | Where-Object { $_.result -eq "Pass" }).Count
    $classFailed = ($tests | Where-Object { $_.result -eq "Fail" }).Count
    $classSkipped = ($tests | Where-Object { $_.result -eq "Skip" }).Count
    
    $html += @"
    <div class="test-class">
        <div class="class-header" onclick="toggleClass(this)">
            <span class="toggle-icon">▼</span>
            <h2>$className</h2>
            <div class="stats">Passed: $classPassed | Failed: $classFailed | Skipped: $classSkipped | Total: $($tests.Count)</div>
        </div>
        <div class="class-body">
"@
    
    foreach ($test in $tests) {
        $outcome = $test.result.ToLower()
        $icon = if ($outcome -eq "pass") { "[PASS]" } elseif ($outcome -eq "fail") { "[FAIL]" } else { "[SKIP]" }
        $testMethodName = $test.method
        $duration = if ($test.time) { "$([math]::Round([double]$test.time * 1000, 0))ms" } else { "0ms" }
        
        $html += @"
        <div class="test-item $outcome">
            <span class="test-icon $outcome">$icon</span>
            <span class="test-name">$testMethodName</span>
            <span class="test-duration">$duration</span>
        </div>
"@
        
        if ($test.failure) {
            $html += @"
            <div class="error-message">$($test.failure.message)</div>
"@
        }
    }
    
    $html += @"
        </div>
    </div>
"@
}

$html += @"
</body>
</html>
"@

# Write output using UTF-8 with BOM to ensure editors/browsers detect encoding
$utf8Bom = New-Object System.Text.UTF8Encoding -ArgumentList $true
[System.IO.File]::WriteAllText($OutputPath, $html, $utf8Bom)
Write-Host "Report generated: $OutputPath" -ForegroundColor Green
