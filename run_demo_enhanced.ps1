# Enhanced OCC vs 2PL Distributed Database Demo
# This script demonstrates Optimistic Concurrency Control vs Two-Phase Locking
# It automatically generates comprehensive visualizations showing performance differences

Write-Host "=== OCC vs 2PL DISTRIBUTED DATABASE DEMONSTRATION ===" -ForegroundColor Green
Write-Host "This demo will:" -ForegroundColor Yellow
Write-Host "  1. Launch distributed database nodes (coordinator + data nodes)" -ForegroundColor Yellow
Write-Host "  2. Run comprehensive performance benchmarks" -ForegroundColor Yellow
Write-Host "  3. Generate automatic visualizations and analysis" -ForegroundColor Yellow
Write-Host "  4. Compare OCC vs 2PL across different conflict scenarios" -ForegroundColor Yellow
Write-Host ""

# Create results directory for visualizations
Write-Host "Setting up visualization output directory..." -ForegroundColor Cyan
if (!(Test-Path "results")) {
    New-Item -ItemType Directory -Path "results"
    Write-Host "   Created 'results' directory for graphs and reports" -ForegroundColor Green
}

# Start Data Nodes
Write-Host "Starting Database Nodes..." -ForegroundColor Cyan
Write-Host "   Launching Node 1 on port 8001..." -ForegroundColor Gray
$node1 = Start-Process -FilePath "python" -ArgumentList "src/node.py --port 8001" -PassThru -NoNewWindow
Write-Host "   Launching Node 2 on port 8002..." -ForegroundColor Gray
$node2 = Start-Process -FilePath "python" -ArgumentList "src/node.py --port 8002" -PassThru -NoNewWindow

# Wait for nodes to initialize
Write-Host "   Waiting for nodes to initialize..." -ForegroundColor Gray
Start-Sleep -Seconds 3

# Start Coordinator
Write-Host "Starting Transaction Coordinator..." -ForegroundColor Cyan
Write-Host "   Launching coordinator with OCC validation logic..." -ForegroundColor Gray
$coord = Start-Process -FilePath "python" -ArgumentList "src/coordinator.py --nodes localhost:8001,localhost:8002" -PassThru -NoNewWindow

# Wait for coordinator to initialize and connect to nodes
Write-Host "   Waiting for coordinator to connect to nodes..." -ForegroundColor Gray
Start-Sleep -Seconds 3

# Set environment for Python imports
$env:PYTHONPATH = "src"
Write-Host "Environment configured (PYTHONPATH=src)" -ForegroundColor Cyan

# Run Comprehensive Analysis
Write-Host ""
Write-Host "=== RUNNING COMPREHENSIVE PERFORMANCE ANALYSIS ===" -ForegroundColor Green
Write-Host "This will compare OCC vs 2PL performance across multiple scenarios..." -ForegroundColor Yellow
Write-Host ""

Write-Host "Starting comprehensive benchmark with automatic visualization..." -ForegroundColor Cyan
python scenarios/benchmark.py --comprehensive

# Check if visualizations were generated
Write-Host ""
Write-Host "=== VISUALIZATION RESULTS ===" -ForegroundColor Green
if (Test-Path "results/*.png") {
    $graphs = Get-ChildItem "results/*.png"
    Write-Host "Generated visualizations:" -ForegroundColor Green
    foreach ($graph in $graphs) {
        Write-Host "   $($graph.Name)" -ForegroundColor Cyan
    }
    Write-Host ""
    Write-Host "Open the 'results' folder to view detailed performance graphs!" -ForegroundColor Yellow
    Write-Host "   • Performance comparison charts (OCC vs 2PL)" -ForegroundColor Gray
    Write-Host "   • Transaction lifecycle diagrams" -ForegroundColor Gray  
    Write-Host "   • Comprehensive analysis reports" -ForegroundColor Gray
} else {
    Write-Host "No visualizations found. Check if matplotlib is installed." -ForegroundColor Yellow
    Write-Host "   Run: pip install matplotlib seaborn" -ForegroundColor Gray
}

# Performance Summary
Write-Host ""
Write-Host "=== KEY INSIGHTS ===" -ForegroundColor Green
Write-Host "The analysis above shows:" -ForegroundColor Yellow
Write-Host "  LOW CONFLICT scenarios: OCC typically outperforms 2PL" -ForegroundColor Green
Write-Host "     • No locking overhead" -ForegroundColor Gray
Write-Host "     • High concurrency without blocking" -ForegroundColor Gray
Write-Host "  HIGH CONFLICT scenarios: 2PL may perform better" -ForegroundColor Yellow
Write-Host "     • OCC suffers from high abort rates" -ForegroundColor Gray
Write-Host "     • Validation failures cause transaction restarts" -ForegroundColor Gray

# Cleanup Section
Write-Host ""
Write-Host "Stopping all processes..." -ForegroundColor Red
Stop-Process -Id $node1.Id -Force -ErrorAction SilentlyContinue
Stop-Process -Id $node2.Id -Force -ErrorAction SilentlyContinue
Stop-Process -Id $coord.Id -Force -ErrorAction SilentlyContinue
Write-Host "Done." -ForegroundColor Green