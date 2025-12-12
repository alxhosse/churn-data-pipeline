# PowerShell script for Windows to setup PostgreSQL database using Docker

Write-Host "Starting PostgreSQL container..." -ForegroundColor Green
docker-compose up -d postgres

Write-Host "Waiting for PostgreSQL to be ready..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# Wait for PostgreSQL to be ready
$maxAttempts = 30
$attempt = 0
do {
    $attempt++
    try {
        docker-compose exec -T postgres pg_isready -U postgres 2>$null
        if ($LASTEXITCODE -eq 0) {
            break
        }
    } catch {
        # Ignore errors
    }
    if ($attempt -lt $maxAttempts) {
        Write-Host "Waiting for PostgreSQL... (attempt $attempt/$maxAttempts)" -ForegroundColor Yellow
        Start-Sleep -Seconds 2
    }
} while ($attempt -lt $maxAttempts)

if ($attempt -ge $maxAttempts) {
    Write-Host "PostgreSQL failed to start. Check logs with: docker-compose logs postgres" -ForegroundColor Red
    exit 1
}

Write-Host "PostgreSQL is ready!" -ForegroundColor Green
Write-Host ""
Write-Host "Database connection details:" -ForegroundColor Cyan
Write-Host "  Host: localhost"
Write-Host "  Port: 5432"
Write-Host "  Database: churn"
Write-Host "  User: postgres"
Write-Host "  Password: postgres"
Write-Host ""
Write-Host "To stop the database: docker-compose down" -ForegroundColor Yellow
Write-Host "To view logs: docker-compose logs -f postgres" -ForegroundColor Yellow

