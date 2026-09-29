param(
    [string]$BaseUrl = 'http://localhost:8080',
    [Parameter(Mandatory = $true)][string]$Email,
    [Parameter(Mandatory = $true)][string]$Password
)

$ErrorActionPreference = 'Stop'
$BaseUrl = $BaseUrl.TrimEnd('/')
$jsonHeaders = @{ 'Content-Type' = 'application/json' }
$credentials = @{ email = $Email; password = $Password } | ConvertTo-Json -Compress

try {
    $auth = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/auth/register" -Headers $jsonHeaders -Body $credentials
} catch {
    try {
        $auth = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/auth/login" -Headers $jsonHeaders -Body $credentials
    } catch {
        throw "Could not register or log in. Check that the email and password are valid (password must be 10–72 characters). $($_.Exception.Message)"
    }
}

$headers = @{
    Authorization = "Bearer $($auth.accessToken)"
    'Content-Type' = 'application/json'
}

function New-DemoJob([string]$Name, [string]$Type, [object]$Payload, [int]$MaxRetries = 3, [string]$ScheduleType = 'IMMEDIATE', [string]$CronExpression = $null) {
    $body = @{
        name = $Name
        description = 'Created by scripts/demo.ps1'
        type = $Type
        payload = $Payload
        scheduleType = $ScheduleType
        priority = 'HIGH'
        maxRetries = $MaxRetries
        timeoutSeconds = 30
    }
    if ($CronExpression) {
        $body.cronExpression = $CronExpression
        $body.timeZone = 'UTC'
    }
    return Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/jobs" -Headers $headers -Body ($body | ConvertTo-Json -Depth 10 -Compress)
}

$retryJob = New-DemoJob -Name "Retry recovery demo $(Get-Date -Format o)" -Type 'DEMO_FAIL' -Payload @{ failAttempts = 2 } -MaxRetries 3
Write-Host "Created retry demo job $($retryJob.id): attempts 1 and 2 should fail, then a retry should succeed."

$scheduledJob = New-DemoJob -Name "Recurring report demo $(Get-Date -Format o)" -Type 'REPORT_GENERATION' -Payload @{ reportType = 'HOURLY_DEMO' } -ScheduleType 'CRON' -CronExpression '0 * * * * *'
Write-Host "Created recurring job $($scheduledJob.id) (every minute, UTC). It remains scheduled for the next occurrence."

$distributionJobs = @()
for ($i = 1; $i -le 6; $i++) {
    $job = New-DemoJob -Name "Worker distribution demo $i $(Get-Date -Format o)" -Type 'DEMO_LONG_RUNNING_TASK' -Payload @{ durationMs = 5000 } -MaxRetries 0
    $distributionJobs += $job
}
Write-Host "Created six 5-second jobs. With workers scaled up, inspect their worker IDs in the dashboard."

$deadline = [DateTime]::UtcNow.AddMinutes(2)
do {
    Start-Sleep -Seconds 1
    $current = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/jobs/$($retryJob.id)" -Headers $headers
} while ($current.status -in @('SCHEDULED', 'QUEUED', 'RUNNING', 'RETRYING') -and [DateTime]::UtcNow -lt $deadline)

$attempts = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/jobs/$($retryJob.id)/executions?page=0&size=10" -Headers $headers
Write-Host "Retry demo finished with status $($current.status); execution records: $($attempts.totalElements)."
if ($current.status -ne 'SUCCESS' -or $attempts.totalElements -lt 3) {
    throw "Expected the retry demo to succeed after at least three attempts. Check the dashboard and worker logs."
}

Write-Host "Open $BaseUrl/ to inspect job states, worker assignments, attempts, and live events."
Write-Host 'For multiple workers, start the stack with: docker compose up --build --scale worker=3'
