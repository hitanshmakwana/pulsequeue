# infra/teardown_gcp.ps1 — PowerShell GCP resource cleanup for PulseQueue on Windows
[CmdletBinding()]
param(
    [string]$ProjectId = $env:PROJECT_ID,
    [string]$Zone = "us-central1-a",
    [string]$InstanceName = "pulsequeue-vm",
    [string]$SaName = "pulsequeue-sa",
    [string]$PubSubTopic = "pulsequeue-jobs",
    [string]$PubSubDlqTopic = "pulsequeue-dlq",
    [string]$PubSubSubscription = "pulsequeue-worker",
    [string]$FirewallRuleName = "allow-pulsequeue-api",
    [switch]$DryRun,
    [switch]$Force
)

$ErrorActionPreference = "Continue"

# Auto-add Cloud SDK to PATH if not present
if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
    $sdkBin = "$env:LOCALAPPDATA\Google\Cloud SDK\google-cloud-sdk\bin"
    if (Test-Path $sdkBin) {
        $env:PATH = "$sdkBin;$env:PATH"
    }
}

function Log-Info ($msg) { Write-Host "[teardown] $msg" -ForegroundColor Cyan }
function Log-Ok ($msg)   { Write-Host "[teardown] [OK] $msg" -ForegroundColor Green }
function Log-Skip ($msg) { Write-Host "[teardown] -> $msg (not found - already cleaned up)" -ForegroundColor Yellow }

if (-not $ProjectId) {
    try {
        $ProjectId = (gcloud config get-value project 2>$null).Trim()
    } catch {}
}

if (-not $ProjectId) {
    Write-Error "ERROR: PROJECT_ID is not set. Run: gcloud config set project YOUR_PROJECT_ID"
    exit 1
}

$SaEmail = "${SaName}@${ProjectId}.iam.gserviceaccount.com"

Write-Host ""
Write-Host "================================================================" -ForegroundColor Red
Write-Host "  PulseQueue GCP Teardown" -ForegroundColor Red
Write-Host "  Project: $ProjectId"
Write-Host "  Zone:    $Zone"
Write-Host "================================================================" -ForegroundColor Red
Write-Host ""

if ($DryRun) {
    Write-Host "DryRun enabled - nothing will actually be deleted." -ForegroundColor Yellow
    Write-Host ""
}

if (-not $DryRun -and -not $Force) {
    $confirm = Read-Host "Delete ALL PulseQueue GCP resources? This is irreversible. [y/N]"
    if ($confirm -ne 'y' -and $confirm -ne 'Y') {
        Write-Host "Aborted."
        exit 0
    }
}

function Invoke-Action ($cmd, $desc) {
    if ($DryRun) {
        Write-Host "[DryRun] would run: $cmd" -ForegroundColor Gray
    } else {
        Invoke-Expression $cmd
    }
}

# 1. VM
Log-Info "Deleting VM $InstanceName..."
$vmExists = gcloud compute instances describe "$InstanceName" --zone="$Zone" --project="$ProjectId" 2>$null
if ($vmExists) {
    Invoke-Action "gcloud compute instances delete '$InstanceName' --zone='$Zone' --project='$ProjectId' --delete-disks=all --quiet" "Delete VM"
    Log-Ok "VM deleted"
} else {
    Log-Skip "VM $InstanceName"
}

# 2. Pub/Sub subscription
Log-Info "Deleting Pub/Sub subscription $PubSubSubscription..."
$subExists = gcloud pubsub subscriptions describe "$PubSubSubscription" --project="$ProjectId" 2>$null
if ($subExists) {
    Invoke-Action "gcloud pubsub subscriptions delete '$PubSubSubscription' --project='$ProjectId' --quiet" "Delete subscription"
    Log-Ok "Subscription deleted"
} else {
    Log-Skip "Subscription $PubSubSubscription"
}

# 3. Pub/Sub topics
foreach ($t in @($PubSubTopic, $PubSubDlqTopic)) {
    Log-Info "Deleting Pub/Sub topic $t..."
    $tExists = gcloud pubsub topics describe "$t" --project="$ProjectId" 2>$null
    if ($tExists) {
        Invoke-Action "gcloud pubsub topics delete '$t' --project='$ProjectId' --quiet" "Delete topic $t"
        Log-Ok "Topic $t deleted"
    } else {
        Log-Skip "Topic $t"
    }
}

# 4. Firewall rule
Log-Info "Deleting firewall rule $FirewallRuleName..."
$fwExists = gcloud compute firewall-rules describe "$FirewallRuleName" --project="$ProjectId" 2>$null
if ($fwExists) {
    Invoke-Action "gcloud compute firewall-rules delete '$FirewallRuleName' --project='$ProjectId' --quiet" "Delete firewall rule"
    Log-Ok "Firewall rule deleted"
} else {
    Log-Skip "Firewall rule $FirewallRuleName"
}

# 5. Service account
Log-Info "Deleting service account $SaEmail..."
$saExists = gcloud iam service-accounts describe "$SaEmail" --project="$ProjectId" 2>$null
if ($saExists) {
    Invoke-Action "gcloud iam service-accounts delete '$SaEmail' --project='$ProjectId' --quiet" "Delete service account"
    Log-Ok "Service account deleted"
} else {
    Log-Skip "Service account $SaEmail"
}

Write-Host ""
Write-Host "================================================================" -ForegroundColor Green
Write-Host "  Teardown complete. All PulseQueue GCP resources deleted." -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Green
