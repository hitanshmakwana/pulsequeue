# infra/setup_gcp.ps1 — PowerShell GCP resource provisioner for PulseQueue on Windows
[CmdletBinding()]
param(
    [string]$ProjectId = $env:PROJECT_ID,
    [string]$Region = "us-central1",
    [string]$Zone = "us-central1-a",
    [string]$InstanceName = "pulsequeue-vm",
    [string]$MachineType = "e2-micro",
    [string]$DiskSize = "30GB",
    [string]$ImageFamily = "debian-12",
    [string]$ImageProject = "debian-cloud",
    [string]$SaName = "pulsequeue-sa",
    [string]$PubSubTopic = "pulsequeue-jobs",
    [string]$PubSubDlqTopic = "pulsequeue-dlq",
    [string]$PubSubSubscription = "pulsequeue-worker",
    [int]$PubSubMaxDeliveryAttempts = 5,
    [int]$PubSubAckDeadline = 60,
    [string]$FirewallRuleName = "allow-pulsequeue-api",
    [int]$ApiPort = 8000
)

$ErrorActionPreference = "Continue"

# Auto-add Cloud SDK to PATH if not present
if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
    $sdkBin = "$env:LOCALAPPDATA\Google\Cloud SDK\google-cloud-sdk\bin"
    if (Test-Path $sdkBin) {
        $env:PATH = "$sdkBin;$env:PATH"
    }
}

function Log-Info ($msg) { Write-Host "[setup] $msg" -ForegroundColor Cyan }
function Log-Ok ($msg)   { Write-Host "[setup] [OK] $msg" -ForegroundColor Green }
function Log-Skip ($msg) { Write-Host "[setup] -> $msg (already exists - skipping)" -ForegroundColor Yellow }

# 1. Check Project ID
if (-not $ProjectId) {
    try {
        $ProjectId = (gcloud config get-value project 2>$null).Trim()
    } catch {}
}

if (-not $ProjectId) {
    Write-Error "ERROR: PROJECT_ID is not set. Run: gcloud config set project YOUR_PROJECT_ID"
    exit 1
}

Log-Info "Using project: $ProjectId"
Log-Info "Using zone:    $Zone"

$SaEmail = "${SaName}@${ProjectId}.iam.gserviceaccount.com"

# 2. Enable APIs
Log-Info "Enabling GCP APIs (idempotent)..."
& gcloud services enable `
    compute.googleapis.com `
    pubsub.googleapis.com `
    monitoring.googleapis.com `
    logging.googleapis.com `
    --project="$ProjectId" `
    --quiet
Log-Ok "APIs enabled"

# 3. Service Account
& gcloud iam service-accounts describe "$SaEmail" --project="$ProjectId" *>$null
if ($LASTEXITCODE -eq 0) {
    Log-Skip "Service account $SaEmail"
} else {
    Log-Info "Creating service account $SaEmail..."
    & gcloud iam service-accounts create "$SaName" `
        --project="$ProjectId" `
        --display-name="PulseQueue Service Account" `
        --description="Least-privilege SA for PulseQueue GCE VM"
    Log-Ok "Service account created"
}

# Bind IAM roles
Log-Info "Binding IAM roles to $SaEmail..."
$roles = @(
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
    "roles/monitoring.metricWriter",
    "roles/logging.logWriter"
)

foreach ($role in $roles) {
    & gcloud projects add-iam-policy-binding "$ProjectId" `
        --member="serviceAccount:$SaEmail" `
        --role="$role" `
        --condition=None `
        --quiet *>$null
    Log-Ok "  $role"
}

# 4. Pub/Sub Topics & Subscription
& gcloud pubsub topics describe "$PubSubTopic" --project="$ProjectId" *>$null
if ($LASTEXITCODE -eq 0) {
    Log-Skip "Pub/Sub topic $PubSubTopic"
} else {
    Log-Info "Creating Pub/Sub topic $PubSubTopic..."
    & gcloud pubsub topics create "$PubSubTopic" --project="$ProjectId"
    Log-Ok "Topic $PubSubTopic created"
}

& gcloud pubsub topics describe "$PubSubDlqTopic" --project="$ProjectId" *>$null
if ($LASTEXITCODE -eq 0) {
    Log-Skip "Pub/Sub DLT $PubSubDlqTopic"
} else {
    Log-Info "Creating Pub/Sub dead-letter topic $PubSubDlqTopic..."
    & gcloud pubsub topics create "$PubSubDlqTopic" --project="$ProjectId"
    Log-Ok "DLT $PubSubDlqTopic created"
}

& gcloud pubsub subscriptions describe "$PubSubSubscription" --project="$ProjectId" *>$null
if ($LASTEXITCODE -eq 0) {
    Log-Skip "Pub/Sub subscription $PubSubSubscription"
} else {
    Log-Info "Creating Pub/Sub subscription $PubSubSubscription..."
    & gcloud pubsub subscriptions create "$PubSubSubscription" `
        --project="$ProjectId" `
        --topic="$PubSubTopic" `
        --ack-deadline=$PubSubAckDeadline `
        --dead-letter-topic="projects/${ProjectId}/topics/${PubSubDlqTopic}" `
        --max-delivery-attempts=$PubSubMaxDeliveryAttempts `
        --expiration-period=never
    Log-Ok "Subscription $PubSubSubscription created (DLT after $PubSubMaxDeliveryAttempts nacks)"
}

# Pub/Sub SA binding
$projectNumber = (& gcloud projects describe "$ProjectId" --format='value(projectNumber)').Trim()
$pubsubSa = "service-${projectNumber}@gcp-sa-pubsub.iam.gserviceaccount.com"
Log-Info "Granting Pub/Sub SA permission to publish to DLT..."
& gcloud pubsub topics add-iam-policy-binding "$PubSubDlqTopic" `
    --project="$ProjectId" `
    --member="serviceAccount:$pubsubSa" `
    --role="roles/pubsub.publisher" `
    --quiet *>$null

& gcloud pubsub subscriptions add-iam-policy-binding "$PubSubSubscription" `
    --project="$ProjectId" `
    --member="serviceAccount:$pubsubSa" `
    --role="roles/pubsub.subscriber" `
    --quiet *>$null
Log-Ok "Pub/Sub DLT IAM configured"

# 5. Firewall Rule
& gcloud compute firewall-rules describe "$FirewallRuleName" --project="$ProjectId" *>$null
if ($LASTEXITCODE -eq 0) {
    Log-Skip "Firewall rule $FirewallRuleName"
} else {
    Log-Info "Creating firewall rule for TCP $ApiPort..."
    & gcloud compute firewall-rules create "$FirewallRuleName" `
        --project="$ProjectId" `
        --allow="tcp:${ApiPort}" `
        --source-ranges="0.0.0.0/0" `
        --target-tags="pulsequeue" `
        --description="Allow inbound traffic to PulseQueue API"
    Log-Ok "Firewall rule created (TCP $ApiPort open)"
}

# 6. Compute Engine VM
& gcloud compute instances describe "$InstanceName" --zone="$Zone" --project="$ProjectId" *>$null
if ($LASTEXITCODE -eq 0) {
    Log-Skip "VM $InstanceName (already exists)"
} else {
    Log-Info "Creating e2-micro VM $InstanceName in $Zone..."
    Log-Info "(This takes ~30-60 seconds)"
    & gcloud compute instances create "$InstanceName" `
        --project="$ProjectId" `
        --zone="$Zone" `
        --machine-type="$MachineType" `
        --image-family="$ImageFamily" `
        --image-project="$ImageProject" `
        --boot-disk-size="$DiskSize" `
        --boot-disk-type="pd-standard" `
        --scopes="https://www.googleapis.com/auth/cloud-platform" `
        --service-account="$SaEmail" `
        --tags="pulsequeue" `
        --metadata="enable-oslogin=TRUE" `
        --metadata-from-file="startup-script=infra/vm_startup.sh"
    Log-Ok "VM $InstanceName created"
}

# 7. Summary
$externalIp = (& gcloud compute instances describe "$InstanceName" `
    --zone="$Zone" `
    --project="$ProjectId" `
    --format="value(networkInterfaces[0].accessConfigs[0].natIP)").Trim()

Write-Host ""
Write-Host "================================================================" -ForegroundColor Green
Write-Host "  PulseQueue GCP Setup Complete" -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  VM Name:      $InstanceName"
Write-Host "  External IP:  $externalIp"
Write-Host "  Zone:         $Zone"
Write-Host "  Machine:      $MachineType (free-tier eligible)"
Write-Host ""
Write-Host "  Pub/Sub Topic:        $PubSubTopic"
Write-Host "  Pub/Sub DLQ:          $PubSubDlqTopic"
Write-Host "  Pub/Sub Subscription: $PubSubSubscription"
Write-Host ""
Write-Host "  SSH into the VM:"
Write-Host "    gcloud compute ssh $InstanceName --zone $Zone" -ForegroundColor Yellow
Write-Host ""
Write-Host "  API will be available at: http://${externalIp}:${ApiPort}"
Write-Host "  Grafana:                  http://${externalIp}:3000"
Write-Host ""
Write-Host "  Run benchmarks from your local machine:"
Write-Host "    python tests/bench_gcp.py --host http://${externalIp}:${ApiPort}" -ForegroundColor Yellow
Write-Host ""
Write-Host "================================================================" -ForegroundColor Green
