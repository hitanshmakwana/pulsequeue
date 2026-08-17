#!/usr/bin/env bash
# infra/setup_gcp.sh — Idempotent GCP resource provisioner for PulseQueue.
#
# Creates everything needed to run PulseQueue on GCP:
#   - Service account with least-privilege IAM roles
#   - Pub/Sub topic + dead-letter topic + subscription
#   - Firewall rule (TCP 8000 inbound from anywhere)
#   - e2-micro VM in us-central1-a (free-tier eligible)
#
# Idempotent: re-running the script skips resources that already exist.
# All resource names are derived from INSTANCE_NAME so cleanup is easy.
#
# Prerequisites:
#   - gcloud CLI installed and authenticated (`gcloud auth login`)
#   - Billing-enabled project (`gcloud config set project YOUR_PROJECT`)
#   - APIs enabled (done automatically below)
#
# Usage:
#   export PROJECT_ID=$(gcloud config get-value project)
#   bash infra/setup_gcp.sh
#
# After setup, the script prints:
#   - The VM's external IP
#   - The SSH command to connect
#   - The deploy command to run on the VM

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration (override via environment variables)
# ---------------------------------------------------------------------------
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-us-central1}"
ZONE="${ZONE:-us-central1-a}"
INSTANCE_NAME="${INSTANCE_NAME:-pulsequeue-vm}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-micro}"        # free-tier eligible
DISK_SIZE="${DISK_SIZE:-30GB}"                   # free-tier eligible (≤30 GB)
IMAGE_FAMILY="${IMAGE_FAMILY:-debian-12}"
IMAGE_PROJECT="${IMAGE_PROJECT:-debian-cloud}"

SA_NAME="${SA_NAME:-pulsequeue-sa}"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

PUBSUB_TOPIC="${PUBSUB_TOPIC:-pulsequeue-jobs}"
PUBSUB_DLQ_TOPIC="${PUBSUB_DLQ_TOPIC:-pulsequeue-dlq}"
PUBSUB_SUBSCRIPTION="${PUBSUB_SUBSCRIPTION:-pulsequeue-worker}"
# Pub/Sub delivers up to 5 times; on the 6th nack the message goes to the DLT.
PUBSUB_MAX_DELIVERY_ATTEMPTS="${PUBSUB_MAX_DELIVERY_ATTEMPTS:-5}"
# ACK deadline: how long the bridge has to process a message before Pub/Sub re-delivers.
PUBSUB_ACK_DEADLINE="${PUBSUB_ACK_DEADLINE:-60}"

FIREWALL_RULE_NAME="allow-pulsequeue-api"
API_PORT="${API_PORT:-8000}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "[setup] $*"; }
ok()   { echo "[setup] ✓ $*"; }
skip() { echo "[setup] ↷ $* (already exists — skipping)"; }

require_project() {
    if [ -z "$PROJECT_ID" ]; then
        echo "ERROR: PROJECT_ID is not set. Run: gcloud config set project YOUR_PROJECT_ID"
        exit 1
    fi
    log "Using project: $PROJECT_ID"
    log "Using zone:    $ZONE"
}

# ---------------------------------------------------------------------------
# Enable required APIs
# ---------------------------------------------------------------------------
enable_apis() {
    log "Enabling GCP APIs (idempotent)..."
    gcloud services enable \
        compute.googleapis.com \
        pubsub.googleapis.com \
        monitoring.googleapis.com \
        logging.googleapis.com \
        --project="$PROJECT_ID" \
        --quiet
    ok "APIs enabled"
}

# ---------------------------------------------------------------------------
# Service Account
# ---------------------------------------------------------------------------
create_service_account() {
    if gcloud iam service-accounts describe "$SA_EMAIL" \
           --project="$PROJECT_ID" &>/dev/null; then
        skip "Service account $SA_EMAIL"
    else
        log "Creating service account $SA_EMAIL..."
        gcloud iam service-accounts create "$SA_NAME" \
            --project="$PROJECT_ID" \
            --display-name="PulseQueue Service Account" \
            --description="Least-privilege SA for PulseQueue GCE VM"
        ok "Service account created"
    fi

    # Grant roles (idempotent — gcloud add-iam-policy-binding is additive)
    log "Binding IAM roles to $SA_EMAIL..."
    for ROLE in \
        "roles/pubsub.publisher" \
        "roles/pubsub.subscriber" \
        "roles/monitoring.metricWriter" \
        "roles/logging.logWriter"; do
        gcloud projects add-iam-policy-binding "$PROJECT_ID" \
            --member="serviceAccount:$SA_EMAIL" \
            --role="$ROLE" \
            --condition=None \
            --quiet &>/dev/null
        ok "  $ROLE"
    done
}

# ---------------------------------------------------------------------------
# Pub/Sub
# ---------------------------------------------------------------------------
create_pubsub() {
    # Main topic
    if gcloud pubsub topics describe "$PUBSUB_TOPIC" \
           --project="$PROJECT_ID" &>/dev/null; then
        skip "Pub/Sub topic $PUBSUB_TOPIC"
    else
        log "Creating Pub/Sub topic $PUBSUB_TOPIC..."
        gcloud pubsub topics create "$PUBSUB_TOPIC" \
            --project="$PROJECT_ID"
        ok "Topic $PUBSUB_TOPIC created"
    fi

    # Dead-letter topic
    if gcloud pubsub topics describe "$PUBSUB_DLQ_TOPIC" \
           --project="$PROJECT_ID" &>/dev/null; then
        skip "Pub/Sub DLT $PUBSUB_DLQ_TOPIC"
    else
        log "Creating Pub/Sub dead-letter topic $PUBSUB_DLQ_TOPIC..."
        gcloud pubsub topics create "$PUBSUB_DLQ_TOPIC" \
            --project="$PROJECT_ID"
        ok "DLT $PUBSUB_DLQ_TOPIC created"
    fi

    # Subscription with dead-letter policy
    if gcloud pubsub subscriptions describe "$PUBSUB_SUBSCRIPTION" \
           --project="$PROJECT_ID" &>/dev/null; then
        skip "Pub/Sub subscription $PUBSUB_SUBSCRIPTION"
    else
        log "Creating Pub/Sub subscription $PUBSUB_SUBSCRIPTION..."
        gcloud pubsub subscriptions create "$PUBSUB_SUBSCRIPTION" \
            --project="$PROJECT_ID" \
            --topic="$PUBSUB_TOPIC" \
            --ack-deadline="$PUBSUB_ACK_DEADLINE" \
            --dead-letter-topic="projects/${PROJECT_ID}/topics/${PUBSUB_DLQ_TOPIC}" \
            --max-delivery-attempts="$PUBSUB_MAX_DELIVERY_ATTEMPTS" \
            --expiration-period=never
        ok "Subscription $PUBSUB_SUBSCRIPTION created (DLT after $PUBSUB_MAX_DELIVERY_ATTEMPTS nacks)"
    fi

    # Grant Pub/Sub SA permission to publish to DLT (required for dead-letter forwarding)
    PUBSUB_SA="service-$(gcloud projects describe "$PROJECT_ID" \
        --format='value(projectNumber)')@gcp-sa-pubsub.iam.gserviceaccount.com"
    log "Granting Pub/Sub SA permission to publish to DLT..."
    gcloud pubsub topics add-iam-policy-binding "$PUBSUB_DLQ_TOPIC" \
        --project="$PROJECT_ID" \
        --member="serviceAccount:$PUBSUB_SA" \
        --role="roles/pubsub.publisher" \
        --quiet &>/dev/null
    gcloud pubsub subscriptions add-iam-policy-binding "$PUBSUB_SUBSCRIPTION" \
        --project="$PROJECT_ID" \
        --member="serviceAccount:$PUBSUB_SA" \
        --role="roles/pubsub.subscriber" \
        --quiet &>/dev/null
    ok "Pub/Sub DLT IAM configured"
}

# ---------------------------------------------------------------------------
# Firewall Rule
# ---------------------------------------------------------------------------
create_firewall() {
    if gcloud compute firewall-rules describe "$FIREWALL_RULE_NAME" \
           --project="$PROJECT_ID" &>/dev/null; then
        skip "Firewall rule $FIREWALL_RULE_NAME"
    else
        log "Creating firewall rule for TCP $API_PORT..."
        gcloud compute firewall-rules create "$FIREWALL_RULE_NAME" \
            --project="$PROJECT_ID" \
            --allow="tcp:${API_PORT}" \
            --source-ranges="0.0.0.0/0" \
            --target-tags="pulsequeue" \
            --description="Allow inbound traffic to PulseQueue API"
        ok "Firewall rule created (TCP $API_PORT open)"
    fi
}

# ---------------------------------------------------------------------------
# Compute Engine VM
# ---------------------------------------------------------------------------
create_vm() {
    if gcloud compute instances describe "$INSTANCE_NAME" \
           --zone="$ZONE" --project="$PROJECT_ID" &>/dev/null; then
        skip "VM $INSTANCE_NAME (already exists)"
    else
        log "Creating e2-micro VM $INSTANCE_NAME in $ZONE..."
        log "(This takes ~30 seconds)"
        gcloud compute instances create "$INSTANCE_NAME" \
            --project="$PROJECT_ID" \
            --zone="$ZONE" \
            --machine-type="$MACHINE_TYPE" \
            --image-family="$IMAGE_FAMILY" \
            --image-project="$IMAGE_PROJECT" \
            --boot-disk-size="$DISK_SIZE" \
            --boot-disk-type="pd-standard" \
            --scopes="https://www.googleapis.com/auth/cloud-platform" \
            --service-account="$SA_EMAIL" \
            --tags="pulsequeue" \
            --metadata="enable-oslogin=TRUE" \
            --metadata-from-file="startup-script=infra/vm_startup.sh"
        ok "VM $INSTANCE_NAME created"
    fi
}

# ---------------------------------------------------------------------------
# Print summary
# ---------------------------------------------------------------------------
print_summary() {
    EXTERNAL_IP=$(gcloud compute instances describe "$INSTANCE_NAME" \
        --zone="$ZONE" \
        --project="$PROJECT_ID" \
        --format="value(networkInterfaces[0].accessConfigs[0].natIP)")

    echo ""
    echo "================================================================"
    echo "  PulseQueue GCP Setup Complete"
    echo "================================================================"
    echo ""
    echo "  VM Name:      $INSTANCE_NAME"
    echo "  External IP:  $EXTERNAL_IP"
    echo "  Zone:         $ZONE"
    echo "  Machine:      $MACHINE_TYPE (free-tier eligible)"
    echo ""
    echo "  Pub/Sub Topic:        $PUBSUB_TOPIC"
    echo "  Pub/Sub DLT:          $PUBSUB_DLQ_TOPIC"
    echo "  Pub/Sub Subscription: $PUBSUB_SUBSCRIPTION"
    echo ""
    echo "  SSH into the VM:"
    echo "    gcloud compute ssh $INSTANCE_NAME --zone $ZONE"
    echo ""
    echo "  After SSH, deploy PulseQueue:"
    echo "    cd /opt/pulsequeue"
    echo "    sudo bash deploy.sh"
    echo ""
    echo "  API will be available at: http://${EXTERNAL_IP}:${API_PORT}"
    echo "  Grafana:                  http://${EXTERNAL_IP}:3000"
    echo ""
    echo "  Run benchmarks from your local machine:"
    echo "    python bench_noop.py 1000 http://${EXTERNAL_IP}:${API_PORT}"
    echo "    python tests/bench_gcp.py --host http://${EXTERNAL_IP}:${API_PORT}"
    echo ""
    echo "  CLEANUP when done (avoids billing):"
    echo "    bash infra/teardown_gcp.sh"
    echo "================================================================"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    require_project
    enable_apis
    create_service_account
    create_pubsub
    create_firewall
    create_vm
    print_summary
}

main "$@"
