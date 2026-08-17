#!/usr/bin/env bash
# infra/teardown_gcp.sh — Complete GCP resource cleanup for PulseQueue.
#
# Deletes EVERYTHING created by setup_gcp.sh:
#   - The GCE VM and its boot disk
#   - All Pub/Sub resources (topics, subscriptions)
#   - The firewall rule
#   - The IAM service account
#
# IMPORTANT: This is irreversible. All job data on the VM will be lost.
# Run this when you are done with the GCP deployment to avoid ongoing charges.
#
# Usage:
#   export PROJECT_ID=$(gcloud config get-value project)
#   bash infra/teardown_gcp.sh
#
# Dry run (print what would be deleted without deleting):
#   DRY_RUN=1 bash infra/teardown_gcp.sh

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-us-central1}"
ZONE="${ZONE:-us-central1-a}"
INSTANCE_NAME="${INSTANCE_NAME:-pulsequeue-vm}"
SA_NAME="${SA_NAME:-pulsequeue-sa}"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
PUBSUB_TOPIC="${PUBSUB_TOPIC:-pulsequeue-jobs}"
PUBSUB_DLQ_TOPIC="${PUBSUB_DLQ_TOPIC:-pulsequeue-dlq}"
PUBSUB_SUBSCRIPTION="${PUBSUB_SUBSCRIPTION:-pulsequeue-worker}"
FIREWALL_RULE_NAME="allow-pulsequeue-api"
DRY_RUN="${DRY_RUN:-0}"

log()  { echo "[teardown] $*"; }
ok()   { echo "[teardown] ✓ $*"; }
skip() { echo "[teardown] ↷ $* (not found — already cleaned up)"; }

run() {
    if [ "$DRY_RUN" = "1" ]; then
        echo "[DRY_RUN] would run: $*"
    else
        "$@"
    fi
}

# ---------------------------------------------------------------------------
echo ""
echo "================================================================"
echo "  PulseQueue GCP Teardown"
echo "  Project: $PROJECT_ID"
echo "  Zone:    $ZONE"
echo "================================================================"
echo ""

if [ "$DRY_RUN" = "1" ]; then
    echo "DRY_RUN=1 — nothing will actually be deleted."
    echo ""
fi

# Confirm unless DRY_RUN or CI
if [ "$DRY_RUN" != "1" ] && [ "${CI:-}" != "true" ]; then
    read -r -p "Delete ALL PulseQueue GCP resources? This is irreversible. [y/N] " CONFIRM
    if [[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]]; then
        echo "Aborted."
        exit 0
    fi
fi

# ---------------------------------------------------------------------------
# 1. VM (and its boot disk)
# ---------------------------------------------------------------------------
log "Deleting VM $INSTANCE_NAME..."
if gcloud compute instances describe "$INSTANCE_NAME" \
       --zone="$ZONE" --project="$PROJECT_ID" &>/dev/null; then
    run gcloud compute instances delete "$INSTANCE_NAME" \
        --zone="$ZONE" \
        --project="$PROJECT_ID" \
        --delete-disks=all \
        --quiet
    ok "VM deleted"
else
    skip "VM $INSTANCE_NAME"
fi

# ---------------------------------------------------------------------------
# 2. Pub/Sub subscription (must delete before topics)
# ---------------------------------------------------------------------------
log "Deleting Pub/Sub subscription $PUBSUB_SUBSCRIPTION..."
if gcloud pubsub subscriptions describe "$PUBSUB_SUBSCRIPTION" \
       --project="$PROJECT_ID" &>/dev/null; then
    run gcloud pubsub subscriptions delete "$PUBSUB_SUBSCRIPTION" \
        --project="$PROJECT_ID" \
        --quiet
    ok "Subscription deleted"
else
    skip "Subscription $PUBSUB_SUBSCRIPTION"
fi

# ---------------------------------------------------------------------------
# 3. Pub/Sub topics
# ---------------------------------------------------------------------------
for TOPIC in "$PUBSUB_TOPIC" "$PUBSUB_DLQ_TOPIC"; do
    log "Deleting Pub/Sub topic $TOPIC..."
    if gcloud pubsub topics describe "$TOPIC" \
           --project="$PROJECT_ID" &>/dev/null; then
        run gcloud pubsub topics delete "$TOPIC" \
            --project="$PROJECT_ID" \
            --quiet
        ok "Topic $TOPIC deleted"
    else
        skip "Topic $TOPIC"
    fi
done

# ---------------------------------------------------------------------------
# 4. Firewall rule
# ---------------------------------------------------------------------------
log "Deleting firewall rule $FIREWALL_RULE_NAME..."
if gcloud compute firewall-rules describe "$FIREWALL_RULE_NAME" \
       --project="$PROJECT_ID" &>/dev/null; then
    run gcloud compute firewall-rules delete "$FIREWALL_RULE_NAME" \
        --project="$PROJECT_ID" \
        --quiet
    ok "Firewall rule deleted"
else
    skip "Firewall rule $FIREWALL_RULE_NAME"
fi

# ---------------------------------------------------------------------------
# 5. Service account
# ---------------------------------------------------------------------------
log "Deleting service account $SA_EMAIL..."
if gcloud iam service-accounts describe "$SA_EMAIL" \
       --project="$PROJECT_ID" &>/dev/null; then
    run gcloud iam service-accounts delete "$SA_EMAIL" \
        --project="$PROJECT_ID" \
        --quiet
    ok "Service account deleted"
else
    skip "Service account $SA_EMAIL"
fi

# ---------------------------------------------------------------------------
echo ""
echo "================================================================"
echo "  Teardown complete. All PulseQueue GCP resources have been"
echo "  deleted. You will not be charged for any of these resources"
echo "  after this point."
echo ""
echo "  Verify in the GCP Console:"
echo "  https://console.cloud.google.com/compute/instances?project=$PROJECT_ID"
echo "  https://console.cloud.google.com/cloudpubsub/topic/list?project=$PROJECT_ID"
echo "================================================================"
