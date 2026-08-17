#!/usr/bin/env bash
# infra/vm_startup.sh — GCE VM startup script.
#
# This script runs automatically when the VM first boots (via --metadata startup-script).
# It installs Docker, Docker Compose, clones the repo, and waits for manual `deploy.sh` call.
#
# The startup script runs as root. It creates /opt/pulsequeue and a deploy.sh
# helper that the user runs after SSH-ing in.
#
# NOTE: This does NOT auto-start PulseQueue because you need to supply
# .env.gcp with your GCP_PROJECT_ID first. The startup script installs
# everything and then waits for you.

set -euo pipefail

log() { echo "[startup] $(date -u +%H:%M:%S) $*" | tee -a /var/log/pulsequeue-startup.log; }

log "=== PulseQueue VM startup script started ==="

# ---------------------------------------------------------------------------
# 1. System update
# ---------------------------------------------------------------------------
log "Updating system packages..."
apt-get update -qq
apt-get install -y -qq \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    git \
    htop \
    lsb-release \
    2>/dev/null

# ---------------------------------------------------------------------------
# 2. Install Docker
# ---------------------------------------------------------------------------
log "Installing Docker..."
if ! command -v docker &>/dev/null; then
    curl -fsSL https://get.docker.com | bash
    log "Docker installed: $(docker --version)"
else
    log "Docker already installed: $(docker --version)"
fi

# Docker Compose V2 (plugin)
if ! docker compose version &>/dev/null; then
    apt-get install -y -qq docker-compose-plugin
fi
log "Docker Compose: $(docker compose version)"

# Add the default user to the docker group so they don't need sudo
usermod -aG docker "$(who | head -1 | awk '{print $1}')" 2>/dev/null || true

# ---------------------------------------------------------------------------
# 3. Install Cloud Ops Agent (Prometheus → Cloud Monitoring)
# ---------------------------------------------------------------------------
log "Installing Google Cloud Ops Agent..."
if ! command -v google-cloud-ops-agent &>/dev/null 2>&1; then
    curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
    bash add-google-cloud-ops-agent-repo.sh --also-install --version=latest
    rm -f add-google-cloud-ops-agent-repo.sh
    log "Ops Agent installed"
else
    log "Ops Agent already installed"
fi

# ---------------------------------------------------------------------------
# 4. Create deploy helper script at /opt/pulsequeue/deploy.sh
# ---------------------------------------------------------------------------
mkdir -p /opt/pulsequeue
cat > /opt/pulsequeue/deploy.sh << 'DEPLOY_EOF'
#!/usr/bin/env bash
# deploy.sh — Run on the VM after SSH-ing in.
#
# Usage:
#   cd /opt/pulsequeue
#   git clone https://github.com/YOUR_USERNAME/pulsequeue.git .  # first time
#   cp .env.gcp.example .env.gcp
#   nano .env.gcp   # fill in GCP_PROJECT_ID
#   sudo bash deploy.sh

set -euo pipefail
REPO_DIR="/opt/pulsequeue/repo"

log() { echo "[deploy] $*"; }

if [ ! -d "$REPO_DIR/.git" ]; then
    log "ERROR: Repo not found at $REPO_DIR"
    log "Run: git clone https://github.com/YOUR_USERNAME/pulsequeue.git $REPO_DIR"
    exit 1
fi

cd "$REPO_DIR"

if [ ! -f ".env.gcp" ]; then
    log "ERROR: .env.gcp not found."
    log "Run: cp .env.gcp.example .env.gcp && nano .env.gcp"
    exit 1
fi

log "Pulling latest code..."
git pull --ff-only

log "Installing Ops Agent Prometheus config..."
if [ -f "infra/ops_agent_config.yml" ]; then
    cp infra/ops_agent_config.yml /etc/google-cloud-ops-agent/config.yaml
    systemctl restart google-cloud-ops-agent
    log "Ops Agent restarted with Prometheus scrape config"
fi

log "Building and starting Docker Compose stack..."
source .env.gcp
export $(grep -v '^#' .env.gcp | xargs)

docker compose \
    -f docker-compose.yml \
    -f docker-compose.gcp.yml \
    --env-file .env.gcp \
    up --build -d

log "Waiting for API to become healthy..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8000/health >/dev/null 2>&1; then
        log "API is healthy!"
        break
    fi
    sleep 2
done

EXTERNAL_IP=$(curl -sf "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip" \
    -H "Metadata-Flavor: Google" 2>/dev/null || echo "unknown")

log "=== Deployment complete ==="
log "API:       http://${EXTERNAL_IP}:8000"
log "Grafana:   http://${EXTERNAL_IP}:3000  (admin/admin)"
log "Benchmark: python bench_noop.py 1000 http://${EXTERNAL_IP}:8000"
DEPLOY_EOF

chmod +x /opt/pulsequeue/deploy.sh
log "deploy.sh created at /opt/pulsequeue/deploy.sh"

# ---------------------------------------------------------------------------
log "=== Startup script complete ==="
log "SSH into this VM and run:"
log "  git clone <YOUR_REPO_URL> /opt/pulsequeue/repo"
log "  cp /opt/pulsequeue/repo/.env.gcp.example /opt/pulsequeue/repo/.env.gcp"
log "  nano /opt/pulsequeue/repo/.env.gcp   # fill in GCP_PROJECT_ID"
log "  sudo bash /opt/pulsequeue/deploy.sh"
