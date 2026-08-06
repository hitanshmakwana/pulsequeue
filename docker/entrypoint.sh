#!/bin/sh
# Entrypoint for both API and worker containers.
#
# Problem: Docker named volumes are owned by root when first created. Both the
# API and worker containers run as the non-root 'pulse' user (uid 1000), so
# they get "Permission denied" when prometheus_client tries to mmap files into
# the shared PROMETHEUS_MULTIPROC_DIR.
#
# Fix: This script runs as root (the container's default user before USER
# directive takes effect in the image layer — but since we removed USER from
# the Dockerfile and rely on this entrypoint, we are root here). We create the
# dir, chown it to pulse, then exec the real process as pulse via su-exec /
# gosu. Since we don't want to add gosu as a dependency, we use 'su' which is
# available in the base image.

set -e

if [ -n "$PROMETHEUS_MULTIPROC_DIR" ]; then
    mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
    chown -R 1000:1000 "$PROMETHEUS_MULTIPROC_DIR"
    chmod 755 "$PROMETHEUS_MULTIPROC_DIR"
fi

# Drop to the pulse user and exec the CMD.
exec su -s /bin/sh pulse -c "exec $*"
