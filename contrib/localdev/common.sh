#!/bin/sh
set -euo pipefail
script_dir="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
log_dir="$HOME"/.hypershift
mkdir -p "$log_dir" 2>/dev/null

HYPERSHIFT_DIR=${HYPERSHIFT_DIR:-$script_dir/../..}
