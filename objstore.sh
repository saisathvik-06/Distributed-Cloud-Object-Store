#!/usr/bin/env bash
# Shortcut for the command-line client (macOS / Linux):   bash objstore.sh <command> [arguments]
# Files are read from and written to the demo/work folder (mounted at /work in the client container).
#
#   bash objstore.sh gen 5.5MB big.bin
#   bash objstore.sh upload big.bin --meta owner=sai
#   bash objstore.sh list
#   bash objstore.sh cluster
cd "$(dirname "$0")" || exit 1
exec docker compose run --rm client "$@"
