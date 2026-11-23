#!/usr/bin/env bash
# Shared helpers for the bash demonstration scripts. Sourced by each script.
# Works on macOS (including its built-in bash 3.2), Linux and Git Bash on Windows.
# Needs only Docker and curl on the host: JSON is read with Python inside the controller container.

export MSYS_NO_PATHCONV=1      # Git Bash on Windows: do not rewrite /data/... paths
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT" || exit 1

API="http://localhost:8000/api/v1"
WORKERS="worker1 worker2 worker3 worker4"

NO_PAUSE=0; QUICK_RESTART=0; BOTH_COPIES=0
for arg in "$@"; do
    case "$arg" in
        --no-pause) NO_PAUSE=1 ;;
        --quick-restart) QUICK_RESTART=1 ;;
        --both-copies) BOTH_COPIES=1 ;;
    esac
done

CYAN=$'\033[36m'; YELLOW=$'\033[33m'; GREEN=$'\033[32m'; RED=$'\033[31m'; MAGENTA=$'\033[35m'; RESET=$'\033[0m'
LINE="===================================================================================================="

step()  { printf '\n%s%s\n  %s\n%s%s\n' "$CYAN" "$LINE" "$1" "$LINE" "$RESET"; }
note()  { printf '%s%s%s\n' "$YELLOW" "$1" "$RESET"; }
pass()  { printf '%sPASS: %s%s\n' "$GREEN" "$1" "$RESET"; }
fail()  { printf '%sFAIL: %s%s\n' "$RED" "$1" "$RESET"; exit 1; }
heading() { printf '%s%s%s\n' "$MAGENTA" "$1" "$RESET"; }

pause_demo() {
    if [ "$NO_PAUSE" != 1 ]; then read -r -p "Press Enter to continue" _; fi
}

# Run the command-line client and show its output.
client() { docker compose run --rm -T -e FORCE_COLOR=1 client "$@" 2>/dev/null; }

# Run the command-line client with --json and print the raw JSON.
client_json() { docker compose run --rm -T client "$@" --json 2>/dev/null; }

# Read JSON from stdin and print a Python expression of it (d = the parsed JSON).
json_field() { docker exec -i controller python -c "import json,sys; d=json.load(sys.stdin); print($1)"; }

# api <path under /api/v1> <Python expression of d>: query the controller and print the result.
api() {
    docker exec controller python -c "import json,sys,urllib.request as u; d=json.load(u.urlopen('http://127.0.0.1:8000/api/v1'+sys.argv[1])); print($2)" "$1"
}

alive_count() { api /admin/cluster "d['alive_workers']"; }
replication() { api /admin/cluster "d['replication']['$1']"; }
worker_state() { api /admin/cluster "[w['state'] for w in d['workers'] if w['worker_id']=='$1'][0]"; }

# SHA-256 of a file on this computer (sha256sum on Linux and Git Bash, shasum on macOS).
file_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
    else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

# wait_until "<what>" <timeout seconds> <command...>: poll every 2 s until the command succeeds.
wait_until() {
    local what=$1 timeout=$2 deadline
    shift 2
    printf 'waiting for: %s ' "$what"
    deadline=$(( $(date +%s) + timeout ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if "$@" >/dev/null 2>&1; then printf '%s done%s\n' "$GREEN" "$RESET"; return 0; fi
        printf '.'
        sleep 2
    done
    echo
    fail "timed out waiting for: $what"
}

all_alive() { [ "$(alive_count)" = "4" ]; }

assert_cluster_ready() {
    if ! curl -s "$API/admin/cluster" >/dev/null; then
        fail "the controller is not reachable; start Docker and run 'docker compose up -d' first"
    fi
    if ! all_alive; then wait_until "all 4 workers ALIVE" 90 all_alive; fi
}

show_controller_log() {  # show_controller_log <regex> [how many lines]
    docker logs controller 2>&1 | grep -E "$1" | tail -n "${2:-12}"
}
