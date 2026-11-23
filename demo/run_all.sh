#!/usr/bin/env bash
# Run all six required demonstrations in order.   bash demo/run_all.sh [--no-pause]
dir="$(cd "$(dirname "$0")" && pwd)"
for name in 01_object_storage 02_chunking_distribution 03_replication 04_metadata 05_worker_failure 06_data_integrity; do
    if ! bash "$dir/$name.sh" "$@"; then
        printf '\033[31mDemonstration %s failed.\033[0m\n' "$name"
        exit 1
    fi
done
echo
printf '\033[32mAll six demonstrations passed.\033[0m\n'
