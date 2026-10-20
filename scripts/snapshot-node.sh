#!/bin/sh
set -eu

base_url=${1:-http://127.0.0.1:2790}
metrics_file=$(mktemp)
trap 'rm -f "$metrics_file"' EXIT HUP INT TERM

curl --fail --silent --show-error --max-time 10 \
    "$base_url/api/v2/metrics" >"$metrics_file"
live=$(curl --silent --show-error --max-time 10 --output /dev/null \
    --write-out '%{http_code}' "$base_url/health/live")
ready=$(curl --silent --show-error --max-time 10 --output /dev/null \
    --write-out '%{http_code}' "$base_url/health/ready")

echo "timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "live_http=$live"
echo "ready_http=$ready"

awk '
    function emit(key, value) { print key "=" value }
    /^process_start_time_seconds / { emit("process_start_time_seconds", $2) }
    /^process_resident_memory_bytes / { emit("process_resident_memory_bytes", $2) }
    /^recentmessages_ingest_accepted_records_total / { emit("accepted_records", $2) }
    /^recentmessages_ingest_queue_depth / { emit("ingest_queue_depth", $2) }
    /^recentmessages_ingest_last_accept_timestamp_seconds / { emit("last_accept_timestamp_seconds", $2) }
    /^recentmessages_ingest_checkpoints_total / { emit("checkpoints", $2) }
    /^recentmessages_ingest_checkpoint_seconds_sum / { emit("checkpoint_seconds_sum", $2) }
    /^recentmessages_ingest_checkpoint_seconds_count / { emit("checkpoint_seconds_count", $2) }
    /^recentmessages_ingest_checkpoint_stage_seconds_sum\{/ { emit($1, $2) }
    /^recentmessages_ingest_checkpoint_stage_seconds_count\{/ { emit($1, $2) }
    /^recentmessages_ingest_last_checkpoint_timestamp_seconds / { emit("last_checkpoint_timestamp_seconds", $2) }
    /^recentmessages_storage_physical_bytes / { emit("storage_physical_bytes", $2) }
    /^recentmessages_storage_effective_max_bytes / { emit("storage_effective_max_bytes", $2) }
    /^recentmessages_storage_evicted_blocks_total / { emit("storage_evicted_blocks", $2) }
    /^recentmessages_store_channels / { emit("store_channels", $2) }
    /^recentmessages_store_blocks / { emit("store_blocks", $2) }
    /^recentmessages_store_messages / { emit("store_messages", $2) }
    /^recentmessages_store_compressed_payload_bytes / { emit("store_compressed_payload_bytes", $2) }
    /^recentmessages_store_uncompressed_bytes / { emit("store_uncompressed_bytes", $2) }
    /^recentmessages_store_journal_batches / { emit("journal_batches", $2) }
    /^recentmessages_store_journal_bytes / { emit("journal_bytes", $2) }
    /^recentmessages_raw_firehose_connected\{/ { emit($1, $2) }
    /^recentmessages_raw_firehose_last_event_timestamp_seconds\{/ { emit($1, $2) }
    /^recentmessages_raw_firehose_events_total\{/ { emit($1, $2) }
    /^recentmessages_adaptive_response_cache_total\{/ { emit($1, $2) }
    /^recentmessages_adaptive_response_cache_entries / { emit("adaptive_response_cache_entries", $2) }
    /^recentmessages_adaptive_response_cache_bytes / { emit("adaptive_response_cache_bytes", $2) }
' "$metrics_file"
