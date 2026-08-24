#!/bin/sh
set -eu

base_url=${1:-http://127.0.0.1:2790}
expected_firehoses=${EXPECTED_FIREHOSES:-4}
max_queue_depth=${MAX_QUEUE_DEPTH:-200}
max_event_age=${MAX_EVENT_AGE_SECONDS:-300}
max_accept_age=${MAX_ACCEPT_AGE_SECONDS:-120}
metrics_file=$(mktemp)
trap 'rm -f "$metrics_file"' EXIT HUP INT TERM

curl --fail --silent --show-error --max-time 10 \
    "$base_url/api/v2/metrics" >"$metrics_file"

ready_status=$(curl --silent --show-error --max-time 10 --output /dev/null \
    --write-out '%{http_code}' "$base_url/health/ready")
[ "$ready_status" = 204 ] || {
    echo "unhealthy: readiness returned HTTP $ready_status" >&2
    exit 1
}

read -r connected total <<EOF
$(awk '
    /^recentmessages_raw_firehose_connected\{/ {
        total += 1
        if ($2 == 1) connected += 1
    }
    END { print connected + 0, total + 0 }
' "$metrics_file")
EOF

queue_depth=$(awk '
    /^recentmessages_ingest_queue_depth / { print $2; found = 1 }
    END { if (!found) print "missing" }
' "$metrics_file")
accepted_records=$(awk '
    /^recentmessages_ingest_accepted_records_total / { print $2; found = 1 }
    END { if (!found) print "missing" }
' "$metrics_file")
now=$(date +%s)
last_accept=$(awk '
    /^recentmessages_ingest_last_accept_timestamp_seconds / { print $2; found = 1 }
    END { if (!found) print 0 }
' "$metrics_file")
stale_sources=$(awk -v now="$now" -v maximum="$max_event_age" '
    function source_name(line, value) {
        value = line
        sub(/^.*source="/, "", value)
        sub(/".*$/, "", value)
        return value
    }
    /^recentmessages_raw_firehose_connected\{/ {
        connected[source_name($1)] = $2
    }
    /^recentmessages_raw_firehose_last_event_timestamp_seconds\{/ {
        last[source_name($1)] = $2
    }
    END {
        separator = ""
        for (source in connected) {
            if (connected[source] != 1 || !(source in last) || last[source] == 0 || now - last[source] > maximum) {
                printf "%s%s", separator, source
                separator = ","
            }
        }
    }
' "$metrics_file")

if [ "$total" -ne "$expected_firehoses" ] || [ "$connected" -ne "$expected_firehoses" ]; then
    echo "unhealthy: firehoses connected=$connected total=$total expected=$expected_firehoses" >&2
    exit 1
fi

if [ "$queue_depth" = missing ] || [ "$queue_depth" -gt "$max_queue_depth" ]; then
    echo "unhealthy: ingest queue depth=$queue_depth maximum=$max_queue_depth" >&2
    exit 1
fi

if [ "$accepted_records" = missing ]; then
    echo "unhealthy: accepted-record metric is missing" >&2
    exit 1
fi

if [ "$last_accept" -eq 0 ] || [ $((now - last_accept)) -gt "$max_accept_age" ]; then
    echo "unhealthy: durable ingest has not progressed recently" >&2
    exit 1
fi

if [ -n "$stale_sources" ]; then
    echo "unhealthy: stale firehose event streams=$stale_sources" >&2
    exit 1
fi

echo "healthy: firehoses=$connected/$total queue_depth=$queue_depth accepted_records=$accepted_records"
