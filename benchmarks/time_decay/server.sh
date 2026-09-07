#!/usr/bin/env bash
set -euo pipefail
bench_root="$(pwd)/${BENCH_DIR:-build}"
mkdir -p "$bench_root/results" "$bench_root/server-data"
cat > "$bench_root/server.xml" <<EOF
<clickhouse>
  <logger><level>information</level><log>$bench_root/server.log</log><errorlog>$bench_root/server.err.log</errorlog><size>20M</size><count>2</count></logger>
  <http_port>18123</http_port><tcp_port>19000</tcp_port><listen_host>127.0.0.1</listen_host>
  <path>$bench_root/server-data/</path><tmp_path>$bench_root/server-data/tmp/</tmp_path>
  <user_directories><users_xml><path>$bench_root/users.xml</path></users_xml></user_directories>
  <max_server_memory_usage>6000000000</max_server_memory_usage>
  <mark_cache_size>134217728</mark_cache_size><uncompressed_cache_size>0</uncompressed_cache_size>
  <max_thread_pool_size>512</max_thread_pool_size>
  <background_pool_size>16</background_pool_size><background_schedule_pool_size>4</background_schedule_pool_size>
  <background_buffer_flush_schedule_pool_size>2</background_buffer_flush_schedule_pool_size>
  <background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size>
  <background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
  <merge_tree><number_of_free_entries_in_pool_to_execute_mutation>2</number_of_free_entries_in_pool_to_execute_mutation><number_of_free_entries_in_pool_to_execute_optimize_entire_partition>2</number_of_free_entries_in_pool_to_execute_optimize_entire_partition></merge_tree>
  <query_log><database>system</database><table>query_log</table><flush_interval_milliseconds>1000</flush_interval_milliseconds></query_log>
</clickhouse>
EOF
cat > "$bench_root/users.xml" <<'EOF'
<clickhouse><profiles><default><max_threads>2</max_threads></default></profiles>
<users><default><password></password><networks><ip>127.0.0.1</ip><ip>::1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users>
<quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time></interval></default></quotas></clickhouse>
EOF
"${BENCH_BINARY:-build/clickhouse}" server --config-file="$bench_root/server.xml" > "$bench_root/server-console.log" 2>&1 &
bench_server_pid=$!
trap 'kill "$bench_server_pid" 2>/dev/null || true; wait "$bench_server_pid" 2>/dev/null || true' EXIT
python3 - <<'PY'
import time, urllib.request
for attempt in range(60):
    try:
        with urllib.request.urlopen('http://127.0.0.1:18123/ping',timeout=1) as r:
            if r.read()==b'Ok.\n':break
    except OSError:
        time.sleep(1)
else: raise RuntimeError('Server failed to start')
PY
bench_log="$bench_root/test_benchmark_$(date -u +%Y%m%dT%H%M%S%N).log"
python3 benchmarks/time_decay/run.py "$@" > "$bench_log" 2>&1
