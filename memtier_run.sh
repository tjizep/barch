#!/bin/bash
# usage: memtier_run.sh <label> <barchd binary> <ordered_keys yes|no> <secs> <threads>
# fills 10M keys of 64 bytes, then runs 1:10 SET:GET for <secs>.
set -u
S=/tmp/scratchpd
label=$1 bin=$2 ordered=$3 secs=${4:-60} trs=${5:-1}
port=14301
dir=$S/mt/$label
rm -rf "$dir"; mkdir -p "$dir"
 BARCH_INTERNAL_SHARDS=${SHARDS:-17} "$bin" --port $port --dir "$dir" --no-save-on-exit -c ordered_keys=$ordered -c max_memory_bytes=16g \
    > "$dir/barchd.log" 2>&1 &
pid=$!
for i in $(seq 100); do redis-cli -p $port ping 2>/dev/null | grep -q PONG && break; sleep 0.2; done
common="-s 127.0.0.1 -p $port --protocol=redis -d 64 --key-minimum=1 --key-maximum=10000000 --pipeline=50 -t $trs -c 16 --hide-histogram"
memtier_benchmark $common --ratio=1:0 --key-pattern=P:P -n allkeys > "$dir/fill.txt" 2>&1
echo "$label fill: $(grep -E '^Sets|^Totals' "$dir/fill.txt" | tail -1)"
echo "$label dbsize: $(redis-cli -p $port dbsize)"
memtier_benchmark $common --ratio=1:10 --key-pattern=R:R --distinct-client-seed --test-time=$secs > "$dir/mix.txt" 2>&1
grep -E '^(Sets|Gets|Totals)' "$dir/mix.txt" | sed "s/^/$label /"
echo "$label rss_kb: $(ps -o rss= -p $pid)"
kill $pid; wait $pid 2>/dev/null