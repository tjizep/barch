#!/usr/bin/env bash
#
# Run the TSan test set under CPU pressure, several times over.
#
# A race that needs an unlucky interleaving turns up on a loaded CI runner and
# not on a workstation with sixteen idle cores, which is exactly what happened
# with the two in DONE 215. This squeezes the run into a few cores and puts
# something greedy on those same cores, so the threads have to fight for time.
#
#   ci/tsan-stress.sh                       # 3 runs of the short set on 2 cpus
#   ci/tsan-stress.sh -n 10 -c 1            # 10 runs, everything on cpu 0
#   ci/tsan-stress.sh -q 20%                # starve with a cgroup quota instead
#   ci/tsan-stress.sh -- -R TestFetchLuau   # anything after -- goes to ctest
#
# Needs a build configured with -DSANITIZE=thread; see ci/README.md. Exit is
# non-zero if any iteration failed, and with SANITIZE_EXITCODE at its default
# of 66 a TSan report is one of the ways an iteration fails.
set -uo pipefail

iters=3
cpus=2
hogs=""
quota=""
build="build-tsan"
scale="${BARCH_TEST_SCALE:-0.05}"

usage() {
    sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

while [ $# -gt 0 ]; do
    case "$1" in
        -n) iters=$2; shift 2 ;;
        -c) cpus=$2; shift 2 ;;
        -j) hogs=$2; shift 2 ;;
        -q) quota=$2; shift 2 ;;
        -b) build=$2; shift 2 ;;
        -s) scale=$2; shift 2 ;;
        -h|--help) usage 0 ;;
        --) shift; break ;;
        *) echo "unknown argument: $1" >&2; usage 1 ;;
    esac
done
ctest_args=("$@")
[ ${#ctest_args[@]} -eq 0 ] && ctest_args=(-L short)

[ -d "$build" ] || { echo "no build directory '$build' - see ci/README.md" >&2; exit 1; }
grep -q "^SANITIZE:STRING=thread" "$build/CMakeCache.txt" 2>/dev/null || \
    echo "warning: '$build' is not configured with -DSANITIZE=thread" >&2

# two hogs per cpu by default: one would just take the other half of a core,
# two means the test threads are behind somebody in the queue whenever they wake
[ -n "$hogs" ] || hogs=$((cpus * 2))
cpulist="0-$((cpus - 1))"
[ "$cpus" -eq 1 ] && cpulist="0"

hog_pids=()
cleanup() {
    for p in ${hog_pids+"${hog_pids[@]}"}; do
        kill "$p" 2>/dev/null
    done
    wait 2>/dev/null
}
trap cleanup EXIT INT TERM

# Deliberately not nice'd. A polite neighbour yields the moment the tests want
# the cpu, which is the opposite of the thing being reproduced.
for _ in $(seq "$hogs"); do
    taskset -c "$cpulist" bash -c 'while :; do :; done' &
    hog_pids+=($!)
done

runner=(taskset -c "$cpulist")
if [ -n "$quota" ]; then
    # A quota throttles the whole run rather than crowding it: the cgroup gets
    # a slice of wall clock and is frozen for the rest of it. Needs a user
    # systemd session, which a GitHub runner does not have - taskset is the
    # portable lever, this one is for a workstation.
    runner=(systemd-run --user --scope --quiet -p "CPUQuota=$quota" "${runner[@]}")
fi

echo "build=$build cpus=$cpulist hogs=$hogs quota=${quota:-none} scale=$scale iters=$iters"
echo "ctest ${ctest_args[*]}"

log_dir=$(mktemp -d)
failed=0
for i in $(seq "$iters"); do
    start=$SECONDS
    ( cd "$build" && BARCH_TEST_SCALE="$scale" "${runner[@]}" \
        ctest "${ctest_args[@]}" --output-on-failure ) > "$log_dir/run$i.log" 2>&1
    rc=$?
    # --output-on-failure is what puts a failing test's stdout in the log, and a
    # TSan report only ever appears there. Counting without it counts nothing.
    reports=$(grep -c "WARNING: ThreadSanitizer" "$log_dir/run$i.log")
    passed=$(grep -o "[0-9]*% tests passed" "$log_dir/run$i.log" | head -1)
    printf "run %2d: rc=%d %-22s tsan reports=%-4s %ds  %s\n" \
        "$i" "$rc" "$passed" "$reports" "$((SECONDS - start))" "$log_dir/run$i.log"
    [ "$rc" -ne 0 ] && failed=$((failed + 1))
done

echo
if [ "$failed" -eq 0 ]; then
    echo "$iters/$iters clean. Logs in $log_dir"
else
    echo "$failed of $iters failed. Logs in $log_dir"
    echo "grep -n 'WARNING: ThreadSanitizer' $log_dir/run*.log"
fi
exit $((failed > 0))
