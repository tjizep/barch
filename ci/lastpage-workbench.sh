#!/usr/bin/env bash
# Workbench for the feat/last_page_size branch - TODO 639.
#
# Three slim build trees next to the usual ones, so the cmake-build-* trees CLion
# uses are left alone:
#   cmake-build-lp-asan     address sanitizer, for correctness
#   cmake-build-lp-tsan     thread sanitizer, for correctness
#   cmake-build-lp-release  no sanitizer, only for RSS numbers (shadow memory
#                           makes RSS under asan/tsan meaningless)
# Cluster is off and only barchd gets built. The tests are the barchd based ones
# that save, load, copy on write or walk pages. Delete this before merging.
#
#   ci/lastpage-workbench.sh configure [asan|tsan|release|all]
#   ci/lastpage-workbench.sh build     [asan|tsan|release|all]
#   ci/lastpage-workbench.sh test      [asan|tsan|all]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JOBS="${JOBS:-6}"   # -j16 sanitizer builds have locked this machine up

# the venv has to exist before anything else runs, so those two lead
TESTS="TestInstallVENV|TestInstallRedisPy|TestBarchd|TestEmptySave|TestLoadResult|TestRetrieve|TestRollback|TestSavePair|TestSaveFreeze|TestSaveCopyOnWrite|TestCrossShardSave|TestFailedLoadSave|TestSnapshotPair|TestPageWalk|TestStreamBackup|TestDefragTomb"

dir_of() { echo "$ROOT/cmake-build-lp-$1"; }

configure() {
    local kind=$1 san=""
    case $kind in
        asan) san=address ;;
        tsan) san=thread ;;
        release) ;;
        *) echo "unknown build: $kind" >&2; exit 2 ;;
    esac
    # RelWithDebInfo for the sanitizers: Release turns on -flto
    local type=RelWithDebInfo
    [ "$kind" = release ] && type=Release
    cmake -S "$ROOT" -B "$(dir_of "$kind")" -G "Unix Makefiles" \
        -DCMAKE_BUILD_TYPE=$type -DSANITIZE=$san -DBARCH_CLUSTER=OFF \
        -DGEN_LUA=OFF -DGEN_JNI=OFF -DCOVERAGE=OFF
}

build() {
    local d; d="$(dir_of "$1")"
    [ -f "$d/Makefile" ] || configure "$1"
    nice make -C "$d" -j"$JOBS" barchd
    # a lock-up mid build leaves empty objects that make thinks are current
    if find "$d" \( -name '*.o' -o -name '*.so' \) -size 0 | grep -q .; then
        echo "zero byte objects in $d - remove them and build again" >&2
        exit 1
    fi
}

run_tests() {
    local d; d="$(dir_of "$1")"
    (cd "$d" && BARCH_TEST_SCALE="${BARCH_TEST_SCALE:-0.05}" \
        ctest -R "^($TESTS)\$" --output-on-failure)
}

each() {
    local what=$1 kind=${2:-all}
    if [ "$kind" = all ]; then
        local k
        for k in asan tsan $([ "$what" = run_tests ] || echo release); do
            "$what" "$k"
        done
    else
        "$what" "$kind"
    fi
}

case ${1:-} in
    configure) each configure "${2:-all}" ;;
    build) each build "${2:-all}" ;;
    test) each run_tests "${2:-all}" ;;
    *) sed -n '2,16p' "$0"; exit 2 ;;
esac
