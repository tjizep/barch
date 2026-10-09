#!/usr/bin/env python3
"""
Cluster benchmarks - TODO 624. Not a test: it measures, and prints what it found.

It starts its own barchd and drives them with memtier_benchmark, so the numbers come
from the binary given in BARCHD and nothing else. Use a Release build and a quiet
machine; the scenarios are short, so anything else running shows in them.

Scenarios:
  plain     one barchd, no cluster: SET and GET on space db1
  raft      three barchd, db1 replicated with Raft: SET and GET on its leader
  raft2     raft's SETs again with one follower stopped, so two nodes sync
  mixed     on that leader, GETs on db2, which isn't replicated, alone and then
            while writers fill db1 - a write waits for its commit on the RESP
            thread it arrived on, so readers sharing that thread wait with it

Each runs at 1, 8 and 64 connections without pipelining, and at 64 with a pipeline
of 32. For replicated SETs, each node's Raft log counters (CLUSTER INFO) say how many
syncs it made and how many entries each covered - TODO 630. memtier reaches a space through SELECT: database 1 is db1.

  BARCHD=build/barchd python3 test/clusterbench.py [--seconds 10] [--out bench.json]
         [--only plain,raft,mixed,raft2]

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import argparse
import json
import re
import os
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

# the server's own default, not the tests' 32 (see clusternodes) - TODO 631
BENCH_SETTINGS = ["raft_shards=128"]
KEYS = 100000
DATA = 64
SHAPES = [(1, 1), (8, 1), (64, 1), (64, 32)]       # (connections, pipeline)


def memtier(port, db, ratio, conns, pipeline, seconds, extra=()):
    """one memtier run; ops/s and latency percentiles in ms for the op it did"""
    threads = min(4, conns)
    out = tempfile.NamedTemporaryFile(suffix=".json", delete=False).name
    cmd = ["memtier_benchmark", "-s", "127.0.0.1", "-p", str(port), "--select-db", str(db),
           "-t", str(threads), "-c", str(max(1, conns // threads)), "--pipeline", str(pipeline),
           "--ratio", ratio, "--data-size", str(DATA), "--key-maximum", str(KEYS),
           "--key-pattern", "R:R", "--test-time", str(seconds), "--hide-histogram",
           "--print-percentiles", "50,99,99.9", "--json-out-file", out] + list(extra)
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    try:
        with open(out) as f:
            stats = json.load(f)["ALL STATS"]
    finally:
        os.remove(out)
    op = "Sets" if ratio.startswith("1:0") else "Gets"
    s = stats.get(op, {})
    p = s.get("Percentile Latencies", {})
    return {
        "ops": round(s.get("Ops/sec", 0.0)),
        "p50": p.get("p50.00"),
        "p99": p.get("p99.00"),
        "p999": p.get("p99.90"),
    }


def fill(port, db):
    """every key once, so a GET finds something"""
    subprocess.run(["memtier_benchmark", "-s", "127.0.0.1", "-p", str(port), "--select-db", str(db),
                    "-t", "4", "-c", "8", "--pipeline", "32", "--ratio", "1:0", "--data-size", str(DATA),
                    "--key-minimum", "1", "--key-maximum", str(KEYS), "--key-pattern", "P:P",
                    "-n", "allkeys", "--hide-histogram"],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


results = []


def log_counts(nodes):
    """each live node's (appended, entry syncs, other syncs) for db1's group - TODO 630"""
    out = {}
    for n in nodes:
        if not n.proc:
            continue
        line = n.group_line("db1")
        vals = [int(m.group(1)) if m else 0 for m in (re.search(r"\b%s (\d+)" % k, line)
                                                     for k in ("log_appended", "log_syncs", "other_syncs"))]
        out[n.i + 1] = tuple(vals)
    return out


def sync_report(before, after, seconds, ops):
    """per node: syncs a second and entries a sync over one run, and syncs per write"""
    parts, total = [], 0.0
    per_node = {}
    for i in sorted(after):
        a0, s0, o0 = before.get(i, (0, 0, 0))
        a1, s1, o1 = after[i]
        syncs = (s1 - s0) / seconds
        total += syncs + (o1 - o0) / seconds
        per = (a1 - a0) / max(1, s1 - s0)
        per_node[i] = {"syncs": round(syncs), "entries_per_sync": round(per, 1), "other": o1 - o0}
        parts.append("n%d %5d/s %5.1f/sync%s" % (i, syncs, per, " +%d other" % (o1 - o0) if o1 != o0 else ""))
    print("          syncs: %s | %d nodes up, %d syncs/s in all, %.2f per write" % (
        ", ".join(parts), len(after), total, total / ops if ops else 0), flush=True)
    return {"nodes_up": len(after), "syncs_per_sec": round(total), "syncs_per_write": round(total / ops, 3) if ops else None,
            "per_node": per_node}


def record(scenario, op, conns, pipeline, r):
    row = dict(scenario=scenario, op=op, conns=conns, pipeline=pipeline, **r)
    results.append(row)
    fmt = lambda v: "-" if v is None else "%.3f" % v
    print("  %-7s %-4s %4d conn  pipe %-3d %10d ops/s   p50 %8s  p99 %8s  p99.9 %8s ms" % (
        scenario, op, conns, pipeline, r["ops"], fmt(r["p50"]), fmt(r["p99"]), fmt(r["p999"])), flush=True)


def run_shapes(scenario, port, seconds, nodes=None, sets_only=False):
    for conns, pipeline in SHAPES:
        before = log_counts(nodes) if nodes else None
        r = memtier(port, 1, "1:0", conns, pipeline, seconds)
        record(scenario, "SET", conns, pipeline, r)
        if nodes:
            results[-1]["syncs"] = sync_report(before, log_counts(nodes), seconds, r["ops"])
    if sets_only:
        return
    fill(port, 1)
    for conns, pipeline in SHAPES:
        record(scenario, "GET", conns, pipeline, memtier(port, 1, "0:1", conns, pipeline, seconds))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=10)
    ap.add_argument("--out", default="clusterbench.json")
    ap.add_argument("--only", default="plain,raft,mixed,raft2")
    a = ap.parse_args()
    if not shutil.which("memtier_benchmark"):
        print("SKIP: no memtier_benchmark")
        return 0
    binary = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
    if not os.path.exists(binary):
        print("SKIP: no barchd at %s" % binary)
        return 0
    wanted = set(a.only.split(","))
    out_path = os.path.abspath(a.out)
    scale.workdir()
    base = scale.port(default=27100)
    head = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
                          cwd=os.path.dirname(os.path.abspath(__file__))).stdout.strip()
    print("barchd %s, tree %s (plus working changes), %d cpus, %ds a run" % (
        binary, head, os.cpu_count(), a.seconds), flush=True)

    if "plain" in wanted:
        print("plain: one barchd, no cluster")
        n = clusternodes.Node(0, base, binary, BENCH_SETTINGS)
        try:
            n.start()
            run_shapes("plain", n.port, a.seconds)
        finally:
            n.stop()

    if wanted & {"raft", "mixed", "raft2"}:
        print("raft: three barchd, db1 replicated")
        nodes = [clusternodes.Node(i, base, binary, BENCH_SETTINGS) for i in range(3)]
        try:
            for n in nodes:
                n.start()
            nodes[0].client().execute_command("CLUSTER", "INIT")
            for n in nodes[1:]:
                n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
            nodes[0].client("configuration").execute_command("SET", "db1.raft", "on")
            if not wait_for(60, lambda: leader_of(nodes, "db1") is not None and all(
                    "members 3" in n.group_line("db1") and "learners 0" in n.group_line("db1") for n in nodes)):
                print("FAILED: db1 never had three voters and a leader")
                return 1
            time.sleep(2)
            leader = leader_of(nodes, "db1")
            print("  db1 is led by node %d" % (leader.i + 1))
            if "raft" in wanted:
                run_shapes("raft", leader.port, a.seconds, nodes)
            if "mixed" in wanted:
                print("mixed: GETs on db2 (not replicated) on the leader, alone and beside db1's writers")
                fill(leader.port, 2)
                for conns in (8, 64):
                    record("alone", "GET", conns, 1, memtier(leader.port, 2, "0:1", conns, 1, a.seconds))
                    writers = subprocess.Popen(
                        ["memtier_benchmark", "-s", "127.0.0.1", "-p", str(leader.port), "--select-db", "1",
                         "-t", "4", "-c", "16", "--ratio", "1:0", "--data-size", str(DATA),
                         "--key-maximum", str(KEYS), "--key-pattern", "R:R", "--test-time", str(a.seconds + 4),
                         "--hide-histogram"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    time.sleep(2)
                    record("beside", "GET", conns, 1, memtier(leader.port, 2, "0:1", conns, 1, a.seconds))
                    writers.wait()
            if "raft2" in wanted:
                # the same writes with one follower down: two nodes left to sync
                follower = next(n for n in nodes if n is not leader)
                print("raft2: node %d stopped, %d nodes up" % (follower.i + 1, len(nodes) - 1))
                follower.stop()
                if not wait_for(30, lambda: leader_of([n for n in nodes if n.proc], "db1") is not None):
                    print("FAILED: no leader with node %d down" % (follower.i + 1))
                    return 1
                leader = leader_of([n for n in nodes if n.proc], "db1")
                time.sleep(2)
                run_shapes("raft2", leader.port, a.seconds, nodes, sets_only=True)
        finally:
            for n in nodes:
                n.stop()

    with open(out_path, "w") as f:
        json.dump({"barchd": binary, "tree": head, "cpus": os.cpu_count(), "seconds": a.seconds,
                   "results": results}, f, indent=1)
    print("results in %s" % out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
