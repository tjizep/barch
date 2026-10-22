#!/usr/bin/env python3
"""
A warehouse: a product sits in several bins. Pickers fill a pick by walking the bins
in their own order and taking what they need, while others move stock from bin to bin.

The rules: no bin goes below zero, and the bins hold what was there less what was
picked. Without a transaction a picker and a mover can both count on the same units.

    python3 warehouse.py --port 14000
"""
import random
import sys

from txretry import (
    Refused, connect, get_for_update, in_luau, in_transaction, load_luau, options,
    report, run_clients
)

opts = options(__doc__)
SPACE = "example_warehouse"
BINS = ["bin:%s" % b for b in ("A1", "A2", "B1", "B2", "C1")]
OPENING = 60

setup = connect(opts.host, opts.port, SPACE)
if opts.luau:
    load_luau(setup, "warehouse")         # luau/warehouse.luau, run with CALLF
for b in BINS:
    setup.set(b, OPENING)
setup.delete("picked")


def pick(wanted, route):
    def work(r):
        took = []
        for b in route:
            if wanted == sum(n for _, n in took):
                break
            have = int(get_for_update(r, b))
            n = min(have, wanted - sum(n for _, n in took))
            if n:
                took.append((b, n))
                r.set(b, have - n)
        if sum(n for _, n in took) < wanted:
            raise Refused()                 # not enough in the whole warehouse
        r.incrby("picked", wanted)
        return took
    return work


def move(source, target, count):
    def work(r):
        have = int(get_for_update(r, source))
        if have < count:
            raise Refused()
        r.set(source, have - count)
        r.set(target, int(get_for_update(r, target)) + count)
        return count
    return work


def worker(i):
    r = connect(opts.host, opts.port, SPACE)
    for _ in range(30):
        if i == 0:
            source, target = random.sample(BINS, 2)
            count = random.randint(1, 10)
            if opts.luau:
                in_luau(r, "warehouse", "move", source, target, count)
            else:
                in_transaction(r, move(source, target, count))
        else:
            wanted, route = random.randint(1, 8), random.sample(BINS, len(BINS))
            if opts.luau:
                in_luau(r, "warehouse", "pick", wanted, ",".join(route))
            else:
                in_transaction(r, pick(wanted, route))


run_clients(opts.clients, worker)

bins = [int(v) for v in setup.mget(BINS)]
picked = int(setup.get("picked") or 0)
print("picked %d, bins %s" % (picked, ", ".join("%s=%d" % (b[4:], n) for b, n in zip(BINS, bins))))
sys.exit(report([
    ("no bin below zero", min(bins) >= 0),
    ("the bins hold what was there less what was picked", sum(bins) == OPENING * len(BINS) - picked),
]))
