#!/usr/bin/env python3
"""
Double entry bookkeeping: money moves between accounts, and an auditor adds up every
balance while it does. Every transfer is also written to a journal with no gaps, so
the books can be replayed.

The rules: money is never made or lost, no account goes below zero, and the auditor
always sees the same total. Without a transaction two transfers from one account can
both read the old balance, and the auditor can catch a transfer half done.

    python3 ledger.py --port 14000
"""
import random
import sys
import time

from txretry import (
    Refused, connect, get_for_update, in_luau, in_transaction, load_luau, money, options,
    report, run_clients
)

opts = options(__doc__)
SPACE = "example_ledger"
ACCOUNTS = ["acct:%d" % n for n in range(8)]
OPENING = 100_000                       # cents: 1,000.00 in each account
TOTAL = OPENING * len(ACCOUNTS)

setup = connect(opts.host, opts.port, SPACE)
if opts.luau:
    load_luau(setup, "ledger")         # luau/ledger.luau, run with CALLF
for a in ACCOUNTS:
    setup.set(a, OPENING)
setup.delete("journal:next")


def transfer(source, target, cents):
    def work(r):
        # both read for update, since both are written next, and locked in the same
        # order by every transfer, so A to B and B to A can't each hold one
        both = sorted([source, target])
        have, there = (int(get_for_update(r, a)) for a in both)
        if both[0] != source:
            have, there = there, have
        if have < cents:
            raise Refused()             # no overdrafts
        r.set(source, have - cents)
        r.set(target, there + cents)
        # both legs in one journal entry, numbered with no gaps
        entry = r.incr("journal:next")
        r.set("journal:%d" % entry, "%s %s %d" % (source, target, cents))
        return entry
    return work


def audit(r):
    return sum(int(v) for v in r.mget(ACCOUNTS))


totals_seen = []


def teller(i):
    r = connect(opts.host, opts.port, SPACE)
    for _ in range(40):
        source, target = random.sample(ACCOUNTS, 2)
        cents = random.randint(1, 25_000)       # up to 250.00
        if opts.luau:
            in_luau(r, "ledger", "transfer", source, target, cents)
        else:
            in_transaction(r, transfer(source, target, cents))


def auditor(done):
    r = connect(opts.host, opts.port, SPACE)
    while not done.is_set():
        totals_seen.append(in_luau(r, "ledger", "audit", *ACCOUNTS) if opts.luau else in_transaction(r, audit))
        time.sleep(0.01)    # an audit reads every account: back to back, it'd keep the tellers out


run_clients(opts.clients, teller, auditor)

balances = [int(v) for v in setup.mget(ACCOUNTS)]
entries = int(setup.get("journal:next") or 0)
replayed = {a: OPENING for a in ACCOUNTS}
for n in range(1, entries + 1):
    source, target, cents = setup.get("journal:%d" % n).split()
    replayed[source] -= int(cents)
    replayed[target] += int(cents)

print("%d transfers, balances %s" % (entries, ", ".join(money(b) for b in balances)))
sys.exit(report([
    ("money is neither made nor lost (%s)" % money(sum(balances)), sum(balances) == TOTAL),
    ("no account below zero", min(balances) >= 0),
    ("the auditor saw %s every time (%d audits)" % (money(TOTAL), len(totals_seen)),
     all(t == TOTAL for t in totals_seen)),
    ("replaying the journal gives every balance", [replayed[a] for a in ACCOUNTS] == balances),
]))
