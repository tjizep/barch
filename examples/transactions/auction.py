#!/usr/bin/env python3
"""
An auction: a bid has to beat the top bid on an item. The bidder's money is held back
from their wallet, and whoever they outbid gets theirs back.

The rules: every item's bids go up, and wallets plus the money held for top bids is
always what the bidders started with. Without a transaction two bidders can both beat
the same old top bid, and one refund goes missing.

    python3 auction.py --port 14000
"""
import random
import sys

from txretry import (
    Refused, connect, get_for_update, in_luau, in_transaction, load_luau, money, options,
    report, run_clients
)

opts = options(__doc__)
SPACE = "example_auction"
ITEMS = 3
WALLET = 300_000                        # cents: 3,000.00 each bidder brings
RESERVE = 5_000                         # cents: no bid under 50.00

setup = connect(opts.host, opts.port, SPACE)
if opts.luau:
    load_luau(setup, "auction")         # luau/auction.luau, run with CALLF
for b in range(opts.clients):
    setup.set("wallet:%d" % b, WALLET)
for i in range(ITEMS):
    setup.delete("item:%d:top" % i, "item:%d:bids" % i)


def bid(me, item, raise_by):
    def work(r):
        top = get_for_update(r, "item:%d:top" % item)    # "bidder cents", or none yet
        leader, best = top.split() if top else (None, RESERVE)
        if leader == str(me):
            raise Refused()                              # already winning
        offer = int(best) + raise_by
        wallet = int(get_for_update(r, "wallet:%d" % me))
        if wallet < offer:
            raise Refused()
        r.set("wallet:%d" % me, wallet - offer)
        if leader is not None:
            r.set("wallet:%s" % leader, int(get_for_update(r, "wallet:%s" % leader)) + int(best))
        r.set("item:%d:top" % item, "%d %d" % (me, offer))
        n = r.incr("item:%d:bids" % item)
        r.set("item:%d:bid:%d" % (item, n), offer)
        return offer
    return work


def bidder(me):
    r = connect(opts.host, opts.port, SPACE)
    for _ in range(40):
        item, raise_by = random.randrange(ITEMS), random.randint(100, 2_500)
        if opts.luau:
            in_luau(r, "auction", "bid", me, item, raise_by, RESERVE)
        else:
            in_transaction(r, bid(me, item, raise_by))


run_clients(opts.clients, bidder)

held, rising = 0, True
for i in range(ITEMS):
    top = setup.get("item:%d:top" % i)
    if top:
        leader, best = top.split()
        held += int(best)
        print("item %d goes to bidder %s for %s" % (i, leader, money(int(best))))
    bids = [int(setup.get("item:%d:bid:%d" % (i, n))) for n in range(1, int(setup.get("item:%d:bids" % i) or 0) + 1)]
    rising = rising and all(a < b for a, b in zip(bids, bids[1:]))
wallets = [int(setup.get("wallet:%d" % b)) for b in range(opts.clients)]
sys.exit(report([
    ("every item's bids went up", rising),
    ("wallets plus held money is what bidders brought (%s)" % money(sum(wallets) + held),
     sum(wallets) + held == WALLET * opts.clients),
    ("no wallet below zero", min(wallets) >= 0),
]))
