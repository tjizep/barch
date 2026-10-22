#!/usr/bin/env python3
"""
Workloads that need serializable transactions, on BEGINTX PESSIMISTIC - TODO 725.

Each one is a small version of a real job where two clients interleaving the wrong way
loses money, sells a seat twice or leaves a hospital with nobody on call. Many clients
run it at once, each doing its part in a pessimistic transaction, and retry from the
top when the server answers TXDEADLOCK or TXLOCKTIMEOUT, the way an application would.
Afterwards each job's invariants are checked against what's stored.

  ledger     double entry transfers between accounts, an auditor summing every
             balance while they run, and a gapless journal to replay
  auction    bids that must beat the top one, with the bidder's money held back and
             the outbid bidder refunded
  cinema     blocks of adjacent seats booked and cancelled, never one seat twice
  orders     orders of several lines taken from stock, restocked as they go, priced
             with tax and charged to the customer's credit, each with the next
             invoice number, no gaps
  warehouse  picks taken from several bins of a product, while stock is moved
             between bins
  on call    doctors going off call only while another stays on (write skew,
             which snapshot isolation alone lets through)

Money is whole cents throughout, stored and worked out as integers, never floats, so
every check is exact: a lost cent fails it. Tax is rounded in integer arithmetic too.

The invariants are the point. Interleaved without a transaction, the same code breaks
them, which `--mode none` shows (and `--mode optimistic` runs them on plain BEGINTX).
"""
import argparse
import os
import random
import re
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

parser = argparse.ArgumentParser()
parser.add_argument("--mode", choices=["pessimistic", "optimistic", "none"], default="pessimistic")
parser.add_argument("--forupdate", action="store_true",
                    help="read the keys a transaction writes with GETFORUPDATE (TODO 729)")
parser.add_argument("--clients", type=int, default=8)
parser.add_argument("--ops", type=int, default=60, help="transactions per client per workload")
parser.add_argument("--only", default="", help="comma separated workloads to run")
parser.add_argument("--seed", type=int, default=int(os.environ.get("TXWORKLOAD_SEED", "0")) or None)
opts = parser.parse_args()
SEED = opts.seed or random.randrange(1, 1 << 30)

scale.workdir()
PORT = scale.port(default=14816)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-74s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


WAIT_MS = 2000
DATA = os.path.join(os.getcwd(), "txworkload_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txworkload.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000", "-c", "tx_lock_wait_ms=%d" % WAIT_MS],
                     stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

STAMP = str(int(time.time()))


def client(space):
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.response_callbacks.pop("INFO", None)
    c.execute_command("USE", space)
    return c


class GiveUp(Exception):
    """the job itself says no: not enough money, stock or free seats"""


class Tally:
    def __init__(self):
        self.m = threading.Lock()
        self.n = {"committed": 0, "gave up": 0, "deadlock": 0, "lock timeout": 0, "conflict": 0, "other": 0}
        self.others = []
        self.most = 0       # the most tries one transaction took

    def tried(self, n):
        with self.m:
            self.most = max(self.most, n)

    def add(self, what, detail=None):
        with self.m:
            self.n[what] += 1
            if detail and len(self.others) < 5:
                self.others.append(detail)

    def said(self):
        return ", ".join("%s %d" % (k, v) for k, v in self.n.items() if v) + ", at most %d tries" % self.most


def kind(msg):
    if "TXDEADLOCK" in msg:
        return "deadlock"
    if "TXLOCKTIMEOUT" in msg:
        return "lock timeout"
    if "TXCONFLICT" in msg:
        return "conflict"
    return "other"


def backoff(rnd, attempt):
    """a random wait that doubles with each try, up to 100 ms, as a client would retry"""
    time.sleep(rnd.random() * min(0.1, 0.001 * (2 ** min(attempt, 10))))


def transact(c, body, tally, rnd, tries=500):
    """
    `body(c)` as one transaction, retried from the top on a deadlock, a lock timeout or
    a conflict. Its return value once committed; None when the job gave up.
    """
    for attempt in range(tries):
        if opts.mode == "none":
            try:
                out = body(c)
                tally.add("committed")
                return out
            except GiveUp:
                tally.add("gave up")
                return None
        c.execute_command("BEGINTX", "PESSIMISTIC") if opts.mode == "pessimistic" else c.execute_command("BEGINTX")
        try:
            out = body(c)
        except GiveUp:
            c.execute_command("ABORTTX")
            tally.add("gave up")
            return None
        except redis.ResponseError as e:
            try:
                c.execute_command("ABORTTX")
            except redis.ResponseError:
                pass
            k = kind(str(e))
            tally.add(k, str(e)[:100] if k == "other" else None)
            backoff(rnd, attempt)
            continue
        try:
            r = c.execute_command("COMMITTX")
        except redis.ResponseError as e:
            r = str(e)
        if r == "OK" or r is True:
            tally.add("committed")
            tally.tried(attempt + 1)
            return out
        k = kind(str(r))
        tally.add(k, str(r)[:100] if k == "other" else None)
        backoff(rnd, attempt)
    raise RuntimeError("no commit in %d tries" % tries)


def rw(c, key):
    """a read of a key the transaction writes next: GETFORUPDATE with --forupdate"""
    return c.execute_command("GETFORUPDATE", key) if opts.forupdate else c.get(key)


def mrw(c, keys):
    return c.execute_command("MGETFORUPDATE", *keys) if opts.forupdate else c.mget(keys)


def num(v, default=0):
    return default if v is None else int(v)


def run_clients(space, n, work):
    """`work(c, i, rnd, tally)` on `n` clients at once; the tally of their transactions"""
    tally = Tally()
    errors = []

    def go(i):
        rnd = random.Random(SEED * 1000 + i)
        c = client(space)
        try:
            work(c, i, rnd, tally)
        except Exception as e:      # reported, not raised: the checks say what broke
            errors.append("%s: %s" % (type(e).__name__, e))
        finally:
            c.close()
    threads = [threading.Thread(target=go, args=(i,)) for i in range(n)]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    took = time.time() - t0
    print("    %s in %.1f s" % (tally.said(), took), flush=True)
    for o in tally.others:
        print("    other error: %s" % o)
    return tally, errors


def finished(tally, errors, gave_up_ok=True):
    check(not errors, "every client finished (%s)" % (errors[:2] or "ok"))
    check(tally.n["other"] == 0, "no unexpected errors (%d)" % tally.n["other"])
    if opts.mode == "pessimistic":
        check(tally.n["conflict"] == 0, "no COMMITTX conflicts, which locks should rule out (%d)" % tally.n["conflict"])


# ---------------------------------------------------------------- ledger

def ledger():
    print("ledger: double entry transfers, an auditor, a journal to replay")
    space = "ledger_" + STAMP
    accounts, opening = 12, 100_000      # cents: 1,000.00 each
    total = accounts * opening
    setup = client(space)
    for a in range(accounts):
        setup.set("acct:%d" % a, opening)
    names = ["acct:%d" % a for a in range(accounts)]
    seen = []          # totals the auditor saw
    stop = threading.Event()

    def transfer(c, src, dst, amount):
        def body(c):
            have = num(rw(c, src))
            if have < amount:
                raise GiveUp()
            there = num(rw(c, dst))
            c.set(src, have - amount)
            c.set(dst, there + amount)
            n = c.incr("journal:next")
            # both legs of the entry: what left one account and arrived in the other
            c.set("journal:%d" % n, "%s,%s,%d" % (src, dst, amount))
            return n
        return body

    def work(c, i, rnd, tally):
        if i == 0:
            # the auditor: every balance at once, which must always add up
            while not stop.is_set():
                def audit(c):
                    return sum(num(v) for v in c.mget(names))
                s = transact(c, audit, tally, rnd)
                if s is not None:
                    seen.append(s)
            return
        for _ in range(opts.ops):
            src, dst = rnd.sample(names, 2)
            transact(c, transfer(c, src, dst, rnd.randint(1, 30_000)), tally, rnd)     # up to 300.00

    def workers(c, i, rnd, tally):
        if i == 0:
            work(c, i, rnd, tally)
            return
        try:
            work(c, i, rnd, tally)
        finally:
            done[i] = True
            if all(done[1:]):
                stop.set()
    done = [False] * opts.clients
    tally, errors = run_clients(space, opts.clients, workers)
    finished(tally, errors)
    final = [num(v) for v in setup.mget(names)]
    check(sum(final) == total, "money is neither made nor lost (%d of %d)" % (sum(final), total))
    check(min(final) >= 0, "no account overdrawn (lowest %d)" % min(final))
    check(len(seen) > 0 and all(s == total for s in seen),
          "the auditor always saw %d (%d audits, %s)" % (total, len(seen), sorted(set(seen))[:4]))
    n = num(setup.get("journal:next"))
    entries = [setup.get("journal:%d" % k) for k in range(1, n + 1)]
    check(all(entries) and setup.get("journal:%d" % (n + 1)) is None,
          "the journal has entries 1 to %d, no gaps" % n)
    replay = {a: opening for a in names}
    lowest = opening
    for e in entries:
        if e:
            src, dst, amount = e.split(",")
            replay[src] -= int(amount)
            replay[dst] += int(amount)
            lowest = min(lowest, replay[src])
    check([replay[a] for a in names] == final, "replaying the journal gives every balance")
    check(lowest >= 0, "and no entry in it overdraws an account (lowest %d)" % lowest)


# ---------------------------------------------------------------- auction

def auction():
    print("auction: each bid beats the top one, money held back, the outbid refunded")
    space = "auction_" + STAMP
    items, wallet, reserve = 4, 500_000, 10_000      # cents: 5,000.00 to spend, bids from 100.00
    bidders = opts.clients
    setup = client(space)
    for b in range(bidders):
        setup.set("wallet:%d" % b, wallet)

    def bid(me, item, raise_by):
        def body(c):
            top = rw(c, "item:%d:top" % item)
            who, amount = (top.split(",") if top else (None, reserve))
            amount = int(amount)
            if who == str(me):
                raise GiveUp()
            offer = amount + raise_by
            mine = num(rw(c, "wallet:%d" % me))
            if mine < offer:
                raise GiveUp()
            c.set("wallet:%d" % me, mine - offer)
            if who is not None:
                # the one outbid gets their money back
                theirs = num(rw(c, "wallet:%s" % who))
                c.set("wallet:%s" % who, theirs + amount)
            c.set("item:%d:top" % item, "%d,%d" % (me, offer))
            n = c.incr("item:%d:bids" % item)
            c.set("item:%d:bid:%d" % (item, n), "%d,%d" % (me, offer))
            return offer
        return body

    def work(c, i, rnd, tally):
        for _ in range(opts.ops):
            transact(c, bid(i, rnd.randrange(items), rnd.randint(1, 4_000)), tally, rnd)   # raise by up to 40.00

    tally, errors = run_clients(space, bidders, work)
    finished(tally, errors)
    wallets = [num(setup.get("wallet:%d" % b)) for b in range(bidders)]
    held = 0
    rising, logged = True, True
    for item in range(items):
        top = setup.get("item:%d:top" % item)
        n = num(setup.get("item:%d:bids" % item))
        log_ = [setup.get("item:%d:bid:%d" % (item, k)) for k in range(1, n + 1)]
        amounts = [int(e.split(",")[1]) for e in log_ if e]
        if top:
            held += int(top.split(",")[1])
        if any(b <= a for a, b in zip(amounts, amounts[1:])):
            rising = False
        if (top or None) != (log_[-1] if log_ else None) or len(amounts) != n:
            logged = False
    check(sum(wallets) + held == bidders * wallet,
          "wallets and top bids add up to what bidders brought (%d of %d)" % (sum(wallets) + held, bidders * wallet))
    check(min(wallets) >= 0, "no wallet below nothing (lowest %d)" % min(wallets))
    check(rising, "every item's bids rose, each beating the one before")
    check(logged, "each item's top bid is the last one logged, with no gaps")


# ---------------------------------------------------------------- cinema

def cinema():
    print("cinema: adjacent seats booked and cancelled, never one seat twice")
    space = "cinema_" + STAMP
    shows, rows, per_row = 3, 6, 10
    setup = client(space)

    def seat(show, row, s):
        return "seat:%d:%d:%d" % (show, row, s)

    def book(me, show, row, first, k):
        seats = [seat(show, row, first + j) for j in range(k)]

        def body(c):
            if any(v is not None for v in mrw(c, seats)):
                raise GiveUp()
            n = c.incr("booking:next")
            for s in seats:
                c.set(s, n)
            c.set("booking:%d" % n, "%d,%d,%d,%d,%d" % (me, show, row, first, k))
            c.incrby("sold:%d" % show, k)
            return n
        return body

    def cancel(n):
        def body(c):
            b = rw(c, "booking:%d" % n)
            if not b or b == "cancelled":
                raise GiveUp()
            _, show, row, first, k = map(int, b.split(","))
            seats = [seat(show, row, first + j) for j in range(k)]
            if c.mget(seats) != [str(n)] * k:
                raise RuntimeError("booking %d lost a seat to another" % n)
            c.delete(*seats)
            c.set("booking:%d" % n, "cancelled")
            c.decrby("sold:%d" % show, k)
            return n
        return body

    def work(c, i, rnd, tally):
        mine = []
        for _ in range(opts.ops):
            if mine and rnd.random() < 0.2:
                transact(c, cancel(mine.pop(rnd.randrange(len(mine)))), tally, rnd)
                continue
            k = rnd.randint(1, 4)
            n = transact(c, book(i, rnd.randrange(shows), rnd.randrange(rows), rnd.randint(0, per_row - k), k),
                         tally, rnd)
            if n is not None:
                mine.append(n)

    tally, errors = run_clients(space, opts.clients, work)
    finished(tally, errors)
    owner = {}
    for show in range(shows):
        for row in range(rows):
            for s in range(per_row):
                v = setup.get(seat(show, row, s))
                if v is not None:
                    owner[(show, row, s)] = int(v)
    n = num(setup.get("booking:next"))
    whole, live_seats = True, {}
    for b in range(1, n + 1):
        rec = setup.get("booking:%d" % b)
        if rec is None:
            whole = False
            continue
        if rec == "cancelled":
            continue
        _, show, row, first, k = map(int, rec.split(","))
        for j in range(k):
            if (show, row, first + j) in live_seats:
                whole = False
            live_seats[(show, row, first + j)] = b
    check(whole, "every booking is there, none sharing a seat (%d made)" % n)
    check(owner == live_seats, "each taken seat names the booking that holds it, and only those are taken")
    sold = [num(setup.get("sold:%d" % show)) for show in range(shows)]
    counted = [sum(1 for (sh, _, _) in owner if sh == show) for show in range(shows)]
    check(sold == counted, "seats sold per show match the seats taken (%s, %s)" % (sold, counted))


# ---------------------------------------------------------------- orders

def orders():
    print("orders: stock, prices and tax in cents, customer credit, gapless invoices")
    space = "orders_" + STAMP
    products, opening = 10, 150
    credit = 2_000_000          # cents each customer may spend: 20,000.00
    tax_pct = 15
    setup = client(space)
    prices = [199 + 1_000 * q + 37 * q * q for q in range(products)]     # cents, like 1.99 and 10.36
    for q in range(products):
        setup.set("stock:%d" % q, opening)
        setup.set("price:%d" % q, prices[q])
    for i in range(opts.clients):
        setup.set("credit:%d" % i, credit)

    def tax_of(subtotal):
        # tax_pct of the subtotal in cents, half a cent rounded up, in integers only
        return (subtotal * tax_pct + 50) // 100

    def order(me, lines):
        def body(c):
            have = [num(rw(c, "stock:%d" % q)) for q, _ in lines]
            if any(h < n for h, (_, n) in zip(have, lines)):
                raise GiveUp()
            unit = [num(c.get("price:%d" % q)) for q, _ in lines]
            subtotal = sum(u * n for u, (_, n) in zip(unit, lines))
            total = subtotal + tax_of(subtotal)
            left = num(rw(c, "credit:%d" % me))
            if left < total:
                raise GiveUp()
            for h, (q, n) in zip(have, lines):
                c.set("stock:%d" % q, h - n)
                c.incrby("sold:%d" % q, n)
            c.set("credit:%d" % me, left - total)
            c.incrby("revenue", total)
            inv = num(rw(c, "invoice:next")) + 1
            c.set("invoice:next", inv)
            # customer|product:quantity:unit cents;...|subtotal|tax|total
            c.set("invoice:%d" % inv, "%d|%s|%d|%d|%d" % (
                me, ";".join("%d:%d:%d" % (q, n, u) for u, (q, n) in zip(unit, lines)),
                subtotal, total - subtotal, total))
            return inv
        return body

    def restock(q, n):
        def body(c):
            c.incrby("received:%d" % q, n)
            c.set("stock:%d" % q, num(rw(c, "stock:%d" % q)) + n)
            return n
        return body

    def work(c, i, rnd, tally):
        for _ in range(opts.ops):
            if i == 0:
                transact(c, restock(rnd.randrange(products), rnd.randint(5, 20)), tally, rnd)
                continue
            picked = rnd.sample(range(products), rnd.randint(1, 4))
            transact(c, order(i, [(q, rnd.randint(1, 5)) for q in picked]), tally, rnd)

    tally, errors = run_clients(space, opts.clients, work)
    finished(tally, errors)
    n = num(setup.get("invoice:next"))
    invoices = [setup.get("invoice:%d" % k) for k in range(1, n + 1)]
    check(all(invoices) and setup.get("invoice:%d" % (n + 1)) is None, "invoices 1 to %d, no gaps or doubles" % n)
    sold = [0] * products
    spent = [0] * opts.clients
    sums_right, billed = True, 0
    for inv in invoices:
        if not inv:
            continue
        who, lines, subtotal, tax, total = inv.split("|")
        worked = 0
        for line in lines.split(";"):
            q, k, unit = map(int, line.split(":"))
            sold[q] += k
            worked += k * unit
            if unit != prices[q]:
                sums_right = False
        if worked != int(subtotal) or int(tax) != tax_of(worked) or int(total) != worked + int(tax):
            sums_right = False
        spent[int(who)] += int(total)
        billed += int(total)
    credits = [num(setup.get("credit:%d" % i)) for i in range(opts.clients)]
    revenue = num(setup.get("revenue"))
    check(sums_right, "each invoice's lines, tax and total add up to the cent")
    check(revenue == billed, "revenue is what the invoices billed (%d cents)" % billed)
    check(all(credits[i] == credit - spent[i] for i in range(opts.clients)) and min(credits) >= 0,
          "each customer's credit is what they had less what they were billed")
    stock = [num(setup.get("stock:%d" % q)) for q in range(products)]
    received = [num(setup.get("received:%d" % q)) for q in range(products)]
    counted = [num(setup.get("sold:%d" % q)) for q in range(products)]
    check(min(stock) >= 0, "no stock below nothing (lowest %d)" % min(stock))
    check(counted == sold, "sold per product matches the invoices")
    check(all(stock[q] == opening + received[q] - sold[q] for q in range(products)),
          "stock is what was there, plus received, less sold")


# ---------------------------------------------------------------- warehouse

def warehouse():
    print("warehouse: picks from several bins, stock moved between bins")
    space = "warehouse_" + STAMP
    products, bins, opening = 3, 4, 200
    setup = client(space)
    for q in range(products):
        for b in range(bins):
            setup.set("bin:%d:%d" % (q, b), opening)

    def pick(q, want, route):
        def body(c):
            took, left = [], want
            for b in route:
                if left == 0:
                    break
                have = num(rw(c, "bin:%d:%d" % (q, b)))
                n = min(have, left)
                if n:
                    took.append((b, n, have))
                    left -= n
            if left:
                raise GiveUp()
            for b, n, have in took:
                c.set("bin:%d:%d" % (q, b), have - n)
            k = c.incr("pick:next")
            c.set("pick:%d" % k, "%d|%s" % (q, ";".join("%d:%d" % (b, n) for b, n, _ in took)))
            return k
        return body

    def move(q, src, dst, n):
        def body(c):
            have = num(rw(c, "bin:%d:%d" % (q, src)))
            if have < n:
                raise GiveUp()
            there = num(rw(c, "bin:%d:%d" % (q, dst)))
            c.set("bin:%d:%d" % (q, src), have - n)
            c.set("bin:%d:%d" % (q, dst), there + n)
            return n
        return body

    def work(c, i, rnd, tally):
        for _ in range(opts.ops):
            q = rnd.randrange(products)
            if i < 2:
                src, dst = rnd.sample(range(bins), 2)
                transact(c, move(q, src, dst, rnd.randint(1, 10)), tally, rnd)
            else:
                route = list(range(bins))
                rnd.shuffle(route)          # each picker walks the aisle their own way
                transact(c, pick(q, rnd.randint(1, 12), route), tally, rnd)

    tally, errors = run_clients(space, opts.clients, work)
    finished(tally, errors)
    picked = [0] * products
    n = num(setup.get("pick:next"))
    whole = True
    for k in range(1, n + 1):
        rec = setup.get("pick:%d" % k)
        if not rec:
            whole = False
            continue
        q, lines = rec.split("|")
        for line in lines.split(";"):
            picked[int(q)] += int(line.split(":")[1])
    binq = [[num(setup.get("bin:%d:%d" % (q, b))) for b in range(bins)] for q in range(products)]
    check(whole, "picks 1 to %d recorded, no gaps" % n)
    check(min(min(r) for r in binq) >= 0, "no bin below nothing")
    check(all(sum(binq[q]) == bins * opening - picked[q] for q in range(products)),
          "each product's bins hold what was there less what was picked (%s, %s)"
          % ([sum(r) for r in binq], picked))


# ---------------------------------------------------------------- on call

def on_call():
    print("on call: a doctor goes off only while another stays on")
    space = "oncall_" + STAMP
    shifts, doctors = 3, 3
    setup = client(space)
    for s in range(shifts):
        for d in range(doctors):
            setup.set("oncall:%d:%d" % (s, d), 1)
    lows = []
    stop = threading.Event()

    def names(s):
        return ["oncall:%d:%d" % (s, d) for d in range(doctors)]

    def go_off(s, d):
        def body(c):
            on = [num(v) for v in mrw(c, names(s))]
            if not on[d] or sum(on) < 2:
                raise GiveUp()
            c.set("oncall:%d:%d" % (s, d), 0)
            return True
        return body

    def go_on(s, d):
        def body(c):
            c.set("oncall:%d:%d" % (s, d), 1)
            return True
        return body

    def work(c, i, rnd, tally):
        if i == 0:
            while not stop.is_set():
                for s in range(shifts):
                    n = transact(c, lambda c, s=s: sum(num(v) for v in c.mget(names(s))), tally, rnd)
                    if n is not None:
                        lows.append(n)
            return
        try:
            for _ in range(opts.ops):
                s, d = rnd.randrange(shifts), rnd.randrange(doctors)
                transact(c, go_off(s, d) if rnd.random() < 0.6 else go_on(s, d), tally, rnd)
        finally:
            done[i] = True
            if all(done[1:]):
                stop.set()
    done = [False] * opts.clients
    tally, errors = run_clients(space, opts.clients, work)
    finished(tally, errors)
    final = [sum(num(v) for v in setup.mget(names(s))) for s in range(shifts)]
    check(min(final) >= 1, "every shift ends with a doctor on call (%s)" % final)
    check(lows and min(lows) >= 1, "and the watcher never saw one without (%d looks, lowest %s)"
          % (len(lows), min(lows) if lows else None))


WORKLOADS = {"ledger": ledger, "auction": auction, "cinema": cinema, "orders": orders,
             "warehouse": warehouse, "oncall": on_call}

try:
    print("mode %s%s, %d clients, %d transactions each, seed %d (TXWORKLOAD_SEED to repeat)"
          % (opts.mode, ", reads for update" if opts.forupdate else "", opts.clients, opts.ops, SEED))
    chosen = [w for w in opts.only.split(",") if w] or list(WORKLOADS)
    for name in chosen:
        WORKLOADS[name]()
    info = client("txcheck_" + STAMP).execute_command("INFO", "transactions")
    m = re.search(r"tx_deadlocks:(\d+)", info)
    print("tx_deadlocks %s" % (m.group(1) if m else "?"))
    check(not [l for l in info.splitlines() if re.match(r"tx\d+:", l)], "no transaction left open")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
