"""
The one helper every example here uses: run a function as a transaction, and try it
again from the top when the server says another transaction got in the way.

    def transfer(r):
        ...reads and writes on r...

    in_transaction(r, transfer)

With BEGINTX PESSIMISTIC each key is locked as it's read or written, and held until
COMMITTX. When two transactions would wait on each other forever the server answers
one of them TXDEADLOCK straight away; a wait that takes too long answers TXLOCKTIMEOUT.
Either way that transaction can only be ended, so this aborts it and runs it again,
after a short random pause that doubles each time. That's all an application needs.

Read a key you're going to write with `get_for_update` (GETFORUPDATE), not a plain
GET. A GET takes a shared lock, so two transactions that both read a balance and then
write it each wait for the other's read lock to go, and one gets TXDEADLOCK. A read
for update takes the write lock straight away, so the second just waits its turn.

Raise `Refused` from the function to give up on purpose (not enough money, the seat's
taken): the transaction is aborted and in_transaction returns None.

With `--luau` an example runs its job as the Luau stored functions in luau/ instead,
the whole transaction inside the server: `load_luau` installs them with SETF, and
`in_luau` calls one with CALLF, trying again after a pause when it says TXRETRY.

Every number the examples keep (balances, stock, counters) is a 64 bit integer, stored
as the 8 bytes Luau's setInt64At writes: little endian, signed. `int64(n)` makes those
bytes to SET, `whole(v)` reads them back, and `add` is INCRBY for them. Records like
a journal entry stay text.
"""
import os
import random
import struct
import time

import redis

RETRY_ON = ("TXDEADLOCK", "TXLOCKTIMEOUT", "TXCONFLICT")
retries = [0]       # how many times any transaction was run again, for the report


class Refused(Exception):
    """the job said no, so nothing it wrote should stay"""


def options(about):
    """--host, --port and --clients, the same for every example"""
    import argparse
    a = argparse.ArgumentParser(description=about)
    a.add_argument("--host", default="127.0.0.1")
    a.add_argument("--port", type=int, default=14000)
    a.add_argument("--clients", type=int, default=6)
    a.add_argument("--luau", action="store_true", help="run the job as the stored functions in luau/")
    return a.parse_args()


def connect(host, port, space):
    # one connection per client, so a transaction stays on it. Replies are decoded so
    # text reads as str, and surrogateescape lets an int64's bytes through unharmed,
    # for whole() to turn back into bytes
    r = redis.Redis(host=host, port=port, decode_responses=True, encoding_errors="surrogateescape",
                    single_connection_client=True)
    r.execute_command("USE", space)
    return r


def in_transaction(r, work, pessimistic=True, tries=200):
    """`work(r)` as one transaction: its result once committed, None if it was Refused"""
    for attempt in range(tries):
        r.execute_command("BEGINTX", "PESSIMISTIC") if pessimistic else r.execute_command("BEGINTX")
        try:
            result = work(r)
            if r.execute_command("COMMITTX") in ("OK", True):
                return result
        except Refused:
            r.execute_command("ABORTTX")
            return None
        except redis.ResponseError as e:
            end_it(r)
            if not str(e).startswith(RETRY_ON):
                raise
            retries[0] += 1
        time.sleep(random.random() * min(0.1, 0.001 * 2 ** attempt))
    raise RuntimeError("gave up after %d tries" % tries)


def load_luau(r, name):
    """install luau/<name>.luau, and the txretry module it uses, as stored functions"""
    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "luau")
    for f in ("txretry", name):
        with open(os.path.join(here, f + ".luau")) as source:
            r.execute_command("SETF", f, source.read())


def in_luau(r, function, *args, tries=200):
    """
    CALLF a function that runs its own transaction: what it returns, None if it
    refused. A stored function can't sleep, so when it gives up (TXRETRY, or a
    deadlock or timeout it let through) this waits, doubling each time, and calls again.
    """
    for attempt in range(tries):
        try:
            return r.execute_command("CALLF", function, *args)
        except redis.ResponseError as e:
            end_it(r)                   # in case it stopped with its transaction open
            if not any(word in str(e) for word in RETRY_ON + ("TXRETRY",)):
                raise
            retries[0] += 1
        time.sleep(random.random() * min(0.1, 0.001 * 2 ** attempt))
    raise RuntimeError("gave up after %d tries" % tries)


def get_for_update(r, key):
    """GET that locks the key for writing at once, in a pessimistic transaction"""
    return r.execute_command("GETFORUPDATE", key)


def mget_for_update(r, keys):
    """MGET that locks every key for writing at once"""
    return r.execute_command("MGETFORUPDATE", *keys)


def int64(n):
    """n as a key's value: the 8 bytes setInt64At writes"""
    return struct.pack("<q", n)


def whole(v, default=None):
    """a value written by int64() or setInt64At as an int, None as `default`"""
    if v is None:
        return default
    return struct.unpack("<q", v.encode("utf-8", "surrogateescape"))[0]


def add(r, key, by):
    """`by` added to the int64 at `key` (nothing there is 0), and the new number"""
    n = whole(get_for_update(r, key), 0) + by
    r.set(key, int64(n))
    return n


def end_it(r):
    """ABORTTX, unless COMMITTX ended the transaction already by answering the error"""
    try:
        r.execute_command("ABORTTX")
    except redis.ResponseError:
        pass


def money(cents):
    """cents as 1,234.56, for printing only: amounts are always kept in whole cents"""
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    return "%s%s.%02d" % (sign, format(cents // 100, ","), cents % 100)


def run_clients(n, client_work, watcher=None):
    """
    `client_work(i)` on `n` threads at once, each its own client. `watcher(done)`, if
    given, runs alongside until they've all finished, when `done` is set.
    """
    import threading
    done = threading.Event()
    threads = [threading.Thread(target=client_work, args=(i,)) for i in range(n)]
    watching = threading.Thread(target=watcher, args=(done,)) if watcher else None
    for t in threads + ([watching] if watching else []):
        t.start()
    for t in threads:
        t.join()
    done.set()
    if watching:
        watching.join()


def report(rules):
    """print each (rule, held) pair; the exit code for a script"""
    print("  %-60s %d" % ("transactions run again", retries[0]))
    for rule, held in rules:
        print("  %-60s %s" % (rule, "yes" if held else "NO"))
    return 0 if all(held for _, held in rules) else 1
