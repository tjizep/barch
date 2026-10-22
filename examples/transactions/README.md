# Transactions

Small, runnable examples of jobs that need serializable transactions, done with
`BEGINTX PESSIMISTIC`. Each one starts a handful of clients doing the job at once,
then checks the rules the job has to keep, and says whether they held.

| Example | The job | What must hold |
|---|---|---|
| [ledger.py](ledger.py) | double entry transfers between accounts, an auditor adding up every balance, a journal with no gaps | money is never made or lost, no account below zero, the auditor always sees the same total, the journal replays to every balance |
| [auction.py](auction.py) | bids that must beat the top bid, money held back from the bidder, the outbid bidder refunded | every item's bids go up, wallets plus held money is what bidders brought |
| [cinema.py](cinema.py) | blocks of seats side by side booked and cancelled | no seat sold twice |
| [orders.py](orders.py) | orders taken from stock, 15% tax, charged to the customer's credit, numbered invoices | invoice numbers have no gaps, each invoice adds up to the cent, stock never below zero |
| [warehouse.py](warehouse.py) | picks taken from several bins in each picker's own order, stock moved between bins | no bin below zero, the bins hold what was there less what was picked |
| [oncall.py](oncall.py) | doctors going off call only while someone else stays on | a shift always has a doctor on call (the "write skew" case) |

Money is always whole cents, kept and worked out as integers, never floats. The
examples only turn cents into `1,234.56` when they print.

## Running them

Start a barchd, and install redis-py:

```bash
./cmake-build-release/barchd --port 14000
```

```bash
pip install redis
```

Then run any of them from this directory:

```bash
python3 ledger.py --port 14000
```

```
229 transfers, balances 315.86, 1,841.04, 434.41, 203.36, 430.38, 483.70, 2,515.96, 1,775.29
  transactions run again                                       0
  money is neither made nor lost (8,000.00)                    yes
  no account below zero                                        yes
  the auditor saw 8,000.00 every time (6 audits)               yes
  replaying the journal gives every balance                    yes
```

`--clients` sets how many clients run at once (6 by default), `--host` the server, and
`--luau` runs the job as Luau stored functions instead (see below), with the same
checks.
Each example uses its own space (`example_ledger` and so on) and resets it when it
starts, so you can run them again.

## The pattern

Every example writes its job as a plain function of one connection, and
[txretry.py](txretry.py) runs it as a transaction:

```python
def transfer(source, target, cents):
    def work(r):
        have = int(get_for_update(r, source))
        if have < cents:
            raise Refused()             # no overdrafts: nothing is written
        r.set(source, have - cents)
        r.set(target, int(get_for_update(r, target)) + cents)
        return True
    return work

in_transaction(r, transfer("acct:1", "acct:2", 2_500))   # 25.00
```

In a pessimistic transaction each key is locked as it's read (shared) or written
(exclusive), and the locks are held until COMMITTX. Two things can stop a transaction,
and `in_transaction` handles both by aborting and running the function again, after a
short random pause that doubles each time:

- **TXDEADLOCK**: it would have waited on a transaction that waits on it. The server
  says so at once and lets its locks go, so the other one goes on.
- **TXLOCKTIMEOUT**: the lock it wanted wasn't let go of within `tx_lock_wait_ms`.

Raising `Refused` from the function gives up on purpose. The transaction is aborted,
so nothing it wrote is kept.

A few things worth knowing:

- Read a key you're going to write with `get_for_update` (GETFORUPDATE) or
  `mget_for_update` (MGETFORUPDATE). A plain GET takes a shared lock, so two
  transactions that both read a balance and then write it each wait for the other's
  read lock to go, and one of them gets TXDEADLOCK. A read for update takes the write
  lock straight away, so the second one just waits its turn.
- Lock keys in the same order everywhere. A transfer from A to B and one from B to A
  can still each hold one key and want the other. The ledger sorts the two accounts
  and the shop sorts an order's lines, so neither meets a deadlock at all. The
  warehouse is left the other way round on purpose: each picker walks the bins in its
  own order, and the retry sorts it out.
- Back off between tries. With a short fixed pause, a busy key can lose every try.
- Something that reads many keys over and over, like the auditor, holds read locks on
  all of them while it reads. Give it a pause between runs, or it keeps the writers
  out.
- Only string commands are locked in a pessimistic transaction (GET, MGET, SET, INCR,
  DEL and so on). KEYS, RANGE and COUNT are refused in one. A CALLF is taken, and
  the function's reads and writes lock as the client's would.

## In Luau

[luau/](luau/) has every job again as a stored function that runs its whole
transaction inside the server. Each file is one function, with an action as its first
argument:

```text
SETF txretry "<luau/txretry.luau>"
SETF ledger "<luau/ledger.luau>"
CALLF ledger transfer acct:1 acct:2 2500
CALLF ledger audit acct:0 acct:1 acct:2
```

`python3 ledger.py --luau` does that for you and then runs the same clients and checks,
each client calling CALLF instead of sending the commands itself.

The functions share [luau/txretry.luau](luau/txretry.luau), loaded with
`require("txretry")`. Its `in_transaction(work, ...)` is the Luau twin of the Python
one, built on `barch.tx.run`: begin a pessimistic transaction, do the work through
`barch.store`, commit, and abort and run again on TXDEADLOCK, TXLOCKTIMEOUT or
TXCONFLICT. `barch.store.get(key, true)` is the read for update, and in a pessimistic
transaction `barch.store` locks just like the commands do. `tx.refuse()` gives up on
purpose, and the function returns nil.

```lua
local tx = require("txretry")

local function transfer(source, target, cents)
    cents = tx.whole(cents)
    local have = tx.whole(barch.store.get(source, true))
    if have < cents then
        tx.refuse()                     -- no overdrafts: nothing is written
    end
    tx.put(source, have - cents)
    tx.put(target, tx.whole(barch.store.get(target, true)) + cents)
    return true
end

function call(source, target, cents)
    return tx.in_transaction(transfer, source, target, cents)
end
```

What's different in Luau:

- A stored function can't sleep, so it can't back off between tries. It tries ten
  times straight away, then raises TXRETRY, and the client calls it again after a
  pause (`in_luau` in txretry.py). With reads for update in a fixed key order there's
  almost nothing to retry: in the runs here only the warehouse, whose pickers lock
  bins in their own order, ever got a TXRETRY.
- Money is still whole cents. Luau numbers are doubles, which hold every whole number
  up to 2^53 exactly, so stick to `+`, `-`, `*` and `//`. `barch.store` holds strings,
  so `tx.whole()` what you read, and write numbers with `tx.put()`, which writes digits
  where `tostring` might write 1e+15.
- A function that waits for a lock parks on it, the way a client does, so its I/O
  thread goes on serving everyone else, the lock's holder too. Functions and
  client-side transactions mix fine on the same keys.

See [docs/TRANSACTIONS.md](../../docs/TRANSACTIONS.md) for how the locks, waits and
deadlock detection work. `TestTxWorkload` (test/txworkloadtest.py) runs bigger versions
of the same jobs as a test, and `TestTxExamples` runs these, as Python and in Luau.
