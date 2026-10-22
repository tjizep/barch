#!/usr/bin/env python3
"""
An online shop: an order takes its lines from stock, adds 15% tax, charges the
customer's credit and gets the next invoice number. A supplier restocks as it goes.

The rules: invoice numbers have no gaps (many tax offices insist on that), every
invoice adds up to the cent, stock never goes below zero, and what customers paid is
what the shop took. Without a transaction two orders can take the last item, or get
the same invoice number.

Money is whole cents throughout and the tax is rounded in integers, never floats.

    python3 orders.py --port 14000
"""
import random
import sys

from txretry import (
    Refused, connect, get_for_update, in_luau, in_transaction, load_luau, money, options,
    report, run_clients
)

opts = options(__doc__)
SPACE = "example_orders"
PRICES = {"tea": 450, "mug": 1_299, "pot": 3_500, "cosy": 899}    # cents
CREDIT = 100_000                                                    # cents each customer may spend
TAX_PERCENT = 15

setup = connect(opts.host, opts.port, SPACE)
if opts.luau:
    load_luau(setup, "orders")         # luau/orders.luau, run with CALLF
for item, cents in PRICES.items():
    setup.set("price:%s" % item, cents)
    setup.set("stock:%s" % item, 40)
    setup.delete("received:%s" % item)
for c in range(opts.clients):
    setup.set("credit:%d" % c, CREDIT)
setup.delete("invoice:next", "takings")


def tax(cents):
    return (cents * TAX_PERCENT + 50) // 100        # half a cent rounds up


def order(customer, lines):
    def work(r):
        subtotal = 0
        for item, count in lines:
            stock = int(get_for_update(r, "stock:%s" % item))
            if stock < count:
                raise Refused()                     # out of stock
            r.set("stock:%s" % item, stock - count)
            subtotal += count * int(r.get("price:%s" % item))
        total = subtotal + tax(subtotal)
        credit = int(get_for_update(r, "credit:%d" % customer))
        if credit < total:
            raise Refused()
        r.set("credit:%d" % customer, credit - total)
        r.incrby("takings", total)
        number = int(get_for_update(r, "invoice:next") or 0) + 1    # no gaps
        r.set("invoice:next", number)
        r.set("invoice:%d" % number, "%d %d %d %s" % (customer, subtotal, total,
                                                     ",".join("%s:%d" % line for line in lines)))
        return number
    return work


def restock(item, count):
    def work(r):
        r.set("stock:%s" % item, int(get_for_update(r, "stock:%s" % item)) + count)
        r.incrby("received:%s" % item, count)
        return count
    return work


def client(i):
    r = connect(opts.host, opts.port, SPACE)
    for _ in range(30):
        if i == 0:
            item = random.choice(list(PRICES))
            in_luau(r, "orders", "restock", item, 5) if opts.luau else in_transaction(r, restock(item, 5))
        else:
            # sorted, so every order locks the stock it takes in the same order
            items = sorted(random.sample(list(PRICES), random.randint(1, 3)))
            lines = [(item, random.randint(1, 3)) for item in items]
            if opts.luau:
                in_luau(r, "orders", "order", i, ",".join("%s:%d" % line for line in lines))
            else:
                in_transaction(r, order(i, lines))


run_clients(opts.clients, client)

invoices = [setup.get("invoice:%d" % n) for n in range(1, int(setup.get("invoice:next") or 0) + 1)]
right, billed, paid, sold = True, 0, {}, {item: 0 for item in PRICES}
for inv in invoices:
    customer, subtotal, total, lines = inv.split()
    worked = 0
    for line in lines.split(","):
        item, count = line.split(":")
        worked += int(count) * PRICES[item]
        sold[item] += int(count)
    right = right and worked == int(subtotal) and int(total) == worked + tax(worked)
    billed += int(total)
    paid[customer] = paid.get(customer, 0) + int(total)
stock_right = all(int(setup.get("stock:%s" % item)) == 40 + int(setup.get("received:%s" % item) or 0) - sold[item]
                  for item in PRICES)
credit_right = all(int(setup.get("credit:%d" % c)) == CREDIT - paid.get(str(c), 0) for c in range(opts.clients))
print("%d invoices, takings %s" % (len(invoices), money(int(setup.get("takings") or 0))))
sys.exit(report([
    ("invoices 1 to %d, none missing" % len(invoices), all(invoices)),
    ("every invoice adds up to the cent, tax included", right),
    ("takings are what the invoices billed (%s)" % money(billed), int(setup.get("takings") or 0) == billed),
    ("each customer's credit went down by what they were billed", credit_right),
    ("stock is what there was, plus deliveries, less sales", stock_right),
]))
