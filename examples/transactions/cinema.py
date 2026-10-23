#!/usr/bin/env python3
"""
Cinema seats: customers book blocks of seats side by side, and sometimes cancel.

The rule: a seat is never sold twice. Without a transaction two customers can both
see the same seats free and both book them.

    python3 cinema.py --port 14000
"""
import random
import sys

from txretry import (
    Refused, add, connect, get_for_update, mget_for_update, in_luau, in_transaction,
    load_luau, options, report, run_clients, whole
)

opts = options(__doc__)
SPACE = "example_cinema"
ROWS, SEATS = 5, 10

setup = connect(opts.host, opts.port, SPACE)
if opts.luau:
    load_luau(setup, "cinema")         # luau/cinema.luau, run with CALLF
for row in range(ROWS):
    setup.delete(*["seat:%d:%d" % (row, s) for s in range(SEATS)])
setup.delete("booking:next")


def seats_of(row, first, count):
    return ["seat:%d:%d" % (row, first + s) for s in range(count)]


def book(customer, row, first, count):
    seats = seats_of(row, first, count)

    def work(r):
        if any(mget_for_update(r, seats)):
            raise Refused()                  # one of them is taken
        booking = add(r, "booking:next", 1)
        for seat in seats:
            r.set(seat, booking)
        r.set("booking:%d" % booking, "%d %d %d %d" % (customer, row, first, count))
        return booking
    return work


def cancel(booking):
    def work(r):
        _, row, first, count = map(int, get_for_update(r, "booking:%d" % booking).split())
        r.delete(*seats_of(row, first, count))
        r.set("booking:%d" % booking, "cancelled")
        return booking
    return work


def customer(me):
    r = connect(opts.host, opts.port, SPACE)
    mine = []
    for _ in range(30):
        if mine and random.random() < 0.2:
            booking = mine.pop()
            in_luau(r, "cinema", "cancel", booking) if opts.luau else in_transaction(r, cancel(booking))
            continue
        count = random.randint(1, 4)
        row, first = random.randrange(ROWS), random.randint(0, SEATS - count)
        if opts.luau:
            booking = in_luau(r, "cinema", "book", me, row, first, count)
        else:
            booking = in_transaction(r, book(me, row, first, count))
        if booking:
            mine.append(booking)


run_clients(opts.clients, customer)

# every seat that's taken must name a booking that covers it, and the other way round
holder = {}
double = False
for n in range(1, whole(setup.get("booking:next"), 0) + 1):
    b = setup.get("booking:%d" % n)
    if b != "cancelled":
        _, row, first, count = map(int, b.split())
        for seat in seats_of(row, first, count):
            double = double or seat in holder
            holder[seat] = str(n)
taken = {"seat:%d:%d" % (row, s): v for row in range(ROWS) for s in range(SEATS)
         for v in [setup.get("seat:%d:%d" % (row, s))] if v}
for row in range(ROWS):
    print("row %d  %s" % (row, "".join("#" if "seat:%d:%d" % (row, s) in taken else "." for s in range(SEATS))))
sys.exit(report([
    ("no seat is in two bookings", not double),
    ("each taken seat belongs to the booking that says so (%d taken)" % len(taken), taken == holder),
]))
