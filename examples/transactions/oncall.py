#!/usr/bin/env python3
"""
Doctors on call: a doctor may go off call only if someone else on the shift stays on.

The rule: a shift always has a doctor on call. This is the well known "write skew"
case. Two doctors each see the other still on call and both go off, and neither one
wrote anything the other read. Snapshot isolation alone lets that through; a
pessimistic transaction's read locks don't.

    python3 oncall.py --port 14000
"""
import random
import sys
import time

from txretry import (
    Refused, connect, mget_for_update, in_luau, in_transaction, load_luau, options,
    report, run_clients
)

opts = options(__doc__)
SPACE = "example_oncall"
DOCTORS = ["oncall:%s" % d for d in ("ana", "ben", "chen")]

setup = connect(opts.host, opts.port, SPACE)
if opts.luau:
    load_luau(setup, "oncall")         # luau/oncall.luau, run with CALLF
for d in DOCTORS:
    setup.set(d, 1)


def go_off(doctor):
    def work(r):
        on = dict(zip(DOCTORS, (int(v) for v in mget_for_update(r, DOCTORS))))
        if not on[doctor] or sum(on.values()) < 2:
            raise Refused()                 # the last one on call stays
        r.set(doctor, 0)
        return True
    return work


def go_on(doctor):
    def work(r):
        r.set(doctor, 1)
        return True
    return work


fewest = [len(DOCTORS)]


def doctor_on_phone(i):
    r = connect(opts.host, opts.port, SPACE)
    for _ in range(40):
        doctor = random.choice(DOCTORS)
        going_off = random.random() < 0.6
        if opts.luau:
            in_luau(r, "oncall", "off", doctor, *DOCTORS) if going_off else in_luau(r, "oncall", "on", doctor)
        else:
            in_transaction(r, go_off(doctor) if going_off else go_on(doctor))


def rota_checker(done):
    # someone looking at the rota the whole time
    r = connect(opts.host, opts.port, SPACE)
    while not done.is_set():
        if opts.luau:
            n = in_luau(r, "oncall", "count", *DOCTORS)
        else:
            n = in_transaction(r, lambda r: sum(int(v) for v in r.mget(DOCTORS)))
        fewest[0] = min(fewest[0], n)
        time.sleep(0.005)


run_clients(opts.clients, doctor_on_phone, rota_checker)

on = [int(v) for v in setup.mget(DOCTORS)]
print("on call now: %s" % ", ".join(d[7:] for d, v in zip(DOCTORS, on) if v))
sys.exit(report([
    ("someone is on call now", sum(on) >= 1),
    ("and was every time the rota was checked (fewest %d)" % fewest[0], fewest[0] >= 1),
]))
