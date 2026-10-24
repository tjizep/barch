#!/usr/bin/env python3
"""
Open transactions listed in INFO transactions - TODO 720.

When something times out, the listing is how you find the transaction that isn't moving:
one line per open transaction, saying whose session it is, what it's running, what it
holds and what it waits on. Sessions with no transaction aren't listed.

Checked against one barchd with no Raft, which pauses each commit halfway
(BARCH_TEST_TX_HALF_PAUSE_MS), so a commit can be seen holding its space:
  - nothing is listed before any transaction, nor for a connection that has none;
  - an idle transaction: its id, the session CLIENT LIST names, its space, state idle;
  - one running a slow function: state running, the function and CALLF, and still
    so while it's parked between slices;
  - a commit holding the space, and a second waiting on it: committing with every shard
    held, and waiting on the space's shards, on the first one's id;
  - after ABORTTX, COMMITTX and a closed connection, nothing is listed.
"""
import os
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14811)
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


PAUSE_MS = 1500
DATA = os.path.join(os.getcwd(), "txlist_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txlist.log"), "a")
env = dict(os.environ, BARCH_TEST_TX_HALF_PAUSE_MS=str(PAUSE_MS))
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log, env=env)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "txlist_" + str(int(time.time()))


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    # the lines as barchd sends them, not redis-py's parse of them
    c.response_callbacks.pop("INFO", None)
    c.execute_command("USE", SPACE)
    # asked now: CLIENT isn't taken inside a transaction
    info = c.execute_command("CLIENT", "INFO")
    sessions[id(c)] = "client:" + [w for w in info.split() if w.startswith("id=")][0][3:]
    return c


sessions = {}


def session_of(c):
    return sessions[id(c)]


def listed(c):
    """each tx line of INFO transactions as a dict"""
    text = c.execute_command("INFO", "transactions")
    rows = []
    for line in text.splitlines():
        if line.startswith("tx") and line[2:3].isdigit():
            fields = line.split(":", 1)[1]
            rows.append(dict(f.split("=", 1) for f in fields.split(",")))
    return rows


def listed_now():
    """listed(), from a new connection each try. A connection can share its I/O thread
    with one whose COMMITTX is waiting for a commit's shards, which holds that thread
    up until it's done: on Windows which thread a connection gets is close to chance.
    So a try that isn't answered soon is given up and the next goes to another"""
    for _ in range(40):
        c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=0.5, decode_responses=True,
                        single_connection_client=True)
        c.response_callbacks.pop("INFO", None)
        try:
            return listed(c)
        except redis.exceptions.TimeoutError:
            pass
        finally:
            c.close()
    raise AssertionError("no connection got INFO transactions answered")


def wait_for(c, test, seconds=10.0):
    """`c` is no longer asked: see listed_now"""
    deadline = time.time() + scale.scaled_seconds(seconds, floor=seconds)
    while time.time() < deadline:
        rows = listed_now()
        if test(rows):
            return rows
        time.sleep(0.05)
    return listed_now()


try:
    watch = client()
    check(listed(watch) == [], "nothing listed before any transaction")
    watch.execute_command("CONFIG", "SET", "function_deadline_ms", "600000")
    watch.execute_command("SETF", "slow", "function call(k)\n"
                          "  local s = 0\n"
                          "  for i = 1, 150000000 do s = s + 1 end\n"
                          "  return 1\n"
                          "end\n")

    print("an idle transaction")
    a = client()
    check(a.execute_command("BEGINTX") == "OK", "BEGINTX")
    a.execute_command("SET", "ka", "1")
    rows = listed(watch)
    check(len(rows) == 1, "one listed, not the watching connection (%d)" % len(rows))
    row = rows[0] if rows else {}
    check(row.get("session") == session_of(a), "its session is CLIENT LIST's id (%s)" % row.get("session"))
    check(row.get("space", "").endswith(SPACE), "its space (%s)" % row.get("space"))
    check(row.get("state") == "idle" and row.get("command") == "", "idle, running nothing (%s)" % row.get("state"))
    check(len(row.get("id", "")) == 32, "its id (%s)" % row.get("id"))
    time.sleep(0.3)
    check(int(listed(watch)[0].get("age_ms", 0)) >= 300, "its age grows")

    print("a transaction running a slow function")
    b = client()
    b.execute_command("BEGINTX")
    got = {}
    t = threading.Thread(target=lambda: got.setdefault("r", b.execute_command("CALLF", "slow", "x")))
    t.start()
    rows = wait_for(watch, lambda rs: any(r.get("function") == "slow" for r in rs))
    run = [r for r in rows if r.get("function") == "slow"]
    run = run[0] if run else {}
    check(run.get("state") == "running" and run.get("command") == "CALLF",
          "running CALLF, function slow (%s, %s)" % (run.get("state"), run.get("command")))
    # and slices later, when the first look missed it before: a function parks between
    # slices, and parking used to put the row back to idle - TODO 745
    time.sleep(0.5)
    later = [r for r in listed_now() if r.get("id") == run.get("id")]
    later = later[0] if later else {}
    check(not t.is_alive() or (later.get("state") == "running" and later.get("function") == "slow"),
          "still running it half a second on (%s, %s)" % (later.get("state"), later.get("function")))
    t.join()
    check(got.get("r") == 1, "the function answered")
    rows = [r for r in listed(watch) if r.get("id") == run.get("id")]
    check(rows and rows[0].get("state") == "idle" and rows[0].get("function") == "",
          "idle again after it (%s)" % (rows[0].get("state") if rows else None))
    b.execute_command("ABORTTX")

    print("a commit holding the space, and one waiting on it")
    d, e = client(), client()
    d.execute_command("BEGINTX")
    d.execute_command("SET", "kd", "d")
    e.execute_command("BEGINTX")
    e.execute_command("SET", "ke", "e")
    did = [r["id"] for r in listed(watch) if r.get("session") == session_of(d)][0]
    eid = [r["id"] for r in listed(watch) if r.get("session") == session_of(e)][0]
    answers = {}
    td = threading.Thread(target=lambda: answers.setdefault("d", d.execute_command("COMMITTX")))
    td.start()
    wait_for(watch, lambda rs: any(r.get("id") == did and r.get("holds_also") for r in rs))
    te = threading.Thread(target=lambda: answers.setdefault("e", e.execute_command("COMMITTX")))
    te.start()
    rows = wait_for(watch, lambda rs: any(r.get("id") == eid and r.get("state") == "waiting" for r in rs))
    byid = {r["id"]: r for r in rows}
    dr, er = byid.get(did, {}), byid.get(eid, {})
    check(dr.get("state") == "committing" and dr.get("holds_also", "").startswith("every shard"),
          "the first is committing, holding every shard (%s, %s)" % (dr.get("state"), dr.get("holds_also")))
    check(er.get("state") == "waiting" and SPACE in er.get("waiting", "")
          and er.get("waiting", "").endswith("while it reads its own writes"),
          "the second waits on the space's shards (%s)" % er.get("waiting"))
    check(er.get("waiting_on") == did, "on the first one's id (%s)" % er.get("waiting_on"))
    td.join()
    te.join()
    check(answers.get("d") == "OK" and answers.get("e") == "OK", "both commit (%s)" % answers)

    print("after they end")
    a.execute_command("ABORTTX")
    f = client()
    f.execute_command("BEGINTX")
    check(len(listed(watch)) == 1, "a new one is listed")
    f.close()
    rows = wait_for(watch, lambda rs: rs == [])
    check(rows == [], "nothing listed once each has ended, a closed connection too (%d)" % len(rows))
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
