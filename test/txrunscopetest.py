#!/usr/bin/env python3
"""
A transaction for a run with no client session - TODO 709, TODO 697's group 2.

A cron job, a queue handler, an fs source, a sync hook or an HTTP handler runs with no
client behind it. Each run gets a session of its own, so a script's
barch.call('BEGINTX') there opens a transaction its later barch.call and barch.store
calls share. One still open when the run ends is aborted, and the run's answer says so.

Checked against one barchd, a space without Raft, through cron and through HTTP:
  - a run that begins, writes through barch.call and barch.store, and commits makes
    both writes, and counts a commit;
  - a run that returns with its transaction open writes nothing, fails saying why, and
    leaves none live; the next run of it starts afresh rather than finding one open.
"""
import http.client
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14730)
HTTP = PORT + 1
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "txrunscope_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txrunscope.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

TAG = str(int(time.time()))
SPACE = "txr" + TAG

COMMITS = '''function call(k)
    barch.call('BEGINTX')
    barch.call('SET', k, 'v')
    barch.store.set(k .. '_s', 'v')
    barch.call('COMMITTX')
    return 'ok'
end
'''
LEAVES = '''function call(k)
    barch.call('BEGINTX')
    barch.call('SET', k, 'v')
    barch.store.set(k .. '_s', 'v')
    return 'left'
end
'''
CRON = '''function transport()
    return { kind = "cron", space = "%s", call = "%s", args = {"%s"}, every = "300ms", user = "default" }
end'''
HTTPCONF = '''function call() return "http" end
function transport()
    return {kind = "http", port = %d, bind = "127.0.0.1", user = "default", keys = {"HCOMMIT", "HLEAVE"}}
end''' % HTTP
HCOMMIT = '''function call() return "r" end
function h(req, res)
    barch.call('BEGINTX')
    barch.call('SET', 'h1', 'v')
    barch.store.set('h1_s', 'v')
    barch.call('COMMITTX')
    res.body = 'ok'; res.code = 200
end
function transport() return {kind = "resource", route = "/commit", methods = {GET = h}} end'''
HLEAVE = '''function call() return "r" end
function h(req, res)
    barch.call('BEGINTX')
    barch.call('SET', 'h2', 'v')
    barch.store.set('h2_s', 'v')
    res.body = 'left'; res.code = 200
end
function transport() return {kind = "resource", route = "/leave", methods = {GET = h}} end'''


def client(space=SPACE):
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=30, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", space)
    return c


def stats():
    text = client().execute_command("INFO", "transactions")
    if isinstance(text, dict):
        return {k: int(v) for k, v in text.items()}
    out = {}
    for line in str(text).splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            out[k.strip()] = int(v)
    return out


def changed(before, after):
    return {k: after[k] - before.get(k, 0) for k in after if after[k] != before.get(k, 0)}


def cron_line(name):
    st = client().execute_command("FUNCTIONS", "CRON")
    for line in str(st).splitlines():
        if "name=" + name in line:
            return line
    return ""



def get(path):
    for _ in range(50):
        try:
            c = http.client.HTTPConnection("127.0.0.1", HTTP, timeout=10)
            c.request("GET", path)
            r = c.getresponse()
            return r.status, r.read().decode()
        except OSError:
            time.sleep(0.1)
    return 0, ""


try:
    c = client()
    c.execute_command("SET", "seed", "1")
    check(c.execute_command("SETF", "TXCOMMITS", COMMITS) == "OK", "SETF TXCOMMITS")
    check(c.execute_command("SETF", "TXLEAVES", LEAVES) == "OK", "SETF TXLEAVES")
    before = stats()

    print("a cron job")
    c.execute_command("configuration:SETF", "cron/jobs/txc" + TAG, CRON % (SPACE, "TXCOMMITS", "c1"))
    end = time.time() + 15
    while time.time() < end and c.get("c1_s") is None:
        time.sleep(0.2)
    c.execute_command("configuration:REMF", "cron/jobs/txc" + TAG)
    check(c.get("c1") == "v" and c.get("c1_s") == "v", "one that commits makes its barch.call and barch.store writes")
    check(stats()["tx_commits"] > before.get("tx_commits", 0), "and counts a commit")

    # a failed run isn't counted in runs=, so the aborts say how often it ran
    s0 = stats()
    c.execute_command("configuration:SETF", "cron/jobs/txl" + TAG, CRON % (SPACE, "TXLEAVES", "l1"))
    end = time.time() + 15
    while time.time() < end and stats()["tx_aborts"] - s0.get("tx_aborts", 0) < 3:
        time.sleep(0.2)
    line = cron_line("txl" + TAG)
    c.execute_command("configuration:REMF", "cron/jobs/txl" + TAG)
    time.sleep(0.5)
    d = changed(s0, stats())
    check(d.get("tx_begins", 0) >= 3 and d.get("tx_aborts", 0) == d.get("tx_begins", 0),
          "it ran several times, each beginning one and aborting it (%s)" % d)
    check("transaction open" in line and "aborted" in line and "already open" not in line,
          "and fails saying why (%s)" % line[-110:])
    check(c.get("l1") is None and c.get("l1_s") is None, "and writes nothing")
    check(stats()["tx_live"] == 0, "and leaves none live")

    print("an HTTP handler")
    for name, src in (("HCOMMIT", HCOMMIT), ("HLEAVE", HLEAVE), ("HTTPCONF", HTTPCONF)):
        c.execute_command("SETF", name, src)
    try:
        c.execute_command("HTTP", "START", "HTTPCONF", str(HTTP), "127.0.0.1")
        has_http = True
    except redis.exceptions.ResponseError as e:
        print("SKIP the HTTP part: no HTTP server in this build (%s)" % e)
        has_http = False
    if has_http:
        status, body = get("/commit")
        check(status == 200 and c.get("h1") == "v" and c.get("h1_s") == "v",
              "one that commits makes both writes (%s %s)" % (status, body[:60]))
        status, body = get("/leave")
        check(status == 500 and "transaction open" in body and "aborted" in body,
              "one that returns with it open fails saying why (%s %s)" % (status, body[:90]))
        check(c.get("h2") is None and c.get("h2_s") is None, "and writes nothing (%s %s)" % (c.get("h2"), c.get("h2_s")))
        status, body = get("/leave")
        check(status == 500 and "already open" not in body, "the next request starts afresh (%s)" % body[:90])
        check(stats()["tx_live"] == 0, "and leaves none live")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
