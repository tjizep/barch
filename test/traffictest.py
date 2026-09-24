#
# Recording commands to files and replaying them - TODO 316, 317, 318.
#
# The two halves are tested separately on purpose. What the server writes is
# decoded here, so a change to the format fails loudly rather than quietly making
# old recordings unreplayable. The replay is then driven through the same functions
# the trafficreplay.py tool uses, so the tool is what is tested and not a copy.
#
import glob
import http.client
import os
import sys
import threading
import time

import scale
import redis
import barch

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
import trafficreplay

# barch writes its shards to the cwd, so work somewhere of our own
WORK = scale.workdir()

PORT = scale.port(default=14400)
print(f'running {__file__}')
barch.start("0.0.0.0", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.flushall()

# the name of a set, not of a file: capture writes traffic_one.<thread>.dat and
# the reader takes the lot
RECORDING = "traffic_one.dat"

assert r.config_get("traffic_capture")["traffic_capture"] == "off", "capture must be off by default"
assert r.config_get("traffic_max_bytes")["traffic_max_bytes"] == "0"


def connect():
    return redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)


def read(path=RECORDING):
    return trafficreplay.read_recording(path)


# ---- nothing is recorded while capture is off ----

r.config_set("traffic_file", RECORDING)
r.set("before", "1")
assert glob.glob("traffic_one.*.dat") == [], "a file was written with capture off"

# ---- what arrives is what is recorded ----

r.config_set("traffic_capture", "on")
r.set("plain", "one")
r.set("withnl", b"line1\nline2\n3")          # newlines: the format is length prefixed
r.execute_command("USE", "shop")             # switches the space for this connection
r.set("sku1", "widget")
r.execute_command("USE", "")                 # back to the default
r.rpush("mylist", "a", "b", "c")
# CONFIG is never recorded - replaying it would reconfigure the server underneath
# the replay, starting with turning capture back on
r.config_get("traffic_capture")
r.config_set("traffic_capture", "off")       # and this closes the file

records, skipped = read()
assert skipped == 0, f"{skipped} records would not decode"
assert len(records) == 6, f"expected 6 records, got {len(records)}"

got = [(space, [a.decode("utf-8", "replace") for a in argv]) for _, _, space, argv in records]
assert got[0] == ("", ["SET", "plain", "one"]), got[0]
assert got[1] == ("", ["SET", "withnl", "line1\nline2\n3"]), got[1]
assert got[2] == ("", ["USE", "shop"]), got[2]
# the space is the one in force when the command ran, which is what makes a record
# self contained - the client said `SET`, not `shop:SET`
assert got[3] == ("shop", ["SET", "sku1", "widget"]), got[3]
# the second USE is recorded against `shop`, because that is where the connection
# was when it arrived. It still replays bare - see SPACE_SWITCHES in trafficreplay
assert got[4] == ("shop", ["USE", ""]), got[4]
assert got[5] == ("", ["RPUSH", "mylist", "a", "b", "c"]), got[5]
assert not any(a[0].upper() == "CONFIG" for _, a in got), "CONFIG was recorded"

# the records are in arrival order, which is what the replayer's timeline needs
times = [at for at, _, _, _ in records]
assert times == sorted(times), "the recording is out of order"
assert len({conn for _, conn, _, _ in records}) == 1, "one connection recorded as several"

# ---- and it replays ----

r.flushall()
assert r.get("plain") is None
assert r.dbsize() == 0

sent, failed = trafficreplay.replay(records, connect, speed=1.0, jitter=0.3, max_gap=0.5)
assert sent == 6, f"replayed {sent} of 6"
assert failed == 0, f"{failed} commands were refused"

assert r.get("plain") == b"one"
assert r.get("withnl") == b"line1\nline2\n3"
assert r.lrange("mylist", 0, -1) == [b"a", b"b", b"c"]
# the one recorded against `shop` went back to `shop` and not to the default space
assert r.execute_command("shop:GET", "sku1") == b"widget"
assert r.get("sku1") is None, "a spaced command replayed into the wrong space"
# the replay itself was not recorded - capture was off for all of it
again, _ = read()
assert len(again) == len(records), "the replay was appended to the recording"

# ---- a recording only the writer knows the end of still reads ----

shards = glob.glob("traffic_one.*.dat")
assert len(shards) == 1, f"one recording thread should write one file, got {shards}"
with open(shards[0], "rb") as fh:
    whole = fh.read()
TRUNCATED = "traffic_cut.0.dat"
with open(TRUNCATED, "wb") as fh:
    fh.write(whole[:-12])                    # as if the process died mid record
cut, cut_skipped = read("traffic_cut.dat")
assert cut_skipped == 1, f"a half written record should count as one unreadable, got {cut_skipped}"
assert len(cut) == len(records) - 1, f"the records in front of it should survive, got {len(cut)}"

# ---- and junk is refused rather than guessed at ----

JUNK = "traffic_junk.0.dat"
with open(JUNK, "wb") as fh:
    fh.write(b"not a recording at all\n")
try:
    read("traffic_junk.dat")
    raise AssertionError("junk was accepted as a recording")
except trafficreplay.BadRecording:
    pass

# ---- several connections are recorded as several, and replayed as several ----

MANY = "traffic_many.dat"
r.config_set("traffic_file", MANY)
r.flushall()
r.config_set("traffic_capture", "on")

WRITERS = 3
PER = 20


def writer(n):
    w = connect()
    for i in range(PER):
        w.set(f"c{n}:{i}", f"{n}-{i}")
        time.sleep(0.001)


ts = [threading.Thread(target=writer, args=(n,)) for n in range(WRITERS)]
for t in ts:
    t.start()
for t in ts:
    t.join()
r.config_set("traffic_capture", "off")

many_files = sorted(glob.glob("traffic_many.*.dat"))
print(f"{WRITERS} concurrent clients wrote {len(many_files)} file(s)")
records, skipped = read(MANY)
assert skipped == 0
# whatever the threads did, the merge has to account for every record in every
# file and hand them back in arrival order
per_file = sum(len(trafficreplay.read_one(f)[0]) for f in many_files)
assert per_file == len(records), f"merged {len(records)} of {per_file} records"
merged_times = [at for at, _, _, _ in records]
assert merged_times == sorted(merged_times), "the merge did not order the files"
# a client says hello when it connects - redis-py sends CLIENT SETINFO - and that
# arrived too, so it is in the recording. Counted out here rather than filtered by
# the recorder, because what arrived is what a replay should send
writes = [rec for rec in records if rec[3][0].upper() == b"SET"]
assert len(writes) == WRITERS * PER, f"recorded {len(writes)} writes of {WRITERS * PER}"
assert len(records) > len(writes), "the connection handshakes were not recorded"
conns = {conn for _, conn, _, _ in records}
assert len(conns) == WRITERS, f"{WRITERS} connections recorded as {len(conns)}"
# each connection's own commands have to come back in the order it sent them, which
# is what the replay puts on one client each
for c in conns:
    mine = [argv[1].decode() for _, conn, _, argv in writes if conn == c]
    assert mine == sorted(mine, key=lambda k: int(k.split(":")[1])), f"connection {c} out of order"

r.flushall()
sent, failed = trafficreplay.replay(records, connect, speed=2.0, jitter=0.5, max_gap=0.5)
assert sent == len(records) and failed == 0, f"replayed {sent} of {len(records)}, {failed} refused"
for n in range(WRITERS):
    for i in range(PER):
        assert r.get(f"c{n}:{i}") == f"{n}-{i}".encode(), f"c{n}:{i} did not come back"

# ---- a thread keeps its file across bursts, rather than renaming it ----

BURSTS = "traffic_bursts.dat"
r.config_set("traffic_file", BURSTS)
for burst in range(3):
    r.config_set("traffic_capture", "on")
    r.set(f"burst{burst}", "x")
    r.config_set("traffic_capture", "off")
burst_files = glob.glob("traffic_bursts.*.dat")
assert len(burst_files) == 1, f"three bursts on one thread wrote {len(burst_files)} files"
bursts, _ = read(BURSTS)
# appended, not overwritten - all three bursts are in there
assert [a[1] for _, _, _, a in bursts] == [b"burst0", b"burst1", b"burst2"], bursts

# ---- the cap stops it rather than filling the disk ----

CAPPED = "traffic_capped.dat"
r.config_set("traffic_file", CAPPED)
r.config_set("traffic_max_bytes", "2048")
r.config_set("traffic_capture", "on")
for i in range(4000):
    r.set(f"cap:{i}", "x" * 50)
r.config_set("traffic_capture", "off")
size = sum(os.path.getsize(f) for f in glob.glob("traffic_capped.*.dat"))
# the cap is checked before a record, not inside one, so the file can overshoot by
# up to one record - what it cannot do is keep growing
assert size < 2048 + 4096, f"the cap did not hold: {size} bytes"
capped, _ = read(CAPPED)
assert 0 < len(capped) < 4000, f"expected a partial recording, got {len(capped)}"
r.config_set("traffic_max_bytes", "0")

# ---- a password is not written to the recording, and CONFIG cannot sneak in ----
#
# Both of these came out of the cloud review - TODO 328. The recorder sits before
# authorization so it sees every AUTH that arrives, and the dispatcher leaves the
# `<space>:` prefix on args[0], so `shop:CONFIG` was eleven characters where the
# filter only knew about a bare six.

SECRETS = "traffic_secrets.dat"
r.config_set("traffic_file", SECRETS)
r.config_set("traffic_capture", "on")
try:
    r.execute_command("AUTH", "someone", "hunter2")
except redis.exceptions.ResponseError:
    pass                                      # refused is fine; it was still seen
try:
    r.execute_command("shop:CONFIG", "SET", "traffic_capture", "off")
except redis.exceptions.ResponseError:
    pass
r.set("after", "1")                           # so the recording is not empty
r.config_set("traffic_capture", "off")

raw = b""
for f in glob.glob("traffic_secrets.*.dat"):
    with open(f, "rb") as fh:
        raw += fh.read()
assert b"hunter2" not in raw, "the password was written to the recording"
assert b"<redacted>" in raw, "the AUTH was not recorded at all, redacted or otherwise"
secret_records, _ = read(SECRETS)
names = [argv[0].decode().upper() for _, _, _, argv in secret_records]
assert any(n == "AUTH" for n in names), f"AUTH was dropped rather than redacted: {names}"
# the user survives, only the credential goes
auth = next(argv for _, _, _, argv in secret_records if argv[0].upper() == b"AUTH")
assert auth[1] == b"someone", auth
assert auth[2] == b"<redacted>", auth
# and no form of CONFIG is in there, prefixed or not
assert not any("CONFIG" in n for n in names), f"a CONFIG got recorded: {names}"

# ---- an HTTP request is recorded, and replayed as an HTTP request ----
#
# The shop's traffic arrives this way and never passes a command dispatch, so
# without this the recorder sees none of a web application - TODO 319.

HTTP_PORT = scale.port(1, default=18400)

HELLO = """
function call()
    return "hello"
end

function hello(req, res)
    res.body = "hi " .. tostring(req.body)
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/hello",
        methods = {GET = hello, POST = hello},
        send = "text/plain",
    }
end
"""

WEBCONF = """
function call()
    return "web"
end

function transport()
    return {
        kind = "http",
        port = %d,
        bind = "127.0.0.1",
        user = "web",
        keys = {"HELLO"},
    }
end
""" % HTTP_PORT

assert r.execute_command("SETF", "hello", HELLO) == b"OK"
assert r.execute_command("SETF", "webconf", WEBCONF) == b"OK"
r.execute_command("HTTP", "START", "WEBCONF", str(HTTP_PORT), "127.0.0.1")

WEB = "traffic_web.dat"
r.config_set("traffic_file", WEB)
r.config_set("traffic_capture", "on")


def hello_call(method, path, body=None):
    c = http.client.HTTPConnection("127.0.0.1", HTTP_PORT, timeout=10)
    try:
        c.request(method, path, body=body,
                  headers={"Content-Type": "text/plain"} if body else {})
        res = c.getresponse()
        return res.status, res.read()
    finally:
        c.close()


assert hello_call("GET", "/hello?n=1")[0] == 200
assert hello_call("GET", "/hello?n=2")[0] == 200
assert hello_call("POST", "/hello", b"there")[1] == b"hi there"
r.config_set("traffic_capture", "off")

web_records, skipped = read(WEB)
assert skipped == 0
assert len(web_records) == 3, f"expected 3 requests, got {len(web_records)}"
assert all(trafficreplay.is_http(argv) for _, _, _, argv in web_records), web_records
methods = [argv[1].decode() for _, _, _, argv in web_records]
urls = [argv[2].decode() for _, _, _, argv in web_records]
ports = {int(argv[3]) for _, _, _, argv in web_records}
assert methods == ["GET", "GET", "POST"], methods
# the raw url, with the query on it - a replay that dropped it would ask a
# different question
assert urls == ["/hello?n=1", "/hello?n=2", "/hello"], urls
# the port is in the record, so a replay does not have to be told where to send it
assert ports == {HTTP_PORT}, ports
assert web_records[2][3][5] == b"there", "the POST body was not recorded"
assert web_records[2][3][4].startswith(b"text/plain"), web_records[2][3][4]
# nothing asked for headers, so none are in the record - TODO 321
assert all(len(argv) == 6 for _, _, _, argv in web_records), \
    "a header was recorded with traffic_headers empty"
assert all(not trafficreplay.http_headers(argv) for _, _, _, argv in web_records)

# ---- and the headers traffic_headers names, which is how a session replays ----

HDRS = "traffic_hdrs.dat"
r.config_set("traffic_file", HDRS)
r.config_set("traffic_headers", "cookie, x-request-id")
r.config_set("traffic_capture", "on")


def hello_with(headers, path="/hello?n=9"):
    c = http.client.HTTPConnection("127.0.0.1", HTTP_PORT, timeout=10)
    try:
        c.request("GET", path, headers=headers)
        res = c.getresponse()
        return res.status, res.read()
    finally:
        c.close()


assert hello_with({"Cookie": "session=abc123", "X-Request-Id": "r-1",
                   "User-Agent": "not-asked-for"})[0] == 200
r.config_set("traffic_capture", "off")

hdr_records, skipped = read(HDRS)
assert skipped == 0
assert len(hdr_records) == 1, f"expected one request, got {len(hdr_records)}"
got_headers = trafficreplay.http_headers(hdr_records[0][3])
# the two that were named, by the names the config used, and nothing else
assert got_headers == {"cookie": "session=abc123", "x-request-id": "r-1"}, got_headers
raw_hdr = b""
for f in glob.glob("traffic_hdrs.*.dat"):
    with open(f, "rb") as fh:
        raw_hdr += fh.read()
assert b"not-asked-for" not in raw_hdr, "a header nobody asked for was recorded"
assert b"session=abc123" in raw_hdr, "the cookie was not recorded"

# and the replay sends them back - the handler echoes the body, so check the
# request is accepted and the pairs survive a round trip through the tool
sent, failed = trafficreplay.replay(hdr_records, connect, speed=1.0, jitter=0.0,
                                    max_gap=0.5, http_host="127.0.0.1")
assert sent == 1 and failed == 0, f"replayed {sent}, {failed} failed"
r.config_set("traffic_headers", "")

# and it goes back out as HTTP, from the pool rather than on a RESP client
sent, failed = trafficreplay.replay(web_records, connect, speed=1.0, jitter=0.4,
                                    max_gap=0.5, http_host="127.0.0.1")
assert sent == 3 and failed == 0, f"replayed {sent} of 3, {failed} failed"

print("traffic recording and replay ok")
