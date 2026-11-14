import scale
import http.server
import os
import shutil
import struct
import re
import socketserver
import threading
import time
import redis
import barch

# Stored functions written in WebAssembly - TODO 820, WASM-PLAN.md. The modules are
# built byte by byte, so the test needs no compiler: SETF of a binary module,
# calls with arguments and replies, the store and barch.call from inside, the checks
# SETF makes, the instruction cap, the memory cap, and the function's barch section.
# The modules come from wasmmodule.py.

scale.workdir()

PORT = scale.port(default=14112)
WEB_PORT = scale.port(1, default=14113)

from wasmmodule import *  # noqa: E402,F403


# TODO 822: a web server for http_request. /wait holds each request until `expected`
# of them have arrived, so the calls only finish if all of them got as far as asking
# at the same time - which they can't if each one holds a thread while it waits.
arrived = threading.Condition()
waiting = {"n": 0, "expected": 0}


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _send(self, code, body):
        raw = body.encode()
        self.send_response(code)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path == "/hello":
            self._send(200, "hello " + str(self.headers.get("X-Wasm")))
        elif self.path == "/wait":
            with arrived:
                waiting["n"] += 1
                arrived.notify_all()
                # the cap is only so a broken build fails rather than hangs
                arrived.wait_for(lambda: waiting["n"] >= waiting["expected"], timeout=30)
            self._send(200, "waited")
        else:
            self._send(404, "nope")

    def do_POST(self):
        self._send(201, "posted")


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


web = Server(("127.0.0.1", WEB_PORT), Handler)
threading.Thread(target=web.serve_forever, daemon=True).start()
BASE = "http://127.0.0.1:%d" % WEB_PORT

print("start wasm function test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")


def refused(*args):
    try:
        r.execute_command(*args)
        return None
    except redis.exceptions.ResponseError as e:
        return str(e)


def setf(fname, module):
    assert r.execute_command("SETF", fname, module.build()) == b"OK", fname


def stat(section, field):
    return int(r.info(section).get(field, 0))


def rises(section, field, past):
    """waits for a counter to pass `past`; the cap is only so a broken build fails"""
    end = time.time() + 10
    while time.time() < end:
        if stat(section, field) > past:
            return True
        time.sleep(0.01)
    return False


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, single_connection_client=True)


try:
    # a module goes in and comes back out byte for byte, and runs
    ECHO = echo().build()
    assert r.execute_command("SETF", "wecho", ECHO) == b"OK"
    assert r.execute_command("GETF", "wecho") == ECHO
    assert r.execute_command("wecho", "hello") == b"hello"
    assert r.execute_command("CALLF", "wecho", "again") == b"again"
    assert r.execute_command("wecho", "") == b""
    e = refused("wecho")
    assert e and "wrong number of arguments" in e, e
    e = refused("wecho", "a", "b")
    assert e and "wrong number of arguments" in e, e

    # replies of every shape
    setf("warray", array_reply())
    assert r.execute_command("warray") == [7, b"ab", [], [-3]], r.execute_command("warray")

    # the store, through the same store_access a Luau function gets
    setf("wstore", store_roundtrip())
    assert r.execute_command("wstore", "k1", "v1") == b"v1"
    assert r.execute_command("GET", "k1") == b"v1"

    # barch.call: a command's reply, nil, and a command's error handed to the guest
    setf("wget", get_command())
    assert r.execute_command("wget", "k1") == b"v1"
    assert r.execute_command("wget", "missing") is None
    setf("wset", set_command())
    assert r.execute_command("wset", "k2", "v2") == b"OK"
    assert r.execute_command("GET", "k2") == b"v2"

    # a module's globals live as long as its instance, like a Luau state's
    setf("wcount", counter())
    first = r.execute_command("wcount")
    assert r.execute_command("wcount") == first + 1

    # what SETF refuses: a module that imports anything but barch's functions, a
    # barch name that isn't there or has the wrong type, no call, a start section,
    # a bad barch section, and AOT
    wasi = Module(extra_imports=[("wasi_snapshot_preview1", "fd_write",
                                  [I32, I32, I32, I32], [I32])])
    wasi.body = i32(0)
    e = refused("SETF", "wbad", wasi.build())
    assert e and "only import barch" in e, e
    nope = Module(extra_imports=[("barch", "nope", [], [])])
    nope.body = i32(0)
    e = refused("SETF", "wbad", nope.build())
    assert e and "no function nope" in e, e
    wrong = Module(extra_imports=[("barch", "arg_count", [I32], [I32])])
    wrong.body = i32(0)
    e = refused("SETF", "wbad", wrong.build())
    assert e and "no function arg_count" in e, e
    nocall = constant(b"x")
    nocall.export_call = False
    e = refused("SETF", "wbad", nocall.build())
    assert e and "export call" in e, e
    started = constant(b"x")
    started.start = True
    e = refused("SETF", "wbad", started.build())
    assert e and "start section" in e, e
    e = refused("SETF", "wbad", writes(b"not json").build())
    assert e and "barch section" in e, e
    e = refused("SETF", "wbad", b"\x00asm\x01\x00\x00\x00\x01\xff")
    assert e and "past its end" in e, e
    e = refused("SETF", "wbad", constant(b"x").build(), "AOT")
    assert e and "AOT" in e, e
    e = refused("wbad")
    assert e, "a refused module was stored anyway"

    # "write": false in the barch section: the store write is refused, with Luau's words
    setf("wwrite", writes())
    assert r.execute_command("wwrite") == 1
    setf("wreadonly", writes(b'{"write": false}'))
    e = refused("wreadonly")
    assert e and "can't write" in e, e

    # a trap ends the call and says why; the function's own error is passed on
    errors = stat("errors", "function_errors")
    setf("wtrap", trap())
    e = refused("wtrap")
    assert e and "trap: unreachable" in e, e
    setf("woob", out_of_bounds())
    e = refused("woob")
    assert e and "trap: out of bounds" in e, e
    setf("wfails", fails())
    e = refused("wfails")
    assert e and "nope" in e, e
    assert stat("errors", "function_errors") - errors == 3

    # the memory cap: growing past function_wasm_max_mb fails the grow, a small one works
    setf("wgrow", grow())
    assert r.execute_command("wgrow") == [-1, 1], r.execute_command("wgrow")

    # the instruction cap ends a runaway as a timeout, and the session is fine after
    r.execute_command("CONFIG SET function_wasm_insns 100000")
    try:
        timeouts = stat("errors", "function_timeouts")
        setf("wspin", spin())
        e = refused("wspin")
        assert e and "FUNCTION timeout" in e, e
        assert stat("errors", "function_timeouts") - timeouts == 1
    finally:
        r.execute_command("CONFIG SET function_wasm_insns 1000000000")
    assert r.execute_command("wecho", "still here") == b"still here"

    # TODO 821: a long call runs in slices, going back on the pool between them the way
    # a Luau call does, and still gives the right answer at the end
    r.execute_command("CONFIG SET function_slice_insns 100000")
    try:
        setf("wlong", count_to(3000000))
        slices = stat("memory", "wasm_slices")
        assert r.execute_command("wlong") == 3000000
        assert stat("memory", "wasm_slices") - slices >= 2, stat("memory", "wasm_slices") - slices

        # called from another WASM function it runs inside the caller, on the caller's
        # stack, to the end
        setf("wcalls", calls("wlong"))
        assert r.execute_command("wcalls") == 3000000

        # an endless loop ends at its deadline, sliced on the way, with the instruction
        # cap set as high as it goes
        r.execute_command("CONFIG SET function_deadline_ms 50")
        r.execute_command("CONFIG SET function_wasm_insns 2147483647")
        timeouts = stat("errors", "function_timeouts")
        slices = stat("memory", "wasm_slices")
        e = refused("wspin")
        assert e and "FUNCTION timeout" in e, e
        assert stat("errors", "function_timeouts") - timeouts == 1
        assert stat("memory", "wasm_slices") - slices >= 2, stat("memory", "wasm_slices") - slices
    finally:
        r.execute_command("CONFIG SET function_slice_insns 1000000")
        r.execute_command("CONFIG SET function_deadline_ms 1000")
        r.execute_command("CONFIG SET function_wasm_insns 1000000000")
    assert r.execute_command("wecho", "still here") == b"still here"

    # replacing a function: as with Luau, SETF ... RELOAD is what makes a session drop
    # the copy it has, and the next call runs the new module, Luau or WASM
    assert r.execute_command("SETF", "wecho", constant(b"v2").build(), "RELOAD") == b"OK"
    assert r.execute_command("wecho") == b"v2"
    assert r.execute_command("SETF", "wecho", "function call() return 'luau' end", "RELOAD") == b"OK"
    assert r.execute_command("wecho") == b"luau"
    assert r.execute_command("SETF", "wecho", constant(b"v3").build(), "RELOAD") == b"OK"
    assert r.execute_command("wecho") == b"v3"

    # and a Luau function can call a WASM one, which runs inline
    assert r.execute_command("SETF", "wfromluau",
                             "function call() return barch.call('CALLF', 'wecho') end") == b"OK"
    assert r.execute_command("wfromluau") == b"v3"

    # REMF ... RELOAD takes it away, from this session's copy too
    assert r.execute_command("REMF", "wecho", "RELOAD") == 1
    e = refused("CALLF", "wecho")
    assert e, "a removed function still ran"

    # TODO 822: walking the store, as barch.store.count and range
    for k in ("r:a", "r:b", "r:c", "s:x"):
        r.execute_command("SET", k, "1")
    setf("wcount3", counting("r:", "r:~"))
    assert r.execute_command("wcount3") == 3, r.execute_command("wcount3")
    setf("wrange", walk("r:", "r:~"))
    assert r.execute_command("wrange") == [b"r:a", b"r:b", b"r:c"], r.execute_command("wrange")
    setf("wrange2", walk("r:", "r:~", limit=2))
    assert r.execute_command("wrange2") == [b"r:a", b"r:b"], r.execute_command("wrange2")
    setf("wrange3", walk("r:", "r:~", limit=10, offset=1))
    assert r.execute_command("wrange3") == [b"r:b", b"r:c"], r.execute_command("wrange3")
    setf("wrangeneg", walk("r:", "r:~", offset=-1))
    e = refused("wrangeneg")
    assert e and "offset must not be negative" in e, e

    # the time and a UUIDv7, as barch.time and barch.uuid
    setf("wclock", clock())
    before = int(time.time() * 1000)
    ms, u1, u2 = r.execute_command("wclock")
    assert before - 1000 <= ms <= int(time.time() * 1000) + 1000, (before, ms)
    uuid_form = re.compile(rb"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
    assert uuid_form.match(u1) and uuid_form.match(u2), (u1, u2)
    assert u1 != u2

    # another key space, as barch.space.NAME: its keys, not the running space's
    other = client()
    other.execute_command("USE", "wother")
    other.execute_command("SET", "seed", "1")
    setf("wspace", in_space("wother"))
    assert r.execute_command("wspace") == [b"in wother", 1, 0, 2, b"in wother", 1], r.execute_command("wspace")
    assert other.execute_command("GET", "sk") == b"in wother"
    assert r.execute_command("EXISTS", "sk") == 0
    setf("wrangeother", walk("s", "t", space="wother"))
    assert r.execute_command("wrangeother") == [b"seed", b"sk"], r.execute_command("wrangeother")
    setf("wcountother", counting("s", "t", space="wother"))
    assert r.execute_command("wcountother") == 2
    setf("wnospace", opens("wnosuchspace"))
    e = refused("wnospace")
    assert e and "no key space called wnosuchspace" in e, e
    setf("wbadspace", bad_space())
    e = refused("wbadspace")
    assert e and "didn't open" in e, e

    # transactions, as barch.tx: an abort leaves nothing behind
    setf("wtxabort", tx_aborted())
    assert r.execute_command("wtxabort") == [b"OK", b"OK", 0], r.execute_command("wtxabort")
    assert r.execute_command("EXISTS", "txa") == 0

    # http_request: an answer, a header sent, a POST, and a refused method
    setf("whttp", http_get())
    assert r.execute_command("whttp", BASE + "/hello") == [200, b"hello yes"], r.execute_command("whttp", BASE + "/hello")
    assert r.execute_command("whttp", BASE + "/missing") == [404, b"nope"]
    setf("whttppost", http_get(1))
    assert r.execute_command("whttppost", BASE + "/hello") == [201, b"posted"]
    setf("whttpbad", http_get(9))
    e = refused("whttpbad", BASE + "/hello")
    assert e and "isn't one of 0 to 4" in e, e
    # without the outbound category it's refused, as Luau's http.request is
    r.execute_command("ACL", "SETUSER", "wnoout", "on", ">pw", "+read", "+write", "+keys", "+data",
                      "+function", "+connection")
    noout = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, username="wnoout", password="pw")
    try:
        noout.execute_command("whttp", BASE + "/hello")
        assert False, "http without outbound ran"
    except redis.exceptions.ResponseError as ex:
        assert "outbound category" in str(ex), str(ex)

    # a call waiting on a request parks: 16 of them at once, more than barch has I/O
    # threads and pool workers together, all get as far as the server before any answer
    N = 16
    with arrived:
        waiting["n"] = 0
        waiting["expected"] = N
    parks = stat("memory", "wasm_parks")
    got = [None] * N

    def ask(i):
        c = client()
        try:
            got[i] = c.execute_command("whttp", BASE + "/wait")
        except Exception as ex:
            got[i] = ex

    askers = [threading.Thread(target=ask, args=(i,)) for i in range(N)]
    for t in askers:
        t.start()
    for t in askers:
        t.join()
    assert all(g == [200, b"waited"] for g in got), got
    assert stat("memory", "wasm_parks") - parks >= N, stat("memory", "wasm_parks") - parks

    # a write to a key another session's pessimistic transaction holds parks too, and
    # goes on when that transaction commits: a SET through call, as Luau's barch.call,
    # and a store write in a pessimistic transaction of its own, as barch.store in
    # barch.tx (a plain store write outside one doesn't lock, in Luau either)
    r.execute_command("CONFIG SET tx_lock_wait_ms 30000")
    setf("wtxstore", tx_store_set("lk", "from tx"))

    def parks_on_lock(cmd, want):
        holder = client()
        assert holder.execute_command("BEGINTX", "PESSIMISTIC") == b"OK"
        assert holder.execute_command("SET", "lk", "holder") is True
        parks = stat("memory", "wasm_parks")
        out = {}

        def waiter():
            try:
                out["v"] = client().execute_command(*cmd)
            except Exception as ex:
                out["v"] = ex

        w = threading.Thread(target=waiter)
        w.start()
        assert rises("memory", "wasm_parks", parks), "%s never parked" % (cmd,)
        assert "v" not in out, out
        assert holder.execute_command("COMMITTX") == b"OK"
        w.join()
        assert out["v"] == want, out

    parks_on_lock(("wset", "lk", "waiter"), b"OK")
    assert r.execute_command("GET", "lk") == b"waiter"
    parks_on_lock(("wtxstore",), [b"OK", 1, b"OK"])
    assert r.execute_command("GET", "lk") == b"from tx"

    # the open question: a transaction stays open across slices and a park, since it
    # belongs to the session, not to whichever thread runs the slice
    r.execute_command("CONFIG SET function_slice_insns 100000")
    try:
        setf("wtxlong", tx_flow(3000000))
        slices = stat("memory", "wasm_slices")
        parks = stat("memory", "wasm_parks")
        answer = r.execute_command("wtxlong", BASE + "/hello")
        assert answer == [b"OK", 200, 1, b"OK", 0], answer
        assert stat("memory", "wasm_slices") - slices >= 2, stat("memory", "wasm_slices") - slices
        assert stat("memory", "wasm_parks") - parks >= 1, stat("memory", "wasm_parks") - parks
        assert r.execute_command("GET", "txk") == b"in tx"
    finally:
        r.execute_command("CONFIG SET function_slice_insns 1000000")

    # TODO 823: transports in the barch section. A resp transport exposes commands
    # that run other exports, with their own arity and categories
    METHODS = {"getname": {"export": "get_name", "categories": ["read"], "arity": 1},
               "SetName": {"export": "set_name", "categories": ["write", "data"], "arity": 2}}
    setf("wnames", resp_methods(METHODS))
    assert r.execute_command("GETNAME", "k1") == b"get:k1", r.execute_command("GETNAME", "k1")
    assert r.execute_command("SETNAME", "nk", "nv") == 1
    assert r.execute_command("GET", "nk") == b"nv"
    assert r.execute_command("wnames") == b"call"
    e = refused("GETNAME")
    assert e and "wrong number of arguments for 'GETNAME'" in e, e
    e = refused("SETNAME", "only")
    assert e and "wrong number of arguments for 'SETNAME'" in e, e
    # the categories count: a user with only read rights gets GETNAME, not SETNAME
    r.execute_command("ACL", "SETUSER", "wreader", "on", ">wpw", "+read", "+function", "+data")
    reader = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, username="wreader",
                         password="wpw")
    assert reader.execute_command("GETNAME", "k2") == b"get:k2"
    try:
        reader.execute_command("SETNAME", "nk", "other")
        assert False, "SETNAME ran for a user with no write rights"
    except redis.exceptions.ResponseError as ex:
        assert "not authorized" in str(ex), ex
    assert r.execute_command("GET", "nk") == b"nv"
    # a resp module still needs call, as a Luau one does
    e = refused("SETF", "wnamesonly",
                resp_methods({"GETNAME2": {"export": "get_name", "arity": 1}}, with_call=False).build())
    assert e and "has to export call" in e, e

    # what SETF refuses about a transport
    for methods, words in (({"M1": {"export": "nothing_here"}}, "doesn't export"),
                           ({"M1": {"export": "takes_two"}}, "doesn't export"),
                           ({"M1": {"export": "get_name", "categories": ["nonsense"]}}, "unknown category"),
                           ({"M.1": {"export": "get_name"}}, "'.' not allowed")):
        e = refused("SETF", "wbadresp", resp_methods(methods).build())
        assert e and words in e, (methods, e)
    e = refused("SETF", "wbadkind", declares({"kind": "http", "routes": {}}).build())
    assert e and "resp, cron, queue or websocket" in e, e
    e = refused("SETF", "wnothing", declares({}).build())
    assert e and "needs a kind" in e, e
    nocall = Module([])
    nocall.export_call = False
    nocall.body = i32(0)
    e = refused("SETF", "wnocall", nocall.build())
    assert e and "call" in e, e

    # a cron job declared by a module, calling a WASM function
    setf("WTICK", incr("wticks"))
    assert r.execute_command("WTICK") is None   # it replies nothing
    r.execute_command("DEL", "wticks")
    e = refused("configuration:SETF", "cron/jobs/wbad",
                declares({"kind": "cron", "space": "default", "call": "WTICK", "user": "default"}).build())
    assert e and "exactly one of every or cron" in e, e
    assert r.execute_command("configuration:SETF", "cron/jobs/wticker", declares(
        {"kind": "cron", "space": "default", "call": "WTICK", "every": "100ms", "user": "default"}).build()) == b"OK"
    end = time.time() + 10   # the cap is only so a broken build fails
    while time.time() < end and int(r.get("wticks") or 0) < 2:
        time.sleep(0.05)
    assert int(r.get("wticks") or 0) >= 2, r.get("wticks")
    st = r.execute_command("FUNCTIONS", "CRON").decode()
    assert "name=wticker" in st and "call=WTICK" in st, st
    assert r.execute_command("configuration:SETF", "cron/jobs/wticker", declares(
        {"kind": "cron", "space": "default", "call": "WTICK", "every": "100ms", "user": "default",
         "enabled": False}).build()) == b"OK"

    # a queue declared by a module, with a WASM handler
    QDIR = os.path.abspath("wasm_queues")
    shutil.rmtree(QDIR, ignore_errors=True)
    os.makedirs(QDIR)
    r.execute_command("CONFIG", "SET", "queue_dir", QDIR)
    setf("WQ", keeps_message("wqlast"))
    assert r.execute_command("configuration:SETF", "queues/wq", declares(
        {"kind": "queue", "name": "wq", "space": "default", "call": "WQ", "user": "default"}).build()) == b"OK"
    r.execute_command("QUEUE", "PUSH", "wq", "first message")
    end = time.time() + 10
    while time.time() < end and r.get("wqlast") is None:
        time.sleep(0.05)
    assert r.get("wqlast") == b"first message", r.get("wqlast")
    e = refused("configuration:SETF", "queues/wq2",
                declares({"kind": "queue", "space": "default", "call": "WQ", "user": "default"}).build())
    assert e and "needs a name" in e, e

    # TODO 823: locked regions. store_locked runs one of the module's functions, from
    # its table, with the key's shard (or the whole space) write locked
    setf("wlocked", locked_region(has_lock_plus_ctx))
    assert r.execute_command("wlocked", "lk") == 108, r.execute_command("wlocked", "lk")
    assert r.execute_command("GET", "lk") == b"in"
    # a region holds one shard, or the whole space, so a write to another shard is refused
    setf("wshard", shard_of())
    near = r.execute_command("wshard", "lk")
    other = next(k for k in ("k%d" % i for i in range(1000)) if r.execute_command("wshard", k) != near)
    setf("wlockfar", locked_region(writes_lockedk))
    r.execute_command("DEL", "lockedk")
    near_lk = next(k for k in (other, "lk") if r.execute_command("wshard", k) != r.execute_command("wshard", "lockedk"))
    e = refused("wlockfar", near_lk)
    assert e and "second shard" in e, e
    assert r.get("lockedk") is None
    setf("wlockallfar", locked_region(writes_lockedk, whole=True))
    assert r.execute_command("wlockallfar", "lk") == 1
    assert r.get("lockedk") == b"in"
    setf("wlockall", locked_region(has_lock_plus_ctx, whole=True))
    assert r.execute_command("wlockall", "lk") == 108

    # read, add one, write back under the lock, from 8 clients at once, loses nothing
    r.execute_command("DEL", "lcount")
    setf("wbump", locked_region(bumps, region_locals=1))
    lost = []

    def bump():
        cl = client()
        for _ in range(50):
            try:
                cl.execute_command("wbump", "lcount")
            except Exception as ex:
                lost.append(str(ex))

    ts = [threading.Thread(target=bump) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not lost, lost[:3]
    assert struct.unpack("<q", r.get("lcount"))[0] == 400, r.get("lcount")

    # what a region can't do, and the lock is back each time
    for region, words in ((calls_get, "not allowed inside a locked region"),
                          (nests, "already inside a locked region"),
                          (runs_away, "locked region ran too long"),
                          (traps, "unreachable")):
        setf("wlockbad", locked_region(region))
        e = refused("wlockbad", "lk")
        assert e and words in e, (region.__name__, e)
        assert client().execute_command("SET", "lk", region.__name__)
    for slot in (0, 2, 9):
        setf("wlockslot", locked_region(has_lock_plus_ctx, fn=slot))
        e = refused("wlockslot", "lk")
        assert e and "store_locked needs one of the module's own functions" in e, (slot, e)

    # TODO 823: a .wasm file is a stored function in LOADKEYS and in a function
    # repository, the same as a .luau one, and the rest stays keys
    print("wasm files in LOADKEYS and a function repository")
    LDIR = os.path.abspath("wasm_load")
    shutil.rmtree(LDIR, ignore_errors=True)
    os.makedirs(os.path.join(LDIR, "sub"))

    def put_file(rel, body):
        with open(os.path.join(LDIR, rel), "wb") as f:
            f.write(body)

    put_file("wfile.wasm", constant(b"from a file").build())
    put_file("wlua.luau", b'function call() return "from luau" end\n')
    put_file(os.path.join("sub", "wnested.wasm"), constant(b"nested").build())
    put_file("plain.txt", b"just data")
    out = [x.decode() for x in r.execute_command("LOADKEYS", LDIR, "wl")]
    assert "functions=3" in out and "keys=1" in out, out
    assert r.execute_command("wfile") == b"from a file"
    assert r.execute_command("wnested") == b"nested"
    assert r.execute_command("wlua") == b"from luau"
    assert r.get("wl:plain.txt") == b"just data"
    # a .wasm that isn't a module is refused like any bad function, and nothing is written
    put_file("wbroken.wasm", b"\0asm not really")
    put_file("wfile.wasm", constant(b"changed").build())
    e = None
    try:
        r.execute_command("LOADKEYS", LDIR, "wl")
    except redis.ResponseError as x:
        e = str(x)
    assert e and "wbroken" in e.lower(), e
    assert r.execute_command("wfile") == b"from a file"
    names = {n.decode() for n in r.execute_command("KEYSF")}
    assert "WBROKEN" not in names, names
    os.remove(os.path.join(LDIR, "wbroken.wasm"))
    # two files for one name
    put_file("wfile.luau", b'function call() return "luau" end\n')
    e = None
    try:
        r.execute_command("LOADKEYS", LDIR, "wl")
    except redis.ResponseError as x:
        e = str(x)
    assert e and "two files map to function WFILE" in e, e
    os.remove(os.path.join(LDIR, "wfile.luau"))

    # the same directory as a function repository: a change is picked up, a removal
    # removes the function
    for f in ("wlua.luau", "plain.txt"):
        os.remove(os.path.join(LDIR, f))
    shutil.rmtree(os.path.join(LDIR, "sub"))
    put_file("wsynced.wasm", constant(b"synced v1").build())
    r.execute_command("CONFIG", "SET", "functions_dir", LDIR)
    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"
    assert r.execute_command("wsynced") == b"synced v1"
    assert r.execute_command("wfile") == b"changed"
    put_file("wsynced.wasm", constant(b"synced v2").build())
    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"
    assert r.execute_command("wsynced") == b"synced v2"
    os.remove(os.path.join(LDIR, "wsynced.wasm"))
    assert r.execute_command("FUNCTIONS", "SYNC") == b"OK"
    names = {n.decode() for n in r.execute_command("KEYSF")}
    assert "WSYNCED" not in names and "WFILE" in names, names
    r.execute_command("CONFIG", "SET", "functions_dir", "")

    print("complete wasm function test")
finally:
    try:
        barch.stop()
    except Exception:
        pass
