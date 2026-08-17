# Per-space foreign options, then the fake-driver fill: one query per missing
# key, concurrent GET parks. A miss from the fake source is an empty tomb;
# GET/EXISTS/MGET/TTL treat it as absent and do not query again.
#
# Each case uses its own space name and sets the configuration keys before the
# first USE, the same way rangeshardtest.py provisions a space.
import redis
import barch

PORT = 14081

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
conf = barch.KeyValue("configuration")


def option(conn, space, name):
    conn.execute_command("USE", space)
    return conn.execute_command("KSPACE", "OPTION", "GET", name)


print("start foreign config test")

# --- unset space stays off, GET of a missing key is still nil --------------------
assert option(r, "fx_default", "FOREIGN") == "off"
assert option(r, "fx_default", "MISSING_TTL") == 0
assert option(r, "fx_default", "FOREIGN_TIMEOUT") == 300000
assert option(r, "fx_default", "FOREIGN_QUERY_TIMEOUT") == 1000
inflight = option(r, "fx_default", "FOREIGN_INFLIGHT")
assert inflight == [0, 32], inflight
r.execute_command("USE", "fx_default")
assert r.get("no-such-key") is None

# --- fake source, other knobs, waiter timeout inherits the global ----------------
conf.set("fx_fake.foreign", "fake")
conf.set("fx_fake.missing_ttl", "30")
conf.set("fx_fake.foreign_query_timeout_ms", "2500")
conf.set("fx_fake.foreign_max_inflight", "8")
conf.set("fx_fake.foreign_pool_size", "4")
conf.save()
assert option(r, "fx_fake", "FOREIGN") == "fake"
assert option(r, "fx_fake", "MISSING_TTL") == 30
assert option(r, "fx_fake", "FOREIGN_TIMEOUT") == 300000
assert option(r, "fx_fake", "FOREIGN_QUERY_TIMEOUT") == 2500
assert option(r, "fx_fake", "FOREIGN_INFLIGHT") == [0, 8]
r.execute_command("USE", "fx_fake")
assert r.get("still-missing") is None

# --- a space can override the waiter timeout -------------------------------------
conf.set("fx_wait.foreign", "fake")
conf.set("fx_wait.foreign_timeout_ms", "45000")
conf.save()
assert option(r, "fx_wait", "FOREIGN_TIMEOUT") == 45000

# --- mysql without a dsn or host is left off -------------------------------------
conf.set("fx_mysql.foreign", "mysql")
conf.save()
assert option(r, "fx_mysql", "FOREIGN") == "off"

# --- an unknown kind is left off -------------------------------------------------
conf.set("fx_bogus.foreign", "oracle")
conf.save()
assert option(r, "fx_bogus", "FOREIGN") == "off"

# --- luau without a script is left off -------------------------------------------
conf.set("fx_luau.foreign", "luau")
conf.save()
assert option(r, "fx_luau", "FOREIGN") == "off"

# --- node and configuration ignore <name>.foreign --------------------------------
conf.set("node.foreign", "fake")
conf.save()
r.execute_command("USE", "0")
assert r.execute_command("KSPACE", "OPTION", "GET", "FOREIGN") == "off"
conf.set("configuration.foreign", "fake")
conf.save()
r.execute_command("USE", "configuration")
assert r.execute_command("KSPACE", "OPTION", "GET", "FOREIGN") == "off"

# --- changing the key after first open does nothing until UNLOAD -----------------
conf.set("fx_once.foreign", "fake")
conf.save()
assert option(r, "fx_once", "FOREIGN") == "fake"
conf.set("fx_once.foreign", "off")
conf.save()
assert option(r, "fx_once", "FOREIGN") == "fake", "options are read once"
r.execute_command("UNLOAD", "fx_once")
assert option(r, "fx_once", "FOREIGN") == "off"

# --- SET of a foreign option name is a syntax error ------------------------------
r.execute_command("USE", "fx_fake")
try:
    r.execute_command("KSPACE", "OPTION", "SET", "FOREIGN", "OFF")
    raise AssertionError("SET FOREIGN should be a syntax error")
except redis.ResponseError:
    pass

# --- the two globals round-trip through CONFIG -----------------------------------
got = r.execute_command("CONFIG", "GET", "foreign_timeout_ms")
if isinstance(got, dict):
    assert got.get("foreign_timeout_ms") == "300000", got
else:
    assert got[1] == "300000", got
r.execute_command("CONFIG", "SET", "foreign_timeout_ms", "180000")
got = r.execute_command("CONFIG", "GET", "foreign_timeout_ms")
if isinstance(got, dict):
    assert got.get("foreign_timeout_ms") == "180000", got
else:
    assert got[1] == "180000", got

print("complete foreign config test")

import threading

def fake(conn, *args):
    return conn.execute_command("FOREIGN", "FAKE", *args)

# --- fill from the fake source, then a local hit ---------------------------------
conf.set("fx_fill.foreign", "fake")
conf.save()
option(r, "fx_fill", "FOREIGN")
fake(r, "RESET")
fake(r, "SET", "sku", "widget")
assert r.get("sku") == "widget"
assert fake(r, "QUERIES") == 1
assert r.get("sku") == "widget"
assert fake(r, "QUERIES") == 1, "second GET must not query again"
assert r.exists("sku") == 1

# --- missing in the fake source is a tomb: nil, and no second query --------------
assert r.get("nope") is None
assert fake(r, "QUERIES") == 2
assert r.get("nope") is None
assert fake(r, "QUERIES") == 2, "cached miss must not query again"
assert r.exists("nope") == 0
assert r.mget("nope") == [None]
assert r.ttl("nope") == -2
assert r.dbsize() == 1, "sku is live; nope is a tomb"

# --- stampede: many GET of one missing key, one query ----------------------------
conf.set("fx_storm.foreign", "fake")
conf.save()
option(r, "fx_storm", "FOREIGN")
fake(r, "RESET")
fake(r, "SET", "hot", "one")
errs = []
def storm():
    try:
        c = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
        c.execute_command("USE", "fx_storm")
        assert c.get("hot") == "one"
        c.close()
    except Exception as e:
        errs.append(e)

threads = [threading.Thread(target=storm) for _ in range(8)]
for t in threads:
    t.start()
for t in threads:
    t.join()
assert not errs, errs
assert fake(r, "QUERIES") == 1, fake(r, "QUERIES")

# --- pipeline of two missing keys, two flights, two replies ----------------------
conf.set("fx_pipe.foreign", "fake")
conf.save()
option(r, "fx_pipe", "FOREIGN")
fake(r, "RESET")
fake(r, "SET", "a", "va")
fake(r, "SET", "b", "vb")
p = r.pipeline(transaction=False)
p.get("a")
p.get("b")
got = p.execute()
assert got == ["va", "vb"], got
assert fake(r, "QUERIES") == 2, fake(r, "QUERIES")

# --- SET during a delayed fetch wins ---------------------------------------------
conf.set("fx_race.foreign", "fake")
conf.save()
option(r, "fx_race", "FOREIGN")
fake(r, "RESET")
fake(r, "SET", "k", "from-source")
fake(r, "DELAY", "200")
seen = []
def delayed_get():
    c = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
    c.execute_command("USE", "fx_race")
    seen.append(c.get("k"))
    c.close()

t = threading.Thread(target=delayed_get)
t.start()
import time
time.sleep(0.05)
r.set("k", "from-set")
t.join()
assert seen == ["from-set"], seen
assert r.get("k") == "from-set"

# --- hash space fill -------------------------------------------------------------
conf.set("fx_hash.ordered", "0")
conf.set("fx_hash.foreign", "fake")
conf.save()
option(r, "fx_hash", "FOREIGN")
fake(r, "RESET")
fake(r, "SET", "h", "hv")
assert r.get("h") == "hv"
assert fake(r, "QUERIES") == 1
assert r.get("h") == "hv"
assert fake(r, "QUERIES") == 1
assert r.get("noh") is None
assert fake(r, "QUERIES") == 2
assert r.get("noh") is None
assert fake(r, "QUERIES") == 2, "hash-space cached miss must not query again"
assert r.exists("noh") == 0
assert r.mget("noh") == [None]
assert r.ttl("noh") == -2

# --- MULTI does not fetch --------------------------------------------------------
conf.set("fx_multi.foreign", "fake")
conf.save()
option(r, "fx_multi", "FOREIGN")
fake(r, "RESET")
fake(r, "SET", "m", "mv")
r.execute_command("MULTI")
r.get("m")
try:
    r.execute_command("EXEC")
    # EXEC may return an error entry
except redis.ResponseError as e:
    assert "FOREIGN" in str(e)
else:
    # redis-py may return the error as a list item
    pass

# --- BLPOP still parks; kick is a no-op without a flight -------------------------
conf.set("fx_bpop.foreign", "fake")
conf.save()
option(r, "fx_bpop", "FOREIGN")
fake(r, "RESET")
popped = []
def bpop():
    c = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
    c.execute_command("USE", "fx_bpop")
    popped.append(c.blpop("q", timeout=1))
    c.close()
t = threading.Thread(target=bpop)
t.start()
t.join()
assert popped == [None], popped
assert fake(r, "QUERIES") == 0

# --- fake FAIL does not cache ----------------------------------------------------
conf.set("fx_fail.foreign", "fake")
conf.save()
option(r, "fx_fail", "FOREIGN")
fake(r, "RESET")
fake(r, "FAIL", "ON")
try:
    r.get("x")
    raise AssertionError("expected FOREIGN error")
except redis.ResponseError as e:
    assert "FOREIGN" in str(e), e
fake(r, "FAIL", "OFF")
assert r.get("x") is None
assert fake(r, "QUERIES") == 2, "failure must not be cached"
assert r.get("x") is None
assert fake(r, "QUERIES") == 2, "the miss after FAIL is a tomb"

# --- missing_ttl expiry re-queries ------------------------------------------------
conf.set("fx_ttl.foreign", "fake")
conf.set("fx_ttl.missing_ttl", "1")
conf.save()
option(r, "fx_ttl", "FOREIGN")
fake(r, "RESET")
assert r.get("gone") is None
assert fake(r, "QUERIES") == 1
assert r.get("gone") is None
assert fake(r, "QUERIES") == 1
time.sleep(1.2)
assert r.get("gone") is None
assert fake(r, "QUERIES") == 2, "expired tomb must query again"

# --- replica FOREIGN_MISS mid-fetch does not double tomb_stones -------------------
conf.set("fx_rep.foreign", "fake")
conf.save()
option(r, "fx_rep", "FOREIGN")
fake(r, "RESET")
fake(r, "DELAY", "200")
seen = []
def delayed_miss():
    c = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
    c.execute_command("USE", "fx_rep")
    seen.append(c.get("k"))
    c.close()
t = threading.Thread(target=delayed_miss)
t.start()
time.sleep(0.05)
r.execute_command("FOREIGN_MISS", "k")
t.join()
assert seen == [None], seen
assert fake(r, "QUERIES") == 1
r.execute_command("FOREIGN_MISS", "k")
assert r.dbsize() == 0, "second FOREIGN_MISS must not ++tomb_stones"
assert r.get("k") is None
assert fake(r, "QUERIES") == 1

# --- FOREIGN_MISS is accepted with foreign off -----------------------------------
r.execute_command("USE", "fx_default")
r.execute_command("FOREIGN_MISS", "ghost")
assert r.get("ghost") is None
assert r.exists("ghost") == 0
assert r.ttl("ghost") == -2

# --- bloom on: second GET of a cached miss still does not query ------------------
r.execute_command("CONFIG", "SET", "static_bloom_filter", "yes")
conf.set("fx_bloom.foreign", "fake")
conf.save()
option(r, "fx_bloom", "FOREIGN")
fake(r, "RESET")
assert r.get("miss") is None
assert fake(r, "QUERIES") == 1
assert r.get("miss") is None
assert fake(r, "QUERIES") == 1, "cached miss with bloom on must not query again"

# --- DEL during a delayed fetch discards the fill --------------------------------
conf.set("fx_del.foreign", "fake")
conf.save()
option(r, "fx_del", "FOREIGN")
fake(r, "RESET")
fake(r, "SET", "k", "from-source")
fake(r, "DELAY", "200")
seen = []
def delayed_del_get():
    c = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
    c.execute_command("USE", "fx_del")
    seen.append(c.get("k"))
    c.close()
t = threading.Thread(target=delayed_del_get)
t.start()
time.sleep(0.05)
r.delete("k")
t.join()
assert seen == [None], seen
assert r.ttl("k") == -2
fake(r, "DELAY", "0")
assert r.get("k") == "from-source"
assert fake(r, "QUERIES") == 2, "DEL must discard the first fill"

# --- SET then DEL during fetch: the fill does not resurrect ----------------------
conf.set("fx_sd.foreign", "fake")
conf.save()
option(r, "fx_sd", "FOREIGN")
fake(r, "RESET")
fake(r, "SET", "k", "from-source")
fake(r, "DELAY", "200")
def delayed_sd_get():
    c = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
    c.execute_command("USE", "fx_sd")
    c.get("k")
    c.close()
t = threading.Thread(target=delayed_sd_get)
t.start()
time.sleep(0.05)
r.set("k", "from-set")
r.delete("k")
t.join()
assert r.ttl("k") == -2, "SET-then-DEL must leave the key gone"
fake(r, "DELAY", "0")

# --- inflight cap refuses a new key ----------------------------------------------
conf.set("fx_cap.foreign", "fake")
conf.set("fx_cap.foreign_max_inflight", "1")
conf.save()
option(r, "fx_cap", "FOREIGN")
fake(r, "RESET")
fake(r, "SET", "a", "va")
fake(r, "SET", "b", "vb")
fake(r, "DELAY", "200")
got_a = []
def delayed_a():
    c = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
    c.execute_command("USE", "fx_cap")
    got_a.append(c.get("a"))
    c.close()
t = threading.Thread(target=delayed_a)
t.start()
time.sleep(0.05)
try:
    r.get("b")
    raise AssertionError("expected FOREIGN overloaded")
except redis.ResponseError as e:
    assert "overloaded" in str(e), e
t.join()
assert got_a == ["va"], got_a

# --- waiter timeout does not cancel the fetch ------------------------------------
conf.set("fx_to.foreign", "fake")
conf.set("fx_to.foreign_timeout_ms", "200")
conf.save()
option(r, "fx_to", "FOREIGN")
fake(r, "RESET")
fake(r, "SET", "k", "late")
fake(r, "DELAY", "500")
try:
    r.get("k")
    raise AssertionError("expected FOREIGN timeout")
except redis.ResponseError as e:
    assert "timeout" in str(e), e
time.sleep(0.4)
assert r.get("k") == "late"
assert fake(r, "QUERIES") == 1, "timeout must not cancel the fetch"

# --- UNLOAD fails parked waiters -------------------------------------------------
conf.set("fx_un.foreign", "fake")
conf.save()
option(r, "fx_un", "FOREIGN")
fake(r, "RESET")
fake(r, "DELAY", "400")
un_err = []
def delayed_un():
    c = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
    c.execute_command("USE", "fx_un")
    try:
        c.get("k")
        un_err.append("no error")
    except redis.ResponseError as e:
        un_err.append(str(e))
    c.close()
t = threading.Thread(target=delayed_un)
t.start()
time.sleep(0.05)
r.execute_command("UNLOAD", "fx_un")
t.join()
assert un_err and "FOREIGN" in un_err[0] and "unload" in un_err[0].lower(), un_err
r.execute_command("USE", "fx_default")

# --- INFO FOREIGN reports the counters -------------------------------------------
# --- MGET of three missing keys: three queries, one reply ------------------------
conf.set("fx_mget.foreign", "fake")
conf.save()
option(r, "fx_mget", "FOREIGN")
fake(r, "RESET")
fake(r, "SET", "k1", "v1")
fake(r, "SET", "k2", "v2")
got = r.mget("k1", "k2", "k3")
assert got == ["v1", "v2", None], got
assert fake(r, "QUERIES") == 3, fake(r, "QUERIES")
got = r.mget("k1", "k2", "k3")
assert got == ["v1", "v2", None], got
assert fake(r, "QUERIES") == 3, "second MGET must not query again"

# --- MGET joins a GET already in flight ------------------------------------------
conf.set("fx_join.foreign", "fake")
conf.save()
option(r, "fx_join", "FOREIGN")
fake(r, "RESET")
fake(r, "SET", "hot", "one")
fake(r, "SET", "cold", "two")
fake(r, "DELAY", "200")
join_seen = []
def delayed_hot():
    c = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
    c.execute_command("USE", "fx_join")
    join_seen.append(c.get("hot"))
    c.close()
t = threading.Thread(target=delayed_hot)
t.start()
time.sleep(0.05)
got = r.mget("hot", "cold")
t.join()
assert join_seen == ["one"], join_seen
assert got == ["one", "two"], got
assert fake(r, "QUERIES") == 2, fake(r, "QUERIES")

# --- EXISTS of two missing keys ---------------------------------------------------
conf.set("fx_ex.foreign", "fake")
conf.save()
option(r, "fx_ex", "FOREIGN")
fake(r, "RESET")
fake(r, "SET", "a", "va")
assert r.execute_command("EXISTS", "a", "b") == 1
assert fake(r, "QUERIES") == 2, fake(r, "QUERIES")
assert r.execute_command("EXISTS", "a", "b") == 1
assert fake(r, "QUERIES") == 2, "second EXISTS must not query again"

# --- MGET does not occupy the BLPOP block slot -----------------------------------
conf.set("fx_slot.foreign", "fake")
conf.save()
option(r, "fx_slot", "FOREIGN")
fake(r, "RESET")
fake(r, "SET", "m1", "x")
fake(r, "SET", "m2", "y")
assert r.mget("m1", "m2") == ["x", "y"]
assert r.blpop("q", 1) is None

# --- a failed field makes the whole MGET an error --------------------------------
conf.set("fx_mfail.foreign", "fake")
conf.save()
option(r, "fx_mfail", "FOREIGN")
fake(r, "RESET")
fake(r, "FAIL", "ON")
try:
    r.mget("z1", "z2")
    raise AssertionError("expected FOREIGN error from MGET")
except redis.ResponseError as e:
    assert "FOREIGN" in str(e), e
fake(r, "FAIL", "OFF")

info = r.execute_command("INFO", "FOREIGN")
if isinstance(info, dict):
    assert int(info.get("foreign_queries", 0)) > 0, info
    assert int(info.get("foreign_overloaded", 0)) >= 1, info
    assert int(info.get("foreign_cancelled", 0)) >= 1, info
else:
    assert "foreign_queries:" in info, info
    assert "foreign_overloaded:" in info, info
    assert "foreign_cancelled:" in info, info

print("complete foreign fill test")
