# Luau miss scripts: resolve(key, space) fills a GET. nil is a cached miss.
# require and io are refused. The instruction budget is a slice: a script
# that needs more than one still returns. A runaway dies on the query
# timeout, not on the first slice, and does not pin the foreign pool.
import threading
import time

import redis
import barch

PORT = 14082

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
conf = barch.KeyValue("configuration")


def option(conn, space, name):
    conn.execute_command("USE", space)
    return conn.execute_command("KSPACE", "OPTION", "GET", name)


print("start foreign luau test")

# --- a script that returns a string, then a local hit ----------------------------
conf.set(
    "lx_val.foreign_script",
    "-- fill\n"
    "function resolve(key, space)\n"
    "    if key == 'sku' then return 'widget' end\n"
    "    return nil\n"
    "end\n",
)
conf.set("lx_val.foreign", "luau")
conf.save()
assert option(r, "lx_val", "FOREIGN") == "luau"
r.execute_command("USE", "lx_val")
assert r.get("sku") == "widget"
assert r.get("sku") == "widget"
assert r.get("nope") is None
assert r.get("nope") is None
assert r.exists("nope") == 0
assert r.ttl("nope") == -2

# --- a thrown error is not cached ------------------------------------------------
conf.set(
    "lx_err.foreign_script",
    "-- throw\n"
    "function resolve(key, space)\n"
    "    error('boom')\n"
    "end\n",
)
conf.set("lx_err.foreign", "luau")
conf.save()
assert option(r, "lx_err", "FOREIGN") == "luau"
r.execute_command("USE", "lx_err")
try:
    r.get("x")
    raise AssertionError("expected FOREIGN error")
except redis.ResponseError as e:
    assert "FOREIGN" in str(e), e
try:
    r.get("x")
    raise AssertionError("expected FOREIGN error again")
except redis.ResponseError as e:
    assert "FOREIGN" in str(e), e

# --- require is refused ----------------------------------------------------------
conf.set(
    "lx_req.foreign_script",
    "-- require\n"
    "function resolve(key, space)\n"
    "    require('barch')\n"
    "    return 'x'\n"
    "end\n",
)
conf.set("lx_req.foreign", "luau")
conf.save()
assert option(r, "lx_req", "FOREIGN") == "luau"
r.execute_command("USE", "lx_req")
try:
    r.get("x")
    raise AssertionError("expected require to fail")
except redis.ResponseError as e:
    assert "FOREIGN" in str(e), e

# --- io is not there -------------------------------------------------------------
conf.set(
    "lx_io.foreign_script",
    "-- io\n"
    "function resolve(key, space)\n"
    "    return io.open('/etc/passwd'):read('*a')\n"
    "end\n",
)
conf.set("lx_io.foreign", "luau")
conf.save()
assert option(r, "lx_io", "FOREIGN") == "luau"
r.execute_command("USE", "lx_io")
try:
    r.get("x")
    raise AssertionError("expected io to fail")
except redis.ResponseError as e:
    assert "FOREIGN" in str(e), e

# --- a script that needs more than one slice still returns ----------------------
conf.set(
    "lx_slice.foreign_script",
    "-- slice\n"
    "function resolve(key, space)\n"
    "    local s = 0\n"
    "    for i = 1, 20000 do s = s + 1 end\n"
    "    return tostring(s)\n"
    "end\n",
)
conf.set("lx_slice.foreign_script_insns", "200")
conf.set("lx_slice.foreign", "luau")
conf.save()
assert option(r, "lx_slice", "FOREIGN") == "luau"
r.execute_command("USE", "lx_slice")
assert r.get("work") == "20000"

# --- a runaway dies on the query timeout, not the first slice -------------------
conf.set(
    "lx_bud.foreign_script",
    "-- budget\n"
    "function resolve(key, space)\n"
    "    local s = 0\n"
    "    while true do s = s + 1 end\n"
    "    return 'x'\n"
    "end\n",
)
conf.set("lx_bud.foreign_script_insns", "200")
conf.set("lx_bud.foreign_query_timeout_ms", "400")
conf.set("lx_bud.foreign", "luau")
conf.save()
assert option(r, "lx_bud", "FOREIGN") == "luau"
r.execute_command("USE", "lx_bud")
try:
    r.get("x")
    raise AssertionError("expected script timeout")
except redis.ResponseError as e:
    assert "timeout" in str(e).lower() or "FOREIGN" in str(e), e

# --- four runaways must not pin the four foreign workers ------------------------
conf.set(
    "lx_hog.foreign_script",
    "-- hog\n"
    "function resolve(key, space)\n"
    "    local s = 0\n"
    "    while true do s = s + 1 end\n"
    "    return 'x'\n"
    "end\n",
)
conf.set("lx_hog.foreign_script_insns", "200")
conf.set("lx_hog.foreign_query_timeout_ms", "2500")
conf.set("lx_hog.foreign", "luau")
conf.save()
assert option(r, "lx_hog", "FOREIGN") == "luau"

hog_err = []


def hog(i):
    c = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
    c.execute_command("USE", "lx_hog")
    try:
        c.get("k%d" % i)
        hog_err.append("hog %d should have failed" % i)
    except redis.ResponseError as e:
        if "FOREIGN" not in str(e):
            hog_err.append(str(e))


threads = [threading.Thread(target=hog, args=(i,)) for i in range(4)]
for t in threads:
    t.start()
# give the four fetches a moment to occupy the pool
time.sleep(0.15)
cheap = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
cheap.execute_command("USE", "lx_val")
t0 = time.time()
assert cheap.get("sku") == "widget"
assert time.time() - t0 < 1.0, "cheap GET waited on the hog scripts"
for t in threads:
    t.join()
assert not hog_err, hog_err

# --- luau without a script stays off ---------------------------------------------
conf.set("lx_noscript.foreign", "luau")
conf.save()
assert option(r, "lx_noscript", "FOREIGN") == "off"

# --- sql.query without a SQL backend is an error, not a tomb ---------------------
conf.set(
    "lx_sql.foreign_script",
    "-- sql\n"
    "function resolve(key, space)\n"
    "    local row = sql.query('SELECT v FROM t WHERE k = ?', key)\n"
    "    if row == nil then return nil end\n"
    "    return row[1]\n"
    "end\n",
)
conf.set("lx_sql.foreign", "luau")
conf.save()
assert option(r, "lx_sql", "FOREIGN") == "luau"
r.execute_command("USE", "lx_sql")
try:
    r.get("x")
    raise AssertionError("expected sql.query without a driver to fail")
except redis.ResponseError as e:
    assert "FOREIGN" in str(e), e

print("complete foreign luau test")

