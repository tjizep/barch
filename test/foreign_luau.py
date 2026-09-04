# Luau miss scripts: resolve(key, space) fills a GET. nil is a cached miss.
# require and io are refused. The instruction budget is a slice: a script
# that needs more than one still returns. A runaway dies on the query
# timeout, not on the first slice, and does not pin the foreign pool.
import scale
import threading
import time

import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

PORT = scale.port(default=14082)

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
conf = barch.KeyValue("configuration")


def option(conn, space, name):
    conn.execute_command("USE", space)
    return conn.execute_command("KSPACE", "OPTION", "GET", name)


# A fill script is a stored function now, not source in the configuration space -
# see TODO 139. Each case stores its script under the name the configuration points
# at, which it can only do once the space exists, so the order is: configure, USE
# (which builds the space), SETF the script, then fill.
SCRIPTS = {}


def install(space):
    """create the space, then store the fill script it names"""
    r.execute_command("USE", space)
    r.execute_command("SETF", "filler", SCRIPTS[space])


print("start foreign luau test")

# --- a script that returns a string, then a local hit ----------------------------
SCRIPTS["lx_val"] = "-- fill\nfunction call(key, space)\n    if key == 'sku' then return 'widget' end\n    return nil\nend\n"
conf.set("lx_val.foreign_script", "filler")
conf.set("lx_val.foreign", "luau")
conf.save()
assert option(r, "lx_val", "FOREIGN") == "luau"
install("lx_val")
assert r.get("sku") == "widget"
assert r.get("sku") == "widget"
assert r.get("nope") is None
assert r.get("nope") is None
assert r.exists("nope") == 0
assert r.ttl("nope") == -2

# --- a thrown error is not cached ------------------------------------------------
SCRIPTS["lx_err"] = "-- throw\nfunction call(key, space)\n    error('boom')\nend\n"
conf.set("lx_err.foreign_script", "filler")
conf.set("lx_err.foreign", "luau")
conf.save()
assert option(r, "lx_err", "FOREIGN") == "luau"
install("lx_err")
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
SCRIPTS["lx_req"] = "-- require\nfunction call(key, space)\n    require('barch')\n    return 'x'\nend\n"
conf.set("lx_req.foreign_script", "filler")
conf.set("lx_req.foreign", "luau")
conf.save()
assert option(r, "lx_req", "FOREIGN") == "luau"
install("lx_req")
try:
    r.get("x")
    raise AssertionError("expected require to fail")
except redis.ResponseError as e:
    assert "FOREIGN" in str(e), e

# --- io is not there -------------------------------------------------------------
SCRIPTS["lx_io"] = "-- io\nfunction call(key, space)\n    return io.open('/etc/passwd'):read('*a')\nend\n"
conf.set("lx_io.foreign_script", "filler")
conf.set("lx_io.foreign", "luau")
conf.save()
assert option(r, "lx_io", "FOREIGN") == "luau"
install("lx_io")
try:
    r.get("x")
    raise AssertionError("expected io to fail")
except redis.ResponseError as e:
    assert "FOREIGN" in str(e), e

# --- a script that needs more than one slice still returns ----------------------
SCRIPTS["lx_slice"] = '-- slice\nfunction call(key, space)\n    local s = 0\n    for i = 1, 20000 do s = s + 1 end\n    return tostring(s)\nend\n'
conf.set("lx_slice.foreign_script", "filler")
conf.set("lx_slice.foreign_script_insns", "200")
conf.set("lx_slice.foreign", "luau")
conf.save()
assert option(r, "lx_slice", "FOREIGN") == "luau"
install("lx_slice")
assert r.get("work") == "20000"

# --- a runaway dies on the query timeout, not the first slice -------------------
SCRIPTS["lx_bud"] = "-- budget\nfunction call(key, space)\n    local s = 0\n    while true do s = s + 1 end\n    return 'x'\nend\n"
conf.set("lx_bud.foreign_script", "filler")
conf.set("lx_bud.foreign_script_insns", "200")
conf.set("lx_bud.foreign_query_timeout_ms", "400")
conf.set("lx_bud.foreign", "luau")
conf.save()
assert option(r, "lx_bud", "FOREIGN") == "luau"
install("lx_bud")
try:
    r.get("x")
    raise AssertionError("expected script timeout")
except redis.ResponseError as e:
    assert "timeout" in str(e).lower() or "FOREIGN" in str(e), e

# --- four runaways must not pin the four foreign workers ------------------------
SCRIPTS["lx_hog"] = "-- hog\nfunction call(key, space)\n    local s = 0\n    while true do s = s + 1 end\n    return 'x'\nend\n"
conf.set("lx_hog.foreign_script", "filler")
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
SCRIPTS["lx_sql"] = "-- sql\nfunction call(key, space)\n    local row = sql.query('SELECT v FROM t WHERE k = ?', key)\n    if row == nil then return nil end\n    return row[1]\nend\n"
conf.set("lx_sql.foreign_script", "filler")
conf.set("lx_sql.foreign", "luau")
conf.save()
assert option(r, "lx_sql", "FOREIGN") == "luau"
install("lx_sql")
try:
    r.get("x")
    raise AssertionError("expected sql.query without a driver to fail")
except redis.ResponseError as e:
    assert "FOREIGN" in str(e), e

print("complete foreign luau test")

