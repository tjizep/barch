# Incoming keys split on a per-space regex. Unset still means a space.
# An invalid regex is ignored and the space split remains.
import redis
import barch

PORT = 14110

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
conf = barch.KeyValue("configuration")


def option(conn, space, name):
    conn.execute_command("USE", space)
    return conn.execute_command("KSPACE", "OPTION", "GET", name)


print("start key split test")

# --- colon --------------------------------------------------------------------
conf.set("ks_colon.key_split", ":")
conf.save()
assert option(r, "ks_colon", "KEY_SPLIT") == ":"
r.set("dept:42", "x")
assert r.get("dept:42") == "x"
assert r.get("dept 42") is None

# --- comma --------------------------------------------------------------------
conf.set("ks_csv.key_split", ",")
conf.save()
assert option(r, "ks_csv", "KEY_SPLIT") == ","
r.execute_command("USE", "ks_csv")
r.set("a,b", "y")
assert r.get("a,b") == "y"
assert r.get("a b") is None

# --- either colon or comma ----------------------------------------------------
conf.set("ks_re.key_split", "[:,]")
conf.save()
r.execute_command("USE", "ks_re")
r.set("p:q,r", "z")
assert r.get("p:q,r") == "z"

# --- invalid regex keeps the space split --------------------------------------
conf.set("ks_bad.key_split", "(")
conf.save()
assert option(r, "ks_bad", "KEY_SPLIT") == "("
r.execute_command("USE", "ks_bad")
r.set("left right", "ok")
assert r.get("left right") == "ok"

# --- foreign fill uses the same split ----------------------------------------
conf.set("ks_fk.key_split", ":")
conf.set("ks_fk.foreign", "fake")
conf.save()
assert option(r, "ks_fk", "FOREIGN") == "fake"
assert option(r, "ks_fk", "KEY_SPLIT") == ":"
r.execute_command("FOREIGN", "FAKE", "RESET")
r.execute_command("FOREIGN", "FAKE", "SET", "dept:42", "from-source")
assert r.get("dept:42") == "from-source"

# --- $n is those same parts ---------------------------------------------------
got = r.execute_command("FOREIGN", "FAKE", "PARTS", "dept:42")
assert got == ["dept", "42"], got
got = r.execute_command("FOREIGN", "FAKE", "PARTS", "a:b:c")
assert got == ["a", "b", "c"], got

conf.set("ks_sp.foreign", "fake")
conf.save()
assert option(r, "ks_sp", "FOREIGN") == "fake"
got = r.execute_command("FOREIGN", "FAKE", "PARTS", "Smith 42")
assert got == ["Smith", "42"], got

# --- n-gram frames: gram keeps its spaces, offset after | ----------------------
conf.set("ng.key_split", "|")
conf.save()
r.execute_command("USE", "ng")
r.set("his i|1", "1")
r.set("is is|2", "1")
r.set("is is|7", "1")
got = r.execute_command("RANGE", "is is|0", "is is|999999", 100)
assert got == ["is is|2", "is is|7"], got
assert r.get("is is|2") == "1"
assert r.get("is_is 2") is None

print("complete key split test")
