import redis
import barch

# Round trips every registered configuration variable over RESP: read it, set it to
# something else, read it back and check it took, then put the original back.
#
# CONFIG GET reads the live record rather than the reflection strings kept beside it,
# because those are only filled in once something has set the variable - one left at
# its default would read back empty. The values it produces are written in the form
# CONFIG SET accepts, which is what makes this round trip possible at all, so a
# variable that reads back in a shape it cannot be set from is a bug this catches.

PORT = 14000

# every variable barch registers. Kept here rather than taken from CONFIG GET *, so
# that a variable added to the server without being added to the reflection - or the
# other way round - shows up as a failure instead of being quietly skipped.
EXPECTED = {
    "active_defrag", "compression", "db_number_prefix", "eviction_policy",
    "external_host", "foreign_pool_max_age_ms", "foreign_script_insns",
    "foreign_timeout_ms", "function_deadline_ms", "function_max_depth",
    "function_slice_insns",
    "iteration_worker_count", "listen_port", "log_page_access_trace",
    "maintenance_poll_delay", "max_defrag_page_count", "max_memory_bytes",
    "max_modifications_before_save", "max_resp_connections", "max_scan_iterators",
    "min_compressed_size", "min_fragmentation_ratio", "ordered_keys", "hybrid_keys",
    "functions_dir", "functions_sync_ms", "functions_git_pull", "functions_git_branch",
    "functions_git_ssh_key",
    "pre_evict_thresh", "rpc_client_max_wait_ms", "rpc_max_buffer", "save_interval",
    "server_binding", "server_port", "static_bloom_filter",
    "tls_pem_certificate_chain_file", "tls_private_key_file", "tls_tmp_dh_file",
    "use_vmm_mem",
}

# the redis names barch also answers to, and the barch variable each one means. A
# redis client probes for these rather than for barch's own spellings, so CONFIG GET
# has to reach them and CONFIG SET has to resolve them to the variable underneath.
REDIS_ALIASES = {
    "maxmemory": "max_memory_bytes",
    "maxmemory-policy": "eviction_policy",
    "maxclients": "max_resp_connections",
    "bind": "server_binding",
    "port": "server_port",
    "tls-cert-file": "tls_pem_certificate_chain_file",
    "tls-key-file": "tls_private_key_file",
    "tls-dh-params-file": "tls_tmp_dh_file",
}

# redis settings barch reports truthfully but has no way to change. They read, and
# refuse to be set rather than accepting a write that would do nothing.
REDIS_READ_ONLY = {"appendonly", "appendfsync", "cluster-enabled", "daemonize",
                   "timeout", "save"}

REDIS_NAMES = set(REDIS_ALIASES) | REDIS_READ_ONLY
ALL_NAMES = EXPECTED | REDIS_NAMES

# aliases whose barch variable is safe to write - the rest point at the listening
# address, which NOT_WRITTEN already explains
ALIASES_WRITTEN = {n: b for n, b in REDIS_ALIASES.items() if b not in {"server_port", "server_binding"}}

# what to set each one to. Chosen inside the domain the setter will accept - a plain
# "change it to something else" would be refused by the ones that validate a range or
# an enum.
NEW_VALUE = {
    "active_defrag": "off",
    "compression": "zstd",
    # what SELECT <n> puts before the number to name the space. Any word without a colon
    # or a space in it is accepted; ':' is refused because it separates the key space
    # from the command in the prefixed form on the wire
    "db_number_prefix": "database",
    "eviction_policy": "allkeys-lru",
    "external_host": "someotherhost",
    "foreign_pool_max_age_ms": "15000",
    "foreign_script_insns": "2000000",
    "foreign_timeout_ms": "120000",
    "function_deadline_ms": "2500",
    "function_max_depth": "32",
    "function_slice_insns": "500000",
    "iteration_worker_count": "6",
    "log_page_access_trace": "on",
    "maintenance_poll_delay": "120",
    "max_defrag_page_count": "16",
    "max_memory_bytes": "34359738368",
    "max_modifications_before_save": "500000",
    "max_resp_connections": "1500",
    "max_scan_iterators": "64",
    "min_compressed_size": "128",
    "min_fragmentation_ratio": "0.4",
    "ordered_keys": "off",
    "hybrid_keys": "off",
    "functions_dir": "/tmp/barch-functions",
    "functions_sync_ms": "5000",
    "functions_git_pull": "on",
    "functions_git_branch": "master",
    "functions_git_ssh_key": "file:/tmp/barch-deploy-key",
    "pre_evict_thresh": "0.75",
    "rpc_client_max_wait_ms": "15000",
    "rpc_max_buffer": "262144",
    "save_interval": "600000",
    "static_bloom_filter": "off",
    "tls_pem_certificate_chain_file": "other.crt",
    "tls_private_key_file": "other.key",
    "tls_tmp_dh_file": "other.dh",
    "use_vmm_mem": "off",
}

# these decide where the server listens, so changing them out from under a live
# connection would take the connection with it. They are still read back, just not
# written - redispytest.py leaves the same three commented out for the same reason.
NOT_WRITTEN = {"server_port", "server_binding", "listen_port"}


def config_get(r, *patterns):
    res = r.execute_command("CONFIG", "GET", *patterns)
    if isinstance(res, dict):  # RESP3 hands back a map
        return {k.decode(): v.decode() for k, v in res.items()}
    return {res[i].decode(): res[i + 1].decode() for i in range(0, len(res), 2)}


print("start config test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)

# --- CONFIG GET reaches every registered variable --------------------------------
everything = config_get(r, "*")
assert set(everything) == ALL_NAMES, (
    f"CONFIG GET * does not match the registered set.\n"
    f"  missing: {sorted(ALL_NAMES - set(everything))}\n"
    f"  extra:   {sorted(set(everything) - ALL_NAMES)}")
for name, value in everything.items():
    assert value != "", f"{name} read back empty - the live record was not consulted"

# --- patterns behave like redis's ------------------------------------------------
assert set(config_get(r, "max_*")) == {n for n in EXPECTED if n.startswith("max_")}
assert set(config_get(r, "tls_*")) == {n for n in EXPECTED if n.startswith("tls_")}
one = config_get(r, "max_scan_iterators")
assert set(one) == {"max_scan_iterators"}, f"a single name should match only itself, got {set(one)}"
assert config_get(r, "nosuchvariable") == {}, "an unknown name should match nothing"
# several patterns at once, the union of what each matches
both = config_get(r, "tls_private_key_file", "max_scan_iterators")
assert set(both) == {"tls_private_key_file", "max_scan_iterators"}, f"got {set(both)}"
# and matching is case insensitive, as redis does it
assert set(config_get(r, "MAX_SCAN_ITERATORS")) == {"max_scan_iterators"}

# --- every variable is asked for individually, not only through the glob ----------
for name in sorted(EXPECTED):
    single = config_get(r, name)
    assert set(single) == {name}, f"CONFIG GET {name} answered {set(single)}"
    assert single[name] == everything[name], \
        f"{name} read differently on its own ({single[name]!r}) than through '*' ({everything[name]!r})"

# --- set each one, read it back, put it back -------------------------------------
missing_value = (EXPECTED - NOT_WRITTEN) - set(NEW_VALUE)
assert not missing_value, f"no test value chosen for {sorted(missing_value)}"

for name in sorted(EXPECTED - NOT_WRITTEN):
    original = everything[name]
    wanted = NEW_VALUE[name]
    if wanted == original and wanted in ("on", "off"):
        wanted = "off" if original == "on" else "on"
    assert wanted != original, \
        f"the test value for {name} is already its current value, so it proves nothing"

    assert r.execute_command("CONFIG", "SET", name, wanted) == b"OK", f"could not set {name}"
    got = config_get(r, name)[name]
    assert got == wanted, f"{name} was set to {wanted!r} but read back as {got!r}"

    # and the original goes back, which also proves the value CONFIG GET produced in
    # the first place is one CONFIG SET accepts
    assert r.execute_command("CONFIG", "SET", name, original) == b"OK", \
        f"could not restore {name} to the value CONFIG GET produced: {original!r}"
    restored = config_get(r, name)[name]
    assert restored == original, f"{name} restored to {restored!r}, expected {original!r}"

# the ones left alone still have to read back
for name in sorted(NOT_WRITTEN):
    assert config_get(r, name)[name] != "", f"{name} should still be readable"

# --- the shape of the reply -------------------------------------------------------
raw = r.execute_command("CONFIG", "GET", "max_scan_iterators")
assert isinstance(raw, (list, dict)), f"CONFIG GET should answer a map, got {type(raw).__name__}"
if isinstance(raw, list):
    assert len(raw) == 2, f"one variable should be one name and one value, got {raw!r}"

# an unsupported keyword is refused, and says what is supported
try:
    r.execute_command("CONFIG", "FROBNICATE")
    assert False, "an unknown CONFIG keyword should have been refused"
except redis.exceptions.ResponseError as e:
    assert "SET" in str(e) and "GET" in str(e), f"the refusal should name the keywords, said: {e}"

# --- the other subcommands --------------------------------------------------------
# REWRITE is answered, but with the error redis gives when it was started without a
# config file, which is barch's permanent situation - it has none of its own
try:
    r.execute_command("CONFIG", "REWRITE")
    assert False, "CONFIG REWRITE should have said there is no config file"
except redis.exceptions.ResponseError as e:
    assert "config file" in str(e), f"REWRITE should say why, said: {e}"

assert r.execute_command("CONFIG", "HELP"), "CONFIG HELP should answer something"

# RESETSTAT clears the counters but must leave the gauges alone - a gauge is what the
# server is holding right now, and several are decremented later, so zeroing one would
# wrap it rather than reset it
def stats(conn):
    flat = conn.execute_command("STATS")
    out = {}
    for i in range(0, len(flat) - 1, 2):
        k = flat[i].decode() if isinstance(flat[i], bytes) else str(flat[i])
        out[k] = flat[i + 1]
    return out

for i in range(50):
    r.execute_command("SET", f"cfgstat{i}", "v")
before = stats(r)
assert r.execute_command("CONFIG", "RESETSTAT") == b"OK"
after = stats(r)
assert int(after["leaf_nodes"]) == int(before["leaf_nodes"]), \
    f"leaf_nodes is a gauge and should survive RESETSTAT: {before['leaf_nodes']} -> {after['leaf_nodes']}"
assert int(after["vacuum_count"]) < int(before["vacuum_count"]) or int(before["vacuum_count"]) == 0, \
    "vacuum_count is a counter and should have been reset"

# --- the redis names -------------------------------------------------------------
# each alias reads the same value as the barch variable it means
for alias, barch_name in REDIS_ALIASES.items():
    a = config_get(r, alias)[alias]
    b = config_get(r, barch_name)[barch_name]
    if alias == "maxmemory-policy":
        # barch spells "do not evict" as none, redis as noeviction
        b = "noeviction" if b in ("none", "no", "nil", "null") else b
    assert a == b, f"{alias} read {a!r} but {barch_name} read {b!r}"

# a write through the alias lands on the barch variable
assert r.execute_command("CONFIG", "SET", "maxclients", "1234") == b"OK"
assert config_get(r, "max_resp_connections")["max_resp_connections"] == "1234"
assert config_get(r, "maxclients")["maxclients"] == "1234"

# redis byte units, including the ones barch's own parser does not take. redis reads
# k as 1000 and kb as 1024, so these are not interchangeable
for written, expected in [("512mb", "536870912"), ("512m", "512000000"),
                          ("1gb", "1073741824"), ("1k", "1000"), ("1kb", "1024")]:
    assert r.execute_command("CONFIG", "SET", "maxmemory", written) == b"OK", \
        f"CONFIG SET maxmemory {written} was refused"
    got = config_get(r, "maxmemory")["maxmemory"]
    assert got == expected, f"maxmemory {written} became {got}, expected {expected}"

# the policy name translates both ways
assert r.execute_command("CONFIG", "SET", "maxmemory-policy", "noeviction") == b"OK"
assert config_get(r, "eviction_policy")["eviction_policy"] == "none"
assert config_get(r, "maxmemory-policy")["maxmemory-policy"] == "noeviction"
assert r.execute_command("CONFIG", "SET", "maxmemory-policy", "allkeys-lru") == b"OK"
assert config_get(r, "maxmemory-policy")["maxmemory-policy"] == "allkeys-lru"

# and the ones barch cannot change refuse, saying why rather than just failing
for name in sorted(REDIS_READ_ONLY):
    try:
        r.execute_command("CONFIG", "SET", name, "yes")
        assert False, f"CONFIG SET {name} should have been refused"
    except redis.exceptions.ResponseError as e:
        assert name in str(e), f"the refusal for {name} should name it, said: {e}"

# --- and the same over RESP3, where the reply is a real map -----------------------
r3 = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=3)
resp3 = r3.execute_command("CONFIG", "GET", "*")
assert isinstance(resp3, dict), \
    f"CONFIG GET over RESP3 should be a map, got {type(resp3).__name__}"
assert {k.decode() for k in resp3} == ALL_NAMES
r3.close()

r.close()
barch.stop()
print("complete config test")
