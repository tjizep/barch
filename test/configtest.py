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
    "active_defrag", "compression", "eviction_policy", "external_host",
    "iteration_worker_count", "listen_port", "log_page_access_trace",
    "maintenance_poll_delay", "max_defrag_page_count", "max_memory_bytes",
    "max_modifications_before_save", "max_resp_connections", "max_scan_iterators",
    "min_compressed_size", "min_fragmentation_ratio", "ordered_keys",
    "pre_evict_thresh", "rpc_client_max_wait_ms", "rpc_max_buffer", "save_interval",
    "server_binding", "server_port", "static_bloom_filter",
    "tls_pem_certificate_chain_file", "tls_private_key_file", "tls_tmp_dh_file",
    "use_vmm_mem",
}

# what to set each one to. Chosen inside the domain the setter will accept - a plain
# "change it to something else" would be refused by the ones that validate a range or
# an enum.
NEW_VALUE = {
    "active_defrag": "off",
    "compression": "zstd",
    "eviction_policy": "allkeys-lru",
    "external_host": "someotherhost",
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
    "pre_evict_thresh": "0.75",
    "rpc_client_max_wait_ms": "15000",
    "rpc_max_buffer": "262144",
    "save_interval": "600000",
    "static_bloom_filter": "on",
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
assert set(everything) == EXPECTED, (
    f"CONFIG GET * does not match the registered set.\n"
    f"  missing: {sorted(EXPECTED - set(everything))}\n"
    f"  extra:   {sorted(set(everything) - EXPECTED)}")
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
    r.execute_command("CONFIG", "REWRITE")
    assert False, "CONFIG REWRITE should have been refused"
except redis.exceptions.ResponseError as e:
    assert "SET" in str(e) and "GET" in str(e), f"the refusal should name the keywords, said: {e}"

# --- and the same over RESP3, where the reply is a real map -----------------------
r3 = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=3)
resp3 = r3.execute_command("CONFIG", "GET", "*")
assert isinstance(resp3, dict), \
    f"CONFIG GET over RESP3 should be a map, got {type(resp3).__name__}"
assert {k.decode() for k in resp3} == EXPECTED
r3.close()

r.close()
barch.stop()
print("complete config test")
