import re
import redis
import barch

# exercises the "INFO MEMORY" section over RESP - the redis compatible fields, the
# barch specific extras and the startup memory baseline collected while key spaces load

PORT = 14000

# every field redis reports in its own memory section that barch has an answer for
REDIS_FIELDS = [
    "used_memory", "used_memory_human",
    "used_memory_rss", "used_memory_rss_human",
    "used_memory_peak", "used_memory_peak_human", "used_memory_peak_perc",
    "used_memory_overhead", "used_memory_startup",
    "used_memory_dataset", "used_memory_dataset_perc",
    "allocator_allocated", "allocator_active", "allocator_resident", "allocator_muzzy",
    "allocator_frag_ratio", "allocator_frag_bytes",
    "allocator_rss_ratio", "allocator_rss_bytes",
    "rss_overhead_ratio", "rss_overhead_bytes",
    "total_system_memory", "total_system_memory_human",
    "used_memory_lua", "used_memory_vm_eval", "used_memory_lua_human",
    "used_memory_scripts_eval", "number_of_cached_scripts",
    "number_of_functions", "number_of_libraries",
    "used_memory_vm_functions", "used_memory_vm_total", "used_memory_vm_total_human",
    "used_memory_functions", "used_memory_scripts", "used_memory_scripts_human",
    "maxmemory", "maxmemory_human", "maxmemory_policy",
    "mem_fragmentation_ratio", "mem_fragmentation_bytes",
    "mem_not_counted_for_evict", "mem_replication_backlog",
    "mem_total_replication_buffers", "mem_clients_slaves", "mem_clients_normal",
    "mem_cluster_links", "mem_aof_buffer", "mem_allocator",
    "mem_overhead_db_hashtable_lut", "mem_overhead_db_hashtable_rehashing",
    "active_defrag_running", "lazyfree_pending_objects", "lazyfreed_objects",
]

# the numbers redis has no equivalent for
BARCH_FIELDS = [
    "barch_keys", "barch_shards", "barch_pages", "barch_vmm_bytes_allocated",
    "barch_leaf_bytes_logical", "barch_leaf_bytes_physical",
    "barch_interior_bytes_logical", "barch_interior_bytes_physical",
    "barch_bytes_in_free_lists", "barch_value_bytes_compressed",
    "barch_leaf_nodes", "barch_size_4_nodes", "barch_size_16_nodes",
    "barch_size_48_nodes", "barch_size_256_nodes",
    "barch_pages_evicted", "barch_keys_evicted", "barch_pages_defragged",
    "barch_vmm_pages_defragged", "barch_vmm_pages_popped",
    "barch_oom_avoided_inserts", "barch_vacuum_count", "barch_last_vacuum_time",
]

# counters that must always be whole numbers - the ones that can go negative are
# checked separately since a difference is allowed to run the other way
UNSIGNED_FIELDS = [
    "used_memory", "used_memory_rss", "used_memory_peak", "used_memory_overhead",
    "used_memory_startup", "used_memory_dataset", "allocator_allocated",
    "allocator_active", "allocator_resident", "allocator_muzzy",
    "total_system_memory", "number_of_functions", "maxmemory",
    "mem_overhead_db_hashtable_lut", "active_defrag_running",
] + BARCH_FIELDS

SIGNED_FIELDS = [
    "allocator_frag_bytes", "allocator_rss_bytes",
    "rss_overhead_bytes", "mem_fragmentation_bytes",
]

RATIO_FIELDS = [
    "allocator_frag_ratio", "allocator_rss_ratio",
    "rss_overhead_ratio", "mem_fragmentation_ratio",
]

PERC_FIELDS = ["used_memory_peak_perc", "used_memory_dataset_perc"]

HUMAN_FIELDS = [
    "used_memory_human", "used_memory_rss_human", "used_memory_peak_human",
    "total_system_memory_human", "used_memory_lua_human",
    "used_memory_vm_total_human", "used_memory_scripts_human", "maxmemory_human",
]

# redis renders bytes as either a plain byte count or two decimals and a unit
HUMAN_RE = re.compile(r"^(\d+B|\d+\.\d{2}[KMGTP])$")
RATIO_RE = re.compile(r"^-?\d+\.\d{2}$")
PERC_RE = re.compile(r"^-?\d+\.\d{2}%$")


def parse_info(raw):
    """INFO arrives as a raw blob because redis-py only attaches its own parser to a
    bare 'INFO', so take the section header and the fields apart here."""
    if isinstance(raw, bytes):
        raw = raw.decode()
    section = None
    fields = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#"):
            section = line[1:].strip()
            continue
        name, sep, value = line.partition(":")
        assert sep, f"malformed info line {line!r}"
        fields[name] = value
    return section, fields


def info_memory(r, section="MEMORY"):
    got, fields = parse_info(r.execute_command(f"INFO {section}"))
    assert got == "Memory", f"expected a Memory section header, got {got!r}"
    return fields


print("start redis info memory test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, db=0)
r.execute_command("CLEARALL")

fields = info_memory(r)

# every documented field is present and nothing arrived empty
for name in REDIS_FIELDS + BARCH_FIELDS:
    assert name in fields, f"INFO MEMORY is missing {name}"
    assert fields[name] != "", f"INFO MEMORY left {name} empty"

for name in UNSIGNED_FIELDS:
    value = int(fields[name])
    assert value >= 0, f"{name} should not be negative, got {value}"

for name in SIGNED_FIELDS:
    int(fields[name])  # may legitimately be negative, just has to be a whole number

for name in RATIO_FIELDS:
    assert RATIO_RE.match(fields[name]), f"{name} is not a two decimal ratio: {fields[name]!r}"

for name in PERC_FIELDS:
    assert PERC_RE.match(fields[name]), f"{name} is not a two decimal percentage: {fields[name]!r}"

for name in HUMAN_FIELDS:
    assert HUMAN_RE.match(fields[name]), f"{name} is not a human byte count: {fields[name]!r}"

# the section name is matched case insensitively, like redis does it
assert sorted(info_memory(r, "memory").keys()) == sorted(fields.keys())

assert fields["mem_allocator"] in ("barch-vmm", "barch-heap")

# heap memory already counts the mapped vmm arena, so resident is what the process
# actually holds and the remainder is mapped but not paged in
used = int(fields["used_memory"])
rss = int(fields["used_memory_rss"])
assert rss > 0, "rss should have been read from /proc/self/statm"
assert int(fields["allocator_allocated"]) == used
assert int(fields["allocator_resident"]) == rss
assert int(fields["allocator_muzzy"]) == (used - rss if used > rss else 0)
assert int(fields["allocator_active"]) == used + int(fields["barch_bytes_in_free_lists"])
assert int(fields["allocator_frag_bytes"]) == int(fields["allocator_active"]) - used
assert int(fields["allocator_rss_bytes"]) == rss - int(fields["allocator_active"])
assert int(fields["mem_fragmentation_bytes"]) == rss - used
# barch's allocator is the process allocator so there is no rss overhead between them
assert int(fields["rss_overhead_bytes"]) == 0
assert fields["rss_overhead_ratio"] == "1.00"

assert used >= int(fields["used_memory_dataset"])
assert int(fields["used_memory_peak"]) >= used
assert int(fields["barch_shards"]) > 0
assert int(fields["number_of_functions"]) > 0
assert int(fields["total_system_memory"]) > 0

# the startup baseline is accumulated while key spaces load, so it has to be real
# and it has to stay put once the server is up
startup = int(fields["used_memory_startup"])
assert startup > 0, "used_memory_startup was never collected during key space load"
assert startup <= used

before_dataset = int(fields["used_memory_dataset"])
before_keys = int(fields["barch_keys"])

written = 2000
r.mset({f"info:memory:key:{i}": f"value-with-some-padding-{i}" for i in range(written)})

after = info_memory(r)
assert int(after["used_memory_startup"]) == startup, "startup memory moved after a write"
assert int(after["used_memory"]) > used, "used memory did not grow after writing"
assert int(after["used_memory_dataset"]) > before_dataset, "dataset did not grow after writing"
assert int(after["barch_keys"]) >= before_keys + written
assert int(after["barch_keys"]) >= r.dbsize()
assert int(after["used_memory_peak"]) >= int(after["used_memory"])

# the limit and the policy are read straight off the live configuration
r.execute_command("CONFIG SET max_memory_bytes 1073741824")
r.execute_command("CONFIG SET eviction_policy allkeys-lru")
configured = info_memory(r)
assert int(configured["maxmemory"]) == 1073741824
assert configured["maxmemory_human"] == "1.00G"
assert configured["maxmemory_policy"] == "allkeys-lru"

# barch calls having no policy "none", redis calls it "noeviction"
r.execute_command("CONFIG SET eviction_policy none")
assert info_memory(r)["maxmemory_policy"] == "noeviction"

# the other sections still work and an unknown one is still rejected
assert parse_info(r.execute_command("INFO SERVER"))[0] == "Server"
assert parse_info(r.execute_command("INFO SHARD 0"))[0] == "Shard"
try:
    r.execute_command("INFO NOSUCHSECTION")
    assert False, "an unknown INFO section should have been rejected"
except redis.exceptions.ResponseError:
    pass

r.close()
barch.stop()
print("complete redis info memory test")