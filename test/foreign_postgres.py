# Postgres foreign driver. env: and file: that cannot be resolved leave the
# space off. A live GET runs when BARCH_POSTGRES_DSN is set, or when docker
# can start a postgres container, and libpq was linked.
import os
import tempfile
import redis
import barch
import foreign_sql

PORT = 14084

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
conf = barch.KeyValue("configuration")


def option(conn, space, name):
    conn.execute_command("USE", space)
    return conn.execute_command("KSPACE", "OPTION", "GET", name)


print("start foreign postgres test")

# --- postgres without a dsn or host stays off -----------------------------------
conf.set("fp_nodsn.foreign", "postgres")
conf.save()
assert option(r, "fp_nodsn", "FOREIGN") == "off"

# --- a dsn without a query stays off --------------------------------------------
conf.set("fp_noq.foreign_dsn", "host=127.0.0.1")
conf.set("fp_noq.foreign", "postgres")
conf.save()
assert option(r, "fp_noq", "FOREIGN") == "off"

# --- env: of an unset variable leaves foreign off --------------------------------
conf.set("fp_env.foreign_dsn", "env:BARCH_FOREIGN_DSN_UNSET")
conf.set("fp_env.foreign_query", "SELECT v FROM t WHERE k = ?")
conf.set("fp_env.foreign", "postgres")
conf.save()
assert option(r, "fp_env", "FOREIGN") == "off"

# --- file: of a missing path leaves foreign off ----------------------------------
conf.set("fp_miss.foreign_dsn", "file:/no/such/barch/postgres.dsn")
conf.set("fp_miss.foreign_query", "SELECT v FROM t WHERE k = ?")
conf.set("fp_miss.foreign", "postgres")
conf.save()
assert option(r, "fp_miss", "FOREIGN") == "off"

# --- file: of an empty file leaves foreign off -----------------------------------
empty = tempfile.NamedTemporaryFile(prefix="barch-pg-empty-", suffix=".dsn", delete=False)
empty.close()
conf.set("fp_empty.foreign_dsn", "file:" + empty.name)
conf.set("fp_empty.foreign_query", "SELECT v FROM t WHERE k = ?")
conf.set("fp_empty.foreign", "postgres")
conf.save()
assert option(r, "fp_empty", "FOREIGN") == "off"

# --- a query without ?, $n or $$ is left off ------------------------------------
conf.set("fp_noqm.foreign_dsn", "host=127.0.0.1")
conf.set("fp_noqm.foreign_query", "SELECT 1")
conf.set("fp_noqm.foreign", "postgres")
conf.save()
assert option(r, "fp_noqm", "FOREIGN") == "off"

# --- $0 / $1 and $$ are enough to turn the space on -----------------------------
conf.set("fp_macro.foreign_dsn", "host=127.0.0.1")
conf.set("fp_macro.foreign_query", "SELECT name FROM $k.person WHERE surname = $0 AND age = $1")
conf.set("fp_macro.foreign", "postgres")
conf.set("fp_encq.foreign_dsn", "host=127.0.0.1")
conf.set("fp_encq.foreign_query", "SELECT v FROM t WHERE k = $$")
conf.set("fp_encq.foreign", "postgres")
conf.set("fp_esc.foreign_dsn", "host=127.0.0.1")
conf.set("fp_esc.foreign_query", r"SELECT v FROM t WHERE k = ? AND label <> '\$0'")
conf.set("fp_esc.foreign", "postgres")
conf.save()

# --- a dummy host + query turns the space on only when libpq is linked -----------
conf.set("fp_probe.foreign_dsn", "host=127.0.0.1")
conf.set("fp_probe.foreign_query", "SELECT v FROM t WHERE k = ?")
conf.set("fp_probe.foreign", "postgres")
conf.save()
probe = option(r, "fp_probe", "FOREIGN")
built = probe == "postgres"
if built:
    assert option(r, "fp_macro", "FOREIGN") == "postgres"
    assert option(r, "fp_encq", "FOREIGN") == "postgres"
    assert option(r, "fp_esc", "FOREIGN") == "postgres"
    dsn_file = tempfile.NamedTemporaryFile(
        prefix="barch-pg-", suffix=".dsn", delete=False, mode="w"
    )
    dsn_file.write("host=127.0.0.1 user=nobody\n")
    dsn_file.close()
    conf.set("fp_fileok.foreign_dsn", "file:" + dsn_file.name)
    conf.set("fp_fileok.foreign_query", "SELECT v FROM t WHERE k = ?")
    conf.set("fp_fileok.foreign", "postgres")
    conf.save()
    assert option(r, "fp_fileok", "FOREIGN") == "postgres"
else:
    assert probe == "off"

# --- live fill: BARCH_POSTGRES_DSN, else a docker postgres if docker is there ----
box = None
dsn = os.environ.get("BARCH_POSTGRES_DSN")
owned = False
if not built:
    print("SKIP: postgres client not built")
elif dsn:
    pass
else:
    box = foreign_sql.start_postgres()
    if box:
        dsn = box.dsn
        owned = True

if dsn and built:
    os.environ["BARCH_POSTGRES_LIVE"] = dsn
    query = os.environ.get("BARCH_POSTGRES_QUERY", "SELECT v FROM t WHERE k = ?")
    try:
        conf.set("fp_live.foreign_dsn", "env:BARCH_POSTGRES_LIVE")
        conf.set("fp_live.foreign_query", query)
        conf.set("fp_live.foreign", "postgres")
        conf.save()
        kind = option(r, "fp_live", "FOREIGN")
        assert kind == "postgres", kind
        r.execute_command("USE", "fp_live")
        try:
            first = r.get("sku")
            assert r.get("sku") == first
            assert r.get("nope") is None
            assert r.get("nope") is None
            assert r.exists("nope") == 0
            if owned:
                assert first == "widget"
                assert r.get("o'reilly") == "quoted"
                assert r.get("x'OR'1'='1") == "safe"
                assert r.mget("sku", "nope") == ["widget", None]
                conf.set("fp_comp.foreign_dsn", "env:BARCH_POSTGRES_LIVE")
                conf.set(
                    "fp_comp.foreign_query",
                    "SELECT name FROM $k.person WHERE surname = $0 AND age = $1",
                )
                conf.set("fp_comp.foreign", "postgres")
                conf.save()
                assert option(r, "fp_comp", "FOREIGN") == "postgres"
                r.execute_command("USE", "fp_comp")
                assert r.get("Smith 42") == "Jane"
                assert r.get("Smith 42") == "Jane"
                assert r.get("Jones 1") is None
                conf.set("fp_colon.key_split", ":")
                conf.set("fp_colon.foreign_dsn", "env:BARCH_POSTGRES_LIVE")
                conf.set(
                    "fp_colon.foreign_query",
                    "SELECT name FROM person WHERE surname = $0 AND age = $1",
                )
                conf.set("fp_colon.foreign", "postgres")
                conf.save()
                assert option(r, "fp_colon", "FOREIGN") == "postgres"
                r.execute_command("USE", "fp_colon")
                assert r.get("Smith:42") == "Jane"
                conf.set("fp_enc.foreign_dsn", "env:BARCH_POSTGRES_LIVE")
                conf.set("fp_enc.foreign_query", "SELECT v FROM t WHERE k = $$")
                conf.set("fp_enc.foreign", "postgres")
                conf.save()
                assert option(r, "fp_enc", "FOREIGN") == "postgres"
                r.execute_command("USE", "fp_enc")
                assert r.get("Smith 42") == "whole"
                conf.set(
                    "fp_luau.foreign_script",
                    "-- fill\n"
                    "function resolve(key, space)\n"
                    "    local row = sql.query('SELECT v FROM t WHERE k = ?', key)\n"
                    "    if row == nil then return nil end\n"
                    "    return row[1]\n"
                    "end\n",
                )
                conf.set("fp_luau.foreign_dsn", "env:BARCH_POSTGRES_LIVE")
                conf.set("fp_luau.foreign_query_timeout_ms", "5000")
                conf.set("fp_luau.foreign", "luau")
                conf.save()
                assert option(r, "fp_luau", "FOREIGN") == "luau"
                r.execute_command("USE", "fp_luau")
                assert r.get("sku") == "widget"
                assert r.get("nope") is None
                assert r.get("o'reilly") == "quoted"
                r.execute_command("USE", "fp_live")
                box.stop()
                box = None
                assert r.get("sku") == "widget", "cached value must survive the container"
                assert r.get("nope") is None, "tomb must survive the container"
                try:
                    r.get("other")
                    raise AssertionError("expected FOREIGN error after postgres stopped")
                except redis.ResponseError as e:
                    assert "FOREIGN" in str(e), e
        except redis.ResponseError as e:
            if "FOREIGN" in str(e) and not owned:
                print("SKIP: postgres driver on but server unreachable")
            else:
                raise
    finally:
        if box:
            box.stop()

print("complete foreign postgres test")
