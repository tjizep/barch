# MySQL foreign driver. env: and file: that cannot be resolved leave the
# space off. A live GET runs when BARCH_MYSQL_DSN is set, or when docker
# can start a mysql container, and the client library was linked.
import os
import tempfile
import redis
import barch
import foreign_sql

PORT = 14083

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True, protocol=2)
conf = barch.KeyValue("configuration")


def option(conn, space, name):
    conn.execute_command("USE", space)
    return conn.execute_command("KSPACE", "OPTION", "GET", name)


print("start foreign mysql test")

# --- mysql without a dsn or host stays off --------------------------------------
conf.set("fm_nodsn.foreign", "mysql")
conf.save()
assert option(r, "fm_nodsn", "FOREIGN") == "off"

# --- a dsn without a query stays off --------------------------------------------
conf.set("fm_noq.foreign_dsn", "host=127.0.0.1")
conf.set("fm_noq.foreign", "mysql")
conf.save()
assert option(r, "fm_noq", "FOREIGN") == "off"

# --- env: of an unset variable leaves foreign off --------------------------------
conf.set("fm_env.foreign_dsn", "env:BARCH_FOREIGN_DSN_UNSET")
conf.set("fm_env.foreign_query", "SELECT v FROM t WHERE k = ?")
conf.set("fm_env.foreign", "mysql")
conf.save()
assert option(r, "fm_env", "FOREIGN") == "off"

# --- file: of a missing path leaves foreign off ----------------------------------
conf.set("fm_miss.foreign_dsn", "file:/no/such/barch/mysql.dsn")
conf.set("fm_miss.foreign_query", "SELECT v FROM t WHERE k = ?")
conf.set("fm_miss.foreign", "mysql")
conf.save()
assert option(r, "fm_miss", "FOREIGN") == "off"

# --- file: of an empty file leaves foreign off -----------------------------------
empty = tempfile.NamedTemporaryFile(prefix="barch-mysql-empty-", suffix=".dsn", delete=False)
empty.close()
conf.set("fm_empty.foreign_dsn", "file:" + empty.name)
conf.set("fm_empty.foreign_query", "SELECT v FROM t WHERE k = ?")
conf.set("fm_empty.foreign", "mysql")
conf.save()
assert option(r, "fm_empty", "FOREIGN") == "off"

# --- a query without ?, $n or $$ is left off ------------------------------------
conf.set("fm_noqm.foreign_dsn", "host=127.0.0.1")
conf.set("fm_noqm.foreign_query", "SELECT 1")
conf.set("fm_noqm.foreign", "mysql")
conf.save()
assert option(r, "fm_noqm", "FOREIGN") == "off"

# --- $0 / $1 and $$ are enough to turn the space on -----------------------------
conf.set("fm_macro.foreign_dsn", "host=127.0.0.1")
conf.set("fm_macro.foreign_query", "SELECT name FROM $k.person WHERE surname = $0 AND age = $1")
conf.set("fm_macro.foreign", "mysql")
conf.set("fm_encq.foreign_dsn", "host=127.0.0.1")
conf.set("fm_encq.foreign_query", "SELECT v FROM t WHERE k = $$")
conf.set("fm_encq.foreign", "mysql")
conf.set("fm_esc.foreign_dsn", "host=127.0.0.1")
conf.set("fm_esc.foreign_query", r"SELECT v FROM t WHERE k = ? AND label <> '\$0'")
conf.set("fm_esc.foreign", "mysql")
conf.save()

# --- a dummy host + query turns the space on only when the client is linked ------
conf.set("fm_probe.foreign_dsn", "host=127.0.0.1")
conf.set("fm_probe.foreign_query", "SELECT v FROM t WHERE k = ?")
conf.set("fm_probe.foreign", "mysql")
conf.save()
probe = option(r, "fm_probe", "FOREIGN")
built = probe == "mysql"
if built:
    assert option(r, "fm_macro", "FOREIGN") == "mysql"
    assert option(r, "fm_encq", "FOREIGN") == "mysql"
    assert option(r, "fm_esc", "FOREIGN") == "mysql"
    dsn_file = tempfile.NamedTemporaryFile(
        prefix="barch-mysql-", suffix=".dsn", delete=False, mode="w"
    )
    dsn_file.write("host=127.0.0.1 user=nobody\n")
    dsn_file.close()
    conf.set("fm_fileok.foreign_dsn", "file:" + dsn_file.name)
    conf.set("fm_fileok.foreign_query", "SELECT v FROM t WHERE k = ?")
    conf.set("fm_fileok.foreign", "mysql")
    conf.save()
    assert option(r, "fm_fileok", "FOREIGN") == "mysql"
else:
    assert probe == "off"

# --- live fill: BARCH_MYSQL_DSN, else a docker mysql if docker is there ----------
box = None
dsn = os.environ.get("BARCH_MYSQL_DSN")
owned = False
if not built:
    print("SKIP: mysql client not built")
elif dsn:
    pass
else:
    box = foreign_sql.start_mysql()
    if box:
        dsn = box.dsn
        owned = True

if dsn and built:
    os.environ["BARCH_MYSQL_LIVE"] = dsn
    query = os.environ.get("BARCH_MYSQL_QUERY", "SELECT v FROM t WHERE k = ?")
    try:
        conf.set("fm_live.foreign_dsn", "env:BARCH_MYSQL_LIVE")
        conf.set("fm_live.foreign_query", query)
        conf.set("fm_live.foreign", "mysql")
        conf.save()
        kind = option(r, "fm_live", "FOREIGN")
        assert kind == "mysql", kind
        r.execute_command("USE", "fm_live")
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
                conf.set("fm_comp.foreign_dsn", "env:BARCH_MYSQL_LIVE")
                conf.set(
                    "fm_comp.foreign_query",
                    "SELECT name FROM $k.person WHERE surname = $0 AND age = $1",
                )
                conf.set("fm_comp.foreign", "mysql")
                conf.save()
                assert option(r, "fm_comp", "FOREIGN") == "mysql"
                r.execute_command("USE", "fm_comp")
                assert r.get("Smith 42") == "Jane"
                assert r.get("Smith 42") == "Jane"
                assert r.get("Jones 1") is None
                conf.set("fm_colon.key_split", ":")
                conf.set("fm_colon.foreign_dsn", "env:BARCH_MYSQL_LIVE")
                conf.set(
                    "fm_colon.foreign_query",
                    "SELECT name FROM person WHERE surname = $0 AND age = $1",
                )
                conf.set("fm_colon.foreign", "mysql")
                conf.save()
                assert option(r, "fm_colon", "FOREIGN") == "mysql"
                r.execute_command("USE", "fm_colon")
                assert r.get("Smith:42") == "Jane"
                conf.set("fm_enc.foreign_dsn", "env:BARCH_MYSQL_LIVE")
                conf.set("fm_enc.foreign_query", "SELECT v FROM t WHERE k = $$")
                conf.set("fm_enc.foreign", "mysql")
                conf.save()
                assert option(r, "fm_enc", "FOREIGN") == "mysql"
                r.execute_command("USE", "fm_enc")
                assert r.get("Smith 42") == "whole"
                conf.set(
                    "fm_luau.foreign_script",
                    "-- fill\n"
                    "function resolve(key, space)\n"
                    "    local row = sql.query('SELECT v FROM t WHERE k = ?', key)\n"
                    "    if row == nil then return nil end\n"
                    "    return row[1]\n"
                    "end\n",
                )
                conf.set("fm_luau.foreign_dsn", "env:BARCH_MYSQL_LIVE")
                conf.set("fm_luau.foreign_query_timeout_ms", "5000")
                conf.set("fm_luau.foreign", "luau")
                conf.save()
                assert option(r, "fm_luau", "FOREIGN") == "luau"
                r.execute_command("USE", "fm_luau")
                assert r.get("sku") == "widget"
                assert r.get("nope") is None
                assert r.get("o'reilly") == "quoted"
                r.execute_command("USE", "fm_live")
                box.stop()
                box = None
                assert r.get("sku") == "widget", "cached value must survive the container"
                assert r.get("nope") is None, "tomb must survive the container"
                try:
                    r.get("other")
                    raise AssertionError("expected FOREIGN error after mysql stopped")
                except redis.ResponseError as e:
                    assert "FOREIGN" in str(e), e
        except redis.ResponseError as e:
            if "FOREIGN" in str(e) and not owned:
                print("SKIP: mysql driver on but server unreachable")
            else:
                raise
    finally:
        if box:
            box.stop()

print("complete foreign mysql test")
