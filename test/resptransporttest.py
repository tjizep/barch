# transport() with kind = "resp": one stored function key exposing several RESP
# commands under names of its own, each with its own ACL and replication
# categories. TODO 188.
import redis
import barch

PORT = 14112

print("start resp transport test", flush=True)
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")


def refused(conn, *args):
    try:
        conn.execute_command(*args)
        return None
    except redis.exceptions.ResponseError as e:
        return str(e)


NAMER = '''
function call() return "the key itself" end

function get_name(k) return "get:" .. k end
function set_name(k, v) return "set:" .. k .. "=" .. v end

function transport()
    return {
        kind = "resp",
        methods = {GETNAME = get_name, SETNAME = set_name},
        categories = {GETNAME = {"read"}, SETNAME = {"write", "data"}},
        arity = {GETNAME = 1, SETNAME = 2},
    }
end
'''

# no categories declared at all: allowed, and it falls back to needing only what
# calling any stored function needs
BARE = '''
function call() return "bare key" end
function plain() return "plain method" end

function transport()
    return {kind = "resp", methods = {PLAINONE = plain}}
end
'''

try:
    assert r.execute_command("SETF", "namer", NAMER) == b"OK"
    assert r.execute_command("SETF", "bare", BARE) == b"OK"

    print("the key name still works", flush=True)
    assert r.execute_command("NAMER") == b"the key itself"
    assert r.execute_command("BARE") == b"bare key"

    print("exposed names are commands of their own", flush=True)
    assert r.execute_command("GETNAME", "k1") == b"get:k1"
    assert r.execute_command("SETNAME", "k1", "v1") == b"set:k1=v1"
    assert r.execute_command("PLAINONE") == b"plain method"

    print("per method arity", flush=True)
    e = refused(r, "GETNAME", "a", "b")
    assert e and "wrong number of arguments" in e and "GETNAME" in e, e
    e = refused(r, "SETNAME", "only-one")
    assert e and "wrong number of arguments" in e, e

    print("an unknown category is refused at SETF", flush=True)
    e = refused(r, "SETF", "bad1", NAMER.replace('{"read"}', '{"nosuchcategory"}'))
    assert e and "unknown category 'nosuchcategory'" in e, e
    # and nothing was stored for it
    assert b"BAD1" not in r.execute_command("KEYSF", "*")

    print("a malformed resp transport is refused", flush=True)
    e = refused(r, "SETF", "bad2", '''
function call() return 1 end
function transport() return {kind = "resp"} end
''')
    assert e and "methods" in e, e
    e = refused(r, "SETF", "bad3", '''
function call() return 1 end
function transport() return {kind = "resp", methods = {NOTAFN = "no"}} end
''')
    assert e and "not a function" in e, e

    print("a dot is not allowed in an exposed name", flush=True)
    e = refused(r, "SETF", "bad4", '''
function call() return 1 end
function m() return 1 end
function transport() return {kind = "resp", methods = {["A.B"] = m}} end
''')
    assert e and "'.'" in e, e

    print("REMF takes the exposed names with it", flush=True)
    assert r.execute_command("REMF", "bare") == 1
    # The connection that already called it keeps answering, because a session runs
    # whatever it compiled the first time - see TODO 98 C. That is how a plain
    # stored function behaves after REMF too; exposed names inherit it rather than
    # introducing it. A connection that never saw the name does not know it.
    fresh = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    e = refused(fresh, "PLAINONE")
    assert e and "unknown command" in e, e
    # and the key it came from is gone for everyone
    e = refused(fresh, "BARE")
    assert e and "unknown command" in e, e

    # categories are what a caller needs, so two commands from one key are two
    # different rights
    print("FUNCTIONS COMMANDS lists what is exposed", flush=True)
    listed = [x.decode() for x in r.execute_command("FUNCTIONS", "COMMANDS")]
    assert "GETNAME NAMER read" in listed, listed
    assert "SETNAME NAMER write,data" in listed, listed
    # BARE went at REMF, so PLAINONE is not listed any more
    assert not any(x.startswith("PLAINONE") for x in listed), listed

    print("categories gate the commands separately", flush=True)
    assert r.execute_command(
        "ACL", "SETUSER", "reader", "on", ">s3cret", "+read", "+function") == b"OK"
    assert r.execute_command(
        "ACL", "SETUSER", "writer", "on", ">s3cret", "+write", "+data", "+function") == b"OK"

    reader = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    assert reader.execute_command("AUTH", "reader", "s3cret") is True
    assert reader.execute_command("GETNAME", "k9") == b"get:k9"
    e = refused(reader, "SETNAME", "k9", "v")
    assert e and "not authorized" in e, e

    writer = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
    assert writer.execute_command("AUTH", "writer", "s3cret") is True
    assert writer.execute_command("SETNAME", "k9", "v") == b"set:k9=v"
    e = refused(writer, "GETNAME", "k9")
    assert e and "not authorized" in e, e

    # Not covered here: that a write-category exposed command actually reaches a
    # replication destination. The dispatch does it on exactly the condition a
    # builtin uses - is_write() && is_data() && has_destinations(), then the same
    # repl::call(params) - but a single process publishing to itself does not
    # replicate a plain SET either, so a probe built that way proves nothing about
    # the new path. It wants a real two-node test.

    print("complete resp transport test")
finally:
    for u in ("reader", "writer"):
        try:
            r.execute_command("ACL", "DEL", u)
        except Exception:
            pass
    try:
        barch.stop()
    except Exception:
        pass
