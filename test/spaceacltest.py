import redis
import barch

# Rights per key space: the differences from a user's global ones, so a space with no
# rule leaves them exactly as they are. See TODO 135.
#
#     KSPACE ACL [KSNAME] SETUSER alice -write
#
# Its own process because it writes users into the auth store.

PORT = 14099
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0)

print("start space acl test")


def as_user(user, secret):
    c = redis.Redis(host="127.0.0.1", port=PORT, db=0)
    assert c.execute_command("AUTH", user, secret) is True
    return c


def ok(conn, *cmd):
    """redis-py turns +OK into True only for the exact command name, so a prefixed
    `shut:SET` comes back as b'OK' where a bare `SET` comes back as True"""
    got = conn.execute_command(*cmd)
    assert got is True or got == b"OK", f"{cmd} answered {got!r}"
    return True


def refused(conn, *cmd):
    try:
        conn.execute_command(*cmd)
        return None
    except redis.exceptions.ResponseError as e:
        return str(e)


# two spaces, so there is something to tell apart
r.execute_command("open:SET", "k", "open value")
r.execute_command("shut:SET", "k", "shut value")

r.execute_command("ACL", "SETUSER", "alice", "on", ">s3cret", "+read", "+write", "+keys")
try:
    # with no per-space rule she can do both, everywhere
    a = as_user("alice", "s3cret")
    assert a.execute_command("open:GET", "k").decode() == "open value"
    ok(a, "shut:SET", "k", "written")

    # now take writing away in `shut` only
    assert r.execute_command("KSPACE", "ACL", "shut", "SETUSER", "alice",
                             "on", "-write") == b"OK"
    b = as_user("alice", "s3cret")
    ok(b, "open:SET", "k", "still fine")   # the rule names shut; open is untouched
    e = refused(b, "shut:SET", "k", "nope")
    assert e and "not authorized" in e, f"writing in shut should be refused, said: {e}"
    # reading there is still hers
    assert b.execute_command("shut:GET", "k").decode() == "written"

    # the same command name, two spaces, one connection - which is the case the
    # dispatcher used to get wrong: authorization sat inside the lookup cache, so a
    # repeated name was never checked again
    ok(b, "open:SET", "k", "one")
    assert refused(b, "shut:SET", "k", "two") is not None
    ok(b, "open:SET", "k", "three")        # the refusal in shut must not stick here
    assert refused(b, "shut:SET", "k", "four") is not None, \
        "and the success in open must not carry into shut"

    # what the rule says, read back
    shown = r.execute_command("KSPACE", "ACL", "shut", "GETUSER", "alice")
    flat = [x.decode().lstrip("$") for x in shown]
    assert "write" in flat and "false" in flat, f"GETUSER gave {flat}"

    # a rule can widen as well as narrow, which is what makes "only here" expressible
    assert r.execute_command("ACL", "SETUSER", "bob", "on", ">s3cret", "+read") == b"OK"
    assert r.execute_command("KSPACE", "ACL", "open", "SETUSER", "bob",
                             "on", "+write", "+keys") == b"OK"
    c = as_user("bob", "s3cret")
    ok(c, "open:SET", "k", "bob was here")
    assert refused(c, "shut:SET", "k", "no") is not None, "bob may only write in open"

    # and taking the rule away puts the user back to their global rights
    assert r.execute_command("KSPACE", "ACL", "shut", "DEL", "alice") == b"OK"
    d = as_user("alice", "s3cret")
    ok(d, "shut:SET", "k", "allowed again")

    # a secret is the user's, not the space's
    e = refused(r, "KSPACE", "ACL", "shut", "SETUSER", "alice", "on", ">other")
    assert e and "secret" in e, f"said: {e}"
    for conn in (a, b, c, d):
        conn.close()
finally:
    r.execute_command("ACL", "DEL", "alice")
    r.execute_command("ACL", "DEL", "bob")

r.close()
barch.stop()
print("complete space acl test")
