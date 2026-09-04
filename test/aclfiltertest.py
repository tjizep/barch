import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# `ACL SETUSER ... ~pattern` used to be accepted and thrown away. acl_spec parses the
# pattern into `filters`, and nothing ever read that field, so a client asking for key
# level rights got OK and no rights - along with whatever categories it sent in the
# same command, which were applied while the pattern silently was not.
#
# Key patterns are refused until they mean something. See TODO 136.

PORT = scale.port(default=14000)
USER = "aclfiltertestuser"

print("start acl filter test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)

r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)


def getuser(name):
    return r.execute_command("ACL", "GETUSER", name)


try:
    # a pattern on its own is refused, and the refusal says what it did not like
    try:
        r.execute_command("ACL", "SETUSER", USER, "on", ">s3cret", "~key:*")
        assert False, "ACL SETUSER with a key pattern should have been refused"
    except redis.exceptions.ResponseError as e:
        assert "pattern" in str(e).lower(), f"the refusal should name the pattern, said: {e}"

    # and nothing was written on the way to refusing it
    assert not getuser(USER), f"{USER} should not exist after a refused SETUSER"

    # a pattern mixed in with categories is refused too. This is the case that used to
    # do real damage: the categories took effect and the pattern did not, so the user
    # ended up with rights nobody asked for
    try:
        r.execute_command("ACL", "SETUSER", USER, "on", ">s3cret", "+read", "~key:*")
        assert False, "ACL SETUSER with categories and a pattern should have been refused"
    except redis.exceptions.ResponseError as e:
        assert "pattern" in str(e).lower(), f"the refusal should name the pattern, said: {e}"

    assert not getuser(USER), f"{USER} should not exist after a refused SETUSER"

    # the ordinary form still works
    assert r.execute_command("ACL", "SETUSER", USER, "on", ">s3cret", "+read") == b"OK"
    cats = getuser(USER)
    assert cats, f"{USER} should exist after a plain SETUSER"
    names = {c.decode().lstrip("$") for c in cats[0::2]}
    assert "read" in names, f"+read should have been kept, got {sorted(names)}"
finally:
    r.execute_command("ACL", "DEL", USER)
    assert not getuser(USER), f"{USER} should be gone after ACL DEL"
    r.close()
    barch.stop()

print("complete acl filter test")
