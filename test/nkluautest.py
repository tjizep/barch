# NumKong scalars and vectors in stored Luau: construct, + - *, compare.
import redis
import barch

PORT = 14086

print("start nk luau test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")


def refused(*args):
    try:
        r.execute_command(*args)
        return None
    except redis.exceptions.ResponseError as e:
        return str(e)


try:
    assert r.execute_command("SETF", "nkscalar", '''
        function call()
            local a = nkf32(2)
            local b = nk.f32(3)
            local s = a + b
            local d = nkf64(10) - nkf64(4)
            local p = nkf16(4) * nkf16(5)
            local q = nkbf16(9) / nkbf16(3)
            return {
                s:tonumber(),
                d:tonumber(),
                p:tonumber(),
                q:tonumber(),
                (nkf32(1) < nkf32(2)),
                (nkf32(2) <= nkf32(2)),
                (nkf32(2) == nkf32(2)),
                (nkf32(2) == nkf32(3)),
                (-nkf32(4)):tonumber(),
            }
        end
    ''') == b"OK"
    got = r.execute_command("nkscalar")
    assert got[0] == 5, got
    assert got[1] == 6, got
    assert abs(got[2] - 20) < 0.02, got
    assert abs(got[3] - 3) < 0.05, got
    # RESP2 sends Luau false as nil
    assert got[4] and got[5] and got[6] and not got[7], got
    assert got[8] == -4, got

    assert r.execute_command("SETF", "nkvec", '''
        function call()
            local v = nkf32vector(1, 2, 3)
            local w = nk.vector.f32({4, 5, 6})
            local z = nk.f16.vector(2)
            local s = v + w
            local d = w - v
            local m = v * nkf32(2)
            local e = v * w
            local t = nkf64vector(3)
            t[1] = nkf64(7)
            t[2] = 8
            t[3] = 9
            local zf = nk.f32()
            return {
                #v,
                v[2]:tonumber(),
                s[3]:tonumber(),
                m[1]:tonumber(),
                z.size,
                z[1]:tonumber(),
                t[1]:tonumber() + t[2]:tonumber(),
                (v == nkf32vector(1, 2, 3)),
                (v == w),
                (-w)[1]:tonumber(),
                d[1]:tonumber(),
                e[2]:tonumber(),
                zf:tonumber(),
            }
        end
    ''') == b"OK"
    got = r.execute_command("nkvec")
    assert got[0] == 3, got
    assert got[1] == 2, got
    assert got[2] == 9, got
    assert got[3] == 2, got
    assert got[4] == 2, got
    assert got[5] == 0, got
    assert got[6] == 15, got
    assert got[7] and not got[8], got
    assert got[9] == -4, got
    assert got[10] == 3, got
    assert got[11] == 10, got
    assert got[12] == 0, got

    assert r.execute_command("SETF", "nkslice", '''
        function call()
            local v = nkf32vector(1, 2, 3)
            local a = v:slice(1, 5)
            local b = v:slice(2, 2)
            local c = v:slice(10, 20)
            local d = v:slice(2)
            local e = v:slice(-2, -1)
            return {
                #a,
                a[1]:tonumber(),
                a[3]:tonumber(),
                #b,
                b[1]:tonumber(),
                #c,
                #d,
                d[1]:tonumber(),
                e[1]:tonumber() + e[2]:tonumber(),
            }
        end
    ''') == b"OK"
    got = r.execute_command("nkslice")
    assert got[0] == 3, got
    assert got[1] == 1 and got[2] == 3, got
    assert got[3] == 1 and got[4] == 2, got
    assert got[5] == 0, got
    assert got[6] == 2 and got[7] == 2, got
    assert got[8] == 5, got

    assert r.execute_command("SETF", "nkconv", '''
        function call()
            local v = nkf64vector(1, 2, 3)
            local w = nkf32vector(v)
            local x = nkf16vector(w)
            local y = nkbf16vector(x)
            local z = nkf64vector(y)
            local s = nkf16(nkf32(7))
            return {
                #w,
                w[1]:tonumber(),
                x[2]:tonumber(),
                y[3]:tonumber(),
                z[1]:tonumber(),
                s:tonumber(),
                (w == nkf32vector(1, 2, 3)),
            }
        end
    ''') == b"OK"
    got = r.execute_command("nkconv")
    assert got[0] == 3, got
    assert got[1] == 1, got
    assert abs(got[2] - 2) < 0.02, got
    assert abs(got[3] - 3) < 0.05, got
    assert got[4] == 1, got
    assert abs(got[5] - 7) < 0.02, got
    assert got[6], got

    assert r.execute_command("SETF", "nkbuf", '''
        function call()
            local v = nkf32vector(1, 2, 3)
            local b = v:buffer()
            barch.store.set("emb", b)
            local w = nkf32vector(b)
            local u = nkf32vector(barch.store.get("emb"))
            local x = nkf16vector(1, 2)
            local y = nkf16vector(x:buffer())
            return {
                buffer.len(b),
                #w,
                w[2]:tonumber(),
                u[3]:tonumber(),
                (w == v),
                (u == v),
                y[1]:tonumber(),
                y[2]:tonumber(),
            }
        end
    ''') == b"OK"
    got = r.execute_command("nkbuf")
    assert got[0] == 12, got
    assert got[1] == 3, got
    assert got[2] == 2, got
    assert got[3] == 3, got
    assert got[4] and got[5], got
    assert got[6] == 1 and got[7] == 2, got

    assert r.execute_command("SETF", "nkdist", '''
        function call()
            local a = nkf32vector(1, 0)
            local b = nkf32vector(0, 1)
            local c = nkf32vector(1, 0)
            local p = nkf32vector(1, 2, 3)
            local q = nkf32vector(4, 5, 6)
            local d64 = nkf64vector(1, 2, 3):dot(nkf64vector(4, 5, 6))
            return {
                a:dot(b),
                a:euclidean(b),
                a:cosine(b),
                a:dot(c),
                a:euclidean(c),
                a:cosine(c),
                p:dot(q),
                d64,
            }
        end
    ''') == b"OK"
    got = r.execute_command("nkdist")
    def num(x):
        return float(x)
    assert num(got[0]) == 0, got
    assert abs(num(got[1]) - 2 ** 0.5) < 1e-5, got
    assert abs(num(got[2]) - 1) < 1e-5, got
    assert num(got[3]) == 1, got
    assert abs(num(got[4])) < 1e-6, got
    assert abs(num(got[5])) < 1e-6, got
    assert num(got[6]) == 32, got
    assert num(got[7]) == 32, got

    e = refused("SETF", "nkdismis", '''
        function call()
            return nkf32vector(1, 2):dot(nkf32vector(1))
        end
    ''')
    if e is None:
        e = refused("nkdismis")
    assert e and "mismatch" in e.lower(), e

    assert r.execute_command("SETF", "nksum", '''
        function call()
            local v = nkf32vector(1, 2, 3)
            local z = nkf32vector()
            return {
                v:sum(),
                v:average(),
                v:mean(),
                nkf64vector(10, 20):sum(),
                z:sum(),
                z:average(),
            }
        end
    ''') == b"OK"
    got = r.execute_command("nksum")
    assert got[0] == 6, got
    assert got[1] == 2, got
    assert got[2] == 2, got
    assert got[3] == 30, got
    assert got[4] == 0 and got[5] == 0, got

    e = refused("SETF", "nkbad", '''
        function call()
            return nkf32vector(1, 2) + nkf32vector(1)
        end
    ''')
    # SETF succeeds; the size error is at call time
    if e is None:
        e = refused("nkbad")
    assert e and "mismatch" in e.lower(), e

    print("complete nk luau test")
finally:
    try:
        barch.stop()
    except Exception:
        pass
