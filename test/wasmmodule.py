"""
Builds WebAssembly modules byte by byte for the stored WASM function tests, so they
need no compiler. A Module has one `call() -> i32` and imports what it uses from
barch; the helpers below it build the modules the tests store.
"""
import json
import struct


I32, I64, F64 = 0x7f, 0x7e, 0x7c

# what barch exports to a module, with the types WAMR checks the imports against
NATIVES = {
    "arg_count": ([], [I32]),
    "arg_len": ([I32], [I32]),
    "arg_read": ([I32, I32], [I32]),
    "val_kind": ([I32], [I32]),
    "val_len": ([I32], [I32]),
    "val_read": ([I32, I32], [I32]),
    "val_int": ([I32], [I64]),
    "val_free": ([I32], []),
    "reply_nil": ([], []),
    "reply_int": ([I64], []),
    "reply_string": ([I32, I32], []),
    "reply_array": ([I32], []),
    "reply_error": ([I32, I32], []),
    "call": ([I32, I32], [I32]),
    "store_get": ([I32, I32], [I32]),
    "store_set": ([I32, I32, I32, I32], [I32]),
    "store_exists": ([I32, I32], [I32]),
    "val_at": ([I32, I32], [I32]),
    "store_count": ([I32, I32, I32, I32], [I64]),
    "store_range": ([I32, I32, I32, I32, I64, I64], [I32]),
    "store_locked": ([I32, I32, I32, I32], [I32]),
    "store_shard_number": ([I32, I32], [I64]),
    "store_has_lock": ([I32, I32], [I32]),
    "space_open": ([I32, I32], [I32]),
    "space_get": ([I32, I32, I32], [I32]),
    "space_set": ([I32, I32, I32, I32, I32], [I32]),
    "space_exists": ([I32, I32, I32], [I32]),
    "space_count": ([I32, I32, I32, I32, I32], [I64]),
    "space_range": ([I32, I32, I32, I32, I32, I64, I64], [I32]),
    "space_call": ([I32, I32, I32], [I32]),
    "tx_begin": ([I32], [I32]),
    "tx_commit": ([], [I32]),
    "tx_abort": ([], [I32]),
    "tx_open": ([], [I32]),
    "time": ([], [I64]),
    "uuid": ([I32], [I32]),
    "http_request": ([I32] * 8, [I32]),
}


def uleb(n):
    out = bytearray()
    while True:
        b = n & 0x7f
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def sleb(n):
    out = bytearray()
    while True:
        b = n & 0x7f
        n >>= 7
        if (n == 0 and not b & 0x40) or (n == -1 and b & 0x40):
            out.append(b)
            return bytes(out)
        out.append(b | 0x80)


def name(s):
    s = s.encode()
    return uleb(len(s)) + s


def vec(items):
    return uleb(len(items)) + b"".join(items)


def section(sid, body):
    return bytes([sid]) + uleb(len(body)) + body


# opcodes, as small helpers so a body reads in order
def i32(n): return b"\x41" + sleb(n)
def i64(n): return b"\x42" + sleb(n)
def lget(i): return b"\x20" + uleb(i)
def lset(i): return b"\x21" + uleb(i)
def gget(i): return b"\x23" + uleb(i)
def gset(i): return b"\x24" + uleb(i)
DROP = b"\x1a"
ADD = b"\x6a"
EXTEND = b"\xac"           # i64.extend_i32_s
EQZ = b"\x45"
IF = b"\x04\x40"
ELSE = b"\x05"
END = b"\x0b"
LOOP = b"\x03\x40"
BR_IF = b"\x0d"
LT_U = b"\x49"
GE_U = b"\x4f"
EQ = b"\x46"
BLOCK = b"\x02\x40"
RETURN = b"\x0f"
UNREACHABLE = b"\x00"
GROW = b"\x40\x00"


class Module:
    """A module with one `call() -> i32`, importing `uses` from barch."""

    def __init__(self, uses=(), extra_imports=()):
        self.types = []
        self.imports = []
        self.index = {}
        for n in uses:
            p, r = NATIVES[n]
            self._import("barch", n, p, r)
        for mod, n, p, r in extra_imports:
            self._import(mod, n, p, r)
        self.body = b""
        self.locals = 0
        self.globals = []
        self.exports_extra = []
        self.data = []
        self.customs = []
        self.start = False
        self.export_call = True
        self.max_pages = None
        self.next_at = 1024
        # TODO 823: more exported functions answering an i32, as (export name, locals,
        # body) or (export name, locals, body, params); params are i32s, locals 0 up
        self.funcs = []
        # TODO 823: export names of funcs in a function table, from slot 1, as C lays
        # out function pointers; slot 0 stays empty
        self.table = []

    def put(self, b):
        """bytes into memory at a fresh address; answers the address and length as code"""
        if isinstance(b, str):
            b = b.encode()
        at = self.next_at
        self.data.append((at, b))
        self.next_at += len(b) + 8
        return i32(at) + i32(len(b))

    def argv(self, *words):
        """an argv for call or space_call: answers argc and the array's address as code"""
        pairs = b""
        for w in words:
            w = w.encode()
            at = self.next_at
            self.data.append((at, w))
            self.next_at += len(w) + 8
            pairs += struct.pack("<II", at, len(w))
        at = self.next_at
        self.data.append((at, pairs))
        self.next_at += len(pairs) + 8
        return i32(len(words)) + i32(at)

    def _type(self, params, results):
        t = b"\x60" + vec([bytes([p]) for p in params]) + vec([bytes([r]) for r in results])
        if t not in self.types:
            self.types.append(t)
        return self.types.index(t)

    def _import(self, mod, n, params, results):
        self.index[n] = len(self.imports)
        self.imports.append(name(mod) + name(n) + b"\x00" + uleb(self._type(params, results)))

    def call(self, n):
        return b"\x10" + uleb(self.index[n])

    def build(self):
        call_type = self._type([], [I32])
        func_types = [self._type([I32] * len(f[3]) if len(f) > 3 else [], [I32]) for f in self.funcs]
        call_index = len(self.imports)
        out = b"\x00asm" + struct.pack("<I", 1)
        out += section(1, vec(self.types))
        if self.imports:
            out += section(2, vec(self.imports))
        out += section(3, vec([uleb(t) for t in [call_type] + func_types]))
        if self.table:
            out += section(4, vec([b"\x70\x00" + uleb(1 + len(self.table))]))
        limits = b"\x00" + uleb(1) if self.max_pages is None else b"\x01" + uleb(1) + uleb(self.max_pages)
        out += section(5, vec([limits]))
        if self.globals:
            out += section(6, vec([bytes([I32, mut]) + i32(v) + END for v, mut in self.globals]))
        exports = [name("memory") + b"\x02" + uleb(0)]
        if self.export_call:
            exports.append(name("call") + b"\x00" + uleb(call_index))
        exports += self.exports_extra
        at = {f[0]: call_index + 1 + i for i, f in enumerate(self.funcs)}
        at["call"] = call_index
        for n, i in at.items():
            if n != "call":
                exports.append(name(n) + b"\x00" + uleb(i))
        out += section(7, vec(exports))
        if self.start:
            out += section(8, uleb(call_index))
        if self.table:
            out += section(9, vec([b"\x00" + i32(1) + END + vec([uleb(at[n]) for n in self.table])]))
        bodies = []
        for n_locals, body in [(self.locals, self.body)] + [(f[1], f[2]) for f in self.funcs]:
            locals_ = vec([uleb(n_locals) + bytes([I32])]) if n_locals else vec([])
            code = locals_ + body + END
            bodies.append(uleb(len(code)) + code)
        out += section(10, vec(bodies))
        if self.data:
            out += section(11, vec([b"\x00" + i32(at) + END + uleb(len(b)) + b for at, b in self.data]))
        for n, payload in self.customs:
            out += section(0, name(n) + payload)
        return out

    def arity(self, n):
        self.globals.append((n, 0))
        self.exports_extra.append(name("arity") + b"\x03" + uleb(len(self.globals) - 1))
        return self


def echo():
    """arity 1: answers its argument"""
    m = Module(["arg_read", "reply_string"]).arity(1)
    m.body = i32(0) + i32(0) + i32(0) + m.call("arg_read") + m.call("reply_string") + i32(0)
    return m


def constant(text):
    m = Module(["reply_string"])
    m.data.append((16, text))
    m.body = i32(16) + i32(len(text)) + m.call("reply_string") + i32(0)
    return m


def store_roundtrip():
    """arity 2: stores arg 1 under arg 0, then reads it back and answers it"""
    m = Module(["arg_read", "store_set", "store_get", "val_read", "reply_string"]).arity(2)
    m.locals = 3
    m.body = (i32(0) + i32(0) + m.call("arg_read") + lset(0) +
              i32(1) + i32(4096) + m.call("arg_read") + lset(1) +
              i32(0) + lget(0) + i32(4096) + lget(1) + m.call("store_set") + DROP +
              i32(0) + lget(0) + m.call("store_get") + lset(2) +
              i32(8192) + lget(2) + i32(8192) + m.call("val_read") + m.call("reply_string") +
              i32(0))
    return m


def get_command():
    """arity 1: runs GET arg0 through barch.call; nil answers nil, an error its text"""
    m = Module(["arg_read", "call", "val_kind", "val_read", "reply_nil", "reply_string",
                "reply_error"]).arity(1)
    m.locals = 2
    m.data.append((200, b"GET"))
    m.data.append((100, struct.pack("<II", 200, 3)))
    m.body = (i32(0) + i32(300) + m.call("arg_read") + lset(0) +
              i32(108) + i32(300) + b"\x36\x02\x00" +       # pair 1: ptr
              i32(112) + lget(0) + b"\x36\x02\x00" +        # pair 1: len
              i32(2) + i32(100) + m.call("call") + lset(1) +
              lget(1) + m.call("val_kind") + EQZ + IF +
              m.call("reply_nil") +
              ELSE +
              i32(1000) + lget(1) + i32(1000) + m.call("val_read") + m.call("reply_string") +
              END + i32(0))
    return m


def set_command():
    """arity 2: runs SET arg0 arg1; an error from the command comes back as reply_error"""
    m = Module(["arg_read", "call", "val_kind", "val_read", "reply_string", "reply_error"]).arity(2)
    m.locals = 3
    m.data.append((200, b"SET"))
    m.data.append((100, struct.pack("<II", 200, 3)))
    m.body = (i32(0) + i32(300) + m.call("arg_read") + lset(0) +
              i32(1) + i32(600) + m.call("arg_read") + lset(1) +
              i32(108) + i32(300) + b"\x36\x02\x00" + i32(112) + lget(0) + b"\x36\x02\x00" +
              i32(116) + i32(600) + b"\x36\x02\x00" + i32(120) + lget(1) + b"\x36\x02\x00" +
              i32(3) + i32(100) + m.call("call") + lset(2) +
              lget(2) + m.call("val_kind") + i32(5) + b"\x46" + IF +   # i32.eq: an error
              i32(1000) + lget(2) + i32(1000) + m.call("val_read") + m.call("reply_error") +
              i32(1) + b"\x0f" +                                         # return 1
              END +
              i32(1000) + lget(2) + i32(1000) + m.call("val_read") + m.call("reply_string") +
              i32(0))
    return m


def ws_logger():
    """arity 3, a websocket handler: RPUSH arg0 arg2, then answers arg2 - TODO 901"""
    m = Module(["arg_read", "call", "reply_string"]).arity(3)
    m.locals = 2
    m.data.append((200, b"RPUSH"))
    m.data.append((100, struct.pack("<II", 200, 5)))
    m.body = (i32(0) + i32(300) + m.call("arg_read") + lset(0) +
              i32(2) + i32(600) + m.call("arg_read") + lset(1) +
              i32(108) + i32(300) + b"\x36\x02\x00" + i32(112) + lget(0) + b"\x36\x02\x00" +
              i32(116) + i32(600) + b"\x36\x02\x00" + i32(120) + lget(1) + b"\x36\x02\x00" +
              i32(3) + i32(100) + m.call("call") + DROP +
              i32(600) + lget(1) + m.call("reply_string") +
              i32(0))
    return m


def spin():
    m = Module()
    m.body = LOOP + b"\x0c\x00" + END + i32(0)
    return m


def count_to(n):
    """counts a local up to n, a few instructions a time round, and answers it"""
    m = Module(["reply_int"])
    m.locals = 1
    m.body = (LOOP + lget(0) + i32(1) + ADD + lset(0) + lget(0) + i32(n) + LT_U + BR_IF + b"\x00" + END +
              lget(0) + EXTEND + m.call("reply_int") + i32(0))
    return m


def calls(fname):
    """answers what CALLF fname answered, as an integer"""
    m = Module(["call", "val_int", "reply_int"])
    f = fname.encode()
    m.data.append((16, struct.pack("<IIII", 64, 5, 80, len(f))))
    m.data.append((64, b"CALLF"))
    m.data.append((80, f))
    m.body = i32(2) + i32(16) + m.call("call") + m.call("val_int") + m.call("reply_int") + i32(0)
    return m


def grow():
    """answers [what growing past the cap gave, what growing by one page gave]"""
    m = Module(["reply_array", "reply_int"])
    m.body = (i32(2) + m.call("reply_array") +
              i32(2000) + GROW + EXTEND + m.call("reply_int") +
              i32(1) + GROW + EXTEND + m.call("reply_int") +
              i32(0))
    return m


def array_reply():
    """answers [7, "ab", [], [-3]]"""
    m = Module(["reply_array", "reply_int", "reply_string"])
    m.data.append((16, b"ab"))
    m.body = (i32(4) + m.call("reply_array") +
              i64(7) + m.call("reply_int") +
              i32(16) + i32(2) + m.call("reply_string") +
              i32(0) + m.call("reply_array") +
              i32(1) + m.call("reply_array") + i64(-3) + m.call("reply_int") +
              i32(0))
    return m


def counter():
    """a global that survives between calls on the same instance"""
    m = Module(["reply_int"])
    m.globals.append((0, 1))
    m.body = gget(0) + i32(1) + ADD + gset(0) + gget(0) + EXTEND + m.call("reply_int") + i32(0)
    return m


def trap():
    m = Module()
    m.body = UNREACHABLE + i32(0)
    return m


def fails():
    m = Module(["reply_error"])
    m.data.append((16, b"ERR nope"))
    m.body = i32(16) + i32(8) + m.call("reply_error") + i32(1)
    return m


def writes(meta=None):
    m = Module(["store_set", "reply_int"])
    m.data.append((16, b"wk"))
    m.body = i32(16) + i32(2) + i32(16) + i32(2) + m.call("store_set") + EXTEND + m.call("reply_int") + i32(0)
    if meta is not None:
        m.customs.append(("barch", meta))
    return m


def out_of_bounds():
    m = Module(["reply_string"])
    m.body = i32(65530) + i32(100) + m.call("reply_string") + i32(0)
    return m


BUF = 60000   # where replies are read back into, well past any data


def reply_str(m, handle, e):
    """answers the string value `handle` (code) makes, through local e"""
    return handle + lset(e) + i32(BUF) + lget(e) + i32(BUF) + m.call("val_read") + m.call("reply_string")


def reply_strings(m, h, n, i):
    """answers the array of strings in local h as an array, with locals n and i"""
    return (lget(h) + m.call("val_len") + lset(n) + lget(n) + m.call("reply_array") +
            BLOCK + lget(n) + EQZ + BR_IF + b"\x00" +
            LOOP +
            i32(BUF) + lget(h) + lget(i) + m.call("val_at") + i32(BUF) + m.call("val_read") +
            m.call("reply_string") +
            lget(i) + i32(1) + ADD + lset(i) + lget(i) + lget(n) + LT_U + BR_IF + b"\x00" +
            END + END)


WALK = ["val_len", "val_at", "val_read", "reply_array", "reply_string", "reply_int"]


def walk(lo, hi, limit=0, offset=0, space=None):
    """TODO 822: the keys from lo to hi, as barch.store.range or a space's range"""
    m = Module(WALK + ["store_range", "space_open", "space_range"])
    m.locals = 4
    bounds = m.put(lo) + m.put(hi) + i64(limit) + i64(offset)
    if space is None:
        m.body = bounds + m.call("store_range")
    else:
        m.body = m.put(space) + m.call("space_open") + bounds + m.call("space_range")
    m.body += lset(0) + reply_strings(m, 0, 1, 2) + i32(0)
    return m


def counting(lo, hi, space=None):
    """TODO 822: how many keys from lo to hi"""
    m = Module(["store_count", "space_open", "space_count", "reply_int"])
    bounds = m.put(lo) + m.put(hi)
    if space is None:
        m.body = bounds + m.call("store_count")
    else:
        m.body = m.put(space) + m.call("space_open") + bounds + m.call("space_count")
    m.body += m.call("reply_int") + i32(0)
    return m


def clock():
    """TODO 822: [time, uuid, uuid]"""
    m = Module(["time", "uuid", "reply_array", "reply_int", "reply_string"])
    m.body = (i32(3) + m.call("reply_array") + m.call("time") + m.call("reply_int") +
              i32(BUF) + m.call("uuid") + DROP + i32(BUF) + i32(36) + m.call("reply_string") +
              i32(BUF + 64) + m.call("uuid") + DROP + i32(BUF + 64) + i32(36) + m.call("reply_string") +
              i32(0))
    return m


def in_space(space):
    """TODO 822: in `space`, sets sk and answers [get sk, exists sk there, exists sk
    here, count there, GET sk through space_call, the handle again]"""
    m = Module(["space_open", "space_set", "space_get", "space_exists", "store_exists", "space_count",
                "space_call", "val_read", "reply_array", "reply_string", "reply_int"])
    m.locals = 2
    sk = m.put("sk")
    m.body = (m.put(space) + m.call("space_open") + lset(0) +
              lget(0) + sk + m.put("in " + space) + m.call("space_set") + DROP +
              i32(6) + m.call("reply_array") +
              reply_str(m, lget(0) + sk + m.call("space_get"), 1) +
              lget(0) + sk + m.call("space_exists") + EXTEND + m.call("reply_int") +
              sk + m.call("store_exists") + EXTEND + m.call("reply_int") +
              lget(0) + m.put("s") + m.put("t") + m.call("space_count") + m.call("reply_int") +
              reply_str(m, lget(0) + m.argv("GET", "sk") + m.call("space_call"), 1) +
              m.put(space) + m.call("space_open") + EXTEND + m.call("reply_int") +
              i32(0))
    return m


def bad_space():
    """TODO 822: a space handle that was never opened"""
    m = Module(["space_get", "reply_nil"])
    m.body = i32(7) + m.put("k") + m.call("space_get") + DROP + m.call("reply_nil") + i32(0)
    return m


def opens(space):
    m = Module(["space_open", "reply_int"])
    m.body = m.put(space) + m.call("space_open") + EXTEND + m.call("reply_int") + i32(0)
    return m


HTTP = ["arg_read", "http_request", "val_kind", "val_at", "val_int", "val_read", "reply_array",
        "reply_int", "reply_string", "reply_error"]


def http_answer(m, h, e):
    """answers [status, body] of the request in local h, or its error"""
    return (lget(h) + m.call("val_kind") + i32(5) + EQ + IF +
            i32(BUF) + lget(h) + i32(BUF) + m.call("val_read") + m.call("reply_error") + i32(1) + RETURN +
            END +
            i32(2) + m.call("reply_array") +
            lget(h) + i32(0) + m.call("val_at") + m.call("val_int") + m.call("reply_int") +
            reply_str(m, lget(h) + i32(1) + m.call("val_at"), e))


def http_get(method=0):
    """TODO 822: arity 1, a request to the url it's given, with a header of its own"""
    m = Module(HTTP).arity(1)
    m.locals = 3
    m.body = (i32(0) + i32(2000) + m.call("arg_read") + lset(0) +
              i32(method) + i32(2000) + lget(0) + m.put("X-Wasm: yes\n") + m.put("") + i32(5000) +
              m.call("http_request") + lset(1) +
              http_answer(m, 1, 2) + i32(0))
    return m


def tx_flow(spin):
    """TODO 822: arity 1, a url. Opens a pessimistic transaction, writes txk, counts
    to `spin` (slices), parks on a request to the url, then answers [BEGINTX's answer,
    the request's status, tx_open, COMMITTX's answer, tx_open]"""
    m = Module(HTTP + ["tx_begin", "tx_commit", "tx_open", "store_set"]).arity(1)
    m.locals = 4
    m.body = (i32(5) + m.call("reply_array") +
              reply_str(m, i32(1) + m.call("tx_begin"), 3) +
              m.put("txk") + m.put("in tx") + m.call("store_set") + DROP +
              LOOP + lget(2) + i32(1) + ADD + lset(2) + lget(2) + i32(spin) + LT_U + BR_IF + b"\x00" + END +
              i32(0) + i32(2000) + m.call("arg_read") + lset(0) +
              i32(0) + i32(2000) + lget(0) + m.put("") + m.put("") + i32(5000) +
              m.call("http_request") + lset(1) +
              lget(1) + i32(0) + m.call("val_at") + m.call("val_int") + m.call("reply_int") +
              m.call("tx_open") + EXTEND + m.call("reply_int") +
              reply_str(m, m.call("tx_commit"), 3) +
              m.call("tx_open") + EXTEND + m.call("reply_int") +
              i32(0))
    return m


def tx_store_set(k, v):
    """TODO 822: [BEGINTX PESSIMISTIC's answer, store_set's, COMMITTX's]"""
    m = Module(["tx_begin", "tx_commit", "store_set", "val_read", "reply_array", "reply_string",
                "reply_int"])
    m.locals = 1
    m.body = (i32(3) + m.call("reply_array") +
              reply_str(m, i32(1) + m.call("tx_begin"), 0) +
              m.put(k) + m.put(v) + m.call("store_set") + EXTEND + m.call("reply_int") +
              reply_str(m, m.call("tx_commit"), 0) +
              i32(0))
    return m


def tx_aborted():
    """TODO 822: [BEGINTX's answer, ABORTTX's answer, tx_open] around a write of txa"""
    m = Module(["tx_begin", "tx_abort", "tx_open", "store_set", "val_read", "reply_array",
                "reply_string", "reply_int"])
    m.locals = 1
    m.body = (i32(3) + m.call("reply_array") +
              reply_str(m, i32(0) + m.call("tx_begin"), 0) +
              m.put("txa") + m.put("gone") + m.call("store_set") + DROP +
              reply_str(m, m.call("tx_abort"), 0) +
              m.call("tx_open") + EXTEND + m.call("reply_int") +
              i32(0))
    return m


def transport(m, spec):
    """puts a transport in the module's barch section - TODO 823"""
    m.customs.append(("barch", json.dumps({"transport": spec}).encode()))
    return m


def resp_methods(methods, with_call=True):
    """call answers "call"; get_name answers "get:" and arg 0; set_name stores arg 1
    under arg 0 and answers 1. `methods` is the resp transport's methods object."""
    m = Module(["arg_read", "reply_string", "store_set", "reply_int"])
    m.data.append((2000, b"get:"))
    if with_call:
        m.body = m.put("call") + m.call("reply_string") + i32(0)
    else:
        m.export_call = False
        m.body = i32(0)
    # an export of the wrong shape for a method: arg_read takes two i32s
    m.exports_extra.append(name("takes_two") + b"\x00" + uleb(m.index["arg_read"]))
    m.funcs.append(("get_name", 0,
                    i32(2000) + i32(0) + i32(2004) + m.call("arg_read") + i32(4) + ADD +
                    m.call("reply_string") + i32(0)))
    m.funcs.append(("set_name", 0,
                    i32(3000) + i32(0) + i32(3000) + m.call("arg_read") +
                    i32(3500) + i32(1) + i32(3500) + m.call("arg_read") +
                    m.call("store_set") + DROP + i64(1) + m.call("reply_int") + i32(0)))
    return transport(m, {"kind": "resp", "methods": methods})


def incr(key):
    """runs INCR `key` through barch.call and answers 0"""
    m = Module(["call", "val_free"])
    m.body = m.argv("INCR", key) + m.call("call") + m.call("val_free") + i32(0)
    return m


def declares(spec):
    """only a transport, no call: a cron job or a queue declaration"""
    m = Module([])
    m.export_call = False
    m.body = i32(0)
    return transport(m, spec)


def keeps_message(key):
    """a queue handler: stores the message under `key` and answers 0"""
    m = Module(["arg_read", "store_set"])
    m.body = m.put(key) + i32(3000) + i32(0) + i32(3000) + m.call("arg_read") + m.call("store_set") + DROP + i32(0)
    return m


I32_LOAD = b"\x28\x02\x00"
I32_STORE = b"\x36\x02\x00"
I64_LOAD = b"\x29\x03\x00"
I64_STORE = b"\x37\x03\x00"
I64_ADD = b"\x7c"
MUL = b"\x6c"
KEY_AT = 3000   # where call leaves arg 0 for the region, with its length at KEY_AT - 4

LOCKED = ["arg_read", "store_locked", "store_has_lock", "store_shard_number", "store_get", "store_set",
          "val_kind", "val_read", "reply_int", "call", "val_free"]


def locked_region(region, fn=1, whole=False, region_locals=0):
    """arity 1: runs `region` (a body with the ctx 7 in local 0, answering an i32) under
    store_locked on arg 0's shard, or the whole space, and answers what it answered.
    `fn` is the table slot store_locked is handed; the region is in slot 1 and a
    () -> i32 function in slot 2."""
    m = Module(LOCKED).arity(1)
    m.locals = 1
    m.body = (i32(0) + i32(KEY_AT) + m.call("arg_read") + lset(0) +
              i32(KEY_AT - 4) + lget(0) + I32_STORE +
              i32(KEY_AT) + (i32(0) if whole else lget(0)) + i32(fn) + i32(7) + m.call("store_locked") +
              EXTEND + m.call("reply_int") + i32(0))
    m.funcs.append(("region", region_locals, region(m), ["ctx"]))
    m.funcs.append(("no_args", 0, i32(0)))
    m.table = ["region", "no_args"]
    return m


def has_lock_plus_ctx(m):
    """writes "in" to the key, and answers ctx + 1, + 100 when the key's shard is held"""
    return (i32(KEY_AT) + i32(KEY_AT - 4) + I32_LOAD + m.put("in") + m.call("store_set") + DROP +
            lget(0) + i32(1) + ADD +
            i32(KEY_AT) + i32(KEY_AT - 4) + I32_LOAD + m.call("store_has_lock") + i32(100) + MUL + ADD)


def bumps(m):
    """reads the 8 byte count at the key, adds one, writes it back, answers the count"""
    return (i32(KEY_AT) + i32(KEY_AT - 4) + I32_LOAD + m.call("store_get") + lset(1) +
            i32(BUF) + i64(0) + I64_STORE +
            lget(1) + m.call("val_kind") + IF + lget(1) + i32(BUF) + m.call("val_read") + DROP + END +
            i32(BUF) + i32(BUF) + I64_LOAD + i64(1) + I64_ADD + I64_STORE +
            i32(KEY_AT) + i32(KEY_AT - 4) + I32_LOAD + i32(BUF) + i32(8) + m.call("store_set") + DROP +
            i32(BUF) + I32_LOAD)


def calls_get(m):
    """barch.call from inside the region"""
    return m.argv("GET", "x") + m.call("call") + m.call("val_free") + i32(0)


def nests(m):
    """store_locked again from inside the region"""
    return i32(KEY_AT) + i32(KEY_AT - 4) + I32_LOAD + i32(1) + i32(0) + m.call("store_locked")


def runs_away(m):
    return LOOP + b"\x0c\x00" + END + i32(0)


def traps(m):
    return UNREACHABLE + i32(0)


def shard_of():
    """arity 1: answers the shard arg 0 is on"""
    m = Module(["arg_read", "store_shard_number", "reply_int"]).arity(1)
    m.body = i32(KEY_AT) + i32(0) + i32(KEY_AT) + m.call("arg_read") + m.call("store_shard_number") + \
        m.call("reply_int") + i32(0)
    return m


def writes_lockedk(m):
    """writes "in" to lockedk, wherever that is, and answers 1"""
    return m.put("lockedk") + m.put("in") + m.call("store_set") + DROP + i32(1)


def waits_for(key, meta=None):
    """TODO 836: spins until `key` exists, then answers 1. It ends only when the test
    lets it, so whether it got past its deadline is a fact, not a guess at timing"""
    m = Module(["store_exists", "reply_int"])
    m.body = (LOOP + m.put(key) + m.call("store_exists") + EQZ + BR_IF + b"\x00" + END +
              i64(1) + m.call("reply_int") + i32(0))
    if meta is not None:
        m.customs.append(("barch", meta))
    return m
