-- A look at every key space the server has, and - for admins only - a way to change
-- some of it (see `post` below).
--
-- Five spaces hold this example now - the catalog, the accounts, the reviews,
-- the places and the orders - and until this there was no way to see any of it
-- without redis-cli. `/shop/spaces.html` is the page; this is what it reads.
--
-- **This shows everything the HTTP user can see, in every space.** It is a
-- read-only inspector and it is behind a sign-in, but a sign-in on an example
-- storefront is not an admin boundary. Do not put this on a public port.
--
-- Three things had to be worked out rather than assumed, and they shape the rest:
--
--   * `KEYS` is registered asynchronous and a script may not call it, so keys
--     come off the `barch.space[name]` handle's range walk instead.
--   * The `space:COMMAND` prefix that works on a RESP connection does not work
--     through `barch.call`: the runner looks the whole name up in the command
--     table and there is no command called `GEO:DBSIZE`. So anything per-space
--     goes through a handle, and only global commands go through `barch.call`.
--   * A file store is keys. `fs:n:<path>` holds `{id,size,type,version}` and
--     `fs:n:<path>/` holds `{dir:true}`, so one level of the tree is a range.

function call()
    return "spacesapi"
end

local function accounts()
    return require("users:/modules/accounts.luau")
end

local function send(res, body, code)
    res.code = code or 200
    res:header("Content-Type", "application/json")
    res.body = body
end

local function fail(res, why, code)
    send(res, simdjson.encode({ error = why }), code or 400)
end

local function ok(res, out)
    send(res, simdjson.encode(out))
end

-- The prefix walk, again: a range from the prefix to the same string with its
-- last byte nudged up. Empty means the whole space.
local function bounds(prefix)
    if prefix == nil or prefix == "" then return "", "\255" end
    local last = string.byte(prefix, #prefix)
    return prefix, string.sub(prefix, 1, #prefix - 1) .. string.char(last + 1)
end

-- The next key after this one, as a range bound.
--
-- `range` includes its lower bound, so stepping past a key means naming one just
-- above it. Not `key .. "\0"`: a trailing NUL is the terminator a key already
-- carries, so that bound means the key itself and the walk would never move.
-- (It used to mean something worse - the range came back empty and this listing
-- showed one file out of four - which is TODO 294, now fixed to read as the key.)
-- `\1` is the smallest byte that steps past it, and it still sorts below any
-- sibling whose name merely starts the same way.
local function after(key)
    return key .. "\1"
end

-- An integer out of `barch.call` is Luau's native 64 bit `integer`, not a number:
-- `tonumber(reply)` is nil and `reply + 1` raises. `integer.tonumber` is the
-- conversion. Worth knowing because `tonumber(...) or 0` swallows it without a
-- word - every space reported zero keys until this was found.
local function as_number(v)
    if type(v) == "number" then return v end
    if type(v) == "integer" then return integer.tonumber(v) end
    return tonumber(tostring(v)) or 0
end

local function clamp(n, low, high, dflt)
    n = math.floor(tonumber(n) or dflt)
    if n < low then return low end
    if n > high then return high end
    return n
end

-- SPACES answers a flat list: name, size, name, size. The default space comes
-- back as "(default)" and cannot be opened by that name, so it is listed with
-- what it holds and marked as not openable rather than quietly dropped.
local function space_list()
    local flat = barch.call("SPACES") or {}
    local out = {}
    for i = 1, #flat, 2 do
        local name = flat[i]
        out[#out + 1] = {
            name = name,
            keys = as_number(flat[i + 1]),
            openable = name ~= "(default)",
        }
    end
    return out
end

-- The per-space settings are ordinary keys in `configuration`, named
-- `<space>.<option>`. Walking that one space is how this page knows that `geo`
-- is ordered and `shop` has a file source, without asking each space.
local function settings_by_space()
    local conf = barch.space.configuration
    local out = {}
    for _, key in ipairs(conf:range("", "\255", 2000)) do
        local space, option = string.match(key, "^([^.]+)%.(.+)$")
        if space ~= nil then
            out[space] = out[space] or {}
            out[space][option] = conf:get(key)
        end
    end
    return out
end

local function open(name)
    if name == nil or name == "" then return nil, "a space is required" end
    if name == "(default)" then return nil, "the default space cannot be opened by name" end
    local held, sp = pcall(function() return barch.space[name] end)
    if not held then return nil, "no key space called " .. name end
    return sp, nil
end

-- GET /api/admin/spaces - every space, its size, and what it is configured as.
local function spaces(req, res)
    local settings = settings_by_space()
    local out = space_list()
    for _, s in ipairs(out) do
        s.settings = settings[s.name] or {}
        -- a space with a file store has a layout key, which is a cheaper question
        -- than "does it have any files"
        if s.openable then
            local sp = barch.space[s.name]
            s.files = sp:get("fs:layout") ~= nil
        end
    end
    return ok(res, { spaces = out })
end

-- A glob as a Lua pattern, case-insensitive: `*` any run, `?` one character, the
-- rest literal. The subject is lowercased by the caller.
local function glob_pattern(g)
    local p = string.gsub(string.lower(g), "[%^%$%(%)%%%.%[%]%+%-]", "%%%0")
    p = string.gsub(p, "%*", ".*")
    p = string.gsub(p, "%?", ".")
    return "^" .. p .. "$"
end

-- The keys under [lo, hi) whose value matches `vpat` (and whose name matches
-- `kpat` when given), as one window of `limit` after `offset`, and how many there
-- are in all when `count` is set. Nothing can jump to the offset of a match, so this
-- reads every value from the start; SCAN_CAP bounds that and `capped` says it hit it.
local SCAN_CAP = 200000
local SCAN_BATCH = 5000

local function scan(sp, lo, hi, vpat, kpat, offset, limit, count)
    local names, total, seen, capped = {}, 0, 0, false
    local cur, first = lo, true
    while true do
        local batch = sp:range(cur, hi, SCAN_BATCH)
        for i = (first or batch[1] ~= cur) and 1 or 2, #batch do
            local key = batch[i]
            seen = seen + 1
            if seen > SCAN_CAP then capped = true; return names, total, capped end
            if kpat == nil or string.find(string.lower(key), kpat) then
                local raw = vpat ~= nil and sp:get(key) or ""
                if raw ~= nil and (vpat == nil or string.find(string.lower(raw), vpat)) then
                    if total >= offset and #names <= limit then names[#names + 1] = key end
                    total = total + 1
                    -- past the window and nobody asked for the total: stop early
                    if not count and #names > limit then return names, total, capped end
                end
            end
        end
        if #batch < SCAN_BATCH then break end
        cur, first = batch[#batch], false
    end
    return names, total, capped
end

-- GET /api/admin/keys?space=geo&prefix=street:&offset=400&limit=25[&count=1][&match=*sale*][&keymatch=*x*]
--
-- One window of keys, with a short value preview beside each. `offset` is how many
-- keys to step over first, which is what lets the page's slider land anywhere:
-- `sp:range(lo, hi, limit, offset)` jumps to it, so a deep window costs the same as
-- the first. (That needs a barchd with the offset - an older one ignores the fourth
-- argument and answers the first window every time.)
--
-- `count=1` adds `total`. There is no COUNT from Luau, so it is found by probing:
-- double the offset until a range comes back empty, then halve back to the last key.
-- About 2 * log2(n) one-key reads, and the page asks once per space and prefix.
local function count_keys(sp, lo, hi)
    local first = sp:range(lo, hi, 1, 0)
    if #first == 0 then return 0 end
    -- an older barchd ignores the offset and answers the first key every time, which
    -- would make the probe below run forever
    local second = sp:range(lo, hi, 1, 1)
    if #second == 0 then return 1 end
    if second[1] == first[1] then return nil end
    local have, step = 0, 1                      -- `have` is an offset known to exist
    while step < 1073741824 and #sp:range(lo, hi, 1, have + step) > 0 do
        have = have + step
        step = step * 2
    end
    while step > 1 do
        step = step // 2
        if #sp:range(lo, hi, 1, have + step) > 0 then have = have + step end
    end
    return have + 1
end

local function keys(req, res, query)
    local sp, err = open(query.space)
    if sp == nil then return fail(res, err, 404) end
    local limit = clamp(query.limit, 1, 500, 25)
    local offset = clamp(query.offset, 0, 100000000, 0)
    local lo, hi = bounds(query.prefix)

    local filtered = (query.match ~= nil and query.match ~= "") or (query.keymatch ~= nil and query.keymatch ~= "")
    local names, more, total, capped
    if filtered then
        local vpat = (query.match ~= nil and query.match ~= "") and glob_pattern(query.match) or nil
        local kpat = (query.keymatch ~= nil and query.keymatch ~= "") and glob_pattern(query.keymatch) or nil
        names, total, capped = scan(sp, lo, hi, vpat, kpat, offset, limit, query.count == "1")
        more = #names > limit
        if query.count ~= "1" then total = nil end
    else
        -- one over, so the page can say whether there is another
        names = sp:range(lo, hi, limit + 1, offset)
        more = #names > limit
        total = query.count == "1" and count_keys(sp, lo, hi) or nil
    end

    local out = {}
    for i = 1, math.min(#names, limit) do
        local key = names[i]
        local raw = sp:get(key)
        local row = { key = key }
        if raw == nil then
            -- no value means a container rather than an absence: the handle
            -- can say which kind, and that is the interesting part
            row.kind = sp:kind(key) or "absent"
        else
            row.kind = "key"
            row.size = #raw
            row.value = #raw > 240 and (string.sub(raw, 1, 240) .. "…") or raw
        end
        out[#out + 1] = row
    end

    return ok(res, {
        space = query.space, prefix = query.prefix or "",
        keys = out, offset = offset, more = more, total = total, capped = capped,
    })
end

-- GET /api/admin/value?space=shop&key=meta:loaded - one value, whole.
local function value(req, res, query)
    local sp, err = open(query.space)
    if sp == nil then return fail(res, err, 404) end
    if query.key == nil or query.key == "" then return fail(res, "a key is required") end
    local raw = sp:get(query.key)
    if raw == nil then
        return ok(res, { key = query.key, kind = sp:kind(query.key) or "absent" })
    end
    -- a value can be a whole catalog index; the page asked for one key, not 2MB
    local cut = #raw > 64000
    return ok(res, {
        key = query.key, kind = "key", size = #raw, truncated = cut,
        value = cut and string.sub(raw, 1, 64000) or raw,
    })
end

-- GET /api/admin/fs?space=shop&path=/modules&offset=0&limit=25
--
-- One window of one level of the file store, built out of the name records. A
-- directory is a `fs:n:<path>/` key holding `{dir:true}`; a file is `fs:n:<path>`
-- holding the inode id and the size. Both are keys, which is the whole point.
--
-- The listing is folders first, then files by name, and a window of that order
-- cannot be jumped to with the range offset: an offset counts keys, and one folder
-- is as many keys as it has files under it. So the level is walked once, in
-- batches of key names, and the window is cut from the result. The walk steps over
-- each subtree rather than through it, so a folder costs one entry however big it
-- is, and only the window's own files have their record read. `total` is always
-- returned, and it is the slider's length.
local FS_BATCH = 2000

local function files(req, res, query)
    local sp, err = open(query.space)
    if sp == nil then return fail(res, err, 404) end
    if sp:get("fs:layout") == nil then
        return ok(res, { space = query.space, path = "/", files = false, entries = {} })
    end
    local path = query.path or "/"
    if string.sub(path, 1, 1) ~= "/" then path = "/" .. path end
    if path ~= "/" and string.sub(path, -1) == "/" then path = string.sub(path, 1, -2) end
    local limit = clamp(query.limit, 1, 500, 25)
    local offset = clamp(query.offset, 0, 100000000, 0)

    local base = "fs:n:" .. (path == "/" and "/" or path .. "/")
    local _, hi = bounds(base)
    local lo = base
    local dirs, keys = {}, {}

    -- Stepping over the subtrees rather than reading them.
    --
    -- The first version of this walked every key under the prefix and threw away
    -- anything with a slash left in it, which works until a directory is big:
    -- `/catalog` was 7,344 files, so the walk ran out inside it and `/modules`
    -- was simply missing from the listing of `/`. Now a directory is jumped past:
    -- to the first thing that cannot be inside it. '/' is 0x2f, so 0x30 is the
    -- first byte after the whole subtree - and it is still before any sibling
    -- whose name merely starts the same way, which is why this skips `catalog/`
    -- without skipping `catalogue`.
    while #dirs + #keys < 500000 do
        local batch = sp:range(lo, hi, FS_BATCH)
        local i, jump = 1, nil
        while i <= #batch do
            local key = batch[i]
            local rest = string.sub(key, #base + 1)
            if rest == "" then
                i = i + 1
            else
                local cut = string.find(rest, "/", 1, true)
                if cut then
                    local name = string.sub(rest, 1, cut - 1)
                    dirs[#dirs + 1] = name
                    local skip = base .. name .. string.char(0x30)
                    i = i + 1
                    while i <= #batch and batch[i] < skip do i = i + 1 end
                    if i > #batch then jump = skip end     -- the batch ended inside it
                else
                    keys[#keys + 1] = key
                    i = i + 1
                end
            end
        end
        if #batch < FS_BATCH then break end
        lo = jump or after(batch[#batch])
    end
    table.sort(dirs)

    -- a name glob narrows the level before it is windowed
    if query.match ~= nil and query.match ~= "" then
        local pat = glob_pattern(query.match)
        local kept = {}
        for _, name in ipairs(dirs) do
            if string.find(string.lower(name), pat) then kept[#kept + 1] = name end
        end
        dirs = kept
        kept = {}
        for _, key in ipairs(keys) do
            if string.find(string.lower(string.sub(key, #base + 1)), pat) then kept[#kept + 1] = key end
        end
        keys = kept
    end

    local out = {}
    for n = offset + 1, math.min(offset + limit, #dirs + #keys) do
        if n <= #dirs then
            out[#out + 1] = { name = dirs[n], dir = true }
        else
            local key = keys[n - #dirs]
            local entry = { name = string.sub(key, #base + 1), dir = false }
            local meta = sp:get(key)
            if meta ~= nil then
                local m = simdjson.parse(meta)
                entry.size = m.size
                entry.type = m.type
                entry.version = m.version
            end
            out[#out + 1] = entry
        end
    end
    return ok(res, {
        space = query.space, path = path, files = true, entries = out,
        offset = offset, total = #dirs + #keys, dirs = #dirs,
    })
end

-- GET /api/admin/file?space=images&path=/img/B0000AW5SU[&raw=1]
--
-- One stored file, read back out of its chunks: the name record gives the id, the
-- inode `fs:i:<id>` says how many chunks there are, and each is a key
-- `fs:c:<id>:<n>`, both ids fixed width hex - see fs.h. The page uses the type
-- column as its hint for what to do with the bytes.
--
-- By default a JSON answer for the viewer: `text` when the bytes are text and `hex`
-- (the first 512) when they are not, cut at 256 kB. With `raw=1` it is the bytes
-- themselves, for an <img>, and only for the image types a browser cannot run
-- anything out of - a stored .html or .svg served under this origin would be a
-- way to run script as whoever is signed in, so those come back as JSON like the rest.
local PREVIEW = 262144
local RAW_TYPES = { ["image/jpeg"] = true, ["image/png"] = true, ["image/gif"] = true, ["image/webp"] = true }

local function file(req, res, query)
    local sp, err = open(query.space)
    if sp == nil then return fail(res, err, 404) end
    local path = query.path or ""
    if string.sub(path, 1, 1) ~= "/" then path = "/" .. path end
    local name = sp:get("fs:n:" .. path)
    if name == nil then return fail(res, "no such file", 404) end
    local n = simdjson.parse(name)
    if n.id == nil then return fail(res, "not a file", 400) end
    local ino = sp:get(string.format("fs:i:%016x", n.id))
    if ino == nil then return fail(res, "no inode for that file", 404) end
    local meta = simdjson.parse(ino)

    local want = query.raw == "1" and (meta.size or 0) or PREVIEW
    local parts, have = {}, 0
    for c = 0, (meta.chunks or 0) - 1 do
        if have >= want then break end
        local piece = sp:get(string.format("fs:c:%016x:%08x", n.id, c)) or ""
        parts[#parts + 1] = piece
        have = have + #piece
    end
    local bytes = table.concat(parts)
    if #bytes > want then bytes = string.sub(bytes, 1, want) end

    if query.raw == "1" and RAW_TYPES[meta.type or ""] then
        res.code = 200
        res:header("Content-Type", meta.type)
        res:header("X-Content-Type-Options", "nosniff")
        res:header("Cache-Control", "private, max-age=60")
        res.body = bytes
        return
    end

    local istext = string.find(bytes, "[\0-\8\11\12\14-\31]") == nil
    if istext then
        local okay, len = pcall(utf8.len, bytes)
        istext = okay and len ~= nil
    end
    local out = {
        space = query.space, path = path, type = meta.type or "", size = meta.size,
        version = meta.version, truncated = #bytes < (meta.size or 0),
    }
    if istext then
        out.text = bytes
    else
        local hex = {}
        for i = 1, math.min(#bytes, 512) do hex[i] = string.format("%02x", string.byte(bytes, i)) end
        out.hex = table.concat(hex, " ")
    end
    return ok(res, out)
end

-- Settings this page will not change, or only from the machine itself.
--
-- Always locked: where the server listens and binds. Changing either cuts off the
-- connection this page is running on.
--
-- Local only: the directories and files (where it reads, writes and loads code from on
-- the host), TLS, traffic capture (it records requests, sessions and passwords
-- included), git sync (it pulls and runs code) and cgroups. These are open to a
-- request whose socket comes from 127.0.0.1 or ::1 - somebody already on the machine -
-- and refused from anywhere else. `req.remote` is the connection's own peer address;
-- barch does not read X-Forwarded-For, so a browser cannot claim to be local.
local ALWAYS = {
    server_port = true, listen_port = true, port = true, server_binding = true, bind = true,
    external_host = true, daemonize = true, ["cluster-enabled"] = true,
}
local function host_only(name)
    return string.find(name, "_dir$") ~= nil or string.find(name, "_file$") ~= nil
        or string.find(name, "^tls") ~= nil or string.find(name, "^traffic_") ~= nil
        or string.find(name, "^functions_git") ~= nil or string.find(name, "^cgroup_") ~= nil
end
local function is_local(req)
    local ip = req.remote or ""
    return ip == "::1" or string.find(ip, "^127%.") ~= nil or string.find(ip, "^::ffff:127%.") ~= nil
end
-- nil when it may be set; otherwise why not
local function locked(name, here)
    if ALWAYS[name] then return "changing where the server listens would cut this page off" end
    if host_only(name) and not here then return "only from the machine itself (127.0.0.1)" end
    return nil
end

-- GET /api/admin/config - the global settings, and the per space ones beside them.
local function config(req, res)
    -- `CONFIG` is one command for GET and SET and carries `write` and `config`
    -- together, so reading the global settings means the HTTP user can also
    -- change them. setup.sh grants it and says why; without the grant this is a
    -- refusal rather than a crash, and the page says which grant is missing
    -- instead of showing an empty list as though there were no settings.
    local here = is_local(req)
    local held, flat = pcall(function() return barch.call("CONFIG", "GET", "*") end)
    local global, why = {}, nil
    if held then
        for i = 1, #(flat or {}), 2 do
            local lock = locked(flat[i], here)
            global[#global + 1] = { name = flat[i], value = tostring(flat[i + 1]),
                                    locked = lock ~= nil, why = lock, host = host_only(flat[i]) }
        end
        table.sort(global, function(a, b) return a.name < b.name end)
    else
        why = "the HTTP user has no `config` category, so the global settings "
           .. "cannot be read: ACL SETUSER web on +config"
    end
    return ok(res, { global = global, global_error = why, spaces = settings_by_space(), here = here })
end

-- GET /api/admin/stats - what the server says about itself.
--
-- `INFO SHARD` is in here and only ever describes *this* space, because it reads
-- the connection's key space and the route runs in the shop. That is a real
-- limit rather than an omission, and the page says which space it is about.
local function stats(req, res)
    local function pairs_of(reply)
        local out = {}
        for i = 1, #(reply or {}), 2 do
            out[#out + 1] = { name = reply[i], value = tostring(reply[i + 1]) }
        end
        return out
    end
    local function lines_of(text)
        local out = {}
        for line in string.gmatch(tostring(text or ""), "[^\n]+") do
            local k, v = string.match(line, "^([%w_]+):(.*)$")
            if k ~= nil then out[#out + 1] = { name = k, value = v } end
        end
        return out
    end
    return ok(res, {
        server = lines_of(barch.call("INFO", "SERVER")),
        stats = pairs_of(barch.call("STATS")),
        ops = pairs_of(barch.call("OPS")),
        shard = { space = barch.running(), lines = lines_of(barch.call("INFO", "SHARD", "0")) },
    })
end

-- Who may write. Reading needs a sign-in; writing needs an entry in the users
-- space, `admin:<email>` = "1", which is set by hand (redis-cli, USE users) and by
-- nothing in this code - anyone can register, so a customer must never be able to
-- make themselves one. Both the check and the entry are lowercase.
local function is_admin(req)
    local user = accounts().current(req)
    if user == nil or user.email == nil then return false end
    return barch.space.users:get("admin:" .. string.lower(user.email)) ~= nil
end

function get(req, res, params, query)
    -- Signed in, at the least. This is every space the HTTP user can read, so it
    -- is not for a public port whatever the gate says.
    if accounts().current(req) == nil then
        return fail(res, "sign in to use the space viewer", 401)
    end
    local tail = string.sub(req.url, string.len("/api/admin/") + 1)
    local q = string.find(tail, "?", 1, true)
    if q then tail = string.sub(tail, 1, q - 1) end

    if tail == "me" then return ok(res, { admin = is_admin(req) }) end
    if tail == "spaces" then return spaces(req, res) end
    if tail == "keys" then return keys(req, res, query) end
    if tail == "value" then return value(req, res, query) end
    if tail == "fs" then return files(req, res, query) end
    if tail == "file" then return file(req, res, query) end
    if tail == "config" then return config(req, res) end
    if tail == "stats" then return stats(req, res) end
    fail(res, "no such endpoint", 404)
end

-- POST /api/admin/<op>?space=...   the body is the value or the file, as bytes.
--
--   key_put?space&key           set a key to the body (a value is at most 512 kB)
--   key_rm?space&key            delete a key
--   file_put?space&path&type    write the body as a file
--   file_mv?space&path&to       rename a file
--   file_rm?space&path          delete a file
--   resp?space                  run one command in the space; body {"args": ["GET", "k"]}
--   config_set?name             CONFIG SET name to the body (a server setting, not a space's)
--   dir_mk?space&path           make a folder
--   dir_rm?space&path[&recursive=1]   delete a folder; without `recursive` only an empty one
--
-- file_mv renames a folder too: the name records move and no chunk is touched.
--
-- Admins only. The `configuration` space is written like any other - it is where each
-- space's settings live, so the page asks for extra care there. Keys under `fs:` are refused, because they are the file store's
-- own records and a hand-edited one leaves a file half there.
--
-- Files are written through `barch.fs.space(name)`, the file functions bound to the
-- named space (a barchd built after 18-09-2026; older ones have no `barch.fs.space`).
-- A reply out of `sp:call`, as JSON that keeps its RESP type: {t = str|int|num|nil|
-- bool|arr|map|bin, v = ...}. Integers come back as Luau's 64 bit `integer` and are sent
-- as text so none is rounded. Strings that are not UTF-8 go as hex (`bin`) because JSON
-- cannot carry them. Big replies are cut: 64 kB a string, 1000 entries a collection.
local REPLY_STR, REPLY_ITEMS = 65536, 1000

local function reply_of(v, depth)
    local t = type(v)
    if v == nil then return { t = "nil" } end
    if t == "integer" then return { t = "int", v = tostring(v) } end
    if t == "number" then
        if v == math.floor(v) and math.abs(v) < 2^53 then return { t = "int", v = string.format("%d", v) } end
        return { t = "num", v = tostring(v) }
    end
    if t == "boolean" then return { t = "bool", v = v } end
    if t == "string" then
        local cut = #v > REPLY_STR
        local s = cut and string.sub(v, 1, REPLY_STR) or v
        local held, n = pcall(utf8.len, s)
        if held and n ~= nil then return { t = "str", v = s, cut = cut or nil, len = #v } end
        local hex = {}
        for i = 1, math.min(#s, 512) do hex[i] = string.format("%02x", string.byte(s, i)) end
        return { t = "bin", v = table.concat(hex, " "), len = #v }
    end
    if t == "table" then
        if (depth or 0) > 8 then return { t = "str", v = "(nested too deep)" } end
        local out, n = {}, 0
        if #v > 0 or next(v) == nil then
            for i = 1, math.min(#v, REPLY_ITEMS) do out[i] = reply_of(v[i], (depth or 0) + 1) end
            return { t = "arr", v = out, len = #v }
        end
        for k, x in pairs(v) do
            n = n + 1
            if n > REPLY_ITEMS then break end
            out[#out + 1] = { reply_of(k, (depth or 0) + 1), reply_of(x, (depth or 0) + 1) }
        end
        return { t = "map", v = out }
    end
    return { t = "str", v = tostring(v) }
end

local MAX_VALUE = 512000

local function post(req, res, params, query)
    local user = accounts().current(req)
    if user == nil then return fail(res, "sign in to use the space viewer", 401) end
    if not is_admin(req) then return fail(res, "this account may look, not change", 403) end

    local tail = string.sub(req.url, string.len("/api/admin/") + 1)
    local q = string.find(tail, "?", 1, true)
    if q then tail = string.sub(tail, 1, q - 1) end
    local space = query.space
    local body = req.body or ""

    if tail == "key_put" or tail == "key_rm" then
        local sp, err = open(space)
        if sp == nil then return fail(res, err, 404) end
        local key = query.key
        if key == nil or key == "" then return fail(res, "a key is required") end
        if string.sub(key, 1, 3) == "fs:" then
            return fail(res, "keys under fs: belong to the file store; use the Files tab", 403)
        end
        if tail == "key_rm" then
            sp:remove(key)
            return ok(res, { ok = true })
        end
        if #body > MAX_VALUE then
            return fail(res, "a value is at most " .. MAX_VALUE .. " bytes; that is " .. #body, 413)
        end
        sp[key] = body
        return ok(res, { ok = true, size = #body })
    end

    -- The RESP console: one command, run in the named space with `sp:call` - barch.call
    -- bound to that space - as the route user, whose ACL still decides what may run.
    -- Asynchronous commands (KEYS, RANGE, VALUES) are refused by barch in a script.
    if tail == "resp" then
        local sp, err = open(space)
        if sp == nil then return fail(res, err, 404) end
        local held, j = pcall(simdjson.parse, body)
        if not held or type(j) ~= "table" or type(j.args) ~= "table" or #j.args == 0 then
            return fail(res, "a command is required: {\"args\": [\"GET\", \"key\"]}")
        end
        local args = {}
        for i = 1, #j.args do args[i] = tostring(j.args[i]) end
        local ran, out = pcall(function() return sp:call(table.unpack(args)) end)
        if not ran then
            -- "shop:1: FUNCTION ..." is where the error was raised, not what it says
            local why = string.gsub(tostring(out), "^[^:]*:%d+: ", "")
            return ok(res, { ok = false, error = why, space = space })
        end
        return ok(res, { ok = true, reply = reply_of(out), space = space })
    end

    if tail == "config_set" then
        local name = query.name
        if name == nil or name == "" then return fail(res, "a setting name is required") end
        local lock = locked(name, is_local(req))
        if lock then
            return fail(res, name .. " cannot be set here: " .. lock, 403)
        end
        -- only a setting that is already there: SET must not be a way to invent one
        local held, have = pcall(function() return barch.call("CONFIG", "GET", name) end)
        if not held or have == nil or #have == 0 then
            return fail(res, "no such setting: " .. name, 404)
        end
        local set, why = pcall(function() return barch.call("CONFIG", "SET", name, body) end)
        if not set then return fail(res, tostring(why), 409) end
        local now = barch.call("CONFIG", "GET", name)
        return ok(res, { ok = true, name = name, value = now and tostring(now[2]) or body })
    end

    if tail == "file_put" or tail == "file_mv" or tail == "file_rm" or tail == "dir_mk" or tail == "dir_rm" then
        -- `barch.fs` is the running space's; `barch.fs.space(name)` is the same
        -- functions bound to another one, and raises for a space that is not there
        local held, fs = pcall(function() return barch.fs.space(space) end)
        if not held or fs == nil then return fail(res, "no key space called " .. tostring(space), 404) end
        local path = query.path
        if path == nil or string.sub(path, 1, 1) ~= "/" then return fail(res, "an absolute path is required") end
        if tail == "dir_mk" then
            local made, why = pcall(function() return fs.mkdir(path) end)
            if not made then return fail(res, tostring(why), 409) end
            return ok(res, { ok = true })
        end
        if tail == "dir_rm" then
            if path == "/" then return fail(res, "the root cannot be deleted", 403) end
            local gone, why = fs.rmdir(path, query.recursive == "1")
            if not gone then
                res.code = 409
                res:header("Content-Type", "application/json")
                res.body = simdjson.encode({ error = why or "could not delete", notempty = string.find(why or "", "not empty", 1, true) ~= nil })
                return
            end
            return ok(res, { ok = true })
        end
        if tail == "file_rm" then
            if not fs.remove(path) then return fail(res, "no such file", 404) end
            return ok(res, { ok = true })
        end
        if tail == "file_mv" then
            local to = query.to
            if to == nil or string.sub(to, 1, 1) ~= "/" then return fail(res, "an absolute new path is required") end
            local moved, why = fs.rename(path, to)
            if not moved then return fail(res, why or "could not rename", 409) end
            return ok(res, { ok = true })
        end
        -- an existing file is only replaced when the page says so, having asked
        if query.overwrite ~= "1" and fs.stat(path) ~= nil then
            res.code = 409
            res:header("Content-Type", "application/json")
            res.body = simdjson.encode({ error = path .. " exists", exists = true })
            return
        end
        local chunks = fs.put(path, body, (query.type ~= nil and query.type ~= "") and query.type or nil)
        return ok(res, { ok = true, size = #body, chunks = chunks })
    end
    fail(res, "no such endpoint", 404)
end

function transport()
    return {
        kind = "resource",
        route = "/api/admin/*",
        methods = { GET = get, POST = post },
        send = "application/json",
    }
end
