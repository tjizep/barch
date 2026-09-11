-- A read-only look at every key space the server has.
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
-- above it. The obvious `key .. "\0"` does not work - a bound with a trailing
-- NUL matches *nothing at all*, which is how a directory listing came back with
-- one file in it instead of four. `\1` is the smallest byte that behaves, and it
-- still sorts below any sibling whose name merely starts the same way.
local function after(key)
    return key .. "\1"
end

-- An integer out of `barch.call` is its own Luau type here, not a number, and
-- `tonumber` does not take it: `tonumber(reply)` is nil and `reply + 0` raises
-- "attempt to perform arithmetic (add) on integer and number". Going through
-- tostring is what works. This cost an afternoon of every space reporting zero
-- keys, because `tonumber(...) or 0` swallows it without a word.
local function as_number(v)
    if type(v) == "number" then return v end
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

-- GET /api/admin/keys?space=geo&prefix=street:&after=...&limit=100
--
-- One page of keys, with a short value preview beside each. `after` is the last
-- key of the page before, which is where the next one starts - the same
-- continuation SCAN uses, and the reason the walk does not have to hold a
-- cursor anywhere.
local function keys(req, res, query)
    local sp, err = open(query.space)
    if sp == nil then return fail(res, err, 404) end
    local limit = clamp(query.limit, 1, 500, 100)
    local lo, hi = bounds(query.prefix)
    if query.after ~= nil and query.after ~= "" and query.after >= lo then
        lo = query.after
    end
    local out = {}
    -- one more than asked for, so the page can say whether there is another
    local walked = sp:range(lo, hi, limit + 2)
    for _, key in ipairs(walked) do
        if #out >= limit then break end
        if key ~= query.after then
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
    end
    return ok(res, {
        space = query.space, prefix = query.prefix or "",
        keys = out,
        more = #walked > #out + (query.after ~= nil and query.after ~= "" and 1 or 0),
        last = #out > 0 and out[#out].key or nil,
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

-- GET /api/admin/fs?space=shop&path=/modules
--
-- One level of the file store, built out of the name records. A directory is a
-- `fs:n:<path>/` key holding `{dir:true}`; a file is `fs:n:<path>` holding the
-- inode id and the size. Both are keys, which is the whole point.
local function files(req, res, query)
    local sp, err = open(query.space)
    if sp == nil then return fail(res, err, 404) end
    if sp:get("fs:layout") == nil then
        return ok(res, { space = query.space, path = "/", files = false, entries = {} })
    end
    local path = query.path or "/"
    if string.sub(path, 1, 1) ~= "/" then path = "/" .. path end
    if path ~= "/" and string.sub(path, -1) == "/" then path = string.sub(path, 1, -2) end

    local base = "fs:n:" .. (path == "/" and "/" or path .. "/")
    local _, hi = bounds(base)
    local lo = base
    local out = {}

    -- One level, by stepping over the subtrees rather than reading them.
    --
    -- The first version of this walked every key under the prefix and threw away
    -- anything with a slash left in it, which works until a directory is big:
    -- `/catalog` is 7,344 files, so the walk ran out inside it and `/meta` and
    -- `/modules` were simply missing from the listing of `/`. Now each step asks
    -- for one key and then jumps: past a file to the next key, and past a
    -- directory to the first thing that cannot be inside it. '/' is 0x2f, so
    -- 0x30 is the first byte after the whole subtree - and it is still before
    -- any sibling whose name merely starts the same way, which is why this skips
    -- `catalog/` without skipping `catalogue`.
    while #out < 2000 do
        local step = sp:range(lo, hi, 1)
        if #step == 0 then break end
        local key = step[1]
        local rest = string.sub(key, #base + 1)
        if rest == "" then
            lo = after(key)
        else
            local cut = string.find(rest, "/", 1, true)
            if cut then
                local name = string.sub(rest, 1, cut - 1)
                out[#out + 1] = { name = name, dir = true }
                lo = base .. name .. string.char(0x30)
            else
                local entry = { name = rest, dir = false }
                local meta = sp:get(key)
                if meta ~= nil then
                    local m = simdjson.parse(meta)
                    entry.size = m.size
                    entry.type = m.type
                    entry.version = m.version
                end
                out[#out + 1] = entry
                lo = after(key)
            end
        end
    end
    table.sort(out, function(a, b)
        if a.dir ~= b.dir then return a.dir end
        return a.name < b.name
    end)
    return ok(res, { space = query.space, path = path, files = true, entries = out })
end

-- GET /api/admin/config - the global settings, and the per space ones beside them.
local function config(req, res)
    -- `CONFIG` is one command for GET and SET and carries `write` and `config`
    -- together, so reading the global settings means the HTTP user can also
    -- change them. setup.sh grants it and says why; without the grant this is a
    -- refusal rather than a crash, and the page says which grant is missing
    -- instead of showing an empty list as though there were no settings.
    local held, flat = pcall(function() return barch.call("CONFIG", "GET", "*") end)
    local global, why = {}, nil
    if held then
        for i = 1, #(flat or {}), 2 do
            global[#global + 1] = { name = flat[i], value = tostring(flat[i + 1]) }
        end
        table.sort(global, function(a, b) return a.name < b.name end)
    else
        why = "the HTTP user has no `config` category, so the global settings "
           .. "cannot be read: ACL SETUSER web on +config"
    end
    return ok(res, { global = global, global_error = why, spaces = settings_by_space() })
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

function get(req, res, params, query)
    -- Signed in, at the least. This is every space the HTTP user can read, so it
    -- is not for a public port whatever the gate says.
    if accounts().current(req) == nil then
        return fail(res, "sign in to use the space viewer", 401)
    end
    local tail = string.sub(req.url, string.len("/api/admin/") + 1)
    local q = string.find(tail, "?", 1, true)
    if q then tail = string.sub(tail, 1, q - 1) end

    if tail == "spaces" then return spaces(req, res) end
    if tail == "keys" then return keys(req, res, query) end
    if tail == "value" then return value(req, res, query) end
    if tail == "fs" then return files(req, res, query) end
    if tail == "config" then return config(req, res) end
    if tail == "stats" then return stats(req, res) end
    fail(res, "no such endpoint", 404)
end

function transport()
    return {
        kind = "resource",
        route = "/api/admin/*",
        methods = { GET = get },
        send = "application/json",
    }
end
