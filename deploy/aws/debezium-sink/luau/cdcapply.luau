-- Applies one Debezium change to the cache. The handler of the queue
-- "debezium". barchex STREAMS stores each XADD from Debezium and forwards it
-- here (XQUEUE ADD barch.* debezium NOSTORE) as {"stream", "id", "fields"}.
--
-- Each table has a key template, such as order:{order_id}, and a mode:
--
--   invalidate  DEL the row's key. The application reloads it on the next miss.
--   upsert      SET the row's key to the new row as JSON. DEL on delete.
--
-- An UPDATE that changes the key deletes the old key too. A TRUNCATE deletes
-- every key under the template's fixed prefix. The mapping is the key cdc.json,
-- loaded from config/cdc.json.
--
-- An error keeps the change at the head of the queue, and it is tried again.
-- A change that isn't JSON is dropped and counted in cdc:dropped.
--
-- This runs in the space "cdc", where the package loads it, because barchex
-- owns the global space. The keys it writes are in the global space, the one a
-- connection starts in, so every write goes through barch.space["0"].

local CONFIG = "cdc.json"
local BATCH = 1000

local function cache()
    return barch.space["0"]
end

local function fail(why)
    error("CDCAPPLY: " .. why, 0)
end

local function absent(v)
    return v == nil or v == simdjson.null
end

local function table_for(config, schema, tname)
    local tables = config.tables or {}
    local t = tables[schema .. "." .. tname] or tables[tname]
    local d = config.defaults or {}
    if t == nil and d.key == nil then
        return nil
    end
    t = t or {}
    local out = {
        name = schema .. "." .. tname,
        key = t.key or d.key,
        mode = t.mode or d.mode or "invalidate",
        ttl = tonumber(t.ttl or d.ttl or 0) or 0,
    }
    if absent(out.key) then
        fail(out.name .. ": no key, and defaults has none")
    end
    if out.mode ~= "invalidate" and out.mode ~= "upsert" then
        fail(out.name .. ": mode must be invalidate or upsert, not " .. tostring(out.mode))
    end
    return out
end

local function key_for(t, schema, tname, row)
    return (string.gsub(t.key, "{([^}]*)}", function(f)
        if f == "schema" then
            return schema
        end
        if f == "table" then
            return tname
        end
        if absent(row) or row[f] == nil then
            fail(t.name .. ": column '" .. f .. "' is not in the event")
        end
        if row[f] == simdjson.null then
            return "null"
        end
        return tostring(row[f])
    end))
end

-- the fixed start of every key, or "" when a row column comes first
local function prefix_of(t, schema, tname)
    local head, rest = "", t.key
    while true do
        local s, e, f = string.find(rest, "{([^}]*)}")
        if s == nil then
            return head .. rest
        end
        head = head .. string.sub(rest, 1, s - 1)
        if f == "schema" then
            head = head .. schema
        elseif f == "table" then
            head = head .. tname
        else
            return head
        end
        rest = string.sub(rest, e + 1)
    end
end

-- the smallest string greater than every string that starts with p
local function after_prefix(p)
    while #p > 0 and string.byte(p, #p) == 255 do
        p = string.sub(p, 1, #p - 1)
    end
    if #p == 0 then
        return string.rep("\255", 64)
    end
    return string.sub(p, 1, #p - 1) .. string.char(string.byte(p, #p) + 1)
end

local function truncate(t, schema, tname)
    local lower = prefix_of(t, schema, tname)
    if lower == "" then
        -- nothing safe to delete under; the keys stay as they are
        return
    end
    local upper = after_prefix(lower)
    while true do
        -- The range includes lower and leaves out upper. The keys found are
        -- deleted, so each round starts at the prefix again. Not RANGE: a
        -- script can't call an asynchronous command.
        local keys = cache():range(lower, upper, BATCH)
        if #keys == 0 then
            return
        end
        cache():call("DEL", unpack(keys))
    end
end

-- the change itself, out of a schema envelope if there is one
local function unwrap(event)
    if type(event) == "table" and event.payload ~= nil and event.schema ~= nil then
        return event.payload
    end
    return event
end

local function apply(config, event)
    event = unwrap(event)
    if type(event) ~= "table" then
        return
    end
    local source = event.source
    if type(source) ~= "table" then
        return
    end
    local schema = absent(source.schema) and "" or tostring(source.schema)
    local tname = absent(source.table) and "" or tostring(source.table)
    if tname == "" then
        return
    end
    local t = table_for(config, schema, tname)
    if t == nil then
        return
    end
    local op, before, after = event.op, event.before, event.after

    if op == "t" then
        return truncate(t, schema, tname)
    end
    if op == "d" then
        cache():call("DEL", key_for(t, schema, tname, before))
        return
    end
    if op ~= "c" and op ~= "u" and op ~= "r" then
        return
    end

    local key = key_for(t, schema, tname, after)
    -- A changed primary key leaves the old row's key behind. pgoutput sends
    -- the old key columns in before when that happens.
    if not absent(before) then
        local ok, old = pcall(key_for, t, schema, tname, before)
        if ok and old ~= key then
            cache():call("DEL", old)
        end
    end
    if t.mode == "invalidate" then
        cache():call("DEL", key)
    elseif t.ttl > 0 then
        cache():call("SET", key, simdjson.encode(after), "EX", tostring(t.ttl))
    else
        cache():call("SET", key, simdjson.encode(after))
    end
end

local function dropped()
    cache():call("INCR", "cdc:dropped")
end

function call(message, sequence, attempts)
    -- XADD writes only JSON here, but a QUEUE PUSH by hand may not.
    -- Trying it again won't help.
    local parsed, m = pcall(simdjson.parse, message)
    if not parsed or type(m) ~= "table" then
        dropped()
        return "dropped"
    end
    local config = barch.store.getJson(CONFIG)
    if type(config) ~= "table" then
        fail("no mapping: the key " .. CONFIG .. " is missing or not JSON")
    end
    local fields = m.fields or {}
    -- compact format: the field is the change's key, the value is the change
    for i = 2, #fields, 2 do
        local value = fields[i]
        -- "default" is what Debezium sends for a tombstone's value
        if value ~= "default" then
            local ok, event = pcall(simdjson.parse, value)
            if ok and type(event) == "table" then
                apply(config, event)
            else
                dropped()
            end
        end
    end
    return "ok"
end
