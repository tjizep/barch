-- Looking a place up while somebody types, in the key space that holds them.
--
-- Same arrangement as accounts and ratings: this file lives in the `geo` file
-- store and `shopapi.luau` reaches it with
-- `require("geo:/modules/geo.luau")`. The route still runs in `shop`, so
-- `barch.store` here would be the shop - everything below goes through the
-- `geo` handle instead.
--
-- What geo.py put there, all in this one space:
--
--   street:<NAME>:<METRO>      {name, metro, ways, lat, lon}
--   sub:<NAME>:<KIND>:<id>     {name, kind, lat, lon}
--   prov:<NAME>                {name}
--   meta:loaded                what was loaded, and when
--
-- One space does both halves of this because of hybrid keys: the ART owns the
-- leaves so `street:LONG` is an ordered range walk, and the hash indexes them so
-- the full key is a point read. The older shape of this - see
-- examples/flask/overture.py - needed nine spaces to get both.

local function space()
    return barch.space.geo
end

-- Upper case and single-spaced, which is how the keys were written. There is no
-- accent folding here: geo.py already stripped them on the way in, and a browser
-- sending "Ka" for "Kâ" is a search that finds slightly too much rather than a
-- wrong answer.
local function plain(s)
    return string.upper(string.gsub(string.gsub(s or "", "%s+", " "), "^%s*(.-)%s*$", "%1"))
end

-- A prefix walk is a range from the prefix to the prefix with its last byte
-- nudged up. ':' is 0x3a, so `street:LONG` runs to `street:LONH` and stops
-- before anything that is not this prefix.
local function upper_bound(prefix)
    local last = string.byte(prefix, #prefix)
    return string.sub(prefix, 1, #prefix - 1) .. string.char(last + 1)
end

-- How many keys to pull before filtering. A prefix like "A" matches tens of
-- thousands and the picker shows ten, so the walk is bounded and the answer says
-- when it was cut short rather than pretending it was the whole of it.
local scan = 400

local function walk(prefix, limit, decode)
    local sp = space()
    local out = {}
    local keys = sp:range(prefix, upper_bound(prefix), scan)
    for _, key in ipairs(keys) do
        if #out >= limit then break end
        local raw = sp:get(key)
        if raw ~= nil then
            local rec = simdjson.parse(raw)
            out[#out + 1] = decode(rec, key)
        end
    end
    return out, #keys >= scan
end

-- GET /api/places?kind=street&q=lon[&metro=cape-town]
--
-- Streets are one key per name per metro, so the same name in two cities is two
-- entries and the metro is part of what is shown. `metro` narrows it, which is
-- what the address screen does once a city is picked.
function streets(q, metro, limit)
    local p = plain(q)
    if #p < 2 then return {} end
    local want = plain(metro or "")
    local hits, more = walk("street:" .. p, limit + 20, function(rec, key)
        return { name = rec.name, metro = rec.metro, ways = rec.ways,
                 lat = rec.lat, lon = rec.lon }
    end)
    local out = {}
    for _, h in ipairs(hits) do
        if #out >= limit then break end
        if want == "" or plain(h.metro) == want then
            out[#out + 1] = h
        end
    end
    -- a road made of more ways is a longer road, and a longer road is the one
    -- somebody typing three letters more likely means
    table.sort(out, function(a, b) return (a.ways or 0) > (b.ways or 0) end)
    return out, more
end

-- GET /api/places?kind=sub&q=constan
function suburbs(q, limit)
    local p = plain(q)
    if #p < 2 then return {} end
    local out, more = walk("sub:" .. p, limit, function(rec, key)
        return { name = rec.name, kind = rec.kind, lat = rec.lat, lon = rec.lon }
    end)
    return out, more
end

-- The provinces, for the dropdown. Nine of them and they do not change, so this
-- is a walk of the whole prefix rather than anything cleverer.
function provinces()
    local out = walk("prov:", 64, function(rec) return rec.name end)
    table.sort(out)
    return out
end

-- What the loader says it loaded. The address screen shows this, because a
-- picker that finds nothing should say whether the data is missing or the
-- spelling is.
function loaded()
    local raw = space():get("meta:loaded")
    if raw == nil then return nil end
    return simdjson.parse(raw)
end

-- The one endpoint behind all of it: {kind, q, metro, limit} in, a list out.
function lookup(kind, q, metro, limit)
    limit = math.floor(tonumber(limit) or 10)
    if limit < 1 then limit = 1 end
    if limit > 25 then limit = 25 end
    if kind == "street" then
        local hits, more = streets(q, metro, limit)
        return { kind = "street", hits = hits, more = more or false }
    end
    if kind == "sub" then
        local hits, more = suburbs(q, limit)
        return { kind = "sub", hits = hits, more = more or false }
    end
    if kind == "prov" then
        return { kind = "prov", hits = provinces(), more = false }
    end
    return { error = "kind must be street, sub or prov", code = 400 }
end
