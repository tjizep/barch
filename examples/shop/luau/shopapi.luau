-- The shop's API. Everything it serves comes out of this key space:
--
--   /index.json in the file store   one compact row per product, so the front page
--                                   is one read rather than a thousand
--   /catalog/<Top>/<Sub>/<asin>.json  the full record, read for a product page
--   order:<id>                      what checkout writes
--
-- The catalog is an fs directory tree and the directories *are* the category tree,
-- so the category list is `fs.list` and nothing has to maintain an index of it.


-- The catalog lookup is a module in the file store, shared with the other route -
-- see modules/catalog.luau. `require` has to happen inside a call rather than at
-- the top of the file: installing a function runs the chunk, and at that moment
-- there is no store for require to read a module out of ("require has no store
-- here"). It caches, so this costs one lookup per session and not one per request.
local function catalog()
    return require(":/modules/catalog.luau")
end

function call()
    return "shopapi"
end

local function send(res, body, code)
    res.code = code or 200
    res:header("Content-Type", "application/json")
    res:header("Access-Control-Allow-Origin", "*")
    res.body = body
end

local function fail(res, why, code)
    send(res, simdjson.encode({ error = why }), code or 400)
end

-- /api/<what>/<rest>
local function route_of(req)
    local tail = string.sub(req.url, string.len("/api/") + 1)
    local q = string.find(tail, "?", 1, true)
    if q then tail = string.sub(tail, 1, q - 1) end
    return string.match(tail, "^([a-z]+)/?(.*)$")
end

function get(req, res, params, query)
    local what, rest = route_of(req)

    -- the whole index, which the page filters and pages through itself. 300KB
    -- once beats a request per keystroke
    if what == "index" then
        local body = barch.fs.get("/meta/index.json")
        if body == nil then return fail(res, "the catalog is not loaded", 503) end
        return send(res, body)
    end

    -- the category tree, which is just the directories under /catalog
    -- the directories under /catalog *are* the category tree, so nothing keeps an
    -- index of it. names.json only supplies what to show for a slug
    if what == "categories" then
        local names = simdjson.parse(barch.fs.get("/meta/names.json") or "{}")
        local out = {}
        for _, top in ipairs(barch.fs.list("/catalog")) do
            if top.dir then
                local subs = {}
                for _, sub in ipairs(barch.fs.list(top.path)) do
                    if sub.dir then
                        subs[#subs + 1] = { slug = sub.name, name = names[sub.name] or sub.name }
                    end
                end
                out[#out + 1] = { slug = top.name, name = names[top.name] or top.name, subs = subs }
            end
        end
        if #out == 0 then return send(res, "[]") end
        return send(res, simdjson.encode(out))
    end

    -- one product: the file the loader wrote, handed back as it is
    if what == "product" then
        local asin = rest
        if asin == "" then return fail(res, "an asin is required") end
        local hit = catalog().find(asin)
        if hit == nil then return fail(res, "no such product", 404) end
        return send(res, hit)
    end

    fail(res, "no such endpoint", 404)
end

-- POST /api/order  {items:[{asin, qty}], name, email, address}
function post(req, res)
    local what = route_of(req)
    if what ~= "order" then return fail(res, "no such endpoint", 404) end
    if req.body == nil or req.body == "" then return fail(res, "an order is required") end

    local order = simdjson.parse(req.body)
    local items = order.items or {}
    if #items == 0 then return fail(res, "the basket is empty") end

    -- priced here, not in the browser: a basket that arrives with its own totals
    -- is a basket that can arrive with any totals it likes
    local total, lines = 0, {}
    for _, item in ipairs(items) do
        local raw = catalog().find(item.asin)
        if raw == nil then return fail(res, "no such product: " .. tostring(item.asin)) end
        local p = simdjson.parse(raw)
        local qty = math.floor(tonumber(item.qty) or 1)
        if qty < 1 then qty = 1 end
        if qty > 99 then qty = 99 end
        total = total + p.price * qty
        lines[#lines + 1] = { asin = p.asin, title = p.title, price = p.price, qty = qty }
    end

    -- barch.store has no counter, so the command does it. INCRBY replies with an
    -- integer, which comes back clean - a bulk string would not (TODO 261)
    local id = "ord-" .. tostring(barch.call("INCRBY", "order:seq", 1))
    local doc = {
        id = id,
        placed = order.placed or "",
        name = order.name or "",
        email = order.email or "",
        address = order.address or "",
        items = lines,
        total = math.floor(total * 100 + 0.5) / 100,
        currency = "USD",
    }
    barch.store.set("order:" .. id, simdjson.encode(doc))
    send(res, simdjson.encode({ ok = true, order = doc }))
end

function transport()
    return {
        kind = "resource",
        route = "/api/*",
        methods = { GET = get, POST = post },
        send = "application/json",
        cors = "*",
    }
end
