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

-- Password hashing, in pure Luau since there is no crypto library in the
-- sandbox - see modules/sha256.luau.
local function sha256()
    return require(":/modules/sha256.luau")
end

-- Accounts live in their own key space, apart from the catalog: a customer
-- record is not shop data, and dropping/reloading the catalog (as this
-- example has done more than once) must not touch who is registered.
-- `barch.space.users` reaches it cross-space; USE users in setup.sh is what
-- makes the space exist in the first place, since barch.space cannot create
-- one as a side effect of being named.
local function users()
    return barch.space.users
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

-- A random token, hex - used both for a password salt and for a session id.
-- Same shape as the newsid() in examples/http/luau/session.luau.
local function token(nbytes)
    local t = {}
    for i = 1, nbytes do
        t[i] = string.format("%02x", math.random(0, 255))
    end
    return table.concat(t)
end

local function normalize_email(email)
    return string.lower(string.match(email or "", "^%s*(.-)%s*$"))
end

-- The signed-in user, from the sid cookie: nil if there is none or it does
-- not (or no longer) map to an account. Sessions live in the users space
-- alongside the accounts, since a session is data about a registered user.
local function current_user(req)
    local sid = req:cookie("sid")
    if sid == nil or sid == "" then return nil end
    local email = users()["sess:" .. sid]
    if email == nil then return nil end
    local raw = users()["user:" .. email]
    if raw == nil then return nil end
    return simdjson.parse(raw)
end

local function sign_in(res, email)
    local sid = token(16)
    users()["sess:" .. sid] = email
    res:cookie("sid", sid, { path = "/", httponly = true })
end

-- A rating is one document per (asin, email), plus two running counters so
-- the average does not have to be recomputed from every review on each
-- read. Naming them ratingsum:/ratingcount:<asin> keeps them next to
-- rating:<asin>:<email> in key order without colliding with it.
local function rating_key(asin, email)
    return "rating:" .. asin .. ":" .. email
end

-- The reviews for a product, newest first. There is no clock in the Luau
-- sandbox (open_safe has no os library), so "newest" is a per-product
-- sequence counter handed out at write time rather than a timestamp.
-- DIR LS walks the composite key the same way it walks catalog:<top>:<sub>
-- for LOADKEYS (see the README) - one implementation, no index to maintain.
local function reviews_of(asin)
    local out = {}
    local lines = barch.call("DIR", "LS", "rating:" .. asin, "SEP", ":")
    for _, line in ipairs(lines or {}) do
        local email = string.match(line, "^%S+%s+%S+%s+(.*)$")
        if email then
            local raw = barch.store.get(rating_key(asin, email))
            if raw ~= nil then
                out[#out + 1] = simdjson.parse(raw)
            end
        end
    end
    table.sort(out, function(a, b) return (a.seq or 0) > (b.seq or 0) end)
    return out
end

-- GET /api/ratings/<asin>  {average, count, mine, reviews:[{name, stars, comment}]}
local function ratings_get(req, res, asin)
    if asin == "" then return fail(res, "an asin is required") end
    local count = tonumber(barch.store.get("ratingcount:" .. asin) or "0") or 0
    local sum = tonumber(barch.store.get("ratingsum:" .. asin) or "0") or 0
    local u = current_user(req)
    local mine, reviews = nil, {}
    for _, r in ipairs(reviews_of(asin)) do
        if u ~= nil and r.email == u.email then
            mine = { stars = r.stars, comment = r.comment }
        end
        reviews[#reviews + 1] = { name = r.name ~= "" and r.name or "A shopper", stars = r.stars, comment = r.comment }
    end
    send(res, simdjson.encode({
        average = count > 0 and (sum / count) or 0,
        count = count,
        mine = mine,
        reviews = reviews,
    }))
end

-- POST /api/ratings  {asin, stars, comment}
-- One rating per signed-in account per product; posting again on the same
-- product replaces it rather than adding a second line, and the counters
-- are adjusted by the difference rather than re-summed from scratch.
local function rating_post(req, res)
    local u = current_user(req)
    if u == nil then return fail(res, "sign in to rate a product", 401) end
    if req.body == nil or req.body == "" then return fail(res, "an asin and a star rating are required") end
    local j = simdjson.parse(req.body)
    local asin = j.asin
    if type(asin) ~= "string" or asin == "" then return fail(res, "an asin is required") end
    local stars = math.floor(tonumber(j.stars) or 0)
    if stars < 1 or stars > 5 then return fail(res, "stars must be between 1 and 5") end
    if catalog().find(asin) == nil then return fail(res, "no such product", 404) end

    local comment = type(j.comment) == "string" and j.comment or ""
    if #comment > 1000 then comment = string.sub(comment, 1, 1000) end

    local key = rating_key(asin, u.email)
    local prior = barch.store.get(key)
    local seq
    if prior == nil then
        barch.call("INCRBY", "ratingcount:" .. asin, 1)
        barch.call("INCRBYFLOAT", "ratingsum:" .. asin, stars)
        seq = barch.call("INCRBY", "ratingseq:" .. asin, 1)
    else
        local p = simdjson.parse(prior)
        barch.call("INCRBYFLOAT", "ratingsum:" .. asin, stars - p.stars)
        seq = p.seq or 0
    end

    local doc = { asin = asin, email = u.email, name = u.name, stars = stars, comment = comment, seq = seq }
    barch.store.set(key, simdjson.encode(doc))

    local count = tonumber(barch.store.get("ratingcount:" .. asin) or "0") or 0
    local sum = tonumber(barch.store.get("ratingsum:" .. asin) or "0") or 0
    send(res, simdjson.encode({
        ok = true,
        average = count > 0 and (sum / count) or 0,
        count = count,
        mine = { stars = stars, comment = comment },
    }))
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

    -- who this cookie is signed in as, or {user = nil}
    if what == "me" then
        local u = current_user(req)
        if u == nil then return send(res, simdjson.encode({ user = nil })) end
        return send(res, simdjson.encode({ user = { email = u.email, name = u.name } }))
    end

    if what == "ratings" then return ratings_get(req, res, rest) end

    fail(res, "no such endpoint", 404)
end

-- POST /api/register  {name, email, password}
local function register(req, res)
    if req.body == nil or req.body == "" then return fail(res, "name, email and password are required") end
    local j = simdjson.parse(req.body)
    local email = normalize_email(j.email)
    local password = j.password
    if email == "" or type(password) ~= "string" or #password < 6 then
        return fail(res, "an email and a password of at least 6 characters are required")
    end
    local key = "user:" .. email
    if users()[key] ~= nil then return fail(res, "an account with that email already exists", 409) end

    local salt = token(8)
    local doc = {
        email = email,
        name = j.name or "",
        salt = salt,
        hash = sha256().hex(salt .. password),
    }
    users()[key] = simdjson.encode(doc)
    sign_in(res, email)
    send(res, simdjson.encode({ ok = true, user = { email = doc.email, name = doc.name } }))
end

-- POST /api/signon  {email, password}
local function signon(req, res)
    if req.body == nil or req.body == "" then return fail(res, "email and password are required") end
    local j = simdjson.parse(req.body)
    local email = normalize_email(j.email)
    local raw = users()["user:" .. email]
    if raw == nil then return fail(res, "no such account, or the password is wrong", 401) end

    local u = simdjson.parse(raw)
    if sha256().hex(u.salt .. tostring(j.password)) ~= u.hash then
        return fail(res, "no such account, or the password is wrong", 401)
    end
    sign_in(res, u.email)
    send(res, simdjson.encode({ ok = true, user = { email = u.email, name = u.name } }))
end

-- POST /api/signout
local function signout(req, res)
    local sid = req:cookie("sid")
    if sid ~= nil and sid ~= "" then users()["sess:" .. sid] = nil end
    res:cookie("sid", "", { path = "/", max_age = 0, httponly = true })
    send(res, simdjson.encode({ ok = true }))
end

-- POST /api/order  {items:[{asin, qty}], name, email, address}
function post(req, res)
    local what, rest = route_of(req)
    if what == "register" then return register(req, res) end
    if what == "signon" then return signon(req, res) end
    if what == "signout" then return signout(req, res) end
    if what == "ratings" then return rating_post(req, res) end
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
