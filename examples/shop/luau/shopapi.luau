-- The shop's API: the catalog, the basket and the order, plus the routing for the
-- two things that are not shop data at all.
--
--   /meta/index.json in the file store  one compact row per product, so the front
--                                       page is one read rather than a thousand
--   /catalog/<Top>/<Sub>/<asin>.json    the full record, read for a product page
--   order:<id>                          what checkout writes
--
-- The catalog is an fs directory tree and the directories *are* the category tree,
-- so the category list is `fs.list` and nothing has to maintain an index of it.
--
-- Accounts and ratings used to be here too, reaching sideways into `users` and
-- writing ratings into `shop`. They are now modules in the spaces that hold their
-- data - `users:/modules/accounts.luau` and `ratings:/modules/ratings.luau` - and
-- what is left here is the routing and whatever those two need from the shop. A
-- route is a stored function in the server's space and that does not change, so
-- those modules run here and reach their own data through `barch.space.NAME`.


-- Everything the handlers require is required inside a call rather than at the top
-- of the file: installing a function runs the chunk, and at that moment there is no
-- store for require to read a module out of ("require has no store here"). It
-- caches, so this costs one lookup per session and not one per request.

-- The catalog lookup, shared with the image source - see modules/catalog.luau.
local function catalog()
    return require(":/modules/catalog.luau")
end

-- Accounts and sessions, out of the space that holds them. `barch.space.users`
-- looks a space up rather than creating one, and require reads that space's file
-- store with the caller's rights there, so both need `USE users` in setup.sh to
-- have happened first. Same for ratings.
local function accounts()
    return require("users:/modules/accounts.luau")
end

local function ratings()
    return require("ratings:/modules/ratings.luau")
end

-- Street and suburb lookup for the checkout's address step, out of the space
-- geo.py loaded. Same arrangement again: the data and the code that reads it are
-- both in `geo`, and this route reaches them from `shop`.
local function geo()
    return require("geo:/modules/geo.luau")
end

-- Orders, which are not catalog data either - see orders/modules/orders.luau.
local function orders()
    return require("orders:/modules/orders.luau")
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

-- The modules answer with a table either way - {error, code} or the body to send -
-- so that neither of them has to know what an http response looks like.
local function answer(res, out)
    if out.error ~= nil then return fail(res, out.error, out.code) end
    out.code = nil
    send(res, simdjson.encode(out))
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
        local u = accounts().current(req)
        if u == nil then return send(res, simdjson.encode({ user = nil })) end
        return send(res, simdjson.encode({ user = { email = u.email, name = u.name } }))
    end

    -- the ratings space keeps opinions about asins and no catalog, so who is
    -- signed in comes from `users` and the product check stays here
    if what == "ratings" then
        return answer(res, ratings().get(rest, accounts().current(req)))
    end

    -- /api/places?kind=street|sub|prov&q=lon&metro=cape-town - what the address
    -- step calls as you type. No account needed: this is public reference data
    if what == "places" then
        return answer(res, geo().lookup(query.kind, query.q, query.metro, query.limit))
    end

    -- /api/orders and /api/orders/<id> - this account's, and nobody else's. The
    -- email comes off the session rather than the query string, so there is no
    -- version of this that reads somebody else's orders
    if what == "orders" then
        local u = accounts().current(req)
        local email = u ~= nil and u.email or nil
        if rest ~= "" then return answer(res, orders().one(email, rest)) end
        return answer(res, orders().mine(email, query.limit))
    end

    fail(res, "no such endpoint", 404)
end

-- What the shop will take. The page offers exactly these and the server checks
-- against the same list, so a hand-rolled POST cannot invent a payment method.
local pay_methods = { cod = true, ["card-on-delivery"] = true, eft = true }

-- POST /api/order  {items, name, email, phone, address, delivery, payment}
function post(req, res)
    local what, rest = route_of(req)
    if what == "register" then return answer(res, accounts().register(req, res)) end
    if what == "signon" then return answer(res, accounts().signon(req, res)) end
    if what == "signout" then return answer(res, accounts().signout(req, res)) end
    if what == "ratings" then
        return answer(res, ratings().post(req.body, accounts().current(req), function(asin)
            return catalog().find(asin) ~= nil
        end))
    end
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

    -- The address and the payment choice are checked here as well as in the page,
    -- for the same reason the prices are: a basket that arrives without an
    -- address is a basket that can arrive with anything at all. The parts are
    -- kept beside the one-line version so nothing has to parse that back apart.
    local d = order.delivery or {}
    local function said(v) return type(v) == "string" and v ~= "" end
    if not said(order.name) or not said(order.address) then
        return fail(res, "a name and a delivery address are required")
    end
    local method = (order.payment or {}).method or ""
    if not pay_methods[method] then
        return fail(res, "choose one of the payment methods offered")
    end

    -- no id here: `orders.place` gives it one. It used to be
    -- `INCRBY order:seq`, which is a command and so ran against *this* space -
    -- the catalog's - where a reload would reset the counter and start handing
    -- out ids that already existed
    local doc = {
        placed = order.placed or "",
        name = order.name or "",
        email = order.email or "",
        phone = order.phone or "",
        address = order.address or "",
        delivery = {
            unit = d.unit or "",
            -- the street line as it was typed, and split: "12 Long Street" is what
            -- a label prints, `house` and `street` are what anything counting
            -- streets wants, and neither is recoverable from the other for free
            line = d.line or "",
            house = d.house or "", street = d.street or "", suburb = d.suburb or "",
            province = d.province or "", postcode = d.postcode or "", note = d.note or "",
        },
        -- the method and nothing else: no card number is asked for anywhere in
        -- this example, so there is none to store and none to leak
        payment = { method = method, state = "unpaid" },
        items = lines,
        total = math.floor(total * 100 + 0.5) / 100,
        currency = "USD",
    }
    -- filed under the signed-in account when there is one, and under whatever
    -- address was typed when there is not
    local u = accounts().current(req)
    local filed = u ~= nil and u.email or accounts().normalize_email(order.email)
    send(res, simdjson.encode({ ok = true, order = orders().place(doc, filed) }))
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
