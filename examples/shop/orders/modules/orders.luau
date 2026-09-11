-- What checkout writes, and what the account screen reads back.
--
-- Orders were in `shop`, next to the catalog. They are not catalog data: the
-- catalog gets dropped and reloaded whenever the import changes, and an order
-- has to outlive that the same way an account does. It is not a hypothetical -
-- clearing a stale catalog during this work took every order in the space with
-- it while `users` and `ratings` came through untouched. So they live here, with
-- the code that reads them, like everything else that is not the catalog.
--
--   order:<id>                 the document, priced on the server
--   byuser:<email>:<seq>       the id, so one customer's orders are one walk
--
-- The route still runs in `shop`, so `barch.store` and `barch.call` here would
-- be the shop. Everything goes through the `orders` handle.

local function space()
    return barch.space.orders
end

-- An id out of random bytes rather than a counter.
--
-- The obvious thing is `INCRBY order:seq`, which is what this used to do. But
-- `barch.call` runs against the space the *route* is in, which is `shop`, so the
-- counter would sit in the catalog's space and a catalog reload would set it
-- back to zero and start handing out ids that already exist here. Sixteen hex
-- characters has no such failure and needs nothing kept anywhere.
local function newid()
    local t = {}
    for i = 1, 8 do
        t[i] = string.format("%02x", math.random(0, 255))
    end
    return "ord-" .. table.concat(t)
end

-- The per-customer sequence, zero padded so the key sorts the way the number
-- does. Same shape as the ratings sequence and for the same reason: there is no
-- INCRBY to reach from here, so the next one is the highest that is already
-- there plus one, over the handful of orders one account has.
local function index_key(email, seq)
    return string.format("byuser:%s:%08d", email, seq)
end

local function ids_for(email)
    local sp = space()
    local lo = "byuser:" .. email .. ":"
    local hi = "byuser:" .. email .. ";"
    local out = {}
    for _, key in ipairs(sp:range(lo, hi, 500)) do
        local seq = tonumber(string.match(key, ":(%d+)$")) or 0
        out[#out + 1] = { seq = seq, id = sp:get(key) }
    end
    -- newest first, which is the order an account screen wants them in
    table.sort(out, function(a, b) return a.seq > b.seq end)
    return out
end

local function next_seq(email)
    local highest = 0
    for _, row in ipairs(ids_for(email)) do
        if row.seq > highest then highest = row.seq end
    end
    return highest + 1
end

-- Write one. `doc` arrives priced and validated; this gives it an id, a state
-- and its place in the customer's list, and hands the finished thing back.
--
-- `email` is who to file it under: the signed-in account when there is one, and
-- what they typed otherwise, so an order placed signed-out still reaches the
-- account screen if that address later registers.
function place(doc, email)
    local sp = space()
    doc.id = newid()
    -- nothing in this example fulfils an order, so everything is pending. The
    -- field is here because an order without a state is not an order, and
    -- because the account screen has to show something honest
    doc.state = "pending"
    sp:set("order:" .. doc.id, simdjson.encode(doc))
    if email ~= nil and email ~= "" then
        sp:set(index_key(email, next_seq(email)), doc.id)
    end
    return doc
end

-- GET /api/orders - this account's orders, newest first.
function mine(email, limit)
    if email == nil or email == "" then
        return { error = "sign in to see your orders", code = 401 }
    end
    limit = math.floor(tonumber(limit) or 20)
    if limit < 1 then limit = 1 end
    if limit > 100 then limit = 100 end

    local sp = space()
    local out, pending = {}, 0
    for _, row in ipairs(ids_for(email)) do
        if #out >= limit then break end
        local raw = row.id ~= nil and sp:get("order:" .. row.id) or nil
        if raw ~= nil then
            local o = simdjson.parse(raw)
            if o.state == "pending" then pending = pending + 1 end
            out[#out + 1] = {
                id = o.id, state = o.state, placed = o.placed,
                total = o.total, currency = o.currency,
                items = o.items, address = o.address,
                payment = o.payment,
            }
        end
    end
    return { orders = out, pending = pending }
end

-- GET /api/orders/<id> - one order, and only if it is this account's.
--
-- The id is checked against the caller's own index rather than trusted, so
-- knowing an id is not the same as being allowed to read it.
function one(email, id)
    if email == nil or email == "" then
        return { error = "sign in to see your orders", code = 401 }
    end
    local ok = false
    for _, row in ipairs(ids_for(email)) do
        if row.id == id then ok = true break end
    end
    if not ok then return { error = "no such order", code = 404 } end
    local raw = space():get("order:" .. id)
    if raw == nil then return { error = "no such order", code = 404 } end
    return { order = simdjson.parse(raw) }
end
