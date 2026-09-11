-- What shoppers say about a product, in a key space of its own.
--
-- `p.rating`/`p.reviews` on a catalog record are the Amazon numbers prepare.py
-- imported once and never change. A review written on this storefront is a
-- different thing with a different lifetime, so it is not in `shop` next to the
-- catalog it would be dropped with, and not in `users` either - a rating is data
-- about a product, not about an account.
--
-- This file lives in the `ratings` file store and the handlers reach it with
-- `require("ratings:/modules/ratings.luau")`. As with the accounts module, the
-- code moves but the execution does not: the route is still a stored function in
-- `shop`, so `barch.store` and `barch.call` here would write to the shop. Every
-- read and write below goes through the `ratings` handle instead.
--
--   rating:<asin>:<email>   {asin, email, name, stars, comment, seq}
--   ratingcount:<asin>      how many reviews the product has
--   ratingsum:<asin>        their stars added up, so an average is two reads

local function space()
    return barch.space.ratings
end

local function key_of(asin, email)
    return "rating:" .. asin .. ":" .. email
end

-- Every review of one product, newest first.
--
-- The shop version walked this with `DIR LS rating:<asin> SEP :`, which is a
-- command and so would have gone to the space the route runs in. The handle's
-- `range` is the same walk by another door: ':' is 0x3a and ';' is 0x3b, so the
-- half open bound picks up exactly the keys with this asin in them. It is
-- `text_range` underneath, which reads the composite region as well as the plain
-- one - the thing TODO 260 was about - so a key with a space in it still shows up.
--
-- There is no clock in the Luau sandbox (open_safe opens no os library), so
-- "newest" is a per-product sequence number handed out at write time.
local function reviews_of(asin)
    local sp = space()
    local out = {}
    for _, key in ipairs(sp:range("rating:" .. asin .. ":", "rating:" .. asin .. ";", 1000)) do
        local raw = sp:get(key)
        if raw ~= nil then
            out[#out + 1] = simdjson.parse(raw)
        end
    end
    table.sort(out, function(a, b) return (a.seq or 0) > (b.seq or 0) end)
    return out
end

-- Add the reviews up and write the two totals back.
--
-- The shop version moved these with INCRBY and INCRBYFLOAT, which a handle cannot
-- do: it has get, set and range and no read-modify-write in the store. Rather
-- than fake an increment with a get and a set - which loses a vote when two
-- people rate the same product at the same moment, and drifts for good once it
-- has - the totals are recomputed from the reviews that are actually there and
-- stored as a cache. The walk is one product's reviews, it happens on a write and
-- never on a read, and a total that is wrong is corrected by the next one.
local function retotal(asin, reviews)
    local count, sum = 0, 0
    for _, r in ipairs(reviews) do
        count = count + 1
        sum = sum + (r.stars or 0)
    end
    local sp = space()
    sp:set("ratingcount:" .. asin, tostring(count))
    sp:set("ratingsum:" .. asin, tostring(sum))
    return count, sum
end

local function totals(asin)
    local sp = space()
    local count = tonumber(sp:get("ratingcount:" .. asin) or "0") or 0
    local sum = tonumber(sp:get("ratingsum:" .. asin) or "0") or 0
    return count, sum
end

local function averaged(count, sum, mine)
    return {
        average = count > 0 and (sum / count) or 0,
        count = count,
        mine = mine,
    }
end

-- GET /api/ratings/<asin>, for `user` (which may be nil): the average, the count,
-- this shopper's own review if they have one, and the rest to show.
function get(asin, user)
    if asin == nil or asin == "" then return { error = "an asin is required", code = 400 } end
    local count, sum = totals(asin)
    local mine, reviews = nil, {}
    for _, r in ipairs(reviews_of(asin)) do
        if user ~= nil and r.email == user.email then
            mine = { stars = r.stars, comment = r.comment }
        end
        reviews[#reviews + 1] = { name = r.name ~= "" and r.name or "A shopper", stars = r.stars, comment = r.comment }
    end
    local out = averaged(count, sum, mine)
    out.reviews = reviews
    return out
end

-- POST /api/ratings  {asin, stars, comment} from a signed-in `user`.
--
-- One review per account per product: rating something twice replaces the
-- document rather than adding a second line, and keeps the sequence number it
-- already had so an edit does not jump to the top of the list.
--
-- `exists` is handed in by the caller because only the shop knows what a product
-- is - this space holds opinions about asins and no catalog to check them against.
function post(body, user, exists)
    if user == nil then return { error = "sign in to rate a product", code = 401 } end
    if body == nil or body == "" then return { error = "an asin and a star rating are required", code = 400 } end
    local j = simdjson.parse(body)
    local asin = j.asin
    if type(asin) ~= "string" or asin == "" then return { error = "an asin is required", code = 400 } end
    local stars = math.floor(tonumber(j.stars) or 0)
    if stars < 1 or stars > 5 then return { error = "stars must be between 1 and 5", code = 400 } end
    if not exists(asin) then return { error = "no such product", code = 404 } end

    local comment = type(j.comment) == "string" and j.comment or ""
    if #comment > 1000 then comment = string.sub(comment, 1, 1000) end

    local sp = space()
    local key = key_of(asin, user.email)
    local prior = sp:get(key)
    local seq
    if prior ~= nil then
        seq = simdjson.parse(prior).seq or 0
    else
        -- the next sequence number, from the reviews rather than from a counter
        -- key, for the same reason the totals are recomputed rather than bumped
        seq = 0
        for _, r in ipairs(reviews_of(asin)) do
            if (r.seq or 0) > seq then seq = r.seq end
        end
        seq = seq + 1
    end

    local doc = { asin = asin, email = user.email, name = user.name, stars = stars, comment = comment, seq = seq }
    sp:set(key, simdjson.encode(doc))

    local count, sum = retotal(asin, reviews_of(asin))
    local out = averaged(count, sum, { stars = stars, comment = comment })
    out.ok = true
    return out
end
