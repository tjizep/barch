-- Product images, fetched from Amazon the first time somebody looks at a product.
--
-- The catalog holds urls, not pictures: 992 products, about 10MB in total, and no
-- reason to hold any of it until it is asked for. The cache is the `shopimg` key
-- space, which is configured `foreign = luau` - a key that is not there is filled
-- by imgfetch.luau, which fetches the url the key names.
--
-- `fetch` and not `get`: a read that is allowed to go to the network is a different
-- operation from one that is not, and this one can take the source's latency and
-- holds a VM slot while it does. Saying so at the call is the point - see TODO 259,
-- which is also why this can reach another space from a handler at all.

function call()
    return "shopimg"
end

-- The catalog lookup is a module in the file store, shared with the other route -
-- see modules/catalog.luau. `require` has to happen inside a call rather than at
-- the top of the file: installing a function runs the chunk, and at that moment
-- there is no store for require to read a module out of. It caches, so this costs
-- one lookup per session and not one per request.
local function catalog()
    return require(":/modules/catalog.luau")
end

local function kind_of(url)
    if string.find(url, "%.png") then return "image/png" end
    if string.find(url, "%.webp") then return "image/webp" end
    return "image/jpeg"
end

function serve(req, res, params, query)
    local asin = params and params.asin or ""
    if asin == "" then
        res.code = 400
        res.body = "an asin is required"
        return
    end

    local raw = catalog().find(asin)
    if raw == nil then
        res.code = 404
        res.body = "no such product"
        return
    end
    local url = simdjson.parse(raw).image_url

    local pics = barch.space.shopimg
    local body, why = pics:fetch(url)
    if body == nil then
        res.code = 502
        res.body = "no image: " .. tostring(why)
        return
    end

    res:header("Content-Type", kind_of(url))
    -- the bytes for a url never change, so let the browser keep them
    res:header("Cache-Control", "public, max-age=86400")
    res.code = 200
    res.body = body
end

function transport()
    return {
        kind = "resource",
        route = "/img/{asin}",
        methods = { GET = serve },
        cors = "*",
    }
end
