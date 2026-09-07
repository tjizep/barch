-- The file source for this space: given a path this space does not have, produce
-- the file. `configuration:shop.fs_source` names it.
--
-- The catalog holds urls, not pictures: 992 products, about 10MB in total, and no
-- reason to hold any of it until somebody looks at a product. `/img/<asin>` is a
-- plain files route now, and when it asks for a file that is not there this fetches
-- it once. Every request after that is a stored file and no luau runs at all.
--
-- This used to be a fill script on a foreign key space, which cannot work: a fill is
-- handed a key, and of the three keys a file is made of only the name record carries
-- a path - see TODO 263.

function call(path)
    local asin = string.match(path, "^/img/([^/]+)$")
    if asin == nil then
        return nil                    -- nothing else in this space is fetchable
    end
    local raw = require(":/modules/catalog.luau").find(asin)
    if raw == nil then
        return nil                    -- no such product; remembered for missing_ttl
    end
    local url = simdjson.parse(raw).image_url
    if url == nil then
        return nil
    end
    local got = http.request(url):timeout(20000):redirects(2):get()
    if not got.ok or got.status ~= 200 then
        return nil
    end
    -- {body, type} rather than the body alone, because `/img/<asin>` has no
    -- extension to guess a type from and Amazon's urls are a mix of jpeg and webp
    local kind = got.headers and got.headers["content-type"] or nil
    if kind == nil or kind == "" then
        kind = string.find(url, "%.png") and "image/png"
             or string.find(url, "%.webp") and "image/webp"
             or "image/jpeg"
    end
    return { got.body, kind }
end
