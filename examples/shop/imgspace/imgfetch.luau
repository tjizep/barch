-- The fill script for the `shopimg` space: `foreign = luau`, so a RESP client
-- reading a key that is not there gets this, and what it returns is stored.
--
-- The key is the image url, because that is the only thing the fill is given - a
-- fill script has no store to look anything up in, which is the point: it turns a
-- key into a value and nothing else.
function call(key, space)
    if string.sub(key, 1, 4) ~= "http" then
        return nil                    -- not an image url; a cached miss
    end
    local got = http.request(key):timeout(20000):redirects(2):get() -- its asynch an you dont need to even know it
    if not got.ok or got.status ~= 200 then
        return nil
    end
    return got.body
end
