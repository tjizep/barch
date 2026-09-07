-- Finding a product file by asin, shared by the routes that need it.
--
-- A module in the file store rather than a stored function, because the two
-- handlers that want it would otherwise reach each other through
-- `barch.call("CALLF", ...)`, and a bulk string comes back from that with a `$`
-- on the front - see TODO 261. `require` hands back this table and the strings
-- in it are the strings.
--
-- The catalog is `/catalog/<top>/<sub>/<asin>.json`, so a lookup by asin alone
-- has to find the category first. It does that once and remembers it in a plain
-- key, so the walk is paid once per asin for the life of the space.

function find(asin)
    if asin == nil or asin == "" then
        return nil
    end
    local at = barch.store.get("where:" .. asin)
    if at ~= nil then
        return barch.fs.get(at)
    end
    for _, top in ipairs(barch.fs.list("/catalog")) do
        if top.dir then
            for _, sub in ipairs(barch.fs.list(top.path)) do
                if sub.dir then
                    local path = sub.path .. "/" .. asin .. ".json"
                    local body = barch.fs.get(path)
                    if body ~= nil then
                        barch.store.set("where:" .. asin, path)
                        return body
                    end
                end
            end
        end
    end
    return nil
end
