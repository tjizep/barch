-- The product catalog, out of the `inventory` key space.
--
-- Products are plain keys there, not files (see load_inventory.py):
--   p:<asin>     the full record
--   index:<n>    the compact rows, 200 to a key, brackets off
--   names        slug -> display name
--   categories   the category tree
--
-- A module in the file store rather than a stored function, because the
-- handlers that want it would otherwise reach each other through
-- `barch.call("CALLF", ...)`, and a bulk string comes back from that with a `$`
-- on the front - see TODO 261. `require` hands back this table and the strings
-- in it are the strings.
--
-- `barch.space.inventory` looks a space up and does not create one, so it has
-- to have been USEd before the first request - setup.sh and shopctl do that.

local function space()
    return barch.space.inventory
end

function find(asin)
    if asin == nil or asin == "" then
        return nil
    end
    return space():get("p:" .. asin)
end

-- every row, as the JSON array /api/index serves
function index()
    local sp = space()
    local n = tonumber(sp:get("index:n") or "")
    if n == nil then
        return nil
    end
    local parts = {}
    for i = 0, n - 1 do
        local part = sp:get("index:" .. i)
        if part == nil then
            return nil
        end
        parts[#parts + 1] = part
    end
    return "[" .. table.concat(parts, ",") .. "]"
end

function categories()
    return space():get("categories")
end
