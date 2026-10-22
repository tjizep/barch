--[[
Warehouse picking as a stored function. ../warehouse.py has the job and its rules;
`warehouse.py --luau` runs it through this.

    CALLF warehouse pick <count> <bin>[,<bin> ...]   what it took as {bin, count, ...}, nil if refused
    CALLF warehouse move <source> <target> <count>   the count, nil if refused

A picker walks the bins in its own order, so two pickers can lock the same bins the
other way round and meet a deadlock. That's left in on purpose: the retry sorts it out.
]]
local tx = require("txretry")

local function pick(wanted, route)
    wanted = tx.whole(wanted)
    local took, got = {}, 0
    for b in string.gmatch(route, "[^,]+") do
        if got == wanted then
            break
        end
        local have = tx.for_update(b)
        local n = math.min(have, wanted - got)
        if n > 0 then
            tx.put(b, have - n)
            table.insert(took, b)
            table.insert(took, n)
            got += n
        end
    end
    if got < wanted then
        tx.refuse()                             -- not enough in the whole warehouse
    end
    tx.add("picked", wanted)
    return took
end

local function move(source, target, count)
    count = tx.whole(count)
    local have = tx.for_update(source)
    if have < count then
        tx.refuse()
    end
    tx.put(source, have - count)
    tx.add(target, count)
    return count
end

function call(action, ...)
    if action == "pick" then
        return tx.in_transaction(pick, ...)
    elseif action == "move" then
        return tx.in_transaction(move, ...)
    end
    error("warehouse takes pick or move", 0)
end
