--[[
Warehouse picking as a stored function. ../warehouse.py has the job and its rules;
`warehouse.py --luau` runs it through this.

    CALLF warehouse pick <count> <bin>[,<bin> ...]   what it took as {bin, count, ...}, nil if refused
    CALLF warehouse move <source> <target> <count>   the count, nil if refused

Bins hold int64 counts (see txretry.luau).

A picker walks the bins in its own order, so two pickers can lock the same bins the
other way round and meet a deadlock. That's left in on purpose: the retry sorts it out.
]]
local tx = require("txretry")

local ZERO = integer.create(0)

local function pick(wanted, route)
    wanted = tx.whole(wanted)
    local took, got = {}, ZERO
    for b in string.gmatch(route, "[^,]+") do
        if got == wanted then
            break
        end
        local have = tx.for_update(b)
        local n = integer.min(have, tx.minus(wanted, got))
        if integer.gt(n, ZERO) then
            tx.put(b, tx.minus(have, n))
            table.insert(took, b)
            table.insert(took, n)
            got = tx.plus(got, n)
        end
    end
    if integer.lt(got, wanted) then
        tx.refuse()                             -- not enough in the whole warehouse
    end
    tx.add("picked", wanted)
    return took
end

local function move(source, target, count)
    count = tx.whole(count)
    local have = tx.for_update(source)
    if integer.lt(have, count) then
        tx.refuse()
    end
    tx.put(source, tx.minus(have, count))
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
