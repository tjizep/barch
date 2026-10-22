--[[
Cinema seat booking as a stored function. ../cinema.py has the job and its rules;
`cinema.py --luau` runs it through this.

    CALLF cinema book <customer> <row> <first seat> <count>   the booking's number, nil if a seat is taken
    CALLF cinema cancel <booking>                             the booking's number
]]
local tx = require("txretry")

local function seats_of(row, first, count)
    local seats = {}
    for s = 0, count - 1 do
        table.insert(seats, "seat:" .. row .. ":" .. (first + s))
    end
    return seats
end

local function book(customer, row, first, count)
    local seats = seats_of(tx.whole(row), tx.whole(first), tx.whole(count))
    -- for update: free seats are written next, and a taken one ends it anyway
    for _, seat in seats do
        if barch.store.get(seat, true) ~= nil then
            tx.refuse()                         -- one of them is taken
        end
    end
    local booking = string.format("%d", tx.add("booking:next", 1))
    for _, seat in seats do
        barch.store.set(seat, booking)
    end
    barch.store.set("booking:" .. booking, table.concat({customer, row, first, count}, " "))
    return booking
end

local function cancel(booking)
    local b = barch.store.get("booking:" .. booking, true)
    local _, row, first, count = string.match(b, "^(%S+) (%d+) (%d+) (%d+)$")
    for _, seat in seats_of(tx.whole(row), tx.whole(first), tx.whole(count)) do
        barch.store.remove(seat)
    end
    barch.store.set("booking:" .. booking, "cancelled")
    return booking
end

function call(action, ...)
    if action == "book" then
        return tx.in_transaction(book, ...)
    elseif action == "cancel" then
        return tx.in_transaction(cancel, ...)
    end
    error("cinema takes book or cancel", 0)
end
