--[[
An online shop as a stored function. ../orders.py has the job and its rules;
`orders.py --luau` runs it through this.

    CALLF orders order <customer> <item:count>[,<item:count> ...]   the invoice number, nil if refused
    CALLF orders restock <item> <count>                            the count

Prices, stock, credit and takings are int64s (see txretry.luau), money in whole
cents, and the 15% tax is rounded in integers, half a cent up. An invoice is a
record, so it stays text.
]]
local tx = require("txretry")

local ONE = integer.create(1)
local TAX_PERCENT = integer.create(15)
local HALF = integer.create(50)
local HUNDRED = integer.create(100)

local function tax(cents)
    return integer.idiv(tx.plus(tx.times(cents, TAX_PERCENT), HALF), HUNDRED)
end

local function order(customer, lines)
    local items = {}
    for item, count in string.gmatch(lines, "([^,:]+):(%d+)") do
        table.insert(items, {item = item, count = tx.whole(count)})
    end
    -- sorted, so every order locks the stock it takes in the same order
    table.sort(items, function(a, b) return a.item < b.item end)
    local subtotal = integer.create(0)
    for _, line in items do
        local stock = tx.for_update("stock:" .. line.item)
        if integer.lt(stock, line.count) then
            tx.refuse()                         -- out of stock
        end
        tx.put("stock:" .. line.item, tx.minus(stock, line.count))
        subtotal = tx.plus(subtotal, tx.times(line.count, tx.read("price:" .. line.item)))
    end
    local total = tx.plus(subtotal, tax(subtotal))
    local credit = tx.for_update("credit:" .. customer)
    if integer.lt(credit, total) then
        tx.refuse()
    end
    tx.put("credit:" .. customer, tx.minus(credit, total))
    tx.add("takings", total)
    local number = tx.add("invoice:next", ONE)                 -- no gaps
    local sold = {}
    for _, line in items do
        table.insert(sold, line.item .. ":" .. tostring(line.count))
    end
    barch.store.set("invoice:" .. tostring(number),
        table.concat({customer, tostring(subtotal), tostring(total), table.concat(sold, ",")}, " "))
    return number
end

local function restock(item, count)
    count = tx.whole(count)
    tx.add("stock:" .. item, count)
    tx.add("received:" .. item, count)
    return count
end

function call(action, ...)
    if action == "order" then
        return tx.in_transaction(order, ...)
    elseif action == "restock" then
        return tx.in_transaction(restock, ...)
    end
    error("orders takes order or restock", 0)
end
