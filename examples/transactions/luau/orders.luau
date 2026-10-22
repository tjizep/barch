--[[
An online shop as a stored function. ../orders.py has the job and its rules;
`orders.py --luau` runs it through this.

    CALLF orders order <customer> <item:count>[,<item:count> ...]   the invoice number, nil if refused
    CALLF orders restock <item> <count>                            the count

Money is whole cents and the 15% tax is rounded in whole numbers, half a cent up.
]]
local tx = require("txretry")

local TAX_PERCENT = 15

local function tax(cents)
    return (cents * TAX_PERCENT + 50) // 100
end

local function order(customer, lines)
    local items = {}
    for item, count in string.gmatch(lines, "([^,:]+):(%d+)") do
        table.insert(items, {item = item, count = tx.whole(count)})
    end
    -- sorted, so every order locks the stock it takes in the same order
    table.sort(items, function(a, b) return a.item < b.item end)
    local subtotal = 0
    for _, line in items do
        local stock = tx.for_update("stock:" .. line.item)
        if stock < line.count then
            tx.refuse()                         -- out of stock
        end
        tx.put("stock:" .. line.item, stock - line.count)
        subtotal += line.count * tx.read("price:" .. line.item)
    end
    local total = subtotal + tax(subtotal)
    local credit = tx.for_update("credit:" .. customer)
    if credit < total then
        tx.refuse()
    end
    tx.put("credit:" .. customer, credit - total)
    tx.add("takings", total)
    local number = tx.add("invoice:next", 1)                   -- no gaps
    local sold = {}
    for _, line in items do
        table.insert(sold, line.item .. ":" .. line.count)
    end
    barch.store.set("invoice:" .. string.format("%d", number),
        string.format("%s %d %d %s", customer, subtotal, total, table.concat(sold, ",")))
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
