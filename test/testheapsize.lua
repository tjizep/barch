local vk
vk = redis
local result = {}
local index = 0
local inc = function()
    index = index + 1
    return index
end
result[inc()] = {[[ODSTATS]], vk.call('B.STATS')}
result[inc()] = {[[ODHEAPBYTES]], vk.call('B.HEAPBYTES')}


return result