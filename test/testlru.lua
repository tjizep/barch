local vk
vk = redis
local result = {}
local index = 0
local inc = function()
    index = index + 1
    return index
end


result[inc()] = {[[ODSTATS]], vk.call('B.STATS')}
local start = vk.call('B.MILLIS')
result[inc()] = {[[ODEVICT]], vk.call('B.EVICT')}
result[inc()] = {[['EVICT TIME']], vk.call('B.MILLIS') - start}
result[inc()] = {[[ODHEAPBYTES]], vk.call('B.HEAPBYTES')}

return result