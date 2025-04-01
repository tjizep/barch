local vk
vk = redis
local result = {}
local index = 0
local inc = function()
    index = index + 1
    return index
end
vk.call('B.CLEAR')
result[inc()] = {[['B.STATS']], vk.call('B.STATS')}
result[inc()] = {[['B.HEAPBYTES']], vk.call('B.HEAPBYTES')}
local start = vk.call('B.MILLIS')
result[inc()] = {[['B.VACUUM']], vk.call('B.VACUUM')}
result[inc()] = {[['VAC TIME']], vk.call('B.MILLIS') - start}
result[inc()] = {[['B.HEAPBYTES']], vk.call('B.HEAPBYTES')}

return result