local vk
vk = redis
local result = {}
local index = 0
local inc = function()
    index = index + 1
    return index
end

result[inc()] = {[['ODSTATS']], vk.call('ODSTATS')}
result[inc()] = {[['ODHEAPBYTES']], vk.call('ODHEAPBYTES')}
local start = vk.call('ODMILLIS')
result[inc()] = {[['ODVACUUM']], vk.call('ODVACUUM')}
result[inc()] = {[['VAC TIME']], vk.call('ODMILLIS') - start}
result[inc()] = {[['ODHEAPBYTES']], vk.call('ODHEAPBYTES')}

return result