local vk
vk = redis
local result = {}
local index = 0
local inc = function()
    index = index + 1
    return index
end

result[inc()] = {[[ODHEAPBYTES]], vk.call('ODHEAPBYTES')}
result[inc()] = {[[ODSTATS]], vk.call('ODSTATS')}

return result