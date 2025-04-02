local vk
vk = redis

local count = 1000000
local result = {}
local i = 1
local index = 0
local convert
local tests = 0
local failures = 0
local successes = 0
local numbers = {}
local mem = {}
local window = 100
local inc = function()
    index = index + 1
    return index
end
local shuffle = function(array)
    -- Fisher-Yates
    local output = {}
    local random = math.random

    for index = 1, #array do
        local offset = index - 1
        local value = array[index]
        local randomIndex = offset * random()
        local flooredIndex = randomIndex - randomIndex % 1

        if flooredIndex == offset then
            output[#output + 1] = value
        else
            output[#output + 1] = output[flooredIndex + 1]
            output[flooredIndex + 1] = value
        end
    end

    return output
end

for i = 1, window do
    numbers[i] = count - i
end
numbers = shuffle(numbers)

local tocharsnum = function(num)
    return num * numbers[math.mod(num,window) + 1]
end

local test = function()
    result[inc()] ={'CLEAR B', vk.call('B.CLEAR')}

    tests = tests + 1
    local t
    t = vk.call('B.MILLIS')
    for i = 1, count do
        local k = convert(i-1)
        vk.call('B.SET',k,i)
    end
    result[inc()] = {'TIME', vk.call('B.MILLIS')-t}

    result[inc()] = {'HEAP', vk.call('B.HEAPBYTES')}
    result[inc()] = {'SIZE', vk.call('B.SIZE')}
    result[inc()] = {'CLEAR B', vk.call('B.CLEAR')}

end


result[inc()] = vk.call("B.CONFIG", "SET","max_memory_bytes", "64m")
result[inc()] = vk.call("B.CONFIG", "SET","active_defrag", "off")
result[inc()] = vk.call("B.CONFIG", "SET","compression", "off")
result[inc()] = vk.call("B.CONFIG", "SET","save_interval", "1000000")
--[[ Testing ints,doubles and string key types]]
convert = tocharsnum
test()
collectgarbage()
return result