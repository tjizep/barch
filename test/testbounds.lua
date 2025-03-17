local vk
vk = redis
local chars= {'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'}
local count = 10
local result = {}
local index = 0
local convert
local tests = 0
local successes = 0
local inc = function()
    index = index + 1
    return index
end

local tochars123 = function(num)
    return chars[num+1]
end

local test = function()

    tests = tests + 1
    result[inc()] = {"running test "..tests}

    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        vk.call('ODSET',k,v)

    end
    local rtest = vk.call('ODRANGE',convert(2), convert(8), 6)
    if #rtest == 6 then
        successes = successes + 1
    end
    if vk.call('ODMIN') == 'a' then
        successes = successes + 1
    end
    if vk.call('ODMAX') == 'j' then
        successes = successes + 1
    end
    result[inc()] = {"'ODRANGE',convert(2), convert(8), 6", rtest}
    result[inc()] = {[['ODMIN']], vk.call('ODMIN')}

    result[inc()] = {[['ODMAX']], vk.call('ODMAX')}
    result[inc()] = {"succeses for test "..tests..": "..successes}
end



--[[ Testing num hash string key types]]

convert = tochars123
test()
assert(successes==3, "test failures")
--assert(failures==0, "test failures")

return result