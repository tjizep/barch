require("barch")
local conf = barch.KeyValue("configuration")
conf:set("test.ordered","1")
local kv = barch.KeyValue("test")
kv:set("test","test1")
kv:clear()

local count = 1000000
local start = os.clock()
local internal = false
if internal then
    barch.testKv()
else
    for i = 1, count do
        kv:seti(i,i)
    end
end

print(os.clock() - start, kv:size())