local vk
vk = redis
vk.call('B.CLEAR')
vk.call('B.SAVE')
-- the port comes from routetest.py as ARGV[1] rather than being written here:
-- python routes to this barch, so one literal in two files is one of them being
-- wrong the first time anything moves. See TODO 295.
vk.call('B.START','127.0.0.1',ARGV[1])

vk.call('B.SET','1','one:test')
vk.call('B.SET','2','two:test')
vk.call('B.SET','3','three:test')
for i = 4, 10000 do
    vk.call('B.SET',i,'data'..i)
end
vk.call('B.SAVE')
return vk.call('B.SIZE')