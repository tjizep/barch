for cnt in range(1,5):
    import redis
    import barch
    print(f"start redis test {cnt}")
    barch.start("0.0.0.0", 14000)
    barch.stop()
    barch.start(14000)

    barch.ping("127.0.0.1", 14000)

    # connect redis client to barch running inside this process
    r = redis.Redis(host="127.0.0.0", port=14000, db=0)
    r.execute_command("CLIENT INFO")

    r.execute_command("CLEARALL")
    r.execute_command("SAVEALL")
    r.execute_command("INFO SERVER")
    r.execute_command("INFO SHARD 0")
    r.execute_command("INFO SHARD k")
    r.execute_command("CONFIG SET rpc_max_buffer 64k")
    r.execute_command("CONFIG SET rpc_max_buffer 1m")
    r.execute_command("CONFIG SET rpc_max_buffer 1g")
    #r.execute_command("CONFIG SET listen_port 14000")
    #r.execute_command("CONFIG SET server_port 14100")
    r.execute_command("CONFIG SET log_page_access_trace off")
    #r.execute_command("CONFIG SET rpc_client_max_wait_ms 15000")
    r.execute_command("CONFIG SET min_compressed_size 128")

    p = r.pipeline()
    p.set('a','va')
    assert(p.exists('a'))
    p.set('b','vb')

    r.execute_command("pipe:SET a spca")
    p.set('c','vc')
    r.execute_command("pipe:SET a spca")
    p.execute()
    assert(r.execute_command("pipe:GET a") == b'spca')
    print("r.execute_command('GET a')",r.execute_command("GET a"))
    assert(r.execute_command("GET a") == b'va')
    assert(r.execute_command("SPACES EXIST pipe") == 1)

    r.set("hello","barch")
    assert(r.get("hello") == b'barch')
    assert(r.dbsize() == 4)
    r.execute_command("USE src")
    r.flushdb()
    r.execute_command("SET vkey srcv")
    r.execute_command("SET ukey srcu")
    assert(r.dbsize()==2)
    r.execute_command("USE deputya")
    assert(r.dbsize()==0)
    r.execute_command("USE")
    assert(r.dbsize()==4)
    r.execute_command("USE dep")
    r.flushdb()
    r.set("a","depa")
    r.set("b","depb")
    r.set("c","depc")
    r.set("d","depd")
    assert (r.dbsize()>=4)
    print(r.dbsize())
    r.execute_command("USE src")
    r.flushdb()
    r.set("c","srcc")
    r.set("e","srce")
    r.set("z",b'srcz')
    assert(r.get("z")==b'srcz')
    assert(r.dbsize()==3)
    assert(r.dbsize()-1 == len(r.execute_command("RANGE a z -1")))
    r.execute_command("SPACES DEPENDS dep ON src")
    r.execute_command("USE dep")
    assert(len(r.execute_command("RANGE a z -1"))==5)
    assert(r.dbsize()==7)
    print(r.get("z"))
    assert(r.get("z")==b'srcz')
    r.execute_command("REM z")
    assert(r.get("z")==None)
    #print(r.dbsize())
    r.execute_command("USE src")
    assert(r.get("z")==b'srcz')
    assert(3 == r.dbsize())

    r.execute_command("mspce:SET a mspce_a")
    assert(r.execute_command("mspce:GET a") == b'mspce_a')
    assert(r.execute_command("SPACES DEPENDS a ON mspce") == b'OK')

    assert(r.execute_command("a:GET a") == b'mspce_a')
    assert(r.execute_command("SPACES DROP a") == b'OK')
    assert(r.execute_command("SPACES DROP mspce") == b'OK')
    assert(r.execute_command("a:GET a") == None)
    assert(r.execute_command("SPACES DROP a") == b'OK')
    r.set("i","0")
    r.incr("i")
    r.execute_command("UINCRBY i 1")
    print(r.get("i"))
    assert r.get("i") == b'2'
    r.append("i","0")
    assert r.get("i") == b'20'

    r.execute_command("PREPEND i 1")
    assert r.get("i") == b'120'
    r.decr("i")
    r.execute_command("DECR i")
    assert r.get("i") == b'118'
    r.decrby("i", 1)
    assert r.get("i") == b'117'
    r.execute_command("UDECRBY i 1")
    assert r.get("i") == b'116'
    r.execute_command("MSET j 1 k 2 l 3")
    assert r.get("j") == b'1'
    r.execute_command("ADD 1test1 one")
    assert r.get("1test1") == b'one'
    print(r.execute_command("MGET j k l"))
    assert r.execute_command("MIN") != None
    assert r.execute_command("MAX") != None
    assert r.execute_command("STATS") != None
    assert r.execute_command("OPS") != None
    assert r.execute_command("VALUES *") != None
    assert r.execute_command("KEYS *") != None
    assert r.execute_command("UNLOAD a") != None
    r.execute_command("SPACES")
    assert(r.execute_command("SPACES OPTION GET ORDERED"))
    assert(r.execute_command("SPACES OPTION GET LRU") == False)
    r.execute_command("SPACES OPTION GET RANDOM")
    r.execute_command("SPACES OPTION SET ORDERED OFF")
    r.execute_command("SPACES OPTION SET ORDERED ON")
    r.execute_command("SPACES OPTION SET LRU ON")
    r.execute_command("SPACES OPTION SET LRU OFF")
    r.execute_command("SPACES OPTION SET RANDOM ON")
    r.execute_command("SPACES OPTION SET RANDOM OFF")
    r.set(f"1 a","1a")
    r.set(f"1 b","1b")
    r.set(f"1 c","1c")
    r.set(f"1.1 a","1.1a")
    r.set(f"1.1 b","1.1b")
    r.set(f"1.2 a","1.2a")
    assert r.get(f"1.1 a") == b'1.1a'
    assert r.get(f"1.2 a") == b'1.2a'
    assert r.execute_command(f'RANGE "1.1 a"') != None
    r.close()
    barch.stop()
print(f"complete redis test")