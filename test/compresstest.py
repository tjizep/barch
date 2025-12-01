import threading
import barch
import redis
import time
import requests
def wiki_search(query, results):
    language_code = 'en'
    search_query = query
    number_of_results = results
    headers = {
        # 'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
        'User-Agent': 'barch (chrisep2@gmail.com)'
    }

    base_url = 'https://api.wikimedia.org/core/v1/wikipedia/'
    endpoint = '/search/page'
    url = base_url + language_code + endpoint
    parameters = {'q': search_query, 'limit': number_of_results}
    response = requests.get(url, headers=headers, params=parameters)
    return f"{response.json()}".encode('utf-8')


barch.start("0.0.0.0", 15000)
gr = redis.Redis(host="127.0.0.0", port=15000, db=0)
gr.config_set("compression", "zstd")
gr.flushdb()
def test(num):
    r = redis.Redis(host="127.0.0.0", port=15000, db=0)
    r.flushdb()
    words = ["ten","pi","hash","solar","system","Earth","aleph","beth","bank","transaction","functional","JWT","Json", "fee","fresh","xml","html","medical","surplus","store","zebra","terrific"]
    tr = 512000
    for w in words:
        left = r.execute_command(f'TRAIN {wiki_search(w,10)}')
        assert (left < tr)
        tr = left
        print (left)
    assert (r.execute_command(f'TRAIN') == 0) # this will save a file called barch_dict.dat in the current dir
    test_set = {}
    for w in words:
        print(f"collecting test data for: {w}")
        test_set[w] = wiki_search(w,30)
        r.set(w, test_set[w])
    for w in words:
        print(f"testing:{w}")
        assert r.get(w) == test_set[w]
    print(f"exit thread {num}")

t = [
    threading.Thread(target=test, args=(1,))
]

for i in t:
    i.start()

time.sleep(1)

for i in t:
    i.join()

assert(barch.stats().value_bytes_compressed > 0)
