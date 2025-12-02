import requests
import os
script_dir = f'{os.path.dirname(os.path.realpath(__file__))}'
words = ["ten","pi","hash","solar","system","Earth","aleph","beth","bank","transaction","functional","JWT","Json", "fee","fresh","xml","html","medical","surplus","store","zebra","terrific"]
train_set = {}
test_set = {}

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

def load_set_in(aset, in_):
    loaded = 0
    for w in words:
        name = f'{script_dir}/data/{in_}/{w}.dat'
        try:
            with open(name, 'rb') as file:
                aset[w] = file.read()
                print(f'loaded {in_} data for {w}, {len(aset[w])} bytes')
                loaded = loaded + 1
        except:
            print(f'{name} does not exist')
    return loaded == len(words)
def download_test_data():
    test = {}
    for w in words:
        print(f'download test data for {w}')
        test[w] = wiki_search(w,50)
    return test

def download_train_data():
    train = {}
    for w in words:
        print(f'download train data for {w}')
        train[w] = wiki_search(w,10)
    return train
def load_test_data(_set):
    return load_set_in(_set, 'test')

def load_train_data(_set):
    return load_set_in(_set, 'train')

def save_data(test_, as_):
    for w in words:
        print(f'saving {as_} data for {w}')
        with open(f'{script_dir}/data/{as_}/{w}.dat', 'wb') as f:
            f.write(test_[w])
def save_test_data(test_):
    save_data(test_,'test')

def save_train_data(train_):
    save_data(train_,'train')

def create_if_exists(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        print(f"Directory {dir_path} created")
    else:
        print(f"Directory {dir_path} already exists")

create_if_exists(f"{script_dir}/data/test")
create_if_exists(f"{script_dir}/data/train")

if not load_train_data(train_set):
    train_set = download_train_data()
    save_train_data(train_set)

if not load_test_data(test_set):
    test_set = download_test_data()
    save_test_data(test_set)

assert(len(test_set) == len(words))
assert(len(train_set) == len(words))
