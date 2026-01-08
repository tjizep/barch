from flask import Flask, request, jsonify
from flask_cors import CORS
import barch
app = Flask(__name__)
CORS(app)
kv = barch.KeyValue()
kv.set('abba','')
kv.set('baba','')

kv.set('barber','')
kv.set('barbarian','')
kv.set('barbara','')
kv.set('appalling','')
kv.set('apparition','')
# For better performance with large lists, sort once


@app.route('/autocomplete', methods=['GET'])
def autocomplete():
    """
    Get autocomplete suggestions for a query string.
    Query parameter: q (the search term)
    Optional parameter: limit (max results, default 10)
    """
    query = request.args.get('q', '').lower().strip()
    limit = request.args.get('limit', 10, type=int)

    if not query:
        return jsonify({'suggestions': []})

    # Find matching words - add a tilde '~' because its last in the order`
    vec = kv.range(query,f"{query}~",limit)
    suggestions = []
    for v in vec:
        suggestions.append(v.s())

    return jsonify({'suggestions': suggestions})

@app.route('/search', methods=['GET'])
def search():
    """
    Search for words containing the query (not just starting with).
    Query parameter: q (the search term)
    Optional parameter: limit (max results, default 10)
    """
    query = request.args.get('q', '').lower().strip()
    limit = request.args.get('limit', 10, type=int)

    if not query:
        return jsonify({'results': []})

    # Find words containing the query
    vec = kv.range(query,f"{query}~",limit)
    results = []
    for v in vec:
        results.append(v.s())

    return jsonify({'results': results})

@app.route('/load-words', methods=['POST'])
def load_words():
    """
    Load a custom word list from JSON.
    Expects: {"words": ["word1", "word2", ...]}
    """
    data = request.get_json()

    if not data or 'words' not in data:
        return jsonify({'error': 'Invalid request. Expected {"words": [...]}'}), 400

    before = kv.size()
    for word in data['words']:
        kv.set(word, '')
    return jsonify({'message': f'Loaded {kv.size()-before} words', 'count': kv.size()})

@app.route('/')
def home():
    """Simple home page with usage instructions."""
    return """
    <h1>Autocomplete API</h1>
    <h2>Endpoints:</h2>
    <ul>
        <li><code>/autocomplete?q=QUERY&limit=10</code> - Get words starting with query</li>
        <li><code>/search?q=QUERY&limit=10</code> - Get words containing query</li>
        <li><code>/load-words</code> (POST) - Load custom word list</li>
    </ul>
    <h2>Examples:</h2>
    <ul>
        <li><a href="/autocomplete?q=app">/autocomplete?q=app</a></li>
        <li><a href="/search?q=ash">/search?q=ash</a></li>
    </ul>
    """

if __name__ == '__main__':
    app.run(debug=True, port=5000)