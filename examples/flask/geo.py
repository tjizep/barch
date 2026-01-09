import h3
from flask import Flask, request, jsonify
from flask_cors import CORS
import barch
app = Flask(__name__)
CORS(app)


kv = barch.KeyValue()
kv.use("spatial_data") # give it a name, could be anything
kv.setOrdered(True)
@app.route('/add-points', methods=['POST'])
def add_point():
    """
    Load a custom word list from JSON.
    Expects: {"points": [{"lat": 0, "long":0,"name":"n1"}, {"lat": 1, "long":1,"name":"n2"}, ...]}
    """
    data = request.get_json()
    if not data or 'points' not in data:
        return jsonify({'error': 'Invalid request. Expected {"points": [{"lat": 0, "long":0,"name":"n1"},...]}'}), 400

    for point in data['points']:
        val = f"{point['lat']}_{point['long']}_{point['name']}"
        h3_index = h3.latlng_to_cell(point['lat'], point['long'], 10)
        kv.set(f"{h3_index} {point['name']}" , val)
    #print(jsonify({"name": name, "lat": lat, "long": long}))
    #

    return jsonify({"message": "Point added successfully"}), 200

@app.route('/')
def home():
    """Simple home page with usage instructions."""
    return """
    <h1>Autocomplete API</h1>
    <h2>Endpoints:</h2>
    <ul>
        <li><code>/search?lat=QUERY&long=QUERY&limit=10</code> - Get words containing query</li>
        <li><code>/add-points</code> (POST) - Load some points</li>
    </ul>
    <h2>Examples:</h2>
    <ul>
    </ul>
    """

if __name__ == '__main__':
    app.run(debug=False, use_reloader=False, port=5000)
