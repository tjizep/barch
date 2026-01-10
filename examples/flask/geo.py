import h3
from flask import Flask, request, jsonify
from flask_cors import CORS
import barch
app = Flask(__name__)
CORS(app)
h3_res = 12
h3_parent_res = 8
barch.start("0.0.0.0", 14000)
cnf = barch.KeyValue("configuration")
kv = barch.KeyValue("spatial_data") # give it a name, could be anything
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
        val = f"{point}"
        print(f"Adding point: {point['name']} at {point['lat']}, {point['long']} as {point}")
        h3_index = h3.latlng_to_cell(point['lat'], point['long'], h3_res)
        print("adding h3",int(h3_index, 16))
        kv.set(f"{int(h3_index, 16)} {point['name']}" , val)
    #print(jsonify({"name": name, "lat": lat, "long": long}))
    #

    return jsonify({"message": "Point added successfully"}), 200
@app.route('/search', methods=['GET'])
def search():
    """
    search?lat=LATITUDE&long=LONGITUDE&limit=10
    Search for points using lat long
    Query parameter: lat - latitude (float)
    Query parameter: long - longitude (float)
    Optional parameter: limit (max results, default 10)
    """
    lat = float(request.args.get('lat', '').lower().strip())
    long = float(request.args.get('long', '').lower().strip())
    print ("lat",lat, "long",long)

    limit = request.args.get('limit', 10, type=int)

    if not lat or not long:
        return jsonify({'results': []})

    child_cell = h3.latlng_to_cell(lat, long, h3_res)
    #print("child_cell", child_cell)
    # 1. Get the parent cell from that child
    parent_cell = h3.cell_to_parent(child_cell, h3_parent_res)
    child_count = h3.cell_to_children_size(parent_cell, h3_res)
    # 2. Get the Begin Index (position 0)
    begin_index = int(h3.child_pos_to_cell(parent_cell, h3_res, 0),16)

    # 3. Get the End Index (the last position: count - 1)
    end_index = int(h3.child_pos_to_cell(parent_cell, h3_res,child_count - 1),16)

    #print(f"h3 Search Range: {begin_index} to {end_index}")

    vec = kv.range(f"{begin_index} 0",f"{end_index} 99999999999999",limit)
    results = []
    for v in vec:
        results.append(v.s())

    return jsonify({'results': results})

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
