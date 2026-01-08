import h3
from flask import Flask, request, jsonify
#from flask_cors import CORS
import barch
#app = Flask(__name__)
#CORS(app)


kv = barch.KeyValue()
kv.use("spatial_data") # give it a name, could be anything
kv.setOrdered(True)
def add_point(lat, long, name):
    if lat is None or long is None:
        return jsonify({"error": "Latitude and longitude are required"}), 400

    h3_index = h3.geo_to_h3(lat, long, 10)
    kv.set(h3_index, {"name": name, "lat": lat, "long": long})
    return jsonify({"message": "Point added successfully"}), 200

add_point(0,0, "origin")
