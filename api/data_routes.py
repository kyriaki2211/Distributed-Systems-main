# api/data_routes.py

from flask import Blueprint, request, jsonify

data_routes = Blueprint("data_routes", __name__)  # Use Blueprint for modularity

@data_routes.route("/insert", methods=["POST"])
def insert():
    """Handles inserting a key-value pair into the Chord DHT."""
    data = request.json
    dht.insert(data["key"], data["value"])
    return jsonify({"status": "success"}), 200

@data_routes.route("/query", methods=["GET"])
def query():
    """Handles querying a key from the Chord DHT."""
    key = request.args.get("key")
    values = dht.query(key)
    return jsonify({"values": values}), 200

@data_routes.route("/delete", methods=["DELETE"])
def delete():
    """Handles deleting a key from the Chord DHT."""
    data = request.json
    dht.delete(data["key"])
    return jsonify({"status": "deleted"}), 200

# Function to set the DHT instance dynamically
def set_dht(dht_instance):
    global dht
    dht = dht_instance

