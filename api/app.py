# api/app.py
from flask import Flask
from api.routes import routes, set_chord_node  # Import node management routes
from api.data_routes import data_routes, set_dht   # Import data routes
from nodes.node import ChordNode
from nodes.dht import DHT

app = Flask(__name__)
app.register_blueprint(routes)       # Register node management routes
app.register_blueprint(data_routes)  # Register data management routes

#chord_node = None
#dht = None

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", required=True, help="Node IP address")
    parser.add_argument("--port", required=True, type=int, help="Node port")
    parser.add_argument("--bootstrap", help="Bootstrap node in format ip:port")
    args = parser.parse_args()
    
    bootstrap_ip, bootstrap_port = args.bootstrap.split(":") if args.bootstrap else (None, None)
    bootstrap_port = int(bootstrap_port) if bootstrap_port else None  

    chord_node = ChordNode(args.ip, args.port, bootstrap_ip, bootstrap_port)
    dht = DHT(chord_node)

    # Pass the chord_node instance to routes.py
    set_chord_node(chord_node)
    set_dht(dht)

    app.run(host=args.ip, port=args.port, threaded=True)

