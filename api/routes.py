# api/routes.py
from flask import Blueprint, request, jsonify
import requests

routes = Blueprint("routes", __name__)  # Use Blueprint for modularity

@routes.route('/successor', methods=['GET'])
def get_successor():
    """Returns the successor of this node."""
    return jsonify({"successor": chord_node.successor}), 200


@routes.route('/predecessor', methods=['GET'])
def get_predecessor():
    """Returns the predecessor of this node."""
    return jsonify({"predecessor": chord_node.predecessor}), 200


@routes.route('/update_successor', methods=['POST'])
def update_successor():
    """Updates the successor of this node."""
    data = request.json
    chord_node.successor = (data["new_successor_ip"], data["new_successor_port"])
    return jsonify({"message": "Successor updated"}), 200


@routes.route('/update_predecessor', methods=['POST'])
def update_predecessor():
    """Updates the predecessor of this node."""
    data = request.json
    chord_node.predecessor = (data["new_predecessor_ip"], data["new_predecessor_port"])
    return jsonify({"message": "Predecessor updated"}), 200


@routes.route('/overlay', methods=['GET'])
def get_overlay():
    """Returns the topology of the Chord ring in the correct order, including node IDs, successors, and predecessors."""
    nodes = []
    current_ip, current_port, current_id = chord_node.ip, chord_node.port, chord_node.node_id
    visited = set()

    while (current_ip, current_port, current_id) not in visited:
        visited.add((current_ip, current_port, current_id))

        # Fetch successor and predecessor of the current node
        try:
            successor_response = requests.get(f"http://{current_ip}:{current_port}/successor", timeout=2)
            successor_data = successor_response.json().get("successor", (None, None))

            predecessor_response = requests.get(f"http://{current_ip}:{current_port}/predecessor", timeout=2)
            predecessor_data = predecessor_response.json().get("predecessor", (None, None))
        except requests.exceptions.RequestException:
            print(f"Warning: Node {current_ip}:{current_port} is unreachable. Skipping...")
            break

        node_info = {
            "ip": current_ip,
            "port": current_port,
            "ID": current_id,
            "successor": {"ip": successor_data[0], "port": successor_data[1]},
            "predecessor": {"ip": predecessor_data[0], "port": predecessor_data[1]}
        }

        nodes.append(node_info)

        # Move to the next node in the ring
        next_ip, next_port = successor_data
        if not next_ip or not next_port or (next_ip, next_port) == (chord_node.ip, chord_node.port):
            break  # Stop if we reach the bootstrap node again (full cycle completed)

        current_ip, current_port = next_ip, next_port
        current_id = chord_node.hash_id(f"{current_ip}:{current_port}")  # Compute node ID

    return jsonify({"overlay": nodes}), 200



@routes.route('/join', methods=['POST'])
def join():
    """Handles new nodes joining the Chord ring through the bootstrap node."""
    data = request.json
    new_node_ip = data["ip"]
    new_node_port = data["port"]
    new_node_id = data["node_id"]

    # If only the bootstrap node exists, make the new node its direct neighbor
    if chord_node.successor == (chord_node.ip, chord_node.port):
        chord_node.successor = (new_node_ip, new_node_port)
        chord_node.predecessor = (new_node_ip, new_node_port)

        response_data = {
            "successor_ip": chord_node.ip,  # Bootstrap node
            "successor_port": chord_node.port,
            "predecessor_ip": chord_node.ip,
            "predecessor_port": chord_node.port
        }
    else:
        # Find the correct position for the new node
        successor_ip, successor_port = find_correct_successor(new_node_id)

        # Get the predecessor of the correct successor
        pred_response = requests.get(f"http://{successor_ip}:{successor_port}/predecessor")
        predecessor_ip, predecessor_port = pred_response.json().get("predecessor", (None, None))

        # Update new node's successor and predecessor
        response_data = {
            "successor_ip": successor_ip,
            "successor_port": successor_port,
            "predecessor_ip": predecessor_ip,
            "predecessor_port": predecessor_port
        }

        # Notify predecessor to update its successor
        requests.post(f"http://{predecessor_ip}:{predecessor_port}/update_successor",
                      json={"new_successor_ip": new_node_ip, "new_successor_port": new_node_port})

        # Notify successor to update its predecessor
        requests.post(f"http://{successor_ip}:{successor_port}/update_predecessor",
                      json={"new_predecessor_ip": new_node_ip, "new_predecessor_port": new_node_port})

    print(f"Node {new_node_ip}:{new_node_port} (ID {new_node_id}) joined successfully!")

    return jsonify(response_data), 200



def find_correct_successor(node_id):
    """Finds the correct successor for a new node in the Chord ring."""
    current_ip, current_port = chord_node.ip, chord_node.port
    successor_ip, successor_port = chord_node.successor

    print("Current IP and Port: ", current_ip, current_port)
    print ("Successor IP and Port: ", successor_ip, successor_port)

    while True:
        # Get the ID of the current node and its successor
        current_id = chord_node.hash_id(f"{current_ip}:{current_port}")
        successor_id = chord_node.hash_id(f"{successor_ip}:{successor_port}")

        print("Current ID: ", current_id)
        print("Successor ID: ", successor_id)
        print("My ID: ", node_id)

        # Case 1: Correct place found (node_id should go between current_id and successor_id)
        if (current_id < node_id <= successor_id) or (current_id > successor_id and (node_id > current_id or node_id < successor_id)):
            print("I found the successor: ", successor_ip, successor_port)
            return successor_ip, successor_port  # Found the correct successor

        # Move to the next node in the ring
        response = requests.get(f"http://{successor_ip}:{successor_port}/successor")
        next_successor = response.json().get("successor", (None, None))
        print(f"The successor of ${successor_ip} ${successor_port} is: ", next_successor)

        if next_successor == (None, None):
            break  # Stop if there's an issue retrieving successor info

        # Move to the next node
        current_ip, current_port = successor_ip, successor_port
        successor_ip, successor_port = next_successor

        print("Current IP and Port: ", current_ip, current_port)
        print ("Successor IP and Port: ", successor_ip, successor_port)

        # If we loop back to the bootstrap node, stop
        if (successor_ip, successor_port) == (chord_node.ip, chord_node.port):
            break

    print("Got out of the loop and returned: ", chord_node.successor)
    return chord_node.ip, chord_node.port  # Default to bootstrap node’s successor if loop fails







# Function to set the chord_node instance dynamically
def set_chord_node(node_instance):
    global chord_node
    chord_node = node_instance