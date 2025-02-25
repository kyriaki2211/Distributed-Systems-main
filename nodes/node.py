import hashlib
import socket
import threading
import json
from flask import Flask, request, jsonify
import requests
import argparse


app = Flask(__name__)

class ChordNode:
    def __init__(self, ip, port, bootstrap_ip=None, bootstrap_port=None):
        self.ip = ip
        self.port = port
        self.node_id = self.hash_id(f"{ip}:{port}")
        self.successor = (self.ip, self.port)  # Bootstrap node starts as its own successor

        if (bootstrap_ip is None):
            self.is_bootstrap = True
            self.predecessor = (self.ip, self.port)  # Bootstrap node predecessor is itself
            print(f"Bootstrap Node started: ID {self.node_id}, IP {self.ip}:{self.port}")
        else:
            self.is_bootstrap = False
            self.predecessor = None
            print(f"Joining Chord via Bootstrap Node {bootstrap_ip}:{bootstrap_port}")
            self.join_ring(bootstrap_ip, bootstrap_port)


    def hash_id(self, key):
        return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2**16)  # 32-bit hash



    def join_ring(self, bootstrap_ip, bootstrap_port):
        """Join an existing Chord ring via the bootstrap node."""
        response = requests.post(f"http://{bootstrap_ip}:{bootstrap_port}/join",
                                json={"node_id": self.node_id, "ip": self.ip, "port": self.port})

        if response.status_code == 200:
            data = response.json()
            print("Received response from bootstrap:", data)

            # Set correct successor and predecessor
            self.successor = (data["successor_ip"], data["successor_port"])
            self.predecessor = (data["predecessor_ip"], data["predecessor_port"])

            print(f"Joined ring: Successor -> {self.successor}, Predecessor -> {self.predecessor}")

            # Notify successor to update its predecessor
            requests.post(f"http://{self.successor[0]}:{self.successor[1]}/update_predecessor",
                          json={"new_predecessor_ip": self.ip, "new_predecessor_port": self.port})

            # Notify predecessor to update its successor
            requests.post(f"http://{self.predecessor[0]}:{self.predecessor[1]}/update_successor",
                          json={"new_successor_ip": self.ip, "new_successor_port": self.port})

        else:
            print(f"Failed to join ring: {response.status_code}, {response.text}")



    def find_successor(self, key):
        """Find the responsible node for a given key."""
        if self.successor is None or self.successor == self:
            return self  # Επιστρέφει τον εαυτό του αν είναι ο μόνος κόμβος 

        if self.node_id < key <= self.successor.node_id:
            return self.successor

        return self.successor.find_successor(key)
    

    def insert(self, key, value):
        key_hash = self.hash_id(key)
        if self.is_responsible(key_hash):
            if key_hash in self.data_store:
                self.data_store[key_hash].append(value)
            else:
                self.data_store[key_hash] = [value]
        else:
            self.forward_request(self.successor, "/insert", {"key": key, "value": value})

    def query(self, key):
        key_hash = self.hash_id(key)
        if self.is_responsible(key_hash):
            return self.data_store.get(key_hash, [])
        else:
            return self.forward_request(self.successor, "/query", {"key": key})

    def delete(self, key):
        key_hash = self.hash_id(key)
        if self.is_responsible(key_hash):
            if key_hash in self.data_store:
                del self.data_store[key_hash]
        else:
            self.forward_request(self.successor, "/delete", {"key": key})

    def is_responsible(self, key_hash):
        if self.predecessor is None:
            return True  # Single node case
        return self.predecessor[0] < key_hash <= self.node_id

    def forward_request(self, node, endpoint, data):
        return self.send_request(node, endpoint, data)

    def send_request(self, node, endpoint, data):
        ip, port = node
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((ip, port))
                s.sendall(json.dumps({"endpoint": endpoint, "data": data}).encode())
                response = s.recv(1024)
                return json.loads(response.decode())
        except Exception as e:
            print(f"Failed to send request to {ip}:{port} -> {e}")
            return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", required=True, help="Node IP address")
    parser.add_argument("--port", required=True, type=int, help="Node port")
    parser.add_argument("--bootstrap", help="Bootstrap node in format ip:port")
    args = parser.parse_args()
    
    bootstrap_node = args.bootstrap.split(":") if args.bootstrap else None
    bootstrap_ip, bootstrap_port = bootstrap_node if bootstrap_node else (None, None)
    bootstrap_port = int(bootstrap_port) if bootstrap_port else None  # Ensure port is an integer

    chord_node = ChordNode(args.ip, args.port, bootstrap_ip, bootstrap_port)
    app.run(host=args.ip, port=args.port, threaded=True)

