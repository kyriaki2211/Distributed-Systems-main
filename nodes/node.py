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
        self.successor = (self.ip, self.port)
        self.data_store = {}  # Ήδη προστέθηκε
        self.socket_port = self.port + 1  # Ορισμός για όλους τους κόμβους
        if bootstrap_ip is None:
            self.is_bootstrap = True
            self.predecessor = (self.ip, self.port)
            print(f"Bootstrap Node started: ID {self.node_id}, IP {self.ip}:{self.port}")
        else:
            self.is_bootstrap = False
            self.predecessor = None
            print(f"Joining Chord via Bootstrap Node {bootstrap_ip}:{bootstrap_port}")
            self.join_ring(bootstrap_ip, bootstrap_port)
    
    # Ξεκινάμε τον socket listener
        threading.Thread(target=self.start_socket_listener, daemon=True).start()

    def hash_id(self, key):
        return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2**16)  # 32-bit hash

    def join_ring(self, bootstrap_ip, bootstrap_port):
        """Join an existing Chord ring via the bootstrap node using sockets."""
        data = {"node_id": self.node_id, "ip": self.ip, "port": self.port}
        response = self.send_request((bootstrap_ip, bootstrap_port + 1), "/join", data)  # +1 για socket port
        if response and "status" in response and response["status"] == "success":
            self.successor = (response["successor_ip"], response["successor_port"])
            self.predecessor = (response["predecessor_ip"], response["predecessor_port"])
            print(f"Joined ring: Successor -> {self.successor}, Predecessor -> {self.predecessor}")
            # Contact the socket listener port (Flask port + 1)
            self.send_request((self.successor[0], self.successor[1] + 1), "/update_predecessor",
                            {"new_predecessor_ip": self.ip, "new_predecessor_port": self.port})
            self.send_request((self.predecessor[0], self.predecessor[1] + 1), "/update_successor",
                            {"new_successor_ip": self.ip, "new_successor_port": self.port})

            '''print(f"Joined ring: Successor -> {self.successor}, Predecessor -> {self.predecessor}")
            # Ενημέρωσε successor και predecessor με sockets
            self.send_request(self.successor, "/update_predecessor", {"new_predecessor_ip": self.ip, "new_predecessor_port": self.port})
            self.send_request(self.predecessor, "/update_successor", {"new_successor_ip": self.ip, "new_successor_port": self.port})'''
        else:
            print(f"Failed to join ring: {response}")




    '''def start_socket_listener(self):
        """Socket listener to handle incoming requests from other nodes."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.ip, self.socket_port))
        server.listen()
        print(f"Socket listener started on {self.ip}:{self.socket_port}")
        while True:
            client_socket, addr = server.accept()
            data = client_socket.recv(1024).decode()
            request = json.loads(data)
            endpoint, payload = request["endpoint"], request["data"]
            print(f"Received socket request on {endpoint} from {addr}")
            if endpoint == "/insert":
                self.insert(payload["key"], payload["value"])
                client_socket.sendall(json.dumps({"status": "success"}).encode())
            elif endpoint == "/query":
                response = self.query(payload["key"])
                client_socket.sendall(json.dumps({"values": response}).encode())
            elif endpoint == "/join":
                successor_ip, successor_port = self.find_correct_successor(payload["node_id"])
                pred_response = self.send_request((successor_ip, successor_port + 1), "/predecessor", {})
                predecessor_ip, predecessor_port = pred_response["predecessor_ip"], pred_response["predecessor_port"]
                response_data = {
                    "status": "success",
                    "successor_ip": successor_ip,
                    "successor_port": successor_port,
                    "predecessor_ip": predecessor_ip,
                    "predecessor_port": predecessor_port
                }
                client_socket.sendall(json.dumps(response_data).encode())
            elif endpoint == "/update_predecessor":
                self.predecessor = (payload["new_predecessor_ip"], payload["new_predecessor_port"])
                client_socket.sendall(json.dumps({"status": "success"}).encode())
            elif endpoint == "/update_successor":
                self.successor = (payload["new_successor_ip"], payload["new_successor_port"])
                client_socket.sendall(json.dumps({"status": "success"}).encode())
            elif endpoint == "/predecessor":
                response_data = { "predecessor_ip": self.predecessor[0], "predecessor_port": self.predecessor[1] }
                client_socket.sendall(json.dumps(response_data).encode())
            client_socket.close()'''
    

    def start_socket_listener(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.ip, self.socket_port))
        server.listen()
        print(f"Socket listener started on {self.ip}:{self.socket_port}")
        while True:
            client_socket, addr = server.accept()
            threading.Thread(target=self.handle_connection, args=(client_socket, addr), daemon=True).start()

    def handle_connection(self, client_socket, addr):
        data = client_socket.recv(1024).decode()
        request = json.loads(data)
        endpoint, payload = request["endpoint"], request["data"]
        print(f"Received socket request on {endpoint} from {addr}")
        
        if endpoint == "/insert":
            self.insert(payload["key"], payload["value"])
            client_socket.sendall(json.dumps({"status": "success"}).encode())
        elif endpoint == "/query":
            response = self.query(payload["key"])
            client_socket.sendall(json.dumps({"values": response}).encode())
        elif endpoint == "/join":
            successor_ip, successor_port = self.find_correct_successor(payload["node_id"])
            pred_response = self.send_request((successor_ip, successor_port + 1), "/predecessor", {})
            predecessor_ip, predecessor_port = pred_response["predecessor_ip"], pred_response["predecessor_port"]
            response_data = {
                "status": "success",
                "successor_ip": successor_ip,
                "successor_port": successor_port,
                "predecessor_ip": predecessor_ip,
                "predecessor_port": predecessor_port
            }
            client_socket.sendall(json.dumps(response_data).encode())
        elif endpoint == "/update_predecessor":
            self.predecessor = (payload["new_predecessor_ip"], payload["new_predecessor_port"])
            client_socket.sendall(json.dumps({"status": "success"}).encode())
        elif endpoint == "/update_successor":
            self.successor = (payload["new_successor_ip"], payload["new_successor_port"])
            client_socket.sendall(json.dumps({"status": "success"}).encode())
        elif endpoint == "/predecessor":
            response_data = {"predecessor_ip": self.predecessor[0], "predecessor_port": self.predecessor[1]}
            client_socket.sendall(json.dumps(response_data).encode())
        # <-- Add handling for /successor here:
        elif endpoint == "/successor":
            response_data = {"successor_ip": self.successor[0], "successor_port": self.successor[1]}
            client_socket.sendall(json.dumps(response_data).encode())
        else:
            # Optionally, send an error response if endpoint is unknown
            client_socket.sendall(json.dumps({"status": "error", "message": "Unknown endpoint"}).encode())
        
        client_socket.close()



    def find_correct_successor(self, node_id):
        """Finds the correct successor for a new node in the Chord ring using sockets."""
        current_ip, current_port = self.ip, self.port
        successor_ip, successor_port = self.successor
        while True:
            current_id = self.hash_id(f"{current_ip}:{current_port}")
            successor_id = self.hash_id(f"{successor_ip}:{successor_port}")
            if (current_id < node_id <= successor_id) or (current_id > successor_id and (node_id > current_id or node_id < successor_id)):
                return successor_ip, successor_port
            response = self.send_request((successor_ip, successor_port + 1), "/successor", {})
            next_successor = response["successor_ip"], response["successor_port"]
            if next_successor == (None, None):
                break
            current_ip, current_port = successor_ip, successor_port
            successor_ip, successor_port = next_successor
            if (successor_ip, successor_port) == (self.ip, self.port):
                break
        return self.ip, self.port  # Default to bootstrap node’s successor if loop fails

    def find_successor(self, key):
        """Find the responsible node for a given key."""
        if self.successor == (self.ip, self.port):
            return self
        key_hash = self.hash_id(str(key))  # Βεβαιώσου ότι είναι integer
        if self.node_id < key_hash <= self.hash_id(f"{self.successor[0]}:{self.successor[1]}"):
            return self.successor
        return self.successor  # Απλοποιημένο, χρειάζεται προώθηση με sockets!

    '''def insert(self, key, value):
        key_hash = self.hash_id(key)
        print(f"Inserting {key} (hash {key_hash}) -> {value} at {self.ip}:{self.port}, responsible: {self.is_responsible(key_hash)}")
        if self.is_responsible(key_hash):
            if key_hash in self.data_store:
                self.data_store[key_hash].append(value)
            else:
                self.data_store[key_hash] = [value]
            print(f"Stored in data_store: {self.data_store}")
        else:
            print(f"Forwarding to successor {self.successor}")
            response = self.send_request(self.successor, "/insert", {"key": key, "value": value})
            print(f"Forward response: {response}")'''
    def insert(self, key, value):
        key_hash = self.hash_id(key)
        print(f"Inserting {key} (hash {key_hash}) -> {value} at {self.ip}:{self.port}, responsible: {self.is_responsible(key_hash)}")
        if self.is_responsible(key_hash):
            if key_hash in self.data_store:
                self.data_store[key_hash].append(value)
            else:
                self.data_store[key_hash] = [value]
            print(f"Stored in data_store: {self.data_store}")
        else:
            print(f"Forwarding to successor {self.successor} via socket port {self.successor[1] + 1}")
            # Note: self.successor is stored as (ip, flask_port), so we add 1 to target its socket listener.
            response = self.send_request((self.successor[0], self.successor[1] + 1), "/insert", {"key": key, "value": value})
            print(f"Forward response: {response}")


    def query(self, key):
        key_hash = self.hash_id(key)
        print(f"Querying {key} (hash {key_hash}) at {self.ip}:{self.port}, responsible: {self.is_responsible(key_hash)}")
        if self.is_responsible(key_hash):
            print(f"Found in data_store: {self.data_store.get(key_hash, [])}")
            return self.data_store.get(key_hash, [])
        else:
            print(f"Forwarding query to successor {self.successor}")
            response = self.send_request(self.successor, "/query", {"key": key})
            return response["values"]

    def delete(self, key):
        key_hash = self.hash_id(key)
        print(f"Deleting {key} (hash {key_hash}) at {self.ip}:{self.port}, responsible: {self.is_responsible(key_hash)}")
        if self.is_responsible(key_hash):
            if key_hash in self.data_store:
                del self.data_store[key_hash]
        else:
            self.send_request(self.successor, "/delete", {"key": key})

    def is_responsible(self, key_hash):
        if self.predecessor is None:
            return True  # Single node case
        pred_id = self.hash_id(f"{self.predecessor[0]}:{self.predecessor[1]}")
        return pred_id < key_hash <= self.node_id

    def forward_request(self, node, endpoint, data):
        return self.send_request(node, endpoint, data)

    def send_request(self, node, endpoint, data):
        ip, port = node
        #print(f"Sending {endpoint} to {ip}:{port + 1} with data {data}")
        print(f"Sending {endpoint} to {ip}:{port} with data {data}")
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                #s.connect((ip, self.socket_port))  # Σύνδεση στο socket port του κόμβου
                s.connect((ip, port))
                s.sendall(json.dumps({"endpoint": endpoint, "data": data}).encode())
                response = s.recv(1024)
                return json.loads(response.decode())
        except Exception as e:
            #print(f"Failed to send request to {ip}:{port + 1} -> {e}")
            print(f"Failed to send request to {ip}:{port} -> {e}")
            return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", required=True, help="Node IP address")
    parser.add_argument("--port", required=True, type=int, help="Node port")
    parser.add_argument("--bootstrap", help="Bootstrap node in format ip:port")
    args = parser.parse_args()
    
    bootstrap_ip, bootstrap_port = args.bootstrap.split(":") if args.bootstrap else (None, None)
    bootstrap_port = int(bootstrap_port) if bootstrap_port else None
    chord_node = ChordNode(args.ip, args.port, bootstrap_ip, bootstrap_port)
    app.run(host=args.ip, port=args.port, threaded=True)


'''import hashlib
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
        
        
        self.data_store = {}


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

    #def is_responsible(self, key_hash):
    #    if self.predecessor is None:
    #        return True  # Single node case
    #    return self.predecessor[0] < key_hash <= self.node_id


    def is_responsible(self, key_hash):
        if self.predecessor is None:
            return True  # Single node case
        pred_id = self.hash_id(f"{self.predecessor[0]}:{self.predecessor[1]}")  # Υπολογίζει το node_id του predecessor
        return pred_id < key_hash <= self.node_id

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

'''
