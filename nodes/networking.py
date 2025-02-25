# nodes/networking.py
import json
import socket

class Networking:
    @staticmethod
    def send_request(node, endpoint, data):
        """Handles sending requests between nodes"""
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
