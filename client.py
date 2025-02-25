import sys
import requests

BASE_URL = "http://127.0.0.1:5000"

def send_request(endpoint, method="GET", data=None):
    url = f"{BASE_URL}/{endpoint}"
    try:
        if method == "POST":
            response = requests.post(url, json=data)
        else:
            response = requests.get(url)
        print(response.json())
    except requests.exceptions.ConnectionError:
        print("Failed to connect to the node.")

def main():
    if len(sys.argv) < 2:
        print("Usage: python client.py <command> [key] [value]")
        return

    command = sys.argv[1]
    if command == "insert" and len(sys.argv) == 4:
        send_request("insert", method="POST", data={"key": sys.argv[2], "value": sys.argv[3]})
    elif command == "query" and len(sys.argv) == 3:
        send_request(f"query/{sys.argv[2]}")
    else:
        print("Invalid command")

if __name__ == "__main__":
    main()
