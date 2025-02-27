# nodes/dht.py
from nodes.node import ChordNode

class DHT:
    def __init__(self, node: ChordNode):
        self.node = node
        self.data_store = {}

    def insert(self, key, value):
        key_hash = self.node.hash_id(key)
        if self.node.is_responsible(key_hash):
            if key_hash in self.data_store and value not in self.data_store[key_hash]:
                self.data_store[key_hash].append(value)
            else:
                self.data_store[key_hash] = [value]
            print(self.data_store)
            return {"status": "success", "message": "Data inserted successfully"}
        else:
            temp = list(self.node.successor)  # Convert tuple to list
            temp[1] += 1  # Modify the second element
            #self.node.successor = tuple(temp)  # Convert back to tuple if needed
            temp2 =  tuple(temp)
            result = self.node.forward_request(temp2, "/insert", {"key": key, "value": value})
            return result

    def query(self, key):
        key_hash = self.node.hash_id(key)
        if self.node.is_responsible(key_hash):
            return self.data_store.get(key_hash, [])
        else:
            return self.node.forward_request(self.node.successor, "/query", {"key": key})

    def delete(self, key):
        key_hash = self.node.hash_id(key)
        if self.node.is_responsible(key_hash):
            if key_hash in self.data_store:
                del self.data_store[key_hash]
        else:
            self.node.forward_request(self.node.successor, "/delete", {"key": key})

