import hashlib

def hash_function(key):
    """Hashes a key using SHA-1 and returns an integer ID"""
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2 ** 16)
