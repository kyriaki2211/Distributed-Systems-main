import requests
import sys

SERVER_URL = "http://127.0.0.1:5000"

def insert_song(title, location):
    """Εισάγει ένα τραγούδι στο DHT"""
    response = requests.post(f"{SERVER_URL}/insert", json={"title": title, "location": location})
    print(response.json()["message"])

def delete_song(title):
    """Διαγράφει ένα τραγούδι από το DHT"""
    response = requests.delete(f"{SERVER_URL}/delete/{title}")
    print(response.json()["message"])

def query_song(title):
    """Αναζητά ένα τραγούδι στο DHT"""
    response = requests.get(f"{SERVER_URL}/query/{title}")

    try:
        data = response.json()
        if title == "*":
            print("Stored songs per node:")
            for node, songs in data["songs_per_node"].items():
                print(f"{node}: {songs}")
        else:
            print(f"Location: {data['location']}")
    except requests.exceptions.JSONDecodeError:
        print("Error: Server did not return valid JSON.")


def depart_node():
    """Ο κόμβος αποχωρεί από το δίκτυο"""
    response = requests.post(f"{SERVER_URL}/depart")
    print(response.json()["message"])

def overlay_network():
    """Εκτυπώνει την τοπολογία του δικτύου"""
    response = requests.get(f"{SERVER_URL}/overlay")

    try:
        data = response.json()
        if "nodes" in data and data["nodes"]:
            print("Chord Ring Topology:")
            for node in data["nodes"]:
                print(node)
        else:
            print("No nodes found in the Chord ring.")
    except requests.exceptions.JSONDecodeError:
        print("Error: Could not retrieve the network topology. Server may not be responding.")


def print_help():
    """Εκτυπώνει τις διαθέσιμες εντολές"""
    help_text = """
Chordify CLI - Available Commands:
----------------------------------
insert <title> <location>   - Εισαγωγή τραγουδιού στο DHT
delete <title>              - Διαγραφή τραγουδιού από το DHT
query <title>               - Αναζήτηση τραγουδιού (ή '*' για όλα τα τραγούδια)
depart                      - Αποχώρηση του κόμβου από το δίκτυο
overlay                     - Εκτύπωση της τοπολογίας του δικτύου
help                        - Εμφάνιση αυτής της βοήθειας
    """
    print(help_text)

def main():
    if len(sys.argv) < 2:
        print("Σφάλμα: Δεν δόθηκε εντολή. Δοκιμάστε 'help' για βοήθεια.")
        return

    command = sys.argv[1]

    if command == "insert":
        if len(sys.argv) != 4:
            print("Χρήση: insert <title> <location>")
        else:
            insert_song(sys.argv[2], sys.argv[3])

    elif command == "delete":
        if len(sys.argv) != 3:
            print("Χρήση: delete <title>")
        else:
            delete_song(sys.argv[2])

    elif command == "query":
        if len(sys.argv) != 3:
            print("Χρήση: query <title>")
        else:
            query_song(sys.argv[2])

    elif command == "depart":
        depart_node()

    elif command == "overlay":
        overlay_network()

    elif command == "help":
        print_help()

    else:
        print(f"Άγνωστη εντολή: {command}. Δοκιμάστε 'help' για λίστα εντολών.")

if __name__ == "__main__":
    main()