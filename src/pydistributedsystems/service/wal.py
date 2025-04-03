import json
import os

import requests


class WriteAheadLog:
    def __init__(self, log_file):
        self.log_file = log_file
        self.log_entries = []

        # Load existing log entries if the log file exists
        if os.path.exists(self.log_file):
            self._load_log()

    def _load_log(self):
        with open(self.log_file, "r") as file:
            for line in file:
                try:
                    entry = json.loads(line.strip())
                    self.log_entries.append(entry)
                except json.JSONDecodeError:
                    print("Error decoding log entry:", line.strip())

    def write_entry(self, entry):
        if not self.log_entries or self.log_entries[-1] != entry:
            if entry not in self.log_entries:
                with open(self.log_file, "a") as file:
                    file.write(json.dumps(entry) + "\n")
                self.log_entries.append(entry)

    def read_entries(self):
        return self.log_entries

    def recover(self):
        return self.log_entries


class KeyValueStore:
    def __init__(self, wal):
        self.store = {}
        self.wal = wal
        self._recover_from_log()

    def _recover_from_log(self):
        for entry in self.wal.recover():
            if "operation" in entry and entry["operation"] == "set":
                self.store[entry["key"]] = entry["value"]

    def set(self, key, value):
        self.store[key] = value
        self.wal.write_entry({"operation": "set", "key": key, "value": value})

    def get(self, key):
        return self.store.get(key, None)


class Leader:
    def __init__(self, log_file, followers):
        self.kv_store = KeyValueStore(WriteAheadLog(log_file))
        self.followers = followers

    def set(self, key, value):
        self.kv_store.set(key, value)
        self._replicate_to_followers(key, value)

    def get(self, key):
        return self.kv_store.get(key)

    def _replicate_to_followers(self, key, value):
        for follower in self.followers:
            try:
                requests.post(f"http://{follower}/replicate", json={"key": key, "value": value}, timeout=10)
            except requests.RequestException as e:
                print(f"Error replicating to follower {follower}: {e}")


if __name__ == "__main__":
    # Leader node with multiple followers
    followers = ["localhost:5001", "localhost:5002"]
    leader = Leader("leader_wal.log", followers)

    # Set some key-value pairs
    leader.set("foo", "bar")
    leader.set("baz", "qux")

    # Get key-value pairs
    print("foo:", leader.get("foo"))
    print("baz:", leader.get("baz"))
