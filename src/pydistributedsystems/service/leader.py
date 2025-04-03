import threading
import time

import requests
from flask import Flask, jsonify, request

from pydistributedsystems.service.wal import KeyValueStore, WriteAheadLog

app = Flask(__name__)
leader_kv_store = KeyValueStore(WriteAheadLog("leader_wal.log"))
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = False
followers = ["localhost:5001", "localhost:5002"]
heartbeat_interval = 5  # seconds


@app.route("/replicate", methods=["POST"])
def replicate():
    data = request.get_json()
    key = data["key"]
    value = data["value"]
    leader_kv_store.set(key, value)
    _replicate_to_followers(key, value)
    return jsonify({"status": "success"}), 200


@app.route("/get/<key>", methods=["GET"])
def get(key):
    value = leader_kv_store.get(key)
    if value is not None:
        return jsonify({"key": key, "value": value}), 200
    else:
        return jsonify({"error": "Key not found"}), 404


@app.route("/heartbeat", methods=["GET"])
def heartbeat():
    return jsonify({"status": "alive"}), 200


@app.route("/set", methods=["POST"])
def set_key_value():
    print(request.is_json)
    if request.is_json:
        data = request.get_json()
        key = data["key"]
        value = data["value"]
        leader_kv_store.set(key, value)
        _replicate_to_followers(key, value)
        return jsonify({"status": "success"}), 200
    else:
        return jsonify({"error": "Unsupported Media Type"}), 415


def _replicate_to_followers(key, value):
    for follower in followers:
        try:
            requests.post(f"http://{follower}/replicate", json={"key": key, "value": value}, timeout=10)
        except requests.RequestException as e:
            print(f"Error replicating to follower {follower}: {e}")


def send_heartbeat():
    while True:
        for follower in followers:
            try:
                requests.get(f"http://{follower}/heartbeat", timeout=10)
            except requests.RequestException as e:
                print(f"Error sending heartbeat to follower {follower}: {e}")
        time.sleep(heartbeat_interval)


if __name__ == "__main__":
    heartbeat_thread = threading.Thread(target=send_heartbeat)
    heartbeat_thread.daemon = True
    heartbeat_thread.start()
    app.run(port=5000)
