import threading
import time

import requests
from flask import Flask, jsonify, request

from pydistributedsystems.service.wal import KeyValueStore, WriteAheadLog

app = Flask(__name__)
follower_kv_store = KeyValueStore(WriteAheadLog("follower_wal_1.log"))
leader_address = "http://localhost:5000"
heartbeat_interval = 5  # seconds
leader_timeout = 10  # seconds
last_heartbeat = time.time()


@app.route("/replicate", methods=["POST"])
def replicate():
    data = request.get_json()
    key = data["key"]
    value = data["value"]
    follower_kv_store.set(key, value)
    return jsonify({"status": "success"}), 200


@app.route("/get/<key>", methods=["GET"])
def get(key):
    value = follower_kv_store.get(key)
    if value is not None:
        return jsonify({"key": key, "value": value}), 200
    else:
        return jsonify({"error": "Key not found"}), 404


@app.route("/heartbeat", methods=["GET"])
def heartbeat():
    global last_heartbeat
    last_heartbeat = time.time()
    return jsonify({"status": "alive"}), 200


def check_leader():
    global last_heartbeat
    while True:
        try:
            response = requests.get(f"{leader_address}/heartbeat", timeout=10)
            if response.status_code == 200:
                last_heartbeat = time.time()
        except requests.RequestException:
            pass

        if time.time() - last_heartbeat > leader_timeout:
            print("Leader is down!")
            # Take appropriate action, such as electing a new leader
        time.sleep(heartbeat_interval)


if __name__ == "__main__":
    heartbeat_thread = threading.Thread(target=check_leader)
    heartbeat_thread.daemon = True
    heartbeat_thread.start()
    app.run(port=5001)
