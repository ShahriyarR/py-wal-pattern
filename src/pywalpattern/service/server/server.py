import contextlib
import json
import socket
import threading
import time
from typing import Any

import requests
from flask import Flask, jsonify, request

from pywalpattern.domain.models import Command, Response
from pywalpattern.service.wal.storage import KeyValueStore


class KVServer:
    """
    A server for handling key-value store operations with Write-Ahead Logging (WAL) for durability.

    Attributes:
        host (str): The server's hostname or IP address.
        port (int): The server's port number.
        store (KeyValueStore): The key-value store instance.
        server_socket (socket.socket): The server's socket for accepting client connections.
        running (bool): A flag indicating whether the server is running.
        clients (list): A list of active client connections.
    """

    def __init__(self, host: str, port: int, data_dir: str, is_leader: bool = False):
        """
        Initializes the KVServer with the specified host, port, and data directory.

        Args:
            host (str): The server's hostname or IP address.
            port (int): The server's port number.
            data_dir (str): The directory where data and WAL files are stored.
        """
        self.host = host
        self.port = port
        self.store = KeyValueStore(data_dir)
        self.server_socket = None
        self.running = False
        self.clients = []  # Track active client connections
        self.is_leader = is_leader
        self.followers = []
        self.flask_app = Flask(__name__)
        self._setup_routes()

    def _setup_routes(self):
        @self.flask_app.route("/replicate", methods=["POST"])
        def replicate():
            if not self.is_leader:
                return jsonify({"error": "Only leader can replicate"}), 403
            command_data = request.json
            response = self.process_command(command_data)
            return jsonify(response)

        @self.flask_app.route("/register_follower", methods=["POST"])
        def register_follower():
            follower_address = request.json.get("address")
            if follower_address not in self.followers:
                self.followers.append(follower_address)
            return jsonify({"status": "Follower registered"})

    def start_flask(self):
        self.flask_app.run(host=self.host, port=self.port + 1)

    def replicate_to_followers(self, command_data: dict[str, Any]):
        for follower in self.followers:
            try:
                response = requests.post(f"http://{follower}/replicate", json=command_data, timeout=10)
                if response.status_code != 200:
                    print(f"Failed to replicate to {follower}")
            except Exception as e:
                print(f"Error replicating to {follower}: {e}")

    def start(self):
        """
        Start the server and begin accepting client connections.
        """
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        print(f"Server started on {self.host}:{self.port}")

        # Start background task for log cleanup
        cleanup_thread = threading.Thread(target=self._log_cleanup_task)
        cleanup_thread.daemon = True
        cleanup_thread.start()

        if self.is_leader:
            flask_thread = threading.Thread(target=self.start_flask)
            flask_thread.daemon = True
            flask_thread.start()

        try:
            while self.running:
                client_socket, address = self.server_socket.accept()
                print(f"Client connected from {address}")
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket, address))
                client_thread.daemon = True
                client_thread.start()
                self.clients.append((client_socket, client_thread))
        except KeyboardInterrupt:
            print("Server shutting down...")
        finally:
            self.stop()

    def stop(self):
        """
        Stop the server and close all client connections.
        """
        self.running = False

        # Close all client connections
        for client_socket, _ in self.clients:
            with contextlib.suppress(Exception):
                client_socket.close()

        # Close server socket
        if self.server_socket:
            self.server_socket.close()

        # Close the store
        self.store.close()
        print("Server stopped")

    def handle_client(self, client_socket: socket.socket, address: tuple[str, int]):  # noqa: CCR001 C901
        """
        Handle a client connection, processing commands and sending responses.

        Args:
            client_socket (socket.socket): The client's socket.
            address (tuple[str, int]): The client's address.
        """
        try:
            while self.running:
                # Receive data length (4 bytes)
                length_bytes = client_socket.recv(4)
                if not length_bytes:
                    break

                length = int.from_bytes(length_bytes, byteorder="big")

                # Receive command data
                data = b""
                remaining = length
                while remaining > 0:
                    chunk = client_socket.recv(min(remaining, 4096))
                    if not chunk:
                        break
                    data += chunk
                    remaining -= len(chunk)

                if len(data) != length:
                    print(f"Incomplete data received from {address}")
                    break

                # Process command
                try:
                    command_data = json.loads(data.decode("utf-8"))
                    response = self.process_command(command_data)

                    # Serialize response
                    response_data = json.dumps(response).encode("utf-8")
                    response_length = len(response_data)

                    # Send response length and data
                    client_socket.sendall(response_length.to_bytes(4, byteorder="big"))
                    client_socket.sendall(response_data)

                    # If client sent QUIT, close connection
                    if command_data.get("command") == Command.QUIT:
                        break

                except Exception as e:
                    print(f"Error processing command: {e}")
                    error_response = {"status": Response.ERROR, "message": str(e)}
                    response_data = json.dumps(error_response).encode("utf-8")
                    response_length = len(response_data)
                    client_socket.sendall(response_length.to_bytes(4, byteorder="big"))
                    client_socket.sendall(response_data)

        except Exception as e:
            print(f"Error handling client {address}: {e}")
        finally:
            client_socket.close()
            print(f"Client {address} disconnected")
            # Remove from active clients list
            self.clients = [(s, t) for s, t in self.clients if s != client_socket]

    def process_command(self, command_data: dict[str, Any]) -> dict[str, Any]:  # noqa: C901 CCR001
        """
        Process a client command and return the appropriate response.

        Args:
            command_data (dict[str, Any]): The command data received from the client.

        Returns:
            dict[str, Any]: The response to be sent back to the client.
        """
        command = command_data.get("command")

        if command == Command.GET:
            key = command_data.get("key")
            value = self.store.get(key)
            if value is not None:
                return {"status": Response.RESULT, "value": value}
            else:
                return {"status": Response.ERROR, "message": f"Key: {key} not found"}

        elif command == Command.PUT:
            key = command_data.get("key")
            value = command_data.get("value")
            self.store.put(key, value)
            if self.is_leader:
                self.replicate_to_followers(command_data)
            return {"status": Response.OK}

        elif command == Command.DELETE:
            key = command_data.get("key")
            success = self.store.delete(key)
            if success:
                if self.is_leader:
                    self.replicate_to_followers(command_data)
                return {"status": Response.OK}
            else:
                return {"status": Response.ERROR, "message": f"Key: {key} not found"}

        elif command == Command.KEYS:
            keys = list(self.store.data.keys())
            return {"status": Response.RESULT, "keys": keys}

        elif command == Command.CHECKPOINT:
            self.store.checkpoint()
            return {"status": Response.OK}

        elif command == Command.QUIT:
            return {"status": Response.OK, "message": "Goodbye"}

        else:
            return {"status": Response.ERROR, "message": f"Unknown command: {command}"}

    def _log_cleanup_task(self):
        """
        Background task to periodically check and delete old log segments.
        """
        while self.running:
            with self.store.lock:
                low_water_mark = self.store.low_water_mark  # Use low-water mark set by checkpoint
                snapshot_seq_num = self.store.get_snapshot_seq_num()  # Get the snapshot sequence number
                self.store.wal.delete_old_segments(low_water_mark, snapshot_seq_num)
            time.sleep(60)  # Run every 60 seconds
