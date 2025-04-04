import pickle  # nosec
import socket
from typing import Any

from pywalpattern.domain.models import Command, Response


class KVClient:
    """
    A client for interacting with a key-value store server.

    Attributes:
        host (str): The server's hostname or IP address.
        port (int): The server's port number.
        socket (socket.socket): The socket used for communication with the server.
    """

    def __init__(self, host: str, port: int):
        """
        Initializes the KVClient with the server's host and port.

        Args:
            host (str): The server's hostname or IP address.
            port (int): The server's port number.
        """
        self.host = host
        self.port = port
        self.socket = None

    def connect(self) -> bool:
        """
        Connect to the server.

        Returns:
            bool: True if the connection was successful, False otherwise.
        """
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            return True
        except Exception as e:
            print(f"Error connecting to server: {e}")
            return False

    def disconnect(self):
        """
        Disconnect from the server.
        """
        if self.socket:
            self.send_command(Command.QUIT)
            self.socket.close()
            self.socket = None

    def send_command(self, command: str, **kwargs) -> dict[str, Any]:
        """
        Send a command to the server and return the response.

        Args:
            command (str): The command to send to the server.
            **kwargs: Additional arguments for the command.

        Returns:
            dict[str, Any]: The server's response.

        Raises:
            ConnectionError: If not connected to the server or if the connection is closed by the server.
        """
        if not self.socket:
            raise ConnectionError("Not connected to server")

        # Prepare command
        command_data = {"command": command, **kwargs}
        serialized = pickle.dumps(command_data)  # nosec

        # Send command length and data
        self.socket.sendall(len(serialized).to_bytes(4, byteorder="big"))
        self.socket.sendall(serialized)

        # Receive response length
        length_bytes = self.socket.recv(4)
        if not length_bytes:
            raise ConnectionError("Connection closed by server")

        length = int.from_bytes(length_bytes, byteorder="big")

        # Receive response data
        data = b""
        remaining = length
        while remaining > 0:
            chunk = self.socket.recv(min(remaining, 4096))
            if not chunk:
                break
            data += chunk
            remaining -= len(chunk)

        if len(data) != length:
            raise ConnectionError("Incomplete data received from server")

        # Deserialize response
        return pickle.loads(data)  # nosec

    def get(self, key: str) -> Any:
        """
        Get a value by key.

        Args:
            key (str): The key to retrieve.

        Returns:
            Any: The value associated with the key, or None if the key does not exist.
        """
        response = self.send_command(Command.GET, key=key)
        if response["status"] == Response.RESULT:
            return response["value"]
        elif response["status"] == Response.ERROR:
            print(f"Error: {response.get('message')}")
            return None

    def put(self, key: str, value: Any) -> bool:
        """
        Store a key-value pair.

        Args:
            key (str): The key to store.
            value (Any): The value to store.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        response = self.send_command(Command.PUT, key=key, value=value)
        return response["status"] == Response.OK

    def delete(self, key: str) -> bool:
        """
        Delete a key-value pair.

        Args:
            key (str): The key to delete.

        Returns:
            bool: True if the key was deleted, False if the key does not exist.
        """
        response = self.send_command(Command.DELETE, key=key)
        return response["status"] == Response.OK

    def keys(self) -> list:
        """
        Get all keys.

        Returns:
            list: A list of all keys in the key-value store.
        """
        response = self.send_command(Command.KEYS)
        if response["status"] == Response.RESULT:
            return response["keys"]
        return []

    def checkpoint(self) -> bool:
        """
        Trigger a checkpoint.

        Returns:
            bool: True if the checkpoint was successful, False otherwise.
        """
        response = self.send_command(Command.CHECKPOINT)
        return response["status"] == Response.OK
