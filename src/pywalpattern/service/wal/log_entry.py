import pickle  # nosec
import time
from typing import Any

from pywalpattern.domain.models import OperationType


class LogEntry:
    """
    A class to represent a log entry in the Write-Ahead Log (WAL) system.

    Attributes:
        seq_num (int): The sequence number of the log entry.
        op_type (OperationType): The type of operation (e.g., INSERT, UPDATE, DELETE).
        key (str): The key associated with the log entry.
        value (Any, optional): The value associated with the log entry (default is None).
        timestamp (float): The timestamp when the log entry was created.
    """

    def __init__(self, seq_num: int, op_type: OperationType, key: str, value: Any = None):
        """
        Constructs all the necessary attributes for the LogEntry object.

        Args:
            seq_num (int): The sequence number of the log entry.
            op_type (OperationType): The type of operation (e.g., INSERT, UPDATE, DELETE).
            key (str): The key associated with the log entry.
            value (Any, optional): The value associated with the log entry (default is None).
        """
        self.seq_num = seq_num
        self.op_type = op_type
        self.key = key
        self.value = value
        self.timestamp = time.time()

    def serialize(self) -> bytes:
        """
        Serializes the log entry into bytes.

        Returns:
            bytes: The serialized log entry.
        """
        return pickle.dumps(self)

    @staticmethod
    def deserialize(data: bytes) -> "LogEntry":
        """
        Deserializes bytes back into a LogEntry object.

        Args:
            data (bytes): The bytes to deserialize.

        Returns:
            LogEntry: The deserialized log entry object.
        """
        return pickle.loads(data)  # nosec
