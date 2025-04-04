import json
import time
import zlib
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
            timestamp (float): The timestamp when the log entry was created.
            checksum (int): The CRC checksum of the log entry.
        """
        self.seq_num = seq_num
        self.op_type = op_type
        self.key = key
        self.value = value
        self.timestamp = time.time()
        self.checksum = self.calculate_checksum()

    def calculate_checksum(self) -> int:
        """Calculate the CRC checksum of the log entry."""
        data = f"{self.seq_num}{self.op_type.value}{self.key}{self.value}{self.timestamp}"
        return zlib.crc32(data.encode("utf-8"))

    def to_dict(self) -> dict:
        """Convert LogEntry to a dictionary for JSON serialization"""
        return {
            "seq_num": self.seq_num,
            "op_type": self.op_type.value,  # Store enum as integer
            "key": self.key,
            "value": self.value,
            "timestamp": self.timestamp,
            "checksum": self.checksum,
        }

    @staticmethod
    def from_dict(data: dict) -> "LogEntry":
        """Create LogEntry from a dictionary"""
        entry = LogEntry(
            seq_num=data["seq_num"],
            op_type=OperationType(data["op_type"]),  # Convert integer back to enum
            key=data["key"],
            value=data.get("value"),
        )
        entry.timestamp = data["timestamp"]
        entry.checksum = data.get("checksum", entry.calculate_checksum())  # Use default if missing
        return entry

    def serialize(self) -> bytes:
        """Serialize entry to JSON bytes"""
        return json.dumps(self.to_dict()).encode("utf-8")

    @staticmethod
    def deserialize(data: bytes) -> "LogEntry":
        """Deserialize bytes to LogEntry"""
        entry_dict = json.loads(data.decode("utf-8"))
        entry = LogEntry.from_dict(entry_dict)
        # Verify the checksum
        if entry.checksum != entry.calculate_checksum():
            raise ValueError("Checksum verification failed")
        return entry
