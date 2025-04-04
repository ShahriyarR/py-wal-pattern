import json
import time
import zlib
from typing import Any

from pywalpattern.domain.models import CompressionConfig, CompressionType, OperationType
from pywalpattern.service.wal.compression import CompressionManager


class LogEntry:
    """
    A class to represent a log entry in the Write-Ahead Log (WAL) system.

    Attributes:
        seq_num (int): The sequence number of the log entry.
        op_type (OperationType): The type of operation (e.g., INSERT, UPDATE, DELETE).
        key (str): The key associated with the log entry.
        value (Any, optional): The value associated with the log entry (default is None).
        timestamp (float): The timestamp when the log entry was created.
        checksum (int): The CRC checksum of the log entry.
        format_version (int): The format version of the log entry.
    """

    CURRENT_FORMAT_VERSION = 1

    def __init__(self, seq_num: int, op_type: OperationType, key: str, value: Any = None, format_version: int = CURRENT_FORMAT_VERSION):
        """
        Constructs all the necessary attributes for the LogEntry object.

        Args:
            seq_num (int): The sequence number of the log entry.
            op_type (OperationType): The type of operation (e.g., INSERT, UPDATE, DELETE).
            key (str): The key associated with the log entry.
            value (Any, optional): The value associated with the log entry (default is None).
            format_version (int): The format version of the log entry (default is CURRENT_FORMAT_VERSION).
        """
        self.seq_num = seq_num
        self.op_type = op_type
        self.key = key
        self.value = value
        self.timestamp = time.time()
        self.checksum = self.calculate_checksum()
        self.format_version = format_version

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
            "format_version": self.format_version,
        }

    @staticmethod
    def from_dict(data: dict) -> "LogEntry":
        """Create LogEntry from a dictionary"""
        format_version = data.get("format_version", LogEntry.CURRENT_FORMAT_VERSION)

        entry = LogEntry(
            seq_num=data["seq_num"],
            op_type=OperationType(data["op_type"]),  # Convert integer back to enum
            key=data["key"],
            value=data.get("value"),
            format_version=format_version,
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


class CompressedLogEntry(LogEntry):
    def __init__(self, *args, compression_config: CompressionConfig | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.compression_config = compression_config or CompressionConfig(CompressionType.NONE)
        self._compression_manager = CompressionManager(self.compression_config)
        self.compression_type = CompressionType.NONE

    def serialize(self) -> bytes:
        """Serialize and compress entry"""
        json_data = json.dumps(self.to_dict()).encode("utf-8")
        compressed_data, compression_type = self._compression_manager.compress(json_data)
        self.compression_type = compression_type

        # Format: [compression_type(1 byte)][compressed_data]
        return bytes([compression_type.value]) + compressed_data

    @staticmethod
    def deserialize(data: bytes) -> LogEntry:
        """Deserialize and decompress entry"""
        compression_type = CompressionType(data[0])
        compressed_data = data[1:]

        compression_manager = CompressionManager(CompressionConfig(compression_type))
        decompressed_data = compression_manager.decompress(compressed_data, compression_type)

        entry_dict = json.loads(decompressed_data.decode("utf-8"))
        entry = CompressedLogEntry.from_dict(entry_dict)

        if entry.checksum != entry.calculate_checksum():
            raise ValueError("Checksum verification failed")
        return entry

    def to_dict(self) -> dict:
        data = super().to_dict()
        data["compression_type"] = self.compression_type.value
        return data

    @staticmethod
    def from_dict(data: dict) -> LogEntry:
        compression_type = CompressionType(data.pop("compression_type", CompressionType.NONE.value))
        entry = super(CompressedLogEntry, CompressedLogEntry).from_dict(data)
        entry.compression_config = CompressionConfig(compression_type)
        entry._compression_manager = CompressionManager(entry.compression_config)
        entry.compression_type = compression_type
        return entry
