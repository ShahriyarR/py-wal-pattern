import json
import os
import threading
from pathlib import Path
from typing import Any

from pywalpattern.domain.models import OperationType
from pywalpattern.service.wal.wal import WAL


class KeyValueStore:
    """
    A class to represent a key-value store with Write-Ahead Logging (WAL) for durability.

    Attributes:
        data_dir (str): The directory where data and WAL files are stored.
        wal (WAL): The Write-Ahead Log instance.
        data (dict[str, Any]): The in-memory key-value store.
        lock (threading.RLock): A reentrant lock for thread-safe operations.
    """

    def __init__(self, data_dir: str):
        """
        Constructs all the necessary attributes for the KeyValueStore object.

        Args:
            data_dir (str): The directory where data and WAL files are stored.
        """
        self.data_dir = data_dir
        Path(data_dir).mkdir(exist_ok=True, parents=True)

        self.wal = WAL(os.path.join(data_dir, "wal"))
        self.data: dict[str, Any] = {}
        self.lock = threading.RLock()

        # Recovery from WAL
        self._recover_from_wal()

    def _recover_from_wal(self):
        """
        Apply all operations from WAL to rebuild in-memory state.
        """
        entries = self.wal.read_all_entries()
        for entry in entries:
            if entry.op_type == OperationType.PUT:
                self.data[entry.key] = entry.value
            elif entry.op_type == OperationType.DELETE and entry.key in self.data:
                del self.data[entry.key]

    def put(self, key: str, value: Any) -> None:
        """
        Store a key-value pair.

        Args:
            key (str): The key to store.
            value (Any): The value to store.
        """
        with self.lock:
            # First log the operation
            self.wal.append(OperationType.PUT, key, value)
            # Then update in-memory state
            self.data[key] = value

    def get(self, key: str) -> Any | None:
        """
        Retrieve a value by key.

        Args:
            key (str): The key to retrieve.

        Returns:
            Optional[Any]: The value associated with the key, or None if the key does not exist.
        """
        with self.lock:
            return self.data.get(key)

    def delete(self, key: str) -> bool:
        """
        Delete a key-value pair.

        Args:
            key (str): The key to delete.

        Returns:
            bool: True if the key was deleted, False if the key does not exist.
        """
        with self.lock:
            if key not in self.data:
                return False

            # First log the operation
            self.wal.append(OperationType.DELETE, key)
            # Then update in-memory state
            del self.data[key]
            return True

    def checkpoint(self) -> None:
        """
        Create a snapshot of the current state and compact the WAL.
        """
        with self.lock:
            # Write current state to a snapshot file
            snapshot_path = os.path.join(self.data_dir, "snapshot.json")
            with open(snapshot_path, "w") as f:
                json.dump(self.data, f)

            # Close current WAL file
            self.wal.close()

            # Remove old WAL files
            for f in os.listdir(os.path.join(self.data_dir, "wal")):
                if f.endswith(".log"):
                    os.remove(os.path.join(self.data_dir, "wal", f))

            # Create new WAL
            self.wal = WAL(os.path.join(self.data_dir, "wal"))

            # Log all current data to the new WAL
            for key, value in self.data.items():
                self.wal.append(OperationType.PUT, key, value)

    def close(self) -> None:
        """
        Close the store and ensure all data is persisted.
        """
        with self.lock:
            self.wal.close()
