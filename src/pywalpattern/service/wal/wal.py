import os
from pathlib import Path
from typing import Any

from pywalpattern.domain.models import CompressionConfig, CompressionType, OperationType
from pywalpattern.service.wal.log_entry import CompressedLogEntry, LogEntry


class WAL:
    """
    Write-Ahead Log (WAL) class for ensuring durability of operations in a key-value store.

    Attributes:
        log_dir (str): The directory where log files are stored.
        current_file (file): The current log file being written to.
        seq_num (int): The current sequence number for log entries.
        segment_size (int): The size threshold for rotating the log file.
    """

    def __init__(self, log_dir: str, compression_config: CompressionConfig | None = None, segment_size: int = 10 * 1024 * 1024):
        """
        Initializes the WAL instance, creating the log directory if it doesn't exist,
        and opening the current log file for appending.

        Args:
            log_dir (str): The directory where log files are stored.
            segment_size (int): The size threshold for rotating the log file (default is 10MB).
        """
        self.log_dir = log_dir
        self.current_file = None
        self.seq_num = 0
        self.segment_size = segment_size
        self.compression_config = compression_config or CompressionConfig(CompressionType.ZLIB)
        Path(log_dir).mkdir(exist_ok=True, parents=True)
        self._init_from_disk()
        self._open_current_file()

    def _init_from_disk(self):
        """
        Initializes the WAL from existing log files on disk, setting the sequence number
        to the last used sequence number.
        """
        log_files = sorted([f for f in os.listdir(self.log_dir) if f.endswith(".log")])
        if log_files:
            last_log = log_files[-1]
            # Extract sequence number from filename
            try:
                self.seq_num = int(last_log.split(".")[0])
            except ValueError:
                self.seq_num = 0

    def _open_current_file(self):
        """
        Opens the current log file for appending, creating a new file if necessary.
        """
        filename = os.path.join(self.log_dir, f"{self.seq_num}.log")
        self.current_file = open(filename, "ab+")

    def append(self, op_type: OperationType, key: str, value: Any = None) -> int:
        """
        Appends an entry to the log and returns the sequence number.

        Args:
            op_type (OperationType): The type of operation (e.g., PUT, DELETE).
            key (str): The key associated with the operation.
            value (Any, optional): The value associated with the operation (default is None).

        Returns:
            int: The sequence number of the appended log entry.
        """
        self.seq_num += 1
        entry = CompressedLogEntry(self.seq_num, op_type, key, value, compression_config=self.compression_config)
        serialized = entry.serialize()

        # Write entry length followed by the entry
        length = len(serialized)
        self.current_file.write(length.to_bytes(4, byteorder="big"))
        self.current_file.write(serialized)
        self.current_file.flush()
        os.fsync(self.current_file.fileno())  # Force write to disk

        # Rotate log if it exceeds the segment size
        if self.current_file.tell() > self.segment_size:
            self._rotate_log()

        return self.seq_num

    def _rotate_log(self):
        """
        Rotates the log file by closing the current file and opening a new one.
        """
        self.current_file.close()
        self.seq_num += 1
        self._open_current_file()

    def read_all_entries(self) -> list[LogEntry]:  # noqa: CCR001
        """
        Reads all entries from all log files.

        Returns:
            list[LogEntry]: A list of all log entries.
        """
        entries = []
        log_files = sorted([os.path.join(self.log_dir, f) for f in os.listdir(self.log_dir) if f.endswith(".log")])

        for log_file in log_files:
            with open(log_file, "rb") as f:
                while True:
                    length_bytes = f.read(4)
                    if not length_bytes:
                        break  # End of file

                    length = int.from_bytes(length_bytes, byteorder="big")
                    entry_data = f.read(length)
                    if len(entry_data) != length:
                        break  # Corrupted or incomplete entry

                    entry = CompressedLogEntry.deserialize(entry_data)
                    entries.append(entry)

        return entries

    def _is_snapshot_fresh(self, snapshot_seq_num: int) -> bool:
        """
        Check if the snapshot is fresh by comparing the sequence number in the snapshot
        with the current WAL sequence number.
        """
        return snapshot_seq_num == self.seq_num

    def delete_old_segments(self, low_water_mark: int, snapshot_seq_num: int):
        """
        Delete log segments with sequence numbers lower than the low-water mark,
        ensuring that the snapshot is fresh before performing the cleanup.

        Args:
            low_water_mark (int): The sequence number before which log segments can be safely deleted.
            snapshot_seq_num (int): The sequence number in the snapshot to verify freshness.
        """
        if not self._is_snapshot_fresh(snapshot_seq_num):
            print("Snapshot is not fresh. Skipping log cleanup.")
            return

        log_files = sorted([f for f in os.listdir(self.log_dir) if f.endswith(".log")])
        for log_file in log_files:
            seq_num = int(log_file.split(".")[0])
            if seq_num < low_water_mark:
                os.remove(os.path.join(self.log_dir, log_file))

    def close(self):
        """
        Closes the current log file.
        """
        if self.current_file:
            self.current_file.close()
