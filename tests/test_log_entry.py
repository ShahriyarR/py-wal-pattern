import pytest

from pywalpattern.domain.models import OperationType
from pywalpattern.service.wal.log_entry import LogEntry


def test_crc_check():
    # Create a log entry
    entry = LogEntry(seq_num=1, op_type=OperationType.PUT, key="test_key", value="test_value")

    # Serialize the log entry
    serialized_entry = entry.serialize()

    # Deserialize the log entry
    deserialized_entry = LogEntry.deserialize(serialized_entry)

    # Check if the checksum matches
    assert deserialized_entry.checksum == entry.checksum


def test_crc_check_fail():
    # Create a log entry
    entry = LogEntry(seq_num=1, op_type=OperationType.PUT, key="test_key", value="test_value")

    # Serialize the log entry
    serialized_entry = entry.serialize()

    # Tamper with the serialized data to simulate corruption
    tampered_data = serialized_entry.replace(b"test_value", b"tampered_value")

    # Attempt to deserialize the tampered data and expect a ValueError
    with pytest.raises(ValueError, match="Checksum verification failed"):
        LogEntry.deserialize(tampered_data)
