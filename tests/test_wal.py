import os

from pywalpattern.domain.models import OperationType


def test_write_and_read_single_entry(wal):
    wal.append(OperationType.PUT, "key1", "value1")
    entries = wal.read_all_entries()
    print(f"Entries: {entries}")
    assert len(entries) == 1
    assert entries[0].key == "key1"
    assert entries[0].value == "value1"


def test_write_and_read_multiple_entries(wal):
    wal.append(OperationType.PUT, "key1", "value1")
    wal.append(OperationType.PUT, "key2", "value2")
    entries = wal.read_all_entries()
    print(f"Entries: {entries}")
    assert len(entries) == 2
    assert entries[0].key == "key1"
    assert entries[0].value == "value1"
    assert entries[1].key == "key2"
    assert entries[1].value == "value2"


def test_log_segmentation(wal):
    # Assuming the WAL implementation segments logs after a certain size
    for i in range(10):
        wal.append(OperationType.PUT, f"key{i}", f"value{i}")

    log_files = [f for f in os.listdir(wal.log_dir) if f.endswith(".log")]
    print(f"Log files: {log_files}")
    assert len(log_files) > 1

    entries = wal.read_all_entries()

    print(f"Entries: {entries}")
    assert len(entries) == 10
    for i in range(10):
        print(f"Entry {i}: key={entries[i].key}, value={entries[i].value}")
