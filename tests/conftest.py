import os
import shutil
import time

import pytest

from pywalpattern.domain.models import CompressionConfig, CompressionType
from pywalpattern.service.wal.wal import WAL


@pytest.fixture(scope="function")
def wal():
    log_dir = "/tmp/test_wal"

    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)
        os.makedirs(log_dir)

    compression_config = CompressionConfig(type=CompressionType.ZLIB, level=6)
    wal_instance = WAL(log_dir, compression_config=compression_config, segment_size=10)

    yield wal_instance
    # Clean up after the test has finished
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)
