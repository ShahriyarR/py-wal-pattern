import zlib

from pywalpattern.domain.models import CompressionConfig, CompressionType


class CompressionManager:
    def __init__(self, config: CompressionConfig):
        self.config = config

    def compress(self, data: bytes) -> tuple[bytes, CompressionType]:
        if self.config.type == CompressionType.ZLIB:
            return zlib.compress(data, level=self.config.level), CompressionType.ZLIB
        return data, CompressionType.NONE

    def decompress(self, data: bytes, compression_type: CompressionType) -> bytes:
        if compression_type == CompressionType.ZLIB:
            return zlib.decompress(data)
        return data
