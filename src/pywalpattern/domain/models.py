from dataclasses import dataclass
from enum import Enum


class OperationType(Enum):
    PUT = 1
    DELETE = 2

    def to_dict(self):
        return self.value


class CompressionType(Enum):
    NONE = 0
    ZLIB = 1


@dataclass
class CompressionConfig:
    type: CompressionType
    level: int = 6  # Default zlib compression level


class Command:
    GET = "GET"
    PUT = "PUT"
    DELETE = "DELETE"
    KEYS = "KEYS"
    CHECKPOINT = "CHECKPOINT"
    QUIT = "QUIT"


class Response:
    OK = "OK"
    ERROR = "ERROR"
    RESULT = "RESULT"
