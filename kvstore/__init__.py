"""纯后端 KV 检查点服务（仅依赖 Python 3.11 标准库）。"""

from .engine import (
    ConflictError,
    Engine,
    ValidationError,
    parse_strict_json,
)
from .framing import CorruptRecordError

__all__ = [
    "Engine",
    "ValidationError",
    "ConflictError",
    "CorruptRecordError",
    "parse_strict_json",
]
