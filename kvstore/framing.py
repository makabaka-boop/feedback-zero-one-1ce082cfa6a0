"""磁盘记录的分帧编解码。

每条记录的物理布局（小端字节序）::

    +-------------------+----------------------+-------------------+
    | uint32 json_bytes |  UTF-8 JSON payload  | uint32 crc32(json)|
    +-------------------+----------------------+-------------------+

长度前缀与 CRC 都是 4 字节无符号小端整数；CRC 仅覆盖 JSON 载荷，
取 Python 3.11 的 ``zlib.crc32``（IEEE CRC-32，结果与 0xffffffff 掩码）。
"""

from __future__ import annotations

import struct
import zlib
from typing import Iterator

_UINT32 = struct.Struct("<I")
FRAME_HEADER = _UINT32.size  # 4
FRAME_TRAILER = _UINT32.size  # 4


class CorruptRecordError(Exception):
    """完整记录的 CRC 校验失败（或载荷无法解析为 JSON）。"""


class TruncatedRecord(Exception):
    """文件尾部存在一条不完整记录（半截长度头 / 半截载荷 / 半截 CRC）。"""


def encode_frame(payload: bytes) -> bytes:
    """把一段 JSON 字节编码为一条完整帧。"""
    if not isinstance(payload, (bytes, bytearray)):
        raise TypeError("payload must be bytes")
    if len(payload) == 0:
        raise ValueError("payload must not be empty")
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    return _UINT32.pack(len(payload)) + bytes(payload) + _UINT32.pack(crc)


def read_all_frames(path: str) -> tuple[list[bytes], bool]:
    """读完全部帧，返回 ``(payload 列表, 末尾是否存在半截记录)``。

    完整记录 CRC 失败时抛 :class:`CorruptRecordError`；尾部不完整不抛错，
    而是通过第二个返回值告知调用方，便于恢复时忽略半截数据。
    """
    frames: list[bytes] = []
    truncated = False
    with open(path, "rb") as fh:
        while True:
            header = fh.read(FRAME_HEADER)
            if not header:
                return frames, truncated
            if len(header) < FRAME_HEADER:
                return frames, True
            (length,) = _UINT32.unpack(header)
            if length == 0:
                raise CorruptRecordError(f"{path}: zero-length record")
            payload = fh.read(length)
            if len(payload) < length:
                return frames, True
            trailer = fh.read(FRAME_TRAILER)
            if len(trailer) < FRAME_TRAILER:
                return frames, True
            (stored_crc,) = _UINT32.unpack(trailer)
            actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
            if stored_crc != actual_crc:
                raise CorruptRecordError(
                    f"{path}: crc mismatch (stored={stored_crc:#x}, "
                    f"actual={actual_crc:#x})"
                )
            frames.append(payload)


def iter_frames(path: str) -> Iterator[bytes]:
    """顺序读取帧。

    - 末尾不完整记录：抛出 :class:`TruncatedRecord`（恢复时忽略此前已读出的帧）。
    - 完整记录 CRC 错误或载荷不是合法 JSON：抛出 :class:`CorruptRecordError`，
      恢复流程据此拒绝启动并保留现场。
    - 长度前缀声称 0 字节也视为损坏（本服务从不写入空载荷）。
    """
    with open(path, "rb") as fh:
        while True:
            header = fh.read(FRAME_HEADER)
            if not header:
                return  # 干净的文件结尾
            if len(header) < FRAME_HEADER:
                raise TruncatedRecord(f"{path}: truncated length header")
            (length,) = _UINT32.unpack(header)
            if length == 0:
                raise CorruptRecordError(f"{path}: zero-length record")
            payload = fh.read(length)
            if len(payload) < length:
                raise TruncatedRecord(f"{path}: truncated payload")
            trailer = fh.read(FRAME_TRAILER)
            if len(trailer) < FRAME_TRAILER:
                raise TruncatedRecord(f"{path}: truncated checksum")
            (stored_crc,) = _UINT32.unpack(trailer)
            actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
            if stored_crc != actual_crc:
                raise CorruptRecordError(
                    f"{path}: crc mismatch (stored={stored_crc:#x}, "
                    f"actual={actual_crc:#x})"
                )
            yield payload
