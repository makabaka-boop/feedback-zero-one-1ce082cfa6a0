"""帧分帧层的单元测试：长度前缀小端、CRC32、截断与损坏的区分。"""

from __future__ import annotations

import os
import struct
import sys
import tempfile
import zlib
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kvstore.framing import (
    CorruptRecordError,
    TruncatedRecord,
    encode_frame,
    iter_frames,
    read_all_frames,
)


def main() -> int:
    failures = 0

    def check(name: str, cond: bool) -> None:
        nonlocal failures
        print(("  PASS  " if cond else "  FAIL  ") + name)
        if not cond:
            failures += 1

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "log"

        payloads = [b'{"a":1}', "中文 payload".encode(), b"\x00\x01\x02binary"]
        blob = b"".join(encode_frame(p) for p in payloads)

        # 物理布局核对：4 字节小端长度 + JSON + 4 字节小端 CRC，CRC 仅覆盖 JSON。
        first_len = struct.unpack("<I", blob[:4])[0]
        check("little-endian length prefix", first_len == len(payloads[0]))
        crc_at = 4 + first_len
        check(
            "crc32 covers json only",
            struct.unpack("<I", blob[crc_at : crc_at + 4])[0]
            == (zlib.crc32(payloads[0]) & 0xFFFFFFFF),
        )

        path.write_bytes(blob)
        frames, truncated = read_all_frames(str(path))
        check("roundtrip all frames", frames == payloads and truncated is False)
        check("iterator API matches", list(iter_frames(str(path))) == payloads)

        # 半截长度头 / 半截载荷 / 半截 CRC 都属于“尾部不完整”。
        frame_sizes = [len(encode_frame(p)) for p in payloads]
        # 第一帧载荷的中间位置（4 字节长度头之后、载荷未读完）。
        mid_first_payload = 4 + max(1, len(payloads[0]) // 2)
        cases = [
            (2, "truncated length header", []),
            (mid_first_payload, "truncated payload", []),
            # 第一帧完整，第二帧只写了 2 字节长度头。
            (frame_sizes[0] + 2, "truncated next-frame header", payloads[:1]),
            (len(blob) - 1, "truncated crc trailer", payloads[:-1]),
        ]
        for cut, label, expected_frames in cases:
            path.write_bytes(blob[:cut])
            frames, truncated = read_all_frames(str(path))
            check(label, truncated is True and frames == expected_frames)
            try:
                list(iter_frames(str(path)))
                check(label + " raises TruncatedRecord", False)
            except TruncatedRecord:
                check(label + " raises TruncatedRecord", True)

        # 完整记录 CRC 被破坏：必须报 CorruptRecordError，而不是当截断吞掉。
        damaged = bytearray(blob)
        damaged[4] ^= 0xFF  # 翻转第一条记录载荷首字节
        path.write_bytes(damaged)
        try:
            read_all_frames(str(path))
            check("crc corruption detected", False)
        except CorruptRecordError:
            check("crc corruption detected", True)

        # 零长度记录视为损坏。
        path.write_bytes(struct.pack("<I", 0))
        try:
            read_all_frames(str(path))
            check("zero-length record rejected", False)
        except CorruptRecordError:
            check("zero-length record rejected", True)

        # 空文件：零帧、无截断。
        path.write_bytes(b"")
        frames, truncated = read_all_frames(str(path))
        check("empty file yields zero frames", frames == [] and truncated is False)

    print(f"\nframing: {'all passed' if failures == 0 else f'{failures} failed'}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
