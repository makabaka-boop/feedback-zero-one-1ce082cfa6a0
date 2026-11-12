#!/usr/bin/env python3
"""服务启动入口。

用法::

    python3 runserver.py --data-dir ./data --host 127.0.0.1 --port 8000

端口传 0 时由操作系统分配空闲端口，实际端口打印到标准输出首行，
便于验收脚本解析。若存储损坏（完整记录 CRC 失败等），进程拒绝启动并
以非零状态退出，磁盘现场原样保留。
"""

from __future__ import annotations

import argparse
import os
import sys

# -I（隔离模式，含 -P）不会把脚本目录加入 sys.path，这里显式补上本项目根，
# 保证 `python3 -I runserver.py` 也能导入 kvstore 包。
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kvstore.framing import CorruptRecordError
from kvstore.server import serve


def main() -> int:
    parser = argparse.ArgumentParser(description="KV checkpoint HTTP service")
    parser.add_argument("--data-dir", required=True, help="数据文件与追加日志所在目录")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    try:
        serve(args.host, args.port, args.data_dir)
    except CorruptRecordError as exc:
        print(f"FATAL: storage corrupt, refusing to start: {exc}", file=sys.stderr)
        return 2
    except OSError as exc:
        print(f"FATAL: storage I/O error: {exc}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
