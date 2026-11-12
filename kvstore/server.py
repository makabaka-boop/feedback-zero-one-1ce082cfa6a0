"""HTTP 接口层。

路由::

    POST /load        装载事务（严格 JSON；未知/重复成员、非法结构一律 400）
    GET  /status      状态查询：版本 + 行（key 字节序）+ 断点（源名排序）
    GET  /receipts    全部成功回执（按版本顺序，检查点后仍保留）
    POST /checkpoint  触发检查点并回收此前日志

并发模型：``ThreadingHTTPServer`` 多线程接收，引擎内部用一把锁串行化所有
写操作、为读操作提供一致快照，因此整体串行等价，读不见半个事务。
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .engine import ConflictError, Engine, ValidationError
from .framing import CorruptRecordError

MAX_BODY_BYTES = 64 * 1024 * 1024  # 64 MiB


class _Handler(BaseHTTPRequestHandler):
    server_version = "KVCheckpoint/1.0"

    # BaseHTTPRequestHandler 把 server 属性绑定为实际 server 实例。
    server: "_Server"

    # 静默默认访问日志，避免污染验收输出；错误仍由 send_error 体现。
    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        return

    def _send_json(self, status: int, body: dict[str, Any]) -> None:
        data = json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_body(self) -> bytes:
        length_raw = self.headers.get("Content-Length")
        if length_raw is None:
            raise ValidationError("Content-Length header is required")
        try:
            length = int(length_raw)
        except ValueError as exc:
            raise ValidationError("invalid Content-Length") from exc
        if length < 0 or length > MAX_BODY_BYTES:
            raise ValidationError("request body empty or too large")
        body = self.rfile.read(length)
        if len(body) != length:
            raise ValidationError("truncated request body")
        return body

    def do_GET(self) -> None:  # noqa: N802 (stdlib 命名)
        if self.path == "/status":
            self._send_json(200, self.server.engine.status())
        elif self.path == "/receipts":
            self._send_json(200, self.server.engine.list_receipts())
        else:
            self._send_json(404, {"error": "not found", "path": self.path})

    def do_POST(self) -> None:  # noqa: N802
        try:
            if self.path == "/load":
                body = self._read_body()
                self._send_json(200, self.server.engine.submit(body))
            elif self.path == "/checkpoint":
                self._send_json(200, self.server.engine.checkpoint())
            else:
                self._send_json(404, {"error": "not found", "path": self.path})
        except ValidationError as exc:
            self._send_json(400, {"error": str(exc)})
        except ConflictError as exc:
            self._send_json(409, {"error": str(exc)})
        except CorruptRecordError as exc:  # 检查点期间发现快照损坏等
            self._send_json(500, {"error": f"corrupt storage: {exc}"})

    def do_PUT(self) -> None:  # noqa: N802
        self._send_json(405, {"error": "method not allowed"})

    do_DELETE = do_PUT  # 仅支持 GET/POST


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr: tuple[str, int], engine: Engine) -> None:
        super().__init__(addr, _Handler)
        self.engine = engine
        # 双保险：引擎自带锁，这里再保留一个服务器级生命周期锁。
        self.shutdown_lock = threading.Lock()


def build_server(host: str, port: int, data_dir: str) -> tuple[_Server, Engine]:
    engine = Engine(data_dir)
    server = _Server((host, port), engine)
    return server, engine


def serve(host: str, port: int, data_dir: str) -> None:
    server, engine = build_server(host, port, data_dir)
    try:
        actual_host, actual_port = server.server_address[:2]
        print(f"kvstore listening on http://{actual_host}:{actual_port} data_dir={data_dir}", flush=True)
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
        engine.close()
