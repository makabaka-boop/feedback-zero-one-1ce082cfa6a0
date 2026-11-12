#!/usr/bin/env python3
"""端到端验收脚本（仅标准库；用 python3 -I acceptance_test.py 运行）。

覆盖：
  A. 功能与严格校验（未知/重复成员、布尔整数、空 ops、del 不存在、409 幂等冲突）
  B. 正常重启后回执保留、状态一致
  C. 追加处强退：半帧 -> 尾部截断忽略；全帧 fsync 后崩溃 -> 重启生效、重试幂等
  D. 检查点发布三个点强退（tmp 后 / rename 后 / 日志切换后）均可恢复
  E. 尾截断 vs 完整记录 CRC 损坏：前者恢复、后者拒绝启动且保留现场
  F. 快照完整记录损坏：拒绝启动
  G. 并发提交与检查点串行等价、读不见半个事务
"""

from __future__ import annotations

import http.client
import json
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import threading
import time
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RUNSERVER = ROOT / "runserver.py"

# 与 engine.py 中的故障注入环境变量保持一致。
F_APPEND_WRITE = "KV_FAULT_APPEND_AFTER_WRITE"
F_APPEND_FSYNC = "KV_FAULT_APPEND_AFTER_FSYNC"
F_CKPT_TMP = "KV_FAULT_CKPT_AFTER_TMP"
F_CKPT_RENAME = "KV_FAULT_CKPT_AFTER_RENAME"
F_CKPT_SWITCH = "KV_FAULT_CKPT_AFTER_SWITCH"


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------


class Server:
    def __init__(self, data_dir: Path, env: dict[str, str] | None = None) -> None:
        self.data_dir = data_dir
        # 传端口 0 让内核分配空闲端口，避免与刚关闭的旧服务抢端口；
        # 真实端口从子进程首行标准输出解析。
        full_env = dict(os.environ)
        # 子进程不继承父进程残留的故障开关。
        for key in (F_APPEND_WRITE, F_APPEND_FSYNC, F_CKPT_TMP, F_CKPT_RENAME, F_CKPT_SWITCH):
            full_env.pop(key, None)
        if env:
            full_env.update(env)
        self.proc = subprocess.Popen(
            [sys.executable, "-I", str(RUNSERVER), "--data-dir", str(data_dir),
             "--host", "127.0.0.1", "--port", "0"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=full_env,
            close_fds=True,
        )
        self.port = self._read_listening_port()

    def _read_listening_port(self) -> int:
        assert self.proc.stdout is not None
        line = self.proc.stdout.readline().decode("utf-8")
        if not line:
            out, err = self.proc.communicate(timeout=5)
            raise RuntimeError(
                f"server produced no startup line; code={self.proc.returncode} "
                f"stderr={err.decode()!r}"
            )
        # 形如：kvstore listening on http://127.0.0.1:54321 data_dir=...
        marker = "http://127.0.0.1:"
        idx = line.find(marker)
        if idx < 0:
            raise RuntimeError(f"unexpected startup line: {line!r}")
        tail = line[idx + len(marker):]
        return int(tail.split(" ", 1)[0])

    def wait_ready(self, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        last_err: Exception | None = None
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                out, err = self.proc.communicate(timeout=5)
                raise RuntimeError(
                    f"server exited early code={self.proc.returncode}\n"
                    f"stdout={out.decode()!r}\nstderr={err.decode()!r}"
                )
            try:
                status, _body, _ = self.request("GET", "/status")
                # 就绪判定只看接口可用；重启的服务版本可能非零，不能要求 version==0。
                if status == 200:
                    return
            except OSError as exc:
                last_err = exc
            time.sleep(0.05)
        raise RuntimeError(f"server not ready: {last_err}")

    def request(self, method: str, path: str, body: dict | None = None,
                raw: bytes | None = None) -> tuple[int, dict, bytes]:
        if raw is not None:
            data = raw
        elif body is not None:
            data = json.dumps(body, allow_nan=False).encode("utf-8")
        else:
            data = b""
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            conn.request(method, path, body=data,
                         headers={"Content-Length": str(len(data))})
            try:
                resp = conn.getresponse()
                payload = resp.read()
                status = resp.status
            except http.client.RemoteDisconnected:
                # 故障注入：服务在请求处理中 os._exit，没有 HTTP 响应。
                return -1, {}, b""
        finally:
            conn.close()
        parsed: dict = {}
        if payload:
            parsed = json.loads(payload.decode("utf-8"))
        return status, parsed, payload

    def stop(self) -> tuple[bytes, bytes]:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                return self.proc.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                return self.proc.communicate(timeout=10)
        return self.proc.communicate(timeout=5)

    def expect_dead(self, timeout: float = 10.0) -> int:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            code = self.proc.poll()
            if code is not None:
                return code
            time.sleep(0.05)
        self.proc.kill()
        raise RuntimeError("fault-injected server did not exit")


def expect_refused_start(data_dir: Path, env: dict[str, str] | None = None) -> int:
    """断言服务拒绝启动，返回退出码；数据现场保持不动。"""
    full_env = dict(os.environ)
    for key in (F_APPEND_WRITE, F_APPEND_FSYNC, F_CKPT_TMP, F_CKPT_RENAME, F_CKPT_SWITCH):
        full_env.pop(key, None)
    if env:
        full_env.update(env)
    proc = subprocess.run(
        [sys.executable, "-I", str(RUNSERVER), "--data-dir", str(data_dir),
         "--host", "127.0.0.1", "--port", "0"],
        capture_output=True, env=full_env, timeout=15,
    )
    assert proc.returncode != 0, "corrupt storage must refuse to start"
    assert b"corrupt" in proc.stderr.lower() or b"fatal" in proc.stderr.lower(), proc.stderr
    return proc.returncode


PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} {detail}")


def txn(txid: str, sources: dict, ops: list) -> dict:
    return {"txid": txid, "sources": sources, "ops": ops}


# ---------------------------------------------------------------------------
# A. 功能与严格校验
# ---------------------------------------------------------------------------


def test_functional(work: Path) -> None:
    print("[A] 功能与严格校验")
    d = work / "a"
    srv = Server(d)
    srv.wait_ready()

    # 初始空表、断点为零、版本为零。
    st, body, _ = srv.request("GET", "/status")
    check("initial version=0", body["version"] == 0)
    check("initial rows empty", body["rows"] == {})
    check("initial checkpoints empty", body["checkpoints"] == {})

    # 空 ops 合法；put 字符串与 null；同一事务推进两个源。
    t1 = txn("t1", {"alpha": [0, 5], "beta": [0, 2]}, [
        {"op": "put", "key": "k1", "value": "v1"},
        {"op": "put", "key": "k2", "value": None},
    ])
    code, body, _ = srv.request("POST", "/load", t1)
    check("t1 accepted version=1", code == 200 and body["version"] == 1 and body["idempotent"] is False)

    # put 替换。
    code, body, _ = srv.request("POST", "/load", txn(
        "t2", {"alpha": [5, 5]}, [{"op": "put", "key": "k1", "value": "v2"}]))
    check("t2 no-op interval accepted", code == 200 and body["version"] == 2)

    # del 存在的 key；再 del 必须拒绝。
    code, _, _ = srv.request("POST", "/load", txn(
        "t3", {"beta": [2, 9]}, [{"op": "del", "key": "k2"}]))
    check("t3 del existing key", code == 200)
    code, body, _ = srv.request("POST", "/load", txn(
        "t4", {"beta": [9, 10]}, [{"op": "del", "key": "k2"}]))
    check("del missing key rejected 400", code == 400)

    # 整批拒绝：断点不匹配，且 del 非法时前面的 put 也不得生效。
    code, body, _ = srv.request("POST", "/load", txn(
        "t5", {"alpha": [99, 100]}, [{"op": "put", "key": "ghost", "value": "x"}]))
    check("checkpoint mismatch rejected", code == 400)
    code, body, _ = srv.request("POST", "/load", txn(
        "t6", {"alpha": [5, 6]}, [
            {"op": "put", "key": "tmp", "value": "x"},
            {"op": "del", "key": "nope"},
        ]))
    check("batch rejected atomically", code == 400 and "tmp" not in body)
    st, status, _ = srv.request("GET", "/status")
    check("rejected txn left no rows", "ghost" not in status["rows"] and "tmp" not in status["rows"])
    check("rejected txn left version unchanged", status["version"] == 3)

    # 幂等：同内容（不同 JSON 空白/键序）返回原版本；异内容 409，且不复查断点。
    code, body, _ = srv.request("POST", "/load", raw=json.dumps({
        "ops": [{"key": "k1", "value": "v1", "op": "put"},
                {"op": "put", "key": "k2", "value": None}],
        "sources": {"beta": [0, 2], "alpha": [0, 5]},
        "txid": "t1",
    }).encode())
    check("idempotent retry returns original version", code == 200 and
          body["version"] == 1 and body["idempotent"] is True)
    code, body, _ = srv.request("POST", "/load", txn(
        "t1", {"alpha": [0, 999]}, []))
    check("same txid different payload -> 409", code == 409)

    # 排序：行按 key 字节序、断点按源名。
    srv.request("POST", "/load", txn("t7", {"alpha": [5, 7]}, [
        {"op": "put", "key": "z", "value": "1"},
        {"op": "put", "key": "a", "value": "1"},
        {"op": "put", "key": "m", "value": "1"},
    ]))
    st, status, _ = srv.request("GET", "/status")
    check("rows sorted by byte order", list(status["rows"]) == ["a", "k1", "m", "z"])
    check("checkpoints sorted by source name", list(status["checkpoints"]) == ["alpha", "beta"])

    # 严格 JSON / 结构校验矩阵。
    def reject(name: str, raw: bytes | dict, expect_code: int = 400) -> None:
        if isinstance(raw, dict):
            code, _, _ = srv.request("POST", "/load", raw)
        else:
            code, _, _ = srv.request("POST", "/load", raw=raw)
        check(name, code == expect_code)

    reject("unknown top-level member", {**txn("x1", {"s": [0, 1]}, []), "extra": 1})
    reject("duplicate JSON member", b'{"txid":"x","txid":"y","sources":{"s":[0,1]},"ops":[]}')
    reject("boolean as integer", txn("x2", {"s": [True, 1]}, []))
    reject("boolean as endpoint end", txn("x3", {"s": [0, False]}, []))
    reject("float endpoint", txn("x4", {"s": [0, 1.0]}, []))
    reject("negative endpoint", txn("x5", {"s": [-1, 1]}, []))
    reject("end < start", txn("x6", {"s": [5, 4]}, []))
    reject("int64 overflow", txn("x7", {"s": [0, 9223372036854775808]}, []))
    reject("empty txid", txn("", {"s": [0, 1]}, []))
    reject("empty sources", txn("x8", {}, []))
    reject("bad source name", txn("x9", {"s-1": [0, 1]}, []))
    reject("non-ascii key", txn("x10", {"s": [0, 1]}, [{"op": "put", "key": "键", "value": "v"}]))
    reject("unknown op", txn("x11", {"s": [0, 1]}, [{"op": "incr", "key": "k"}]))
    reject("del carries value", txn("x12", {"s": [0, 1]}, [{"op": "del", "key": "k1", "value": None}]))
    reject("put missing value", txn("x13", {"s": [0, 1]}, [{"op": "put", "key": "k"}]))
    reject("value not string/null", txn("x14", {"s": [0, 1]}, [{"op": "put", "key": "k", "value": 7}]))
    reject("NaN rejected", b'{"txid":"x15","sources":{"s":[0,1]},"ops":[{"op":"put","key":"k","value":NaN}]}')
    reject("top-level array", b"[1,2,3]")
    reject("not json", b"not-json-at-all")

    code, body, _ = srv.request("GET", "/receipts")
    check("receipts listed in version order",
          [r["txid"] for r in body["receipts"]] == ["t1", "t2", "t3", "t7"])
    srv.stop()


# ---------------------------------------------------------------------------
# B. 正常重启
# ---------------------------------------------------------------------------


def test_clean_restart(work: Path) -> None:
    print("[B] 正常重启：回执保留、状态一致")
    d = work / "b"
    srv = Server(d)
    srv.wait_ready()
    srv.request("POST", "/load", txn("r1", {"src": [0, 10]}, [
        {"op": "put", "key": "hello", "value": "world"},
    ]))
    srv.request("POST", "/checkpoint")
    srv.request("POST", "/load", txn("r2", {"src": [10, 20]}, [
        {"op": "put", "key": "hello", "value": None},
    ]))
    before = srv.request("GET", "/status")[1]
    receipts_before = srv.request("GET", "/receipts")[1]
    srv.stop()

    srv2 = Server(d)
    srv2.wait_ready()
    after = srv2.request("GET", "/status")[1]
    receipts_after = srv2.request("GET", "/receipts")[1]
    check("status identical after restart", before == after)
    check("receipts survive restart + checkpoint", receipts_before == receipts_after)

    # 断点继续可用：新事务从 20 推进。
    code, body, _ = srv2.request("POST", "/load", txn("r3", {"src": [20, 25]}, []))
    check("checkpoint continues after restart", code == 200 and body["version"] == 3)
    # 旧 txid 仍幂等。
    code, body, _ = srv2.request("POST", "/load", txn("r1", {"src": [0, 10]}, [
        {"op": "put", "key": "hello", "value": "world"},
    ]))
    check("old txid idempotent after restart", code == 200 and body["version"] == 1)
    srv2.stop()


# ---------------------------------------------------------------------------
# C. 追加处强退
# ---------------------------------------------------------------------------


def test_append_crash(work: Path) -> None:
    print("[C] 追加处强退：半帧截断 / 已 fsync 全帧生效")
    d = work / "c"
    srv = Server(d)
    srv.wait_ready()
    srv.request("POST", "/load", txn("c0", {"s": [0, 1]}, [
        {"op": "put", "key": "keep", "value": "1"}]))
    srv.stop()

    # C1: 写半帧后强退 -> 末尾不完整记录，重启忽略，旧数据完好，txid 未占用。
    crashed = Server(d, env={F_APPEND_WRITE: "1"})
    crashed.wait_ready()
    code, _, _ = crashed.request("POST", "/load", txn("c1", {"s": [1, 2]}, [
        {"op": "put", "key": "half", "value": "1"}]))
    assert code in (200, -1), "server should process then crash"
    crashed.expect_dead()

    log_size_after_half = (d / "log").stat().st_size
    srv2 = Server(d)
    srv2.wait_ready()
    status = srv2.request("GET", "/status")[1]
    check("half-frame transaction not applied", status["version"] == 1 and "half" not in status["rows"])
    check("half-frame tail truncated on recovery", (d / "log").stat().st_size < log_size_after_half)
    # 同 txid 可作为新事务提交（未占用）。
    code, body, _ = srv2.request("POST", "/load", txn("c1", {"s": [1, 2]}, [
        {"op": "put", "key": "half", "value": "recovered"}]))
    check("un-acked txid can commit after recovery", code == 200 and body["version"] == 2)
    srv2.stop()

    # C2: fsync 后、回执前强退 -> 事务完整生效，重试按幂等返回原版本。
    crashed2 = Server(d, env={F_APPEND_FSYNC: "1"})
    crashed2.wait_ready()
    code, _, _ = crashed2.request("POST", "/load", txn("c2", {"s": [2, 3]}, [
        {"op": "put", "key": "dur", "value": "1"}]))
    assert code in (200, -1)
    crashed2.expect_dead()

    srv3 = Server(d)
    srv3.wait_ready()
    status = srv3.request("GET", "/status")[1]
    check("fsynced transaction fully applied after crash",
          status["version"] == 3 and status["rows"].get("dur") == "1"
          and status["checkpoints"]["s"] == 3)
    code, body, _ = srv3.request("POST", "/load", txn("c2", {"s": [2, 3]}, [
        {"op": "put", "key": "dur", "value": "1"}]))
    check("retry of durable txn is idempotent", code == 200 and
          body["version"] == 3 and body["idempotent"] is True)
    receipts = srv3.request("GET", "/receipts")[1]
    check("receipt present after crash recovery",
          {r["txid"] for r in receipts["receipts"]} == {"c0", "c1", "c2"})
    srv3.stop()


# ---------------------------------------------------------------------------
# D. 检查点三个强退点
# ---------------------------------------------------------------------------


def test_checkpoint_crash(work: Path) -> None:
    print("[D] 检查点发布 / 日志切换处强退")
    for fault, label in (
        (F_CKPT_TMP, "tmp-snapshot written"),
        (F_CKPT_RENAME, "snapshot published, pre switch"),
        (F_CKPT_SWITCH, "log switched"),
    ):
        d = work / f"d-{fault}"
        srv = Server(d)
        srv.wait_ready()
        for i in range(1, 4):
            srv.request("POST", "/load", txn(f"d{i}", {"src": [(i - 1), i]}, [
                {"op": "put", "key": f"k{i}", "value": f"v{i}"}]))
        expected = srv.request("GET", "/status")[1]
        expected_receipts = srv.request("GET", "/receipts")[1]

        crashed = Server(d, env={fault: "1"})
        crashed.wait_ready()
        code, _, _ = crashed.request("POST", "/checkpoint")
        assert code in (200, -1), f"checkpoint should run then crash, got {code}"
        crashed.expect_dead()

        srv2 = Server(d)
        srv2.wait_ready()
        actual = srv2.request("GET", "/status")[1]
        actual_receipts = srv2.request("GET", "/receipts")[1]
        check(f"[{label}] state preserved", actual == expected)
        check(f"[{label}] all receipts preserved", actual_receipts == expected_receipts)

        # 恢复后检查点可正常完成，且日志被回收。
        code, body, _ = srv2.request("POST", "/checkpoint")
        check(f"[{label}] checkpoint completes after recovery", code == 200)
        check(f"[{label}] log reclaimed", (d / "log").stat().st_size == 0)
        check(f"[{label}] snapshot retained", (d / "snapshot").stat().st_size > 0)
        check(f"[{label}] no tmp leftovers",
              not (d / "snapshot.tmp").exists() and not (d / "log.tmp").exists())

        # 回收后仍可继续提交，版本接续。
        code, body, _ = srv2.request("POST", "/load", txn("d4", {"src": [3, 4]}, [
            {"op": "put", "key": "k4", "value": "v4"}]))
        check(f"[{label}] commits continue version 4", code == 200 and body["version"] == 4)
        srv2.stop()

        # 再重启一次，验证“快照 + 新日志”链路。
        srv3 = Server(d)
        srv3.wait_ready()
        st = srv3.request("GET", "/status")[1]
        check(f"[{label}] second restart consistent",
              st["version"] == 4 and st["checkpoints"]["src"] == 4 and
              st["rows"]["k4"] == "v4" and st["rows"]["k1"] == "v1")
        srv3.stop()


# ---------------------------------------------------------------------------
# E/F. 尾截断 vs 完整记录损坏
# ---------------------------------------------------------------------------


def _append_raw_frame(path: Path, payload: dict, corrupt_crc: bool = False) -> None:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    crc = zlib.crc32(raw) & 0xFFFFFFFF
    if corrupt_crc:
        crc ^= 0xDEADBEEF
    with open(path, "ab") as fh:
        fh.write(struct.pack("<I", len(raw)) + raw + struct.pack("<I", crc))


def test_corruption(work: Path) -> None:
    print("[E] 日志：尾截断恢复 vs 完整记录 CRC 损坏拒绝启动")
    d = work / "e"
    srv = Server(d)
    srv.wait_ready()
    srv.request("POST", "/load", txn("e1", {"s": [0, 1]}, [
        {"op": "put", "key": "k1", "value": "v1"}]))
    srv.stop()
    log = d / "log"
    good_size = log.stat().st_size

    # E1: 截断最后一条完整帧的一部分 -> 半截帧，忽略后回到空状态现场（本目录只有这一帧）。
    shutil.copy2(log, work / "e-log-backup")
    with open(log, "r+b") as fh:
        fh.truncate(good_size - 3)  # 连 CRC 尾巴一起截掉
    srv2 = Server(d)
    srv2.wait_ready()
    st = srv2.request("GET", "/status")[1]
    check("truncated tail ignored, earlier state kept", st["version"] == 0)
    check("truncated bytes removed on recovery", log.stat().st_size == 0)
    srv2.stop()

    # E2: 恢复完整日志，再制造“完整但 CRC 错误”的记录 -> 拒绝启动，现场保留。
    shutil.copy2(work / "e-log-backup", log)
    bad_record = {
        "type": "txn", "version": 1, "txid": "evil",
        "request": txn("evil", {"s": [0, 1]}, []),
    }
    _append_raw_frame(log, bad_record, corrupt_crc=True)
    size_with_bad = log.stat().st_size
    expect_refused_start(d)
    check("corrupt full record left in place", log.stat().st_size == size_with_bad)

    # 损坏记录之后再追加半截字节，仍然必须因完整记录 CRC 错误而拒绝（不能被当尾截断吞掉）。
    with open(log, "ab") as fh:
        fh.write(b"\x10\x00")
    expect_refused_start(d)
    check("crc error takes precedence over trailing junk", log.stat().st_size == size_with_bad + 2)

    print("[F] 快照完整记录损坏：拒绝启动并保留现场")
    d2 = work / "f"
    srv3 = Server(d2)
    srv3.wait_ready()
    srv3.request("POST", "/load", txn("f1", {"s": [0, 1]}, []))
    srv3.request("POST", "/checkpoint")
    srv3.stop()
    snap = d2 / "snapshot"
    snap_size = snap.stat().st_size
    data = bytearray(snap.read_bytes())
    # 翻转载荷区一个字节（跳过 4 字节长度头），制造 CRC 不匹配。
    data[5] ^= 0xFF
    snap.write_bytes(data)
    check("snapshot size unchanged by corruption", snap.stat().st_size == snap_size)
    expect_refused_start(d2)
    check("corrupt snapshot preserved", snap.read_bytes() == bytes(data))


# ---------------------------------------------------------------------------
# G. 并发串行等价
# ---------------------------------------------------------------------------


def test_concurrency(work: Path) -> None:
    print("[G] 并发提交与检查点：串行等价、读不见半个事务")
    d = work / "g"
    srv = Server(d)
    srv.wait_ready()

    n_threads = 12
    n_per_thread = 8
    errors: list[str] = []
    versions_seen: list[int] = []
    lock = threading.Lock()

    def worker(tid: int) -> None:
        local_versions = []
        for i in range(n_per_thread):
            source = f"src{tid:02d}"
            body = txn(
                f"t{tid:02d}-{i:02d}",
                {source: [i, i + 1]},
                # key 必须是纯 ASCII 字母数字（连字符、下划线都不合法）。
                [{"op": "put", "key": f"key{tid:02d}{i:02d}", "value": f"v{tid}{i}"}],
            )
            try:
                code, resp, _ = srv.request("POST", "/load", body)
            except Exception as exc:  # 线程内异常必须显式上报，不能静默吞掉
                with lock:
                    errors.append(f"thread {tid} iter {i} exception: {exc!r}")
                return
            if code != 200:
                with lock:
                    errors.append(f"thread {tid} iter {i}: status {code} {resp}")
                return
            local_versions.append(resp["version"])
            # 每个线程自己的版本必须严格递增。
            if len(local_versions) > 1 and local_versions[-1] <= local_versions[-2]:
                with lock:
                    errors.append(f"thread {tid}: non-monotonic versions {local_versions}")
        with lock:
            versions_seen.extend(local_versions)

    threads = [threading.Thread(target=worker, args=(tid,)) for tid in range(n_threads)]
    for t in threads:
        t.start()

    # 并发检查点 + 并发读：任何时刻读到的状态必须自洽（版本、行数、断点推进一致）。
    read_errors: list[str] = []
    stop_readers = threading.Event()

    def reader() -> None:
        while not stop_readers.is_set():
            st, status, _ = srv.request("GET", "/status")
            if st != 200:
                read_errors.append(f"status http {st}")
                return
            v = status["version"]
            # 不变量：行数 == 版本号（每事务恰好 put 一个新 key），断点之和 == 版本号。
            if len(status["rows"]) != v:
                read_errors.append(f"torn read: version={v} rows={len(status['rows'])}")
                return
            if sum(status["checkpoints"].values()) != v:
                read_errors.append(
                    f"torn read: version={v} checkpoint sum={sum(status['checkpoints'].values())}")
                return
            time.sleep(0.001)

    readers = [threading.Thread(target=reader) for _ in range(4)]
    for t in readers:
        t.start()

    ckpt_errors: list[str] = []
    def checkpoint_worker() -> None:
        for _ in range(6):
            code, _, _ = srv.request("POST", "/checkpoint")
            if code != 200:
                ckpt_errors.append(f"checkpoint status {code}")
                return
            time.sleep(0.002)

    ckpt_thread = threading.Thread(target=checkpoint_worker)
    ckpt_thread.start()

    for t in threads:
        t.join()
    ckpt_thread.join()
    stop_readers.set()
    for t in readers:
        t.join()

    total = n_threads * n_per_thread
    check("all concurrent commits succeeded", not errors, str(errors[:3]))
    check("no torn reads", not read_errors, str(read_errors[:3]))
    check("concurrent checkpoints succeeded", not ckpt_errors, str(ckpt_errors[:3]))
    check("all versions unique and contiguous 1..N",
          sorted(versions_seen) == list(range(1, total + 1)))

    final = srv.request("GET", "/status")[1]
    check("final version equals total txns", final["version"] == total)
    check("final rows count consistent", len(final["rows"]) == total)
    check("all sources advanced", len(final["checkpoints"]) == n_threads and
          all(v == n_per_thread for v in final["checkpoints"].values()))
    receipts = srv.request("GET", "/receipts")[1]
    check("all receipts retained despite checkpoints",
          len(receipts["receipts"]) == total)
    srv.stop()

    # 重启核对最终状态。
    srv2 = Server(d)
    srv2.wait_ready()
    check("post-stress restart consistent", srv2.request("GET", "/status")[1] == final)
    srv2.stop()


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------


def main() -> int:
    work = Path(tempfile.mkdtemp(prefix="kv-acceptance-"))
    print(f"workdir: {work}")
    try:
        test_functional(work)
        test_clean_restart(work)
        test_append_crash(work)
        test_checkpoint_crash(work)
        test_corruption(work)
        test_concurrency(work)
    finally:
        shutil.rmtree(work, ignore_errors=True)

    print(f"\n==== {PASS} passed, {FAIL} failed ====")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
