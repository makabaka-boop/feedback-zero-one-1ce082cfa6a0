"""KV 存储引擎：内存状态 + 数据文件（检查点快照）+ 追加日志。

不依赖任何数据库或现成事务引擎，仅用普通文件实现：

- ``snapshot``：检查点快照，帧格式与日志相同（长度 + JSON + CRC）。
- ``log``：追加事务日志，每条一帧，写入并 fsync 后事务才算提交。
- 崩溃恢复：先加载快照，再重放日志；日志末尾半截记录忽略，
  完整记录 CRC 失败则拒绝启动、原样保留磁盘现场。

并发：所有写操作（提交、检查点）在同一把锁内串行；读操作在锁内取
一致快照拷贝，因此读永远不会看到半个事务，整体串行等价。
"""

from __future__ import annotations

import json
import os
import struct
import threading
from typing import Any

from .framing import (
    CorruptRecordError,
    encode_frame,
    read_all_frames,
)

# ---------------------------------------------------------------------------
# 常量与异常
# ---------------------------------------------------------------------------

SNAPSHOT_FILE = "snapshot"
SNAPSHOT_TMP = "snapshot.tmp"
LOG_FILE = "log"
LOG_TMP = "log.tmp"

INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1

# 故障注入点（仅验收用）：设置环境变量后下一次对应操作会在指定点 ``os._exit(9)``。
FAULT_APPEND_AFTER_WRITE = "KV_FAULT_APPEND_AFTER_WRITE"  # 写入帧后、fsync 前
FAULT_APPEND_AFTER_FSYNC = "KV_FAULT_APPEND_AFTER_FSYNC"  # fsync 后、返回前
FAULT_CKPT_AFTER_TMP = "KV_FAULT_CKPT_AFTER_TMP"  # 临时快照 fsync 后
FAULT_CKPT_AFTER_RENAME = "KV_FAULT_CKPT_AFTER_RENAME"  # 快照转正后、切日志前
FAULT_CKPT_AFTER_SWITCH = "KV_FAULT_CKPT_AFTER_SWITCH"  # 新日志 fsync 后、旧日志删除前


class ValidationError(Exception):
    """请求语义或 JSON 结构非法（映射为 HTTP 400）。"""


class ConflictError(Exception):
    """已占用 txid 但内容不同（映射为 HTTP 409）。"""


# ---------------------------------------------------------------------------
# 严格 JSON：拒绝重复成员，拒绝 NaN/Infinity，顶层必须是对象
# ---------------------------------------------------------------------------


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """``object_pairs_hook``：遇到重复 key 直接报错。"""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError(f"duplicate JSON member: {key!r}")
        result[key] = value
    return result


def parse_strict_json(raw: bytes | str) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
    except UnicodeDecodeError as exc:
        raise ValidationError(f"body is not valid UTF-8: {exc}") from exc
    try:
        value = json.loads(
            text,
            object_pairs_hook=_reject_duplicates,
            parse_constant=lambda c: (_ for _ in ()).throw(
                ValidationError(f"invalid JSON constant: {c}")
            ),
        )
    except json.JSONDecodeError as exc:
        raise ValidationError(f"invalid JSON: {exc.msg}") from exc
    if not isinstance(value, dict):
        raise ValidationError("request body must be a JSON object")
    return value


def canonical_json(value: Any) -> bytes:
    """规范化序列化：key 排序、无多余空白，用于幂等结构比对。

    数组（ops）保持原有顺序；``ensure_ascii=False`` 并统一以 UTF-8 落盘，
    同一逻辑结构必然产生同一字节串。
    """
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


# ---------------------------------------------------------------------------
# 请求校验
# ---------------------------------------------------------------------------


def _is_ascii_alnum(name: Any) -> bool:
    return (
        isinstance(name, str)
        and len(name) > 0
        and all(("a" <= c <= "z") or ("A" <= c <= "Z") or ("0" <= c <= "9") for c in name)
    )


def _is_int64(value: Any) -> bool:
    # bool 是 int 的子类，整数位禁止布尔，必须显式排除。
    return isinstance(value, int) and not isinstance(value, bool) and INT64_MIN <= value <= INT64_MAX


def validate_request(obj: dict[str, Any]) -> tuple[str, dict[str, int], list[dict[str, Any]]]:
    """校验事务请求，返回 (txid, sources, ops)。

    未知/重复 JSON 成员、类型不符、越界、布尔冒充整数等一律拒绝。
    """
    allowed = {"txid", "sources", "ops"}
    unknown = set(obj) - allowed
    if unknown:
        raise ValidationError(f"unknown JSON member(s): {sorted(unknown)}")

    txid = obj.get("txid")
    if not isinstance(txid, str) or len(txid) == 0:
        raise ValidationError("txid must be a non-empty string")

    sources_raw = obj.get("sources")
    if not isinstance(sources_raw, dict) or len(sources_raw) == 0:
        raise ValidationError("sources must be a non-empty object")
    sources: dict[str, int] = {}
    for name, interval in sources_raw.items():
        if not _is_ascii_alnum(name):
            raise ValidationError(f"source name must be non-empty ASCII alnum: {name!r}")
        if not isinstance(interval, list) or len(interval) != 2:
            raise ValidationError(f"sources[{name!r}] must be a [start, end] pair")
        start, end = interval
        if not _is_int64(start) or not _is_int64(end):
            raise ValidationError(
                f"sources[{name!r}] endpoints must be signed 64-bit integers (booleans rejected)"
            )
        if start < 0 or end < 0:
            raise ValidationError(f"sources[{name!r}] endpoints must be non-negative")
        if end < start:
            raise ValidationError(f"sources[{name!r}]: end must be >= start")
        sources[name] = (start, end)  # type: ignore[assignment]

    ops_raw = obj.get("ops")
    if not isinstance(ops_raw, list):
        raise ValidationError("ops must be an array")
    ops: list[dict[str, Any]] = []
    for index, op in enumerate(ops_raw):
        if not isinstance(op, dict):
            raise ValidationError(f"ops[{index}] must be an object")
        kind = op.get("op")
        if kind not in ("put", "del"):
            raise ValidationError(f"ops[{index}].op must be 'put' or 'del'")
        key = op.get("key")
        if not _is_ascii_alnum(key):
            raise ValidationError(f"ops[{index}].key must be non-empty ASCII alnum")
        if kind == "put":
            if set(op) != {"op", "key", "value"}:
                raise ValidationError(
                    f"ops[{index}] put must contain exactly op, key, value"
                )
            value = op["value"]
            if value is not None and not isinstance(value, str):
                raise ValidationError(f"ops[{index}].value must be a string or null")
            ops.append({"op": "put", "key": key, "value": value})
        else:  # del
            if set(op) != {"op", "key"}:
                raise ValidationError(f"ops[{index}] del must contain exactly op, key")
            ops.append({"op": "del", "key": key})

    return txid, sources, ops


# ---------------------------------------------------------------------------
# 引擎
# ---------------------------------------------------------------------------


class Engine:
    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

        self._lock = threading.RLock()
        self.rows: dict[str, str | None] = {}
        self.checkpoints: dict[str, int] = {}
        self.version: int = 0
        # txid -> 已提交回执（含版本与规范化后的请求内容）
        self.receipts: dict[str, dict[str, Any]] = {}

        self._log_fh = None
        self._recover()
        self._open_log()

    # -- 底层文件工具 ------------------------------------------------------

    def _path(self, name: str) -> str:
        return os.path.join(self.data_dir, name)

    @staticmethod
    def _fsync_dir(path: str) -> None:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _maybe_fault(self, env_name: str) -> None:
        if os.environ.get(env_name):
            # 硬退出，模拟进程被 SIGKILL / 掉电，不执行任何缓冲刷新。
            os._exit(9)

    # -- 恢复 --------------------------------------------------------------

    def _recover(self) -> None:
        # 1) 快照（检查点）。快照是检查点原子发布的结果，任何损坏都拒绝启动。
        snap_path = self._path(SNAPSHOT_FILE)
        if os.path.exists(snap_path):
            self._load_snapshot(snap_path)

        # 2) 追加日志。
        log_path = self._path(LOG_FILE)
        if not os.path.exists(log_path):
            return
        # 末尾不完整记录直接忽略；完整记录 CRC 错误则 CorruptRecordError 向上抛。
        frames, truncated = read_all_frames(log_path)
        for payload in frames:
            self._replay_record(payload)

        # 若存在尾部半截数据，截断到最后一条完整帧边界，保证后续追加安全。
        if truncated:
            self._truncate_to_frames(log_path, len(frames))

    def _load_snapshot(self, path: str) -> None:
        try:
            frames, truncated = read_all_frames(path)
        except CorruptRecordError as exc:
            # CRC 失败、tmp 已 rename 成功但未写完等情况一律拒绝，保留现场。
            raise CorruptRecordError(f"snapshot unusable: {exc}") from exc
        if truncated or len(frames) != 1:
            raise CorruptRecordError(
                f"snapshot must contain exactly one complete record, "
                f"got {len(frames)} frames (truncated={truncated})"
            )
        try:
            snap = json.loads(frames[0].decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CorruptRecordError(f"snapshot payload not valid JSON: {exc}") from exc
        for key in ("version", "rows", "checkpoints", "receipts"):
            if key not in snap:
                raise CorruptRecordError(f"snapshot missing field: {key}")
        self.version = snap["version"]
        self.rows = dict(snap["rows"])
        self.checkpoints = dict(snap["checkpoints"])
        # 回执里保存规范化内容，恢复后继续用于幂等判定。
        self.receipts = {
            txid: {
                "version": rec["version"],
                "canonical": rec["canonical"].encode("utf-8")
                if isinstance(rec["canonical"], str)
                else bytes(rec["canonical"]),
            }
            for txid, rec in snap["receipts"].items()
        }

    def _replay_record(self, payload: bytes) -> None:
        try:
            rec = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CorruptRecordError(f"log record payload not valid JSON: {exc}") from exc
        if rec.get("type") != "txn":
            raise CorruptRecordError(f"unknown log record type: {rec.get('type')!r}")
        txid = rec["txid"]
        if txid in self.receipts:
            # 快照之后又重放到同一事务（检查点切换前的重复段）：幂等跳过，
            # 但版本必须连续一致。
            if self.receipts[txid]["version"] != rec["version"]:
                raise CorruptRecordError(f"version mismatch for duplicate txid {txid!r}")
            return
        if rec["version"] != self.version + 1:
            raise CorruptRecordError(
                f"non-contiguous version in log: expected {self.version + 1}, got {rec['version']}"
            )
        # 重放时仍走完整语义校验，防止损坏记录污染内存状态。
        _txid, sources, ops = validate_request(rec["request"])
        self._apply(sources, ops)
        self.version = rec["version"]
        self.receipts[txid] = {
            "version": self.version,
            "canonical": canonical_json(rec["request"]),
        }

    @staticmethod
    def _truncate_to_frames(path: str, frame_count: int) -> None:
        """把文件截断到第 ``frame_count`` 条完整帧之后，丢弃尾部半截字节。"""
        good_size = 0
        with open(path, "rb") as fh:
            for _ in range(frame_count):
                header = fh.read(4)
                if len(header) < 4:
                    raise CorruptRecordError(f"{path}: frame header vanished during truncate scan")
                (length,) = struct.unpack("<I", header)
                if length == 0:
                    raise CorruptRecordError(f"{path}: zero-length record during truncate scan")
                rest = fh.read(length + 4)
                if len(rest) < length + 4:
                    raise CorruptRecordError(f"{path}: frame vanished during truncate scan")
                good_size = fh.tell()
        if good_size != os.path.getsize(path):
            with open(path, "r+b") as fh:
                fh.truncate(good_size)
                fh.flush()
                os.fsync(fh.fileno())

    def _open_log(self) -> None:
        path = self._path(LOG_FILE)
        # a+b：每次写都追加到当前文件末尾，且不破坏已有数据。
        self._log_fh = open(path, "a+b")

    # -- 状态应用（仅在锁内、校验通过后调用） ------------------------------

    def _apply(self, sources: dict[str, list[int]], ops: list[dict[str, Any]]) -> None:
        for name, (start, end) in sources.items():
            current = self.checkpoints.get(name, 0)
            if start != current:
                # 理论上提交前已检查，重放路径靠它兜底。
                raise ValidationError(
                    f"source {name!r} checkpoint mismatch: expected {current}, got {start}"
                )
        for op in ops:
            if op["op"] == "put":
                self.rows[op["key"]] = op["value"]
            else:
                if op["key"] not in self.rows:
                    raise ValidationError(f"del on non-existing key: {op['key']!r}")
                del self.rows[op["key"]]
        for name, (_start, end) in sources.items():
            self.checkpoints[name] = end

    # -- 公共 API -----------------------------------------------------------

    def submit(self, raw_body: bytes) -> dict[str, Any]:
        obj = parse_strict_json(raw_body)
        txid, sources, ops = validate_request(obj)
        canon = canonical_json(obj)

        with self._lock:
            # 1) 已占用 txid：只按 JSON 结构判幂等，不再检查断点、不再应用。
            if txid in self.receipts:
                prior = self.receipts[txid]
                if prior["canonical"] != canon:
                    raise ConflictError(
                        f"txid {txid!r} already committed with a different payload"
                    )
                return {
                    "txid": txid,
                    "version": prior["version"],
                    "idempotent": True,
                }

            # 2) 新事务：先检查全部源断点（任一不符则整批拒绝，不产生任何副作用）。
            for name, (start, _end) in sources.items():
                if start != self.checkpoints.get(name, 0):
                    raise ValidationError(
                        f"source {name!r} checkpoint mismatch: "
                        f"expected {self.checkpoints.get(name, 0)}, got {start}"
                    )
            # del 不存在的 key 同样整批拒绝（在应用前预检，保证无半成品）。
            for op in ops:
                if op["op"] == "del" and op["key"] not in self.rows:
                    raise ValidationError(f"del on non-existing key: {op['key']!r}")

            new_version = self.version + 1
            record = {
                "type": "txn",
                "version": new_version,
                "txid": txid,
                "request": obj,
            }
            frame = encode_frame(canonical_json(record))

            # 3) 追加并持久化日志。
            assert self._log_fh is not None
            if os.environ.get(FAULT_APPEND_AFTER_WRITE):
                # 验收用：只写半帧就硬退出，确定性制造“末尾不完整记录”。
                self._log_fh.write(frame[: len(frame) // 2])
                self._log_fh.flush()
                os._exit(9)
            self._log_fh.write(frame)
            self._log_fh.flush()
            os.fsync(self._log_fh.fileno())
            self._maybe_fault(FAULT_APPEND_AFTER_FSYNC)

            # 4) 日志落盘成功后，同时提交行、全部断点和回执，版本加一。
            self._apply(sources, ops)
            self.version = new_version
            self.receipts[txid] = {"version": new_version, "canonical": canon}
            return {"txid": txid, "version": new_version, "idempotent": False}

    def checkpoint(self) -> dict[str, Any]:
        """生成检查点并回收此前日志。

        步骤（每一步崩溃都可恢复）::

            1. 全量状态写 snapshot.tmp（fsync 文件 + 目录）
            2. rename(snapshot.tmp, snapshot) + fsync 目录 —— 检查点发布点
            3. 新日志写 log.tmp 并 fsync，rename 为 log（旧 log 被原子替换）
            4. fsync 目录，删除旧日志已由 rename 完成
        """
        with self._lock:
            snapshot = {
                "version": self.version,
                "rows": self.rows,
                "checkpoints": self.checkpoints,
                "receipts": {
                    txid: {"version": rec["version"], "canonical": rec["canonical"].decode("utf-8")}
                    for txid, rec in self.receipts.items()
                },
            }
            frame = encode_frame(canonical_json(snapshot))

            tmp_path = self._path(SNAPSHOT_TMP)
            snap_path = self._path(SNAPSHOT_FILE)
            with open(tmp_path, "wb") as fh:
                fh.write(frame)
                fh.flush()
                os.fsync(fh.fileno())
            self._fsync_dir(self.data_dir)
            self._maybe_fault(FAULT_CKPT_AFTER_TMP)

            os.replace(tmp_path, snap_path)
            self._fsync_dir(self.data_dir)
            self._maybe_fault(FAULT_CKPT_AFTER_RENAME)

            # 切换日志：新空日志先落 tmp，再原子替换旧日志。
            assert self._log_fh is not None
            self._log_fh.close()
            log_tmp = self._path(LOG_TMP)
            log_path = self._path(LOG_FILE)
            with open(log_tmp, "wb") as fh:
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(log_tmp, log_path)
            self._fsync_dir(self.data_dir)
            self._maybe_fault(FAULT_CKPT_AFTER_SWITCH)

            self._log_fh = open(log_path, "a+b")
            return {
                "version": self.version,
                "rows": len(self.rows),
                "checkpoints": len(self.checkpoints),
                "receipts": len(self.receipts),
                "reclaimed": True,
            }

    def status(self) -> dict[str, Any]:
        """一致读：锁内拷贝，行按 key 字节序、断点按源名字典序输出。"""
        with self._lock:
            rows = {k: self.rows[k] for k in sorted(self.rows, key=lambda s: s.encode("utf-8"))}
            checkpoints = {k: self.checkpoints[k] for k in sorted(self.checkpoints)}
            return {
                "version": self.version,
                "rows": rows,
                "checkpoints": checkpoints,
            }

    def list_receipts(self) -> dict[str, Any]:
        """全部回执按版本顺序返回（检查点回收日志后回执仍保留）。"""
        with self._lock:
            items = sorted(
                ((txid, rec["version"]) for txid, rec in self.receipts.items()),
                key=lambda item: item[1],
            )
            return {
                "version": self.version,
                "receipts": [{"txid": txid, "version": ver} for txid, ver in items],
            }

    def close(self) -> None:
        with self._lock:
            if self._log_fh is not None:
                self._log_fh.flush()
                os.fsync(self._log_fh.fileno())
                self._log_fh.close()
                self._log_fh = None
