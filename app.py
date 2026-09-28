#!/usr/bin/env python3
"""Organ allocation and cold-chain coordination service (standard library only).

一位捐献者可捐献多个器官；每个器官独立提出分配、独立流转（接受、转运、
交接、植入、撤回、过期）。捐献者状态由其全部器官的状态派生。
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

PORT = 8203
ROLES = {"viewer", "hospital", "coordinator", "allocation_officer", "auditor"}
STATUSES = {"proposed", "accepted", "in_transit", "handed_off", "implanted", "withdrawn", "expired"}
CLOSED_ALLOCATION = {"withdrawn", "expired", "implanted"}
TERMINAL_ORGAN = {"used", "expired"}

# 器官当前阶段 -> (步骤码, 中文说明, 等待角色)
ORGAN_STAGES = {
    "waiting_proposal": ("等待提出分配", "等待分配员为该器官提出分配", "allocation_officer"),
    "waiting_reproposal": ("等待重新分配", "上一分配已撤回，等待分配员重新提出分配", "allocation_officer"),
    "waiting_accept": ("等待医院接受", "分配已提出，等待接收医院确认接受", "hospital"),
    "waiting_transit": ("等待登记转运", "医院已接受，等待分配员登记冷链转运", "allocation_officer"),
    "waiting_handoff": ("等待发起交接", "器官转运中，等待来源医院发起交接", "hospital"),
    "waiting_handoff_accept": ("等待确认交接", "交接已发起，等待接收医院确认接收", "hospital"),
    "waiting_implant": ("等待确认植入", "交接已完成，等待分配员确认植入", "allocation_officer"),
    "done": ("已完成", "该器官已完成植入", None),
    "expired": ("已过期", "器官已超过可用窗口，流程终止", None),
}
FLOW_STEPS = ["proposed", "accepted", "in_transit", "handed_off", "implanted"]

FRESH_SCHEMA = """
CREATE TABLE IF NOT EXISTS donors(
    id INTEGER PRIMARY KEY AUTOINCREMENT, blood_type TEXT NOT NULL, hospital TEXT NOT NULL,
    region TEXT NOT NULL, available_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'available', revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS organs(
    id INTEGER PRIMARY KEY AUTOINCREMENT, donor_id INTEGER NOT NULL REFERENCES donors(id),
    organ TEXT NOT NULL, available_at TEXT NOT NULL, expires_at TEXT NOT NULL,
    clinical_match INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'available',
    revision INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL,
    UNIQUE(donor_id, organ)
);
CREATE TABLE IF NOT EXISTS candidates(
    id INTEGER PRIMARY KEY AUTOINCREMENT, patient_name TEXT NOT NULL, blood_type TEXT NOT NULL, organ TEXT NOT NULL,
    hospital TEXT NOT NULL, region TEXT NOT NULL, urgency INTEGER NOT NULL, wait_days INTEGER NOT NULL,
    willing INTEGER NOT NULL DEFAULT 1, clinical_match INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'active',
    created_by TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS allocations(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    organ_id INTEGER NOT NULL REFERENCES organs(id), donor_id INTEGER NOT NULL REFERENCES donors(id),
    candidate_id INTEGER NOT NULL REFERENCES candidates(id),
    score REAL NOT NULL, status TEXT NOT NULL DEFAULT 'proposed', revision INTEGER NOT NULL DEFAULT 1,
    cold_chain_temp REAL, delayed_minutes INTEGER NOT NULL DEFAULT 0, created_by TEXT NOT NULL, created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL, accepted_at TEXT, implanted_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_active_allocation_organ
    ON allocations(organ_id) WHERE status NOT IN ('withdrawn','expired','implanted');
CREATE TABLE IF NOT EXISTS handoffs(
    id INTEGER PRIMARY KEY AUTOINCREMENT, allocation_id INTEGER NOT NULL REFERENCES allocations(id), from_hospital TEXT NOT NULL,
    to_hospital TEXT NOT NULL, cold_chain_temp REAL NOT NULL, status TEXT NOT NULL DEFAULT 'initiated',
    initiated_by TEXT NOT NULL, accepted_by TEXT, initiated_at TEXT NOT NULL, accepted_at TEXT,
    UNIQUE(allocation_id)
);
CREATE TABLE IF NOT EXISTS audit_log(
    id INTEGER PRIMARY KEY AUTOINCREMENT, allocation_id INTEGER, donor_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL,
    action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL
);
"""


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message); self.status, self.code, self.message = status, code, message


def utcnow() -> datetime: return datetime.now(timezone.utc)
def iso(value: datetime | None = None) -> str: return (value or utcnow()).replace(microsecond=0).isoformat().replace("+00:00", "Z")
def parse_time(value: str | None) -> datetime:
    if not value: raise ApiError(400, "time_required", "必须提供 ISO 8601 时间")
    try: parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc: raise ApiError(400, "invalid_time", f"时间格式错误: {value}") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def blood_compatible(donor: str, recipient: str) -> bool:
    return {
        "O": {"O", "A", "B", "AB"}, "A": {"A", "AB"}, "B": {"B", "AB"}, "AB": {"AB"},
    }.get(donor.upper(), set()) and recipient.upper() in {"O", "A", "B", "AB"} and recipient.upper() in {
        "O": {"O", "A", "B", "AB"}, "A": {"A", "AB"}, "B": {"B", "AB"}, "AB": {"AB"},
    }.get(donor.upper(), set())


class Repository:
    def __init__(self, path: str | Path):
        self.conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON"); self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self) -> None:
        tables = {r[0] for r in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not tables:
            self.conn.executescript(FRESH_SCHEMA); self.conn.execute("PRAGMA user_version=2"); return
        donor_cols = {r[1] for r in self.conn.execute("PRAGMA table_info(donors)")}
        if "organ" in donor_cols:  # v1：捐献者与器官合一的旧库
            self._migrate_v1()
        self.conn.executescript(FRESH_SCHEMA)
        self.conn.execute("PRAGMA user_version=2")

    def _migrate_v1(self) -> None:
        """旧库 donors 自带 organ/expires_at，allocations.donor_id 唯一；拆出 organs 表并保留历史数据。"""
        conn = self.conn; conn.execute("PRAGMA foreign_keys=OFF"); conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("""CREATE TABLE organs(
                id INTEGER PRIMARY KEY AUTOINCREMENT, donor_id INTEGER NOT NULL REFERENCES donors(id),
                organ TEXT NOT NULL, available_at TEXT NOT NULL, expires_at TEXT NOT NULL,
                clinical_match INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'available',
                revision INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, UNIQUE(donor_id, organ))""")
            conn.execute("""INSERT INTO organs(id,donor_id,organ,available_at,expires_at,clinical_match,status,revision,created_at)
                            SELECT id,id,organ,available_at,expires_at,clinical_match,status,revision,created_at FROM donors""")
            conn.execute("ALTER TABLE allocations RENAME TO allocations_old")
            conn.execute("""CREATE TABLE allocations(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                organ_id INTEGER NOT NULL REFERENCES organs(id), donor_id INTEGER NOT NULL REFERENCES donors(id),
                candidate_id INTEGER NOT NULL REFERENCES candidates(id),
                score REAL NOT NULL, status TEXT NOT NULL DEFAULT 'proposed', revision INTEGER NOT NULL DEFAULT 1,
                cold_chain_temp REAL, delayed_minutes INTEGER NOT NULL DEFAULT 0, created_by TEXT NOT NULL, created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL, accepted_at TEXT, implanted_at TEXT)""")
            conn.execute("""INSERT INTO allocations(id,organ_id,donor_id,candidate_id,score,status,revision,cold_chain_temp,
                                delayed_minutes,created_by,created_at,updated_at,accepted_at,implanted_at)
                            SELECT id,donor_id,donor_id,candidate_id,score,status,revision,cold_chain_temp,delayed_minutes,
                                   created_by,created_at,updated_at,accepted_at,implanted_at FROM allocations_old""")
            conn.execute("DROP TABLE allocations_old")
            conn.execute("""CREATE TABLE donors_new(
                id INTEGER PRIMARY KEY AUTOINCREMENT, blood_type TEXT NOT NULL, hospital TEXT NOT NULL,
                region TEXT NOT NULL, available_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'available', revision INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL, created_at TEXT NOT NULL)""")
            conn.execute("""INSERT INTO donors_new(id,blood_type,hospital,region,available_at,status,revision,created_by,created_at)
                            SELECT id,blood_type,hospital,region,available_at,status,revision,created_by,created_at FROM donors""")
            conn.execute("DROP TABLE donors"); conn.execute("ALTER TABLE donors_new RENAME TO donors")
            conn.execute("""CREATE UNIQUE INDEX idx_active_allocation_organ ON allocations(organ_id)
                            WHERE status NOT IN ('withdrawn','expired','implanted')""")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK"); raise
        finally:
            conn.execute("PRAGMA foreign_keys=ON")

    @contextmanager
    def tx(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn; self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK"); raise

    @staticmethod
    def audit(conn: sqlite3.Connection, allocation_id: int | None, donor_id: int | None, actor: str, role: str, action: str, detail: dict[str, Any]) -> None:
        conn.execute("INSERT INTO audit_log(allocation_id,donor_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?,?)",
                     (allocation_id, donor_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()))


class OrganAllocationService:
    def __init__(self, path: str | Path): self.repo = Repository(path)

    @staticmethod
    def identity(headers: Any) -> tuple[str, str, str]:
        actor, role, hospital = headers.get("X-User-Id", "").strip(), headers.get("X-Role", "").strip(), headers.get("X-Hospital", "").strip()
        if not actor or role not in ROLES: raise ApiError(401, "unauthorized", "需要 X-User-Id 和有效 X-Role")
        if role == "hospital" and not hospital: raise ApiError(401, "hospital_required", "医院角色必须提供 X-Hospital")
        return actor, role, hospital

    @staticmethod
    def _row(row: sqlite3.Row | None) -> dict[str, Any] | None: return dict(row) if row else None

    @staticmethod
    def _effective_organ_status(row: sqlite3.Row | dict[str, Any], now: datetime | None = None) -> str:
        status = row["status"]
        if status in {"available", "allocated"} and parse_time(row["expires_at"]) <= (now or utcnow()):
            return "expired"
        return status

    @staticmethod
    def _derive_donor_status(statuses: list[str]) -> str:
        if statuses and all(s in TERMINAL_ORGAN for s in statuses):
            return "used" if any(s == "used" for s in statuses) else "expired"
        if any(s == "allocated" for s in statuses): return "allocated"
        return "available"

    def _sweep_expired(self, conn: sqlite3.Connection, actor: str, role: str, donor_id: int | None = None) -> list[int]:
        """把已过可用窗口的器官及其有效分配置为过期（彼此独立，只影响到期的器官）。"""
        sql = "SELECT id,donor_id FROM organs WHERE status IN ('available','allocated') AND expires_at<=?"
        params: list[Any] = [iso()]
        if donor_id is not None: sql += " AND donor_id=?"; params.append(donor_id)
        due = [r["id"] for r in conn.execute(sql, params)]
        touched_donors: set[int] = set()
        for organ_id in due:
            organ = conn.execute("SELECT donor_id FROM organs WHERE id=?", (organ_id,)).fetchone()
            conn.execute("UPDATE organs SET status='expired',revision=revision+1 WHERE id=?", (organ_id,))
            alloc = conn.execute("SELECT id,donor_id FROM allocations WHERE organ_id=? AND status NOT IN ('withdrawn','expired','implanted')",
                                 (organ_id,)).fetchone()
            if alloc:
                conn.execute("UPDATE allocations SET status='expired',revision=revision+1,updated_at=? WHERE id=?", (iso(), alloc["id"]))
                Repository.audit(conn, alloc["id"], organ["donor_id"], actor, role, "allocation_expired", {"reason": "organ_window_elapsed"})
            Repository.audit(conn, alloc["id"] if alloc else None, organ["donor_id"], actor, role, "organ_expired", {"organ_id": organ_id})
            touched_donors.add(organ["donor_id"])
        for did in touched_donors: self._sync_donor_status(conn, did)
        return due

    def _sync_donor_status(self, conn: sqlite3.Connection, donor_id: int) -> None:
        """全部器官结束或失效后捐献者才标记 used/expired；否则按是否有器官在流转派生。"""
        rows = conn.execute("SELECT status FROM organs WHERE donor_id=?", (donor_id,)).fetchall()
        if not rows: return
        new_status = self._derive_donor_status([r["status"] for r in rows])
        conn.execute("UPDATE donors SET status=?,revision=revision+1 WHERE id=? AND status<>?", (new_status, donor_id, new_status))

    # ---------- 登记 ----------

    def register_donor(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"coordinator", "allocation_officer"}: raise ApiError(403, "donor_forbidden", "当前角色不能登记器官")
        required = ("blood_type", "hospital", "region", "available_at")
        if any(not body.get(k) for k in required): raise ApiError(400, "missing_fields", f"缺少字段: {', '.join(k for k in required if not body.get(k))}")
        blood = str(body["blood_type"]).upper()
        if blood not in {"O", "A", "B", "AB"}: raise ApiError(400, "invalid_blood_type", "血型必须为 O/A/B/AB")
        donor_available = parse_time(body["available_at"])
        donor_expires = parse_time(body["expires_at"]) if body.get("expires_at") else None
        donor_match = body.get("clinical_match", 0)
        if not isinstance(donor_match, int): raise ApiError(400, "invalid_clinical_match", "clinical_match 必须为整数")

        raw_specs: list[Any]
        if body.get("organs"):
            raw_specs = body["organs"]
            if not isinstance(raw_specs, list): raise ApiError(400, "invalid_organs", "organs 必须为数组")
        elif body.get("organ"):
            raw_specs = [body]
        else:
            raise ApiError(400, "missing_fields", "缺少字段: organ 或 organs")

        specs: list[dict[str, Any]] = []
        seen: set[str] = set()
        for index, raw in enumerate(raw_specs):
            if isinstance(raw, str): spec = {"organ": raw}
            elif isinstance(raw, dict): spec = raw
            else: raise ApiError(400, "invalid_organs", f"第 {index + 1} 个器官格式错误")
            name = str(spec.get("organ", "")).strip().lower()
            if not name: raise ApiError(400, "invalid_organs", f"第 {index + 1} 个器官缺少 organ")
            if name in seen: raise ApiError(409, "duplicate_organ", f"同一捐献者不能重复登记器官: {name}")
            seen.add(name)
            available = parse_time(spec["available_at"]) if spec.get("available_at") else donor_available
            expires = parse_time(spec["expires_at"]) if spec.get("expires_at") else donor_expires
            if expires is None: raise ApiError(400, "missing_fields", f"器官 {name} 缺少 expires_at")
            if expires <= available: raise ApiError(400, "invalid_window", f"器官 {name} 的可用窗口结束时间必须晚于开始时间")
            match = spec.get("clinical_match", donor_match)
            if not isinstance(match, int): raise ApiError(400, "invalid_clinical_match", f"器官 {name} 的 clinical_match 必须为整数")
            specs.append({"organ": name, "available": available, "expires": expires, "clinical_match": match})

        with self.repo.tx() as conn:
            cur = conn.execute("""INSERT INTO donors(blood_type,hospital,region,available_at,created_by,created_at)
                                  VALUES(?,?,?,?,?,?)""",
                               (blood, body["hospital"], body["region"], iso(donor_available), actor, iso()))
            donor_id = cur.lastrowid
            organs: list[dict[str, Any]] = []
            for spec in specs:
                org_cur = conn.execute("""INSERT INTO organs(donor_id,organ,available_at,expires_at,clinical_match,created_at)
                                          VALUES(?,?,?,?,?,?)""",
                                       (donor_id, spec["organ"], iso(spec["available"]), iso(spec["expires"]), spec["clinical_match"], iso()))
                organs.append(dict(conn.execute("SELECT * FROM organs WHERE id=?", (org_cur.lastrowid,)).fetchone()))
            Repository.audit(conn, None, donor_id, actor, role, "donor_registered",
                             {"organs": [{"organ": s["organ"], "expires_at": iso(s["expires"])} for s in specs]})
            return self._donor_payload(conn, conn.execute("SELECT * FROM donors WHERE id=?", (donor_id,)).fetchone(), role, "")

    def register_candidate(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"coordinator", "allocation_officer"}: raise ApiError(403, "candidate_forbidden", "当前角色不能登记候选患者")
        required = ("patient_name", "blood_type", "organ", "hospital", "region")
        if any(not body.get(k) for k in required): raise ApiError(400, "missing_fields", "候选患者基础信息不完整")
        blood = str(body["blood_type"]).upper(); urgency = body.get("urgency"); wait_days = body.get("wait_days", 0)
        if blood not in {"O", "A", "B", "AB"} or not isinstance(urgency, int) or not 1 <= urgency <= 5 or not isinstance(wait_days, int) or wait_days < 0:
            raise ApiError(400, "invalid_candidate", "血型、1-5 紧急度和非负等待天数无效")
        with self.repo.tx() as conn:
            cur = conn.execute("""INSERT INTO candidates(patient_name,blood_type,organ,hospital,region,urgency,wait_days,willing,clinical_match,created_by,created_at)
                                  VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                               (body["patient_name"], blood, str(body["organ"]).lower(), body["hospital"], body["region"], urgency, wait_days,
                                int(body.get("willing", True)), int(body.get("clinical_match", 0)), actor, iso()))
            return dict(conn.execute("SELECT * FROM candidates WHERE id=?", (cur.lastrowid,)).fetchone())

    @staticmethod
    def _score(donor: sqlite3.Row, organ: sqlite3.Row, candidate: sqlite3.Row) -> dict[str, float]:
        region = 250 if donor["region"] == candidate["region"] else 0
        clinical = min(organ["clinical_match"], candidate["clinical_match"]) * 30
        return {"urgency": candidate["urgency"] * 1000, "waiting": candidate["wait_days"] * 2, "region": region, "clinical": clinical,
                "total": candidate["urgency"] * 1000 + candidate["wait_days"] * 2 + region + clinical}

    def _resolve_organ(self, conn: sqlite3.Connection, organ_id: Any, donor_id: Any) -> sqlite3.Row:
        if organ_id is not None:
            if not isinstance(organ_id, int): raise ApiError(400, "ids_required", "organ_id 必须为整数")
            organ = conn.execute("SELECT * FROM organs WHERE id=?", (organ_id,)).fetchone()
            if not organ: raise ApiError(404, "organ_not_found", "器官不存在")
            if isinstance(donor_id, int) and organ["donor_id"] != donor_id:
                raise ApiError(409, "organ_mismatch", "organ_id 与 donor_id 不属于同一捐献者")
            return organ
        if not isinstance(donor_id, int): raise ApiError(400, "ids_required", "organ_id 或 donor_id 必填")
        donor = conn.execute("SELECT 1 FROM donors WHERE id=?", (donor_id,)).fetchone()
        if not donor: raise ApiError(404, "not_found", "捐献者不存在")
        organs = conn.execute("SELECT * FROM organs WHERE donor_id=? ORDER BY id", (donor_id,)).fetchall()
        if len(organs) > 1: raise ApiError(409, "organ_required", "该捐献者有多个器官，请使用 organ_id 指定")
        if not organs: raise ApiError(404, "organ_not_found", "捐献者没有可分配器官")
        return organs[0]

    def ranking(self, donor_id: int | None, role: str, hospital: str, organ_id: int | None = None) -> dict[str, Any]:
        if role not in {"allocation_officer", "auditor"}: raise ApiError(403, "ranking_forbidden", "只有分配员或审计员可以查看完整候选排序")
        with self.repo.tx() as conn:
            if organ_id is None:
                donor = conn.execute("SELECT * FROM donors WHERE id=?", (donor_id,)).fetchone()
                if not donor: raise ApiError(404, "donor_not_found", "捐献者不存在")
                organs = conn.execute("SELECT * FROM organs WHERE donor_id=? ORDER BY id", (donor_id,)).fetchall()
                if len(organs) > 1: raise ApiError(409, "organ_required", "该捐献者有多个器官，请改用 /api/organs/{id}/ranking")
                if not organs: raise ApiError(404, "organ_not_found", "捐献者没有可分配器官")
                organ = organs[0]
            else:
                organ = conn.execute("SELECT * FROM organs WHERE id=?", (organ_id,)).fetchone()
                if not organ: raise ApiError(404, "organ_not_found", "器官不存在")
                donor = conn.execute("SELECT * FROM donors WHERE id=?", (organ["donor_id"],)).fetchone()
            rows = []
            for candidate in conn.execute("SELECT * FROM candidates WHERE organ=? AND status='active' AND willing=1", (organ["organ"],)):
                if blood_compatible(donor["blood_type"], candidate["blood_type"]):
                    item = dict(candidate); item["match"] = self._score(donor, organ, candidate); rows.append(item)
            rows.sort(key=lambda item: (-item["match"]["total"], item["id"]))
            for index, item in enumerate(rows, 1): item["rank"] = index
            return {"donor": self._donor_payload(conn, donor, role, hospital), "organ": dict(organ), "candidates": rows}

    def propose(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "allocation_officer": raise ApiError(403, "allocate_forbidden", "只有分配员可以提出分配")
        organ_id, candidate_id, donor_id = body.get("organ_id"), body.get("candidate_id"), body.get("donor_id")
        if not isinstance(candidate_id, int): raise ApiError(400, "ids_required", "candidate_id 必填")
        with self.repo.tx() as conn:
            organ = self._resolve_organ(conn, organ_id, donor_id)
            self._sweep_expired(conn, actor, role, donor_id=organ["donor_id"])
            organ = conn.execute("SELECT * FROM organs WHERE id=?", (organ["id"],)).fetchone()
            donor = conn.execute("SELECT * FROM donors WHERE id=?", (organ["donor_id"],)).fetchone()
            candidate = conn.execute("SELECT * FROM candidates WHERE id=?", (candidate_id,)).fetchone()
            if not candidate: raise ApiError(404, "not_found", "候选患者不存在")
            if self._effective_organ_status(organ) == "expired": raise ApiError(409, "organ_expired", "器官可用窗口已结束")
            if organ["status"] != "available": raise ApiError(409, "donor_unavailable", "该器官当前不可分配")
            if candidate["status"] != "active" or not candidate["willing"]: raise ApiError(409, "candidate_unavailable", "候选患者当前不可接受分配")
            if organ["organ"] != candidate["organ"] or not blood_compatible(donor["blood_type"], candidate["blood_type"]):
                raise ApiError(409, "medical_mismatch", "器官类型或血型不匹配")
            if conn.execute("SELECT 1 FROM allocations WHERE organ_id=? AND status NOT IN ('withdrawn','expired','implanted')", (organ["id"],)).fetchone():
                raise ApiError(409, "already_allocated", "该器官已有有效分配")
            score = self._score(donor, organ, candidate)
            cur = conn.execute("""INSERT INTO allocations(organ_id,donor_id,candidate_id,score,created_by,created_at,updated_at)
                                  VALUES(?,?,?,?,?,?,?)""",
                               (organ["id"], donor["id"], candidate_id, score["total"], actor, iso(), iso()))
            allocation_id = cur.lastrowid
            conn.execute("UPDATE organs SET status='allocated',revision=revision+1 WHERE id=?", (organ["id"],))
            self._sync_donor_status(conn, donor["id"])
            Repository.audit(conn, allocation_id, donor["id"], actor, role, "allocation_proposed",
                             {"organ_id": organ["id"], "candidate_id": candidate_id, "score": score})
            return self._allocation(conn, allocation_id, role, "")

    def _allocation(self, conn: sqlite3.Connection, allocation_id: int, role: str, hospital: str) -> dict[str, Any]:
        row = conn.execute("""SELECT a.*,o.id organ_id,o.organ,o.status organ_status,o.available_at organ_available_at,o.expires_at organ_expires_at,
                                     d.blood_type donor_blood,d.hospital donor_hospital,d.region donor_region,d.status donor_status,
                                     c.patient_name,c.blood_type candidate_blood,c.hospital candidate_hospital,c.region candidate_region,c.urgency,c.wait_days
                              FROM allocations a
                              JOIN organs o ON o.id=a.organ_id JOIN donors d ON d.id=a.donor_id
                              JOIN candidates c ON c.id=a.candidate_id WHERE a.id=?""", (allocation_id,)).fetchone()
        if not row: raise ApiError(404, "allocation_not_found", "分配不存在")
        result = dict(row)
        # 兼容旧字段名：窗口与器官信息直接挂在分配上
        result["available_at"], result["expires_at"] = result["organ_available_at"], result["organ_expires_at"]
        if role == "hospital" and hospital not in {row["donor_hospital"], row["candidate_hospital"]}:
            raise ApiError(403, "allocation_forbidden", "医院不能查看与本机构无关的分配")
        if role == "hospital" and hospital != row["candidate_hospital"]:
            result["patient_name"] = "***"
        result["handoff"] = self._row(conn.execute("SELECT * FROM handoffs WHERE allocation_id=?", (allocation_id,)).fetchone())
        return result

    def _ensure_active(self, conn: sqlite3.Connection, allocation_id: int, actor: str, role: str) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM allocations WHERE id=?", (allocation_id,)).fetchone()
        if not row: raise ApiError(404, "allocation_not_found", "分配不存在")
        if row["status"] in CLOSED_ALLOCATION: raise ApiError(409, "allocation_closed", "分配已结束")
        self._sweep_expired(conn, actor, role, donor_id=row["donor_id"])
        row = conn.execute("SELECT * FROM allocations WHERE id=?", (allocation_id,)).fetchone()
        if row["status"] == "expired": raise ApiError(409, "organ_expired", "器官已经超过可用时间，禁止继续流转")
        if row["status"] in CLOSED_ALLOCATION: raise ApiError(409, "allocation_closed", "分配已结束")
        return row

    def accept(self, allocation_id: int, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "hospital": raise ApiError(403, "hospital_required", "只有接收医院可以接受器官")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必填")
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
            candidate = conn.execute("SELECT * FROM candidates WHERE id=?", (row["candidate_id"],)).fetchone()
            if candidate["hospital"] != hospital: raise ApiError(403, "wrong_hospital", "只能由候选患者所在医院接受")
            if row["status"] == "accepted": return self._allocation(conn, allocation_id, role, hospital)
            if row["status"] != "proposed": raise ApiError(409, "invalid_transition", "当前状态不能接受")
            if row["revision"] != expected: raise ApiError(409, "revision_conflict", "分配信息已发生变化")
            conn.execute("UPDATE allocations SET status='accepted',accepted_at=?,revision=revision+1,updated_at=? WHERE id=?", (iso(), iso(), allocation_id))
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "allocation_accepted", {"hospital": hospital})
            return self._allocation(conn, allocation_id, role, hospital)

    def mark_transit(self, allocation_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "allocation_officer": raise ApiError(403, "transit_forbidden", "只有分配员可以登记转运")
        temp = body.get("cold_chain_temp")
        if not isinstance(temp, (int, float)) or not -2 <= float(temp) <= 8:
            raise ApiError(409, "cold_chain_violation", "冷链温度必须保持在 -2°C 到 8°C")
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
            if row["status"] != "accepted": raise ApiError(409, "invalid_transition", "只有已接受分配可以进入转运")
            conn.execute("UPDATE allocations SET status='in_transit',cold_chain_temp=?,revision=revision+1,updated_at=? WHERE id=?", (float(temp), iso(), allocation_id))
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "transfer_started", {"cold_chain_temp": temp})
            return self._allocation(conn, allocation_id, role, "")

    def report_delay(self, allocation_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"allocation_officer", "hospital"}: raise ApiError(403, "delay_forbidden", "当前角色不能上报延误")
        minutes, reason = body.get("delayed_minutes"), str(body.get("reason", "")).strip()
        if not isinstance(minutes, int) or minutes <= 0 or not reason: raise ApiError(400, "invalid_delay", "delayed_minutes 必须为正整数且 reason 必填")
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
            conn.execute("UPDATE allocations SET delayed_minutes=delayed_minutes+?,revision=revision+1,updated_at=? WHERE id=?", (minutes, iso(), allocation_id))
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "logistics_delay", {"minutes": minutes, "reason": reason, "at_risk": minutes >= 120})
            return self._allocation(conn, allocation_id, role, "")

    def initiate_handoff(self, allocation_id: int, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "hospital": raise ApiError(403, "handoff_forbidden", "只有医院可以发起交接")
        expected = body.get("expected_revision"); target = str(body.get("to_hospital", "")).strip(); temp = body.get("cold_chain_temp")
        if not isinstance(expected, int) or not target or not isinstance(temp, (int, float)) or not -2 <= float(temp) <= 8:
            raise ApiError(400, "invalid_handoff", "expected_revision、to_hospital 和合规冷链温度必填")
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
            organ = conn.execute("SELECT * FROM organs WHERE id=?", (row["organ_id"],)).fetchone()
            donor = conn.execute("SELECT * FROM donors WHERE id=?", (organ["donor_id"],)).fetchone()
            candidate = conn.execute("SELECT * FROM candidates WHERE id=?", (row["candidate_id"],)).fetchone()
            if hospital != donor["hospital"]: raise ApiError(403, "wrong_hospital", "只能由器官来源医院发起交接")
            if target != candidate["hospital"]: raise ApiError(409, "wrong_destination", "交接目标必须与候选患者医院一致")
            if row["status"] != "in_transit": raise ApiError(409, "invalid_transition", "只有转运中分配可以交接")
            if row["revision"] != expected: raise ApiError(409, "revision_conflict", "分配版本已变化")
            try:
                cur = conn.execute("""INSERT INTO handoffs(allocation_id,from_hospital,to_hospital,cold_chain_temp,initiated_by,initiated_at)
                                      VALUES(?,?,?,?,?,?)""", (allocation_id, hospital, target, float(temp), actor, iso()))
            except sqlite3.IntegrityError as exc:
                raise ApiError(409, "handoff_exists", "交接已经登记") from exc
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "handoff_initiated", {"target": target, "cold_chain_temp": temp})
            return {"handoff": dict(conn.execute("SELECT * FROM handoffs WHERE id=?", (cur.lastrowid,)).fetchone()), "allocation": self._allocation(conn, allocation_id, role, hospital)}

    def accept_handoff(self, allocation_id: int, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "hospital": raise ApiError(403, "handoff_forbidden", "只有医院可以确认交接")
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
            handoff = conn.execute("SELECT * FROM handoffs WHERE allocation_id=?", (allocation_id,)).fetchone()
            if not handoff: raise ApiError(409, "handoff_missing", "尚未发起交接")
            if handoff["to_hospital"] != hospital: raise ApiError(403, "wrong_hospital", "只能由接收医院确认交接")
            if handoff["status"] == "accepted": return self._allocation(conn, allocation_id, role, hospital)
            conn.execute("UPDATE handoffs SET status='accepted',accepted_by=?,accepted_at=? WHERE id=?", (actor, iso(), handoff["id"]))
            conn.execute("UPDATE allocations SET status='handed_off',revision=revision+1,updated_at=? WHERE id=?", (iso(), allocation_id))
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "handoff_accepted", {"handoff_id": handoff["id"]})
            return self._allocation(conn, allocation_id, role, hospital)

    def implant(self, allocation_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "allocation_officer": raise ApiError(403, "implant_forbidden", "只有分配员可以确认植入")
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
            if row["status"] != "handed_off": raise ApiError(409, "invalid_transition", "交接完成后才能确认植入")
            conn.execute("UPDATE allocations SET status='implanted',implanted_at=?,revision=revision+1,updated_at=? WHERE id=?", (iso(), iso(), allocation_id))
            conn.execute("UPDATE organs SET status='used',revision=revision+1 WHERE id=?", (row["organ_id"],))
            self._sync_donor_status(conn, row["donor_id"])
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "organ_implanted",
                             {"organ_id": row["organ_id"], "candidate_id": row["candidate_id"]})
            return self._allocation(conn, allocation_id, role, "")

    def withdraw(self, allocation_id: int, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "hospital": raise ApiError(403, "withdraw_forbidden", "只有医院可以撤回")
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "撤回原因必填")
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
            candidate = conn.execute("SELECT * FROM candidates WHERE id=?", (row["candidate_id"],)).fetchone()
            if hospital != candidate["hospital"]: raise ApiError(403, "wrong_hospital", "只能由候选患者医院撤回")
            if row["status"] not in {"proposed", "accepted"}: raise ApiError(409, "invalid_transition", "转运开始后不能直接撤回")
            conn.execute("UPDATE allocations SET status='withdrawn',revision=revision+1,updated_at=? WHERE id=?", (iso(), allocation_id))
            # 只放回这一个器官；其它器官和同一捐献者的其它分配不受影响
            organ = conn.execute("SELECT * FROM organs WHERE id=?", (row["organ_id"],)).fetchone()
            organ_status = "available" if parse_time(organ["expires_at"]) > utcnow() else "expired"
            conn.execute("UPDATE organs SET status=?,revision=revision+1 WHERE id=?", (organ_status, organ["id"]))
            self._sync_donor_status(conn, row["donor_id"])
            Repository.audit(conn, allocation_id, row["donor_id"], actor, role, "allocation_withdrawn",
                             {"organ_id": organ["id"], "reason": reason})
            return self._allocation(conn, allocation_id, role, hospital)

    # ---------- 查询 ----------

    def _organ_progress(self, conn: sqlite3.Connection, organ: sqlite3.Row, now: datetime, role: str, hospital: str) -> dict[str, Any]:
        item = dict(organ)
        effective = self._effective_organ_status(organ, now)
        item["effective_status"] = effective
        latest = conn.execute("""SELECT a.*,c.patient_name,c.blood_type candidate_blood,c.hospital candidate_hospital,c.urgency,c.wait_days
                                 FROM allocations a JOIN candidates c ON c.id=a.candidate_id
                                 WHERE a.organ_id=? ORDER BY a.id DESC LIMIT 1""", (organ["id"],)).fetchone()
        handoff = None
        if latest:
            handoff = conn.execute("SELECT * FROM handoffs WHERE allocation_id=? ORDER BY id DESC LIMIT 1", (latest["id"],)).fetchone()
        stage = self._stage_for(effective, latest, handoff)
        label, blocked, waiting_for = ORGAN_STAGES[stage]
        item.update({"stage": stage, "stage_label": label, "blocked_reason": blocked, "waiting_for": waiting_for,
                     "flow_steps": FLOW_STEPS, "allocation_id": None, "allocation_status": None, "handoff": None,
                     "candidate": None})
        if latest:
            alloc_status = latest["status"]
            item["allocation_id"], item["allocation_status"] = latest["id"], alloc_status
            item["handoff"] = self._row(handoff)
            patient_name = latest["patient_name"]
            if role == "hospital" and hospital != latest["candidate_hospital"]: patient_name = "***"
            item["candidate"] = {"id": latest["candidate_id"], "patient_name": patient_name,
                                 "hospital": latest["candidate_hospital"], "blood_type": latest["candidate_blood"],
                                 "urgency": latest["urgency"], "wait_days": latest["wait_days"]}
            item["cold_chain_temp"] = latest["cold_chain_temp"]; item["delayed_minutes"] = latest["delayed_minutes"]
            item["updated_at"] = latest["updated_at"]
        return item

    @staticmethod
    def _stage_for(effective_status: str, latest: sqlite3.Row | None, handoff: sqlite3.Row | None) -> str:
        if effective_status == "expired": return "expired"
        if latest and latest["status"] == "implanted": return "done"
        if not latest or latest["status"] in {"withdrawn", "expired"}:
            if latest and latest["status"] == "withdrawn": return "waiting_reproposal"
            return "waiting_proposal"
        return {
            "proposed": "waiting_accept",
            "accepted": "waiting_transit",
            "in_transit": "waiting_handoff_accept" if handoff and handoff["status"] == "initiated" else "waiting_handoff",
            "handed_off": "waiting_implant",
        }[latest["status"]]

    def _donor_payload(self, conn: sqlite3.Connection, donor: sqlite3.Row, role: str, hospital: str) -> dict[str, Any]:
        now = utcnow()
        organ_rows = conn.execute("SELECT * FROM organs WHERE donor_id=? ORDER BY id", (donor["id"],)).fetchall()
        organs = [self._organ_progress(conn, row, now, role, hospital) for row in organ_rows]
        derived = self._derive_donor_status([o["effective_status"] for o in organs])
        result = dict(donor); result["status"] = derived
        result["organ_count"] = len(organs)
        result["organs"] = organs
        summary: dict[str, int] = {}
        for organ in organs: summary[organ["effective_status"]] = summary.get(organ["effective_status"], 0) + 1
        result["organ_summary"] = summary
        return result

    def get_donor(self, donor_id: int, role: str, hospital: str) -> dict[str, Any]:
        conn = self.repo.conn
        donor = conn.execute("SELECT * FROM donors WHERE id=?", (donor_id,)).fetchone()
        if not donor: raise ApiError(404, "donor_not_found", "捐献者不存在")
        if role == "viewer": raise ApiError(403, "donor_forbidden", "访客不能查看捐献者详情")
        if role == "hospital" and donor["hospital"] != hospital: raise ApiError(403, "donor_forbidden", "医院只能查看本机构捐献者")
        return self._donor_payload(conn, donor, role, hospital)

    def get_allocation(self, allocation_id: int, role: str, hospital: str) -> dict[str, Any]:
        return self._allocation(self.repo.conn, allocation_id, role, hospital)

    def audit(self, allocation_id: int, role: str) -> list[dict[str, Any]]:
        if role not in {"auditor", "allocation_officer"}: raise ApiError(403, "audit_forbidden", "当前角色不能查看审计记录")
        return [dict(r) for r in self.repo.conn.execute("SELECT actor,role,action,detail_json,created_at FROM audit_log WHERE allocation_id=? ORDER BY id", (allocation_id,))]

    def state(self, role: str, hospital: str) -> dict[str, Any]:
        conn = self.repo.conn
        if role == "hospital":
            donor_rows = conn.execute("SELECT * FROM donors WHERE hospital=? ORDER BY id DESC", (hospital,)).fetchall()
            candidates = [dict(r) for r in conn.execute("SELECT * FROM candidates WHERE hospital=?", (hospital,))]
            allocated = [dict(r) for r in conn.execute("SELECT a.* FROM allocations a JOIN candidates c ON c.id=a.candidate_id WHERE c.hospital=?", (hospital,))]
            donors = [self._donor_payload(conn, r, role, hospital) for r in donor_rows]
        elif role == "viewer":
            donors, candidates = [], []
            allocated = [dict(r) for r in conn.execute("SELECT id,status,updated_at FROM allocations WHERE status='implanted' ORDER BY id DESC")]
        else:
            donor_rows = conn.execute("SELECT * FROM donors ORDER BY id DESC").fetchall()
            donors = [self._donor_payload(conn, r, role, hospital) for r in donor_rows]
            candidates = [dict(r) for r in conn.execute("SELECT * FROM candidates ORDER BY id DESC")]
            allocated = [dict(r) for r in conn.execute("SELECT * FROM allocations ORDER BY id DESC")]
        return {"donors": donors, "candidates": candidates, "allocations": allocated, "server_time": iso()}


def json_reply(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode(); handler.send_response(status); handler.send_header("Content-Type", "application/json; charset=utf-8"); handler.send_header("Content-Length", str(len(raw))); handler.end_headers(); handler.wfile.write(raw)


class Handler(BaseHTTPRequestHandler):
    service: OrganAllocationService; web_root: Path
    def log_message(self, fmt: str, *args: Any) -> None: print(f"{self.address_string()} - {fmt % args}")
    def read_body(self) -> dict[str, Any]:
        size = int(self.headers.get("Content-Length", "0"))
        if not size: return {}
        try: body = json.loads(self.rfile.read(size))
        except json.JSONDecodeError as exc: raise ApiError(400, "invalid_json", "请求体不是有效 JSON") from exc
        if not isinstance(body, dict): raise ApiError(400, "invalid_json", "请求体必须是对象")
        return body
    def dispatch_get(self, path: str) -> tuple[int, Any]:
        if path == "/health": return 200, {"status": "ok", "service": "organ-allocation"}
        actor, role, hospital = self.service.identity(self.headers)
        if path == "/api/state": return 200, self.service.state(role, hospital)
        parts = [p for p in path.split("/") if p]
        if len(parts) == 4 and parts[:2] == ["api", "donors"] and parts[2].isdigit() and parts[3] == "ranking":
            return 200, self.service.ranking(int(parts[2]), role, hospital)
        if len(parts) == 4 and parts[:2] == ["api", "organs"] and parts[2].isdigit() and parts[3] == "ranking":
            return 200, self.service.ranking(None, role, hospital, organ_id=int(parts[2]))
        if len(parts) == 3 and parts[:2] == ["api", "donors"] and parts[2].isdigit():
            return 200, self.service.get_donor(int(parts[2]), role, hospital)
        if len(parts) == 3 and parts[:2] == ["api", "allocations"] and parts[2].isdigit():
            return 200, self.service.get_allocation(int(parts[2]), role, hospital)
        if len(parts) == 4 and parts[:2] == ["api", "allocations"] and parts[2].isdigit() and parts[3] == "audit":
            return 200, {"audit": self.service.audit(int(parts[2]), role)}
        raise ApiError(404, "not_found", "接口不存在")
    def dispatch_post(self, path: str) -> tuple[int, Any]:
        actor, role, hospital = self.service.identity(self.headers); body = self.read_body(); parts = [p for p in path.split("/") if p]
        actions = {
            "/api/donors": lambda: (201, self.service.register_donor(actor, role, body)),
            "/api/candidates": lambda: (201, self.service.register_candidate(actor, role, body)),
            "/api/allocations": lambda: (201, self.service.propose(actor, role, body)),
        }
        if path in actions: return actions[path]()
        if len(parts) == 4 and parts[:2] == ["api", "allocations"] and parts[2].isdigit():
            aid, action = int(parts[2]), parts[3]
            routes = {
                "accept": lambda: self.service.accept(aid, actor, role, hospital, body),
                "withdraw": lambda: self.service.withdraw(aid, actor, role, hospital, body),
                "transit": lambda: self.service.mark_transit(aid, actor, role, body),
                "delay": lambda: self.service.report_delay(aid, actor, role, body),
                "handoff": lambda: self.service.initiate_handoff(aid, actor, role, hospital, body),
                "handoff-accept": lambda: self.service.accept_handoff(aid, actor, role, hospital, body),
                "implant": lambda: self.service.implant(aid, actor, role, body),
            }
            if action in routes: return 200, routes[action]()
        raise ApiError(404, "not_found", "接口不存在")
    def handle_any(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/":
                raw = (self.web_root / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw); return
            status, payload = self.dispatch_get(parsed.path) if method == "GET" else self.dispatch_post(parsed.path)
            json_reply(self, status, payload)
        except ApiError as exc: json_reply(self, exc.status, {"error": exc.code, "message": exc.message})
        except Exception as exc: print(f"unhandled error: {exc!r}"); json_reply(self, 500, {"error": "internal_error", "message": str(exc)})
    def do_GET(self) -> None: self.handle_any("GET")
    def do_POST(self) -> None: self.handle_any("POST")


def create_server(db_path: str | Path, host: str = "127.0.0.1", port: int = PORT) -> ThreadingHTTPServer:
    service = OrganAllocationService(db_path); handler = type("OrganHandler", (Handler,), {"service": service, "web_root": Path(__file__).resolve().parent / "static"}); return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--host", default="127.0.0.1"); parser.add_argument("--port", type=int, default=PORT); parser.add_argument("--db", default=os.environ.get("ORGAN_DB", "organ_allocation.db")); args = parser.parse_args()
    server = create_server(args.db, args.host, args.port); print(f"organ-allocation listening on http://{args.host}:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__ == "__main__": main()
