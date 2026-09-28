#!/usr/bin/env python3
"""Organ allocation and cold-chain coordination service (standard library only)."""
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
ORGAN_STATUSES = {"available", "allocated", "implanted", "withdrawn", "expired"}
TERMINAL_ORGAN_STATUSES = {"implanted", "withdrawn", "expired"}
FLOW_STEPS = ["提出分配", "医院接受", "冷链转运", "双方交接", "确认植入"]
STEP_ORDER = {"proposed": 0, "accepted": 1, "in_transit": 2, "handed_off": 3, "implanted": 4}


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
        self.path = str(path)
        self.conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()
        self.conn.execute("PRAGMA foreign_keys=ON")

    def _init_schema(self) -> None:
        version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 1:
            return
        tables = {r[0] for r in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not tables:
            self.conn.executescript("""
            CREATE TABLE donors(
                id INTEGER PRIMARY KEY AUTOINCREMENT, blood_type TEXT NOT NULL, hospital TEXT NOT NULL,
                region TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1, created_by TEXT NOT NULL, created_at TEXT NOT NULL
            );
            CREATE TABLE organs(
                id INTEGER PRIMARY KEY AUTOINCREMENT, donor_id INTEGER NOT NULL REFERENCES donors(id), organ TEXT NOT NULL,
                clinical_match INTEGER NOT NULL DEFAULT 0, available_at TEXT NOT NULL, expires_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'available', revision INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL, created_at TEXT NOT NULL
            );
            CREATE INDEX idx_organs_donor ON organs(donor_id);
            CREATE TABLE candidates(
                id INTEGER PRIMARY KEY AUTOINCREMENT, patient_name TEXT NOT NULL, blood_type TEXT NOT NULL, organ TEXT NOT NULL,
                hospital TEXT NOT NULL, region TEXT NOT NULL, urgency INTEGER NOT NULL, wait_days INTEGER NOT NULL,
                willing INTEGER NOT NULL DEFAULT 1, clinical_match INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'active',
                created_by TEXT NOT NULL, created_at TEXT NOT NULL
            );
            CREATE TABLE allocations(
                id INTEGER PRIMARY KEY AUTOINCREMENT, organ_id INTEGER NOT NULL REFERENCES organs(id),
                candidate_id INTEGER NOT NULL REFERENCES candidates(id), score REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'proposed', revision INTEGER NOT NULL DEFAULT 1,
                cold_chain_temp REAL, delayed_minutes INTEGER NOT NULL DEFAULT 0, created_by TEXT NOT NULL, created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL, accepted_at TEXT, implanted_at TEXT
            );
            CREATE INDEX idx_allocations_organ ON allocations(organ_id);
            CREATE TABLE handoffs(
                id INTEGER PRIMARY KEY AUTOINCREMENT, allocation_id INTEGER NOT NULL REFERENCES allocations(id), from_hospital TEXT NOT NULL,
                to_hospital TEXT NOT NULL, cold_chain_temp REAL NOT NULL, status TEXT NOT NULL DEFAULT 'initiated',
                initiated_by TEXT NOT NULL, accepted_by TEXT, initiated_at TEXT NOT NULL, accepted_at TEXT,
                UNIQUE(allocation_id)
            );
            CREATE TABLE audit_log(
                id INTEGER PRIMARY KEY AUTOINCREMENT, allocation_id INTEGER, donor_id INTEGER, organ_id INTEGER,
                actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL
            );
            """)
            self.conn.execute("PRAGMA user_version=1")
            return
        self._migrate_v0(tables)

    def _migrate_v0(self, tables: set[str]) -> None:
        """把 v0 的「一供体一器官」结构升级为「供体-器官-分配」三级结构。"""
        self.conn.execute("PRAGMA foreign_keys=OFF")
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            if "donors" in tables:
                self.conn.execute("ALTER TABLE donors RENAME TO donors_old")
                self.conn.execute("""CREATE TABLE donors(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, blood_type TEXT NOT NULL, hospital TEXT NOT NULL,
                    region TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1, created_by TEXT NOT NULL, created_at TEXT NOT NULL)""")
                self.conn.execute("""INSERT INTO donors(id,blood_type,hospital,region,revision,created_by,created_at)
                                     SELECT id,blood_type,hospital,region,revision,created_by,created_at FROM donors_old""")
                self.conn.execute("""CREATE TABLE organs(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, donor_id INTEGER NOT NULL REFERENCES donors(id), organ TEXT NOT NULL,
                    clinical_match INTEGER NOT NULL DEFAULT 0, available_at TEXT NOT NULL, expires_at TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'available', revision INTEGER NOT NULL DEFAULT 1,
                    created_by TEXT NOT NULL, created_at TEXT NOT NULL)""")
                # v0 donor 状态映射：available→available, allocated→allocated, used→implanted, expired→expired
                self.conn.execute("""INSERT INTO organs(donor_id,organ,clinical_match,available_at,expires_at,status,revision,created_by,created_at)
                                     SELECT id,organ,clinical_match,available_at,expires_at,
                                            CASE status WHEN 'used' THEN 'implanted' WHEN 'available' THEN 'available' ELSE status END,
                                            revision,created_by,created_at FROM donors_old""")
                self.conn.execute("CREATE INDEX idx_organs_donor ON organs(donor_id)")
                self.conn.execute("DROP TABLE donors_old")
            else:
                self.conn.execute("""CREATE TABLE donors(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, blood_type TEXT NOT NULL, hospital TEXT NOT NULL,
                    region TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1, created_by TEXT NOT NULL, created_at TEXT NOT NULL)""")
                self.conn.execute("""CREATE TABLE organs(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, donor_id INTEGER NOT NULL REFERENCES donors(id), organ TEXT NOT NULL,
                    clinical_match INTEGER NOT NULL DEFAULT 0, available_at TEXT NOT NULL, expires_at TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'available', revision INTEGER NOT NULL DEFAULT 1,
                    created_by TEXT NOT NULL, created_at TEXT NOT NULL)""")
                self.conn.execute("CREATE INDEX idx_organs_donor ON organs(donor_id)")
            if "allocations" in tables:
                self.conn.execute("ALTER TABLE allocations RENAME TO allocations_old")
                self.conn.execute("""CREATE TABLE allocations(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, organ_id INTEGER NOT NULL REFERENCES organs(id),
                    candidate_id INTEGER NOT NULL REFERENCES candidates(id), score REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'proposed', revision INTEGER NOT NULL DEFAULT 1,
                    cold_chain_temp REAL, delayed_minutes INTEGER NOT NULL DEFAULT 0, created_by TEXT NOT NULL, created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL, accepted_at TEXT, implanted_at TEXT)""")
                self.conn.execute("""INSERT INTO allocations(id,organ_id,candidate_id,score,status,revision,cold_chain_temp,
                                    delayed_minutes,created_by,created_at,updated_at,accepted_at,implanted_at)
                                     SELECT a.id,o.id,a.candidate_id,a.score,a.status,a.revision,a.cold_chain_temp,
                                            a.delayed_minutes,a.created_by,a.created_at,a.updated_at,a.accepted_at,a.implanted_at
                                     FROM allocations_old a JOIN organs o ON o.donor_id=a.donor_id""")
                self.conn.execute("CREATE INDEX idx_allocations_organ ON allocations(organ_id)")
                self.conn.execute("DROP TABLE allocations_old")
            if "audit_log" in tables:
                self.conn.execute("ALTER TABLE audit_log RENAME TO audit_log_old")
                self.conn.execute("""CREATE TABLE audit_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, allocation_id INTEGER, donor_id INTEGER, organ_id INTEGER,
                    actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL)""")
                self.conn.execute("""INSERT INTO audit_log(id,allocation_id,donor_id,organ_id,actor,role,action,detail_json,created_at)
                                     SELECT l.id,l.allocation_id,l.donor_id,o.id,l.actor,l.role,l.action,l.detail_json,l.created_at
                                     FROM audit_log_old l LEFT JOIN organs o ON o.donor_id=l.donor_id""")
                self.conn.execute("DROP TABLE audit_log_old")
            else:
                self.conn.execute("""CREATE TABLE audit_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, allocation_id INTEGER, donor_id INTEGER, organ_id INTEGER,
                    actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL)""")
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK"); raise
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA user_version=1")

    @contextmanager
    def tx(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn; self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK"); raise

    @staticmethod
    def audit(conn: sqlite3.Connection, allocation_id: int | None, donor_id: int | None, actor: str, role: str,
              action: str, detail: dict[str, Any], organ_id: int | None = None) -> None:
        conn.execute("INSERT INTO audit_log(allocation_id,donor_id,organ_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
                     (allocation_id, donor_id, organ_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()))


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

    # ------------------------------------------------------------------ 登记
    @staticmethod
    def _organ_specs(body: dict[str, Any]) -> list[dict[str, Any]]:
        if isinstance(body.get("organs"), list) and body["organs"]:
            raw_specs, using_list = body["organs"], True
        else:
            raw_specs, using_list = [body], False
        specs: list[dict[str, Any]] = []
        for index, item in enumerate(raw_specs):
            where = f"organs[{index}]" if using_list else "登记信息"
            if not isinstance(item, dict): raise ApiError(400, "missing_fields", f"{where} 必须是对象")
            name = str(item.get("organ") or "").strip().lower()
            available_raw = item.get("available_at", body.get("available_at"))
            expires_raw = item.get("expires_at", body.get("expires_at"))
            if not name or not available_raw or not expires_raw:
                raise ApiError(400, "missing_fields", f"{where} 缺少 organ/available_at/expires_at")
            available, expires = parse_time(available_raw), parse_time(expires_raw)
            if expires <= available: raise ApiError(400, "invalid_window", f"{where} 可用窗口结束时间必须晚于开始时间")
            clinical = item.get("clinical_match", body.get("clinical_match", 0))
            if isinstance(clinical, bool) or not isinstance(clinical, int) or clinical < 0:
                raise ApiError(400, "invalid_match", f"{where} clinical_match 必须为非负整数")
            specs.append({"organ": name, "available_at": available, "expires_at": expires, "clinical_match": clinical})
        return specs

    def register_donor(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"coordinator", "allocation_officer"}: raise ApiError(403, "donor_forbidden", "当前角色不能登记器官")
        required = ("blood_type", "hospital", "region")
        if any(not body.get(k) for k in required): raise ApiError(400, "missing_fields", f"缺少字段: {', '.join(k for k in required if not body.get(k))}")
        blood = str(body["blood_type"]).upper()
        if blood not in {"O", "A", "B", "AB"}: raise ApiError(400, "invalid_blood_type", "血型必须为 O/A/B/AB")
        specs = self._organ_specs(body)
        with self.repo.tx() as conn:
            cur = conn.execute("INSERT INTO donors(blood_type,hospital,region,created_by,created_at) VALUES(?,?,?,?,?)",
                               (blood, body["hospital"], body["region"], actor, iso()))
            donor_id = cur.lastrowid
            for spec in specs:
                conn.execute("""INSERT INTO organs(donor_id,organ,clinical_match,available_at,expires_at,created_by,created_at)
                                VALUES(?,?,?,?,?,?,?)""",
                             (donor_id, spec["organ"], spec["clinical_match"], iso(spec["available_at"]), iso(spec["expires_at"]), actor, iso()))
                Repository.audit(conn, None, donor_id, actor, role, "organ_registered",
                                 {"organ": spec["organ"], "expires_at": iso(spec["expires_at"]), "clinical_match": spec["clinical_match"]})
            return self._donor_view(conn, donor_id, role, "")

    def add_organ(self, donor_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"coordinator", "allocation_officer"}: raise ApiError(403, "donor_forbidden", "当前角色不能追加器官")
        specs = self._organ_specs(body)
        with self.repo.tx() as conn:
            donor = conn.execute("SELECT * FROM donors WHERE id=?", (donor_id,)).fetchone()
            if not donor: raise ApiError(404, "donor_not_found", "捐献者不存在")
            for spec in specs:
                cur = conn.execute("""INSERT INTO organs(donor_id,organ,clinical_match,available_at,expires_at,created_by,created_at)
                                      VALUES(?,?,?,?,?,?,?)""",
                                   (donor_id, spec["organ"], spec["clinical_match"], iso(spec["available_at"]), iso(spec["expires_at"]), actor, iso()))
                conn.execute("UPDATE donors SET revision=revision+1 WHERE id=?", (donor_id,))
                Repository.audit(conn, None, donor_id, actor, role, "organ_registered",
                                 {"organ_id": cur.lastrowid, "organ": spec["organ"], "expires_at": iso(spec["expires_at"]),
                                  "clinical_match": spec["clinical_match"]})
            return self._donor_view(conn, donor_id, role, "")

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

    # ------------------------------------------------------------------ 视图
    @staticmethod
    def _effective_status(organ: sqlite3.Row, now: datetime | None = None) -> str:
        now = now or utcnow()
        if organ["status"] not in TERMINAL_ORGAN_STATUSES and parse_time(organ["expires_at"]) <= now:
            return "expired"
        return organ["status"]

    def _progress(self, organ: dict[str, Any], allocation: dict[str, Any] | None, handoff: dict[str, Any] | None) -> dict[str, Any]:
        status = organ["effective_status"]
        steps = [{"step": label, "state": "pending"} for label in FLOW_STEPS]
        stage: str | None
        blocked: str | None
        if status == "available":
            stage, blocked = None, "等待分配员提出分配"
        elif status == "implanted":
            stage, blocked = "implanted", None
            for step in steps: step["state"] = "done"
        elif status == "expired":
            stage, blocked = "expired", "器官已超过可用窗口，流程终止"
            if allocation and allocation["status"] != "implanted":
                reach = min(STEP_ORDER.get(allocation["status"], 0), 3)
                for index in range(reach + 1): steps[index]["state"] = "failed"
        elif status == "withdrawn":
            stage, blocked = "withdrawn", "分配已撤回，器官可另行分配"
            if allocation and allocation["status"] == "withdrawn":
                for index in range(STEP_ORDER["accepted"] + 1): steps[index]["state"] = "failed"
        else:
            if not allocation:
                return {"stage": None, "blocked": "等待分配员提出分配", "steps": steps}
            stage = allocation["status"]
            reach = STEP_ORDER.get(allocation["status"], 0)
            for index in range(reach):
                steps[index]["state"] = "done"
            steps[reach]["state"] = "current"
            if allocation["status"] == "proposed":
                blocked = f"等待候选患者医院 {allocation.get('candidate_hospital') or ''} 接受".strip()
            elif allocation["status"] == "accepted":
                blocked = "等待分配员登记冷链转运"
            elif allocation["status"] == "in_transit":
                if handoff and handoff["status"] == "initiated":
                    steps[2]["state"] = "done"; steps[3]["state"] = "current"
                    blocked = f"等待接收医院 {handoff['to_hospital']} 确认交接"
                else:
                    blocked = f"等待来源医院 {allocation.get('donor_hospital') or ''} 发起交接".strip()
            elif allocation["status"] == "handed_off":
                blocked = "等待分配员确认植入"
            else:
                blocked = "等待下一步处置"
        return {"stage": stage, "blocked": blocked, "steps": steps}

    def _latest_allocation(self, conn: sqlite3.Connection, organ_id: int, role: str, hospital: str) -> dict[str, Any] | None:
        row = conn.execute("""SELECT a.*,o.donor_id,o.organ donor_organ,o.expires_at organ_expires_at,
                                     d.blood_type donor_blood,d.hospital donor_hospital,d.region donor_region,
                                     c.patient_name,c.blood_type candidate_blood,c.hospital candidate_hospital,
                                     c.region candidate_region,c.urgency,c.wait_days
                              FROM allocations a
                              JOIN organs o ON o.id=a.organ_id JOIN donors d ON d.id=o.donor_id
                              JOIN candidates c ON c.id=a.candidate_id
                              WHERE a.organ_id=? ORDER BY a.id DESC LIMIT 1""", (organ_id,)).fetchone()
        if not row: return None
        result = dict(row)
        if role == "hospital" and hospital != result["candidate_hospital"]:
            result["patient_name"] = "***"
        result["handoff"] = self._row(conn.execute("SELECT * FROM handoffs WHERE allocation_id=? ORDER BY id DESC LIMIT 1",
                                                   (result["id"],)).fetchone())
        return result

    def _organ_view(self, conn: sqlite3.Connection, organ_id: int, role: str, hospital: str,
                    now: datetime | None = None) -> dict[str, Any]:
        now = now or utcnow()
        organ = conn.execute("SELECT * FROM organs WHERE id=?", (organ_id,)).fetchone()
        result = dict(organ)
        result["effective_status"] = self._effective_status(organ, now)
        remaining = parse_time(organ["expires_at"]) - now
        result["remaining_minutes"] = max(0, int(remaining.total_seconds() // 60)) if result["effective_status"] != "expired" else 0
        allocation = self._latest_allocation(conn, organ["id"], role, hospital)
        handoff = allocation["handoff"] if allocation else None
        result["allocation"] = allocation
        result["progress"] = self._progress(result, allocation, handoff)
        return result

    def _donor_view(self, conn: sqlite3.Connection, donor_id: int, role: str, hospital: str,
                    now: datetime | None = None) -> dict[str, Any]:
        donor = conn.execute("SELECT * FROM donors WHERE id=?", (donor_id,)).fetchone()
        if not donor: raise ApiError(404, "donor_not_found", "捐献者不存在")
        now = now or utcnow()
        organ_rows = list(conn.execute("SELECT * FROM organs WHERE donor_id=? ORDER BY id", (donor_id,)))
        if role == "hospital":
            related = hospital == donor["hospital"] or conn.execute(
                """SELECT 1 FROM allocations a JOIN organs o ON o.id=a.organ_id JOIN candidates c ON c.id=a.candidate_id
                   WHERE o.donor_id=? AND c.hospital=? LIMIT 1""", (donor_id, hospital)).fetchone()
            if not related:
                raise ApiError(403, "donor_forbidden", "医院不能查看与本机构无关的捐献者")
        organs: list[dict[str, Any]] = []
        effective_statuses: list[str] = []
        for organ_row in organ_rows:
            view = self._organ_view(conn, organ_row["id"], role, hospital, now)
            organs.append(view)
            effective_statuses.append(view["effective_status"])
        if not organs:
            donor_status = "available"
        elif all(s in TERMINAL_ORGAN_STATUSES for s in effective_statuses):
            donor_status = "used"
        elif any(s == "allocated" for s in effective_statuses):
            donor_status = "allocating"
        else:
            # 仅剩待分配/已失效器官，没有任何器官在流程中
            donor_status = "available"
        result = dict(donor)
        result["status"] = donor_status
        result["organ_count"] = len(organs)
        result["organs"] = organs
        return result

    def list_donors(self, role: str, hospital: str) -> dict[str, Any]:
        if role not in {"coordinator", "allocation_officer", "auditor", "hospital"}:
            raise ApiError(403, "donor_forbidden", "当前角色不能查看捐献者")
        conn = self.repo.conn
        donor_rows = list(conn.execute("SELECT * FROM donors ORDER BY id DESC"))
        donors: list[dict[str, Any]] = []
        for donor in donor_rows:
            try:
                donors.append(self._donor_view(conn, donor["id"], role, hospital))
            except ApiError as exc:
                if exc.code == "donor_forbidden": continue
                raise
        return {"donors": donors, "server_time": iso()}

    # ------------------------------------------------------------------ 排序
    def _ranking(self, conn: sqlite3.Connection, organ: sqlite3.Row, donor: sqlite3.Row) -> dict[str, Any]:
        rows = []
        for candidate in conn.execute("SELECT * FROM candidates WHERE organ=? AND status='active' AND willing=1", (organ["organ"],)):
            if blood_compatible(donor["blood_type"], candidate["blood_type"]):
                item = dict(candidate); item["match"] = self._score(organ, donor, candidate); rows.append(item)
        rows.sort(key=lambda item: (-item["match"]["total"], item["id"]))
        for index, item in enumerate(rows, 1): item["rank"] = index
        return rows

    @staticmethod
    def _score(organ: sqlite3.Row, donor: sqlite3.Row, candidate: sqlite3.Row) -> dict[str, float]:
        region = 250 if donor["region"] == candidate["region"] else 0
        clinical = min(organ["clinical_match"], candidate["clinical_match"]) * 30
        return {"urgency": candidate["urgency"] * 1000, "waiting": candidate["wait_days"] * 2, "region": region, "clinical": clinical,
                "total": candidate["urgency"] * 1000 + candidate["wait_days"] * 2 + region + clinical}

    def organ_ranking(self, organ_id: int, role: str, hospital: str, conn: sqlite3.Connection | None = None) -> dict[str, Any]:
        if role not in {"allocation_officer", "auditor"}: raise ApiError(403, "ranking_forbidden", "只有分配员或审计员可以查看完整候选排序")
        if conn is not None:
            return self._organ_ranking(conn, organ_id, role, hospital)
        with self.repo.tx() as conn:
            return self._organ_ranking(conn, organ_id, role, hospital)

    def _organ_ranking(self, conn: sqlite3.Connection, organ_id: int, role: str, hospital: str) -> dict[str, Any]:
        organ = conn.execute("SELECT * FROM organs WHERE id=?", (organ_id,)).fetchone()
        if not organ: raise ApiError(404, "organ_not_found", "器官不存在")
        donor = conn.execute("SELECT * FROM donors WHERE id=?", (organ["donor_id"],)).fetchone()
        return {"donor": dict(donor), "organ": self._organ_view(conn, organ["id"], role, hospital),
                "candidates": self._ranking(conn, organ, donor)}

    def donor_ranking(self, donor_id: int, role: str, hospital: str) -> dict[str, Any]:
        with self.repo.tx() as conn:
            donor = conn.execute("SELECT * FROM donors WHERE id=?", (donor_id,)).fetchone()
            if not donor: raise ApiError(404, "donor_not_found", "捐献者不存在")
            organs = list(conn.execute("SELECT * FROM organs WHERE donor_id=? ORDER BY id", (donor_id,)))
            active = [o for o in organs if self._effective_status(o) not in TERMINAL_ORGAN_STATUSES]
            if not active: raise ApiError(409, "donor_used", "该捐献者没有可分配的器官")
            if len(active) > 1: raise ApiError(409, "organ_required", "捐献者有多个器官，请按 organ_id 查询排序")
            return self._organ_ranking(conn, active[0]["id"], role, hospital)

    def _resolve_organ(self, conn: sqlite3.Connection, body: dict[str, Any]) -> tuple[sqlite3.Row, sqlite3.Row]:
        organ_id, donor_id = body.get("organ_id"), body.get("donor_id")
        if organ_id is not None:
            if not isinstance(organ_id, int): raise ApiError(400, "ids_required", "organ_id 必须为整数")
            organ = conn.execute("SELECT * FROM organs WHERE id=?", (organ_id,)).fetchone()
        elif donor_id is not None:
            if not isinstance(donor_id, int): raise ApiError(400, "ids_required", "donor_id 必须为整数")
            organs = list(conn.execute("SELECT * FROM organs WHERE donor_id=? AND status NOT IN ('implanted','withdrawn','expired')", (donor_id,)))
            organs = [o for o in organs if self._effective_status(o) != "expired"]
            if not organs: raise ApiError(409, "organ_unavailable", "该捐献者没有可分配器官")
            if len(organs) > 1: raise ApiError(409, "organ_required", "捐献者有多个器官，请指定 organ_id")
            organ = organs[0]
        else:
            raise ApiError(400, "ids_required", "organ_id 必填")
        if not organ: raise ApiError(404, "organ_not_found", "器官不存在")
        donor = conn.execute("SELECT * FROM donors WHERE id=?", (organ["donor_id"],)).fetchone()
        return organ, donor

    def propose(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "allocation_officer": raise ApiError(403, "allocate_forbidden", "只有分配员可以提出分配")
        candidate_id = body.get("candidate_id")
        if not isinstance(candidate_id, int): raise ApiError(400, "ids_required", "candidate_id 必填")
        with self.repo.tx() as conn:
            organ, donor = self._resolve_organ(conn, body)
            candidate = conn.execute("SELECT * FROM candidates WHERE id=?", (candidate_id,)).fetchone()
            if not candidate: raise ApiError(404, "not_found", "候选患者不存在")
            if organ["status"] != "available": raise ApiError(409, "organ_unavailable", "该器官当前不可分配")
            if parse_time(organ["expires_at"]) <= utcnow():
                conn.execute("UPDATE organs SET status='expired',revision=revision+1 WHERE id=?", (organ["id"],))
                Repository.audit(conn, None, donor["id"], actor, role, "organ_expired",
                                 {"candidate_id": candidate_id}, organ["id"])
                raise ApiError(409, "organ_expired", "器官可用窗口已结束")
            if candidate["status"] != "active" or not candidate["willing"]: raise ApiError(409, "candidate_unavailable", "候选患者当前不可接受分配")
            if organ["organ"] != candidate["organ"] or not blood_compatible(donor["blood_type"], candidate["blood_type"]):
                raise ApiError(409, "medical_mismatch", "器官类型或血型不匹配")
            if conn.execute("SELECT 1 FROM allocations WHERE organ_id=? AND status NOT IN ('withdrawn','expired')", (organ["id"],)).fetchone():
                raise ApiError(409, "already_allocated", "该器官已有有效分配")
            score = self._score(organ, donor, candidate)
            cur = conn.execute("""INSERT INTO allocations(organ_id,candidate_id,score,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?)""",
                               (organ["id"], candidate_id, score["total"], actor, iso(), iso()))
            allocation_id = cur.lastrowid
            conn.execute("UPDATE organs SET status='allocated',revision=revision+1 WHERE id=?", (organ["id"],))
            Repository.audit(conn, allocation_id, donor["id"], actor, role, "allocation_proposed",
                             {"candidate_id": candidate_id, "score": score}, organ["id"])
            return self._allocation(conn, allocation_id, role, "")

    def _allocation(self, conn: sqlite3.Connection, allocation_id: int, role: str, hospital: str) -> dict[str, Any]:
        row = conn.execute("""SELECT a.*,o.donor_id,o.organ donor_organ,o.available_at organ_available_at,o.expires_at organ_expires_at,o.status organ_status,
                                     d.blood_type donor_blood,d.hospital donor_hospital,d.region donor_region,
                                     c.patient_name,c.blood_type candidate_blood,c.hospital candidate_hospital,c.region candidate_region,c.urgency,c.wait_days
                              FROM allocations a
                              JOIN organs o ON o.id=a.organ_id JOIN donors d ON d.id=o.donor_id
                              JOIN candidates c ON c.id=a.candidate_id WHERE a.id=?""", (allocation_id,)).fetchone()
        if not row: raise ApiError(404, "allocation_not_found", "分配不存在")
        result = dict(row)
        if role == "hospital" and hospital not in {row["donor_hospital"], row["candidate_hospital"]}:
            raise ApiError(403, "allocation_forbidden", "医院不能查看与本机构无关的分配")
        if role == "hospital" and hospital != row["candidate_hospital"]:
            result["patient_name"] = "***"
        result["handoff"] = self._row(conn.execute("SELECT * FROM handoffs WHERE allocation_id=?", (allocation_id,)).fetchone())
        return result

    def _ensure_active(self, conn: sqlite3.Connection, allocation_id: int, actor: str, role: str) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM allocations WHERE id=?", (allocation_id,)).fetchone()
        if not row: raise ApiError(404, "allocation_not_found", "分配不存在")
        if row["status"] in {"withdrawn", "expired", "implanted"}: raise ApiError(409, "allocation_closed", "分配已结束")
        organ = conn.execute("SELECT * FROM organs WHERE id=?", (row["organ_id"],)).fetchone()
        if parse_time(organ["expires_at"]) <= utcnow():
            conn.execute("UPDATE allocations SET status='expired',revision=revision+1,updated_at=? WHERE id=?", (iso(), allocation_id))
            conn.execute("UPDATE organs SET status='expired',revision=revision+1 WHERE id=?", (organ["id"],))
            Repository.audit(conn, allocation_id, organ["donor_id"], actor, role, "allocation_expired",
                             {"reason": "organ_window_elapsed"}, organ["id"])
            raise ApiError(409, "organ_expired", "器官已经超过可用时间，禁止继续流转")
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
            Repository.audit(conn, allocation_id, self._donor_id(conn, row),
                             actor, role, "allocation_accepted", {"hospital": hospital}, row["organ_id"])
            return self._allocation(conn, allocation_id, role, hospital)

    @staticmethod
    def _donor_id(conn: sqlite3.Connection, row: sqlite3.Row) -> int:
        return conn.execute("SELECT donor_id FROM organs WHERE id=?", (row["organ_id"],)).fetchone()[0]

    def mark_transit(self, allocation_id: int, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "allocation_officer": raise ApiError(403, "transit_forbidden", "只有分配员可以登记转运")
        temp = body.get("cold_chain_temp")
        if not isinstance(temp, (int, float)) or isinstance(temp, bool) or not -2 <= float(temp) <= 8:
            raise ApiError(409, "cold_chain_violation", "冷链温度必须保持在 -2°C 到 8°C")
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
            if row["status"] != "accepted": raise ApiError(409, "invalid_transition", "只有已接受分配可以进入转运")
            conn.execute("UPDATE allocations SET status='in_transit',cold_chain_temp=?,revision=revision+1,updated_at=? WHERE id=?", (float(temp), iso(), allocation_id))
            Repository.audit(conn, allocation_id, self._donor_id(conn, row), actor, role, "transfer_started",
                             {"cold_chain_temp": temp}, row["organ_id"])
            return self._allocation(conn, allocation_id, role, hospital)

    def report_delay(self, allocation_id: int, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"allocation_officer", "hospital"}: raise ApiError(403, "delay_forbidden", "当前角色不能上报延误")
        minutes, reason = body.get("delayed_minutes"), str(body.get("reason", "")).strip()
        if not isinstance(minutes, int) or isinstance(minutes, bool) or minutes <= 0 or not reason:
            raise ApiError(400, "invalid_delay", "delayed_minutes 必须为正整数且 reason 必填")
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
            conn.execute("UPDATE allocations SET delayed_minutes=delayed_minutes+?,revision=revision+1,updated_at=? WHERE id=?", (minutes, iso(), allocation_id))
            Repository.audit(conn, allocation_id, self._donor_id(conn, row), actor, role, "logistics_delay",
                             {"minutes": minutes, "reason": reason, "at_risk": minutes >= 120}, row["organ_id"])
            return self._allocation(conn, allocation_id, role, hospital)

    def initiate_handoff(self, allocation_id: int, actor: str, role: str, hospital: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "hospital": raise ApiError(403, "handoff_forbidden", "只有医院可以发起交接")
        expected = body.get("expected_revision"); target = str(body.get("to_hospital", "")).strip(); temp = body.get("cold_chain_temp")
        if not isinstance(expected, int) or not target or not isinstance(temp, (int, float)) or isinstance(temp, bool) or not -2 <= float(temp) <= 8:
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
            Repository.audit(conn, allocation_id, donor["id"], actor, role, "handoff_initiated",
                             {"target": target, "cold_chain_temp": temp}, organ["id"])
            return {"handoff": dict(conn.execute("SELECT * FROM handoffs WHERE id=?", (cur.lastrowid,)).fetchone()),
                    "allocation": self._allocation(conn, allocation_id, role, hospital)}

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
            Repository.audit(conn, allocation_id, self._donor_id(conn, row), actor, role, "handoff_accepted",
                             {"handoff_id": handoff["id"]}, row["organ_id"])
            return self._allocation(conn, allocation_id, role, hospital)

    def implant(self, allocation_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "allocation_officer": raise ApiError(403, "implant_forbidden", "只有分配员可以确认植入")
        with self.repo.tx() as conn:
            row = self._ensure_active(conn, allocation_id, actor, role)
            if row["status"] != "handed_off": raise ApiError(409, "invalid_transition", "交接完成后才能确认植入")
            conn.execute("UPDATE allocations SET status='implanted',implanted_at=?,revision=revision+1,updated_at=? WHERE id=?", (iso(), iso(), allocation_id))
            conn.execute("UPDATE organs SET status='implanted',revision=revision+1 WHERE id=?", (row["organ_id"],))
            Repository.audit(conn, allocation_id, self._donor_id(conn, row), actor, role, "organ_implanted",
                             {"candidate_id": row["candidate_id"]}, row["organ_id"])
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
            organ = conn.execute("SELECT * FROM organs WHERE id=?", (row["organ_id"],)).fetchone()
            organ_status = "available" if parse_time(organ["expires_at"]) > utcnow() else "expired"
            conn.execute("UPDATE organs SET status=?,revision=revision+1 WHERE id=?", (organ_status, row["organ_id"]))
            Repository.audit(conn, allocation_id, organ["donor_id"], actor, role, "allocation_withdrawn",
                             {"reason": reason, "organ_status": organ_status}, organ["id"])
            return self._allocation(conn, allocation_id, role, hospital)

    def get_allocation(self, allocation_id: int, role: str, hospital: str) -> dict[str, Any]:
        return self._allocation(self.repo.conn, allocation_id, role, hospital)

    def get_donor(self, donor_id: int, role: str, hospital: str) -> dict[str, Any]:
        if role == "viewer": raise ApiError(403, "donor_forbidden", "当前角色不能查看捐献者")
        return self._donor_view(self.repo.conn, donor_id, role, hospital)

    def audit(self, allocation_id: int, role: str) -> list[dict[str, Any]]:
        if role not in {"auditor", "allocation_officer"}: raise ApiError(403, "audit_forbidden", "当前角色不能查看审计记录")
        return [dict(r) for r in self.repo.conn.execute(
            "SELECT actor,role,action,detail_json,created_at FROM audit_log WHERE allocation_id=? ORDER BY id", (allocation_id,))]

    def donor_audit(self, donor_id: int, role: str) -> dict[str, Any]:
        if role not in {"auditor", "allocation_officer"}: raise ApiError(403, "audit_forbidden", "当前角色不能查看审计记录")
        conn = self.repo.conn
        if not conn.execute("SELECT 1 FROM donors WHERE id=?", (donor_id,)).fetchone(): raise ApiError(404, "donor_not_found", "捐献者不存在")
        logs = [dict(r) for r in conn.execute(
            "SELECT organ_id,allocation_id,actor,role,action,detail_json,created_at FROM audit_log WHERE donor_id=? ORDER BY id", (donor_id,))]
        return {"donor_id": donor_id, "audit": logs}

    def state(self, role: str, hospital: str) -> dict[str, Any]:
        conn = self.repo.conn
        if role == "hospital":
            donor_ids = {r[0] for r in conn.execute("SELECT id FROM donors WHERE hospital=?", (hospital,))}
            donor_ids |= {r[0] for r in conn.execute(
                "SELECT DISTINCT d.id FROM donors d JOIN organs o ON o.donor_id=d.id JOIN allocations a ON a.organ_id=o.id JOIN candidates c ON c.id=a.candidate_id WHERE c.hospital=?",
                (hospital,))}
            donors = []
            for donor_id in sorted(donor_ids, reverse=True):
                try: donors.append(self._donor_view(conn, donor_id, role, hospital))
                except ApiError as exc:
                    if exc.code != "donor_forbidden": raise
            candidates = [dict(r) for r in conn.execute("SELECT * FROM candidates WHERE hospital=?", (hospital,))]
            allocated = [dict(r) for r in conn.execute(
                """SELECT a.* FROM allocations a JOIN candidates c ON c.id=a.candidate_id
                   JOIN organs o ON o.id=a.organ_id WHERE c.hospital=? OR o.donor_id IN
                   (SELECT id FROM donors WHERE hospital=?) ORDER BY a.id DESC""", (hospital, hospital))]
        elif role == "viewer":
            donors = []
            candidates = []
            allocated = [dict(r) for r in conn.execute("SELECT id,organ_id,status,updated_at FROM allocations WHERE status='implanted' ORDER BY id DESC")]
        else:
            donors = []
            for donor in conn.execute("SELECT id FROM donors ORDER BY id DESC"):
                donors.append(self._donor_view(conn, donor["id"], role, hospital))
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
        if len(parts) == 4 and parts[:2] == ["api", "organs"] and parts[2].isdigit() and parts[3] == "ranking":
            return 200, self.service.organ_ranking(int(parts[2]), role, hospital)
        if len(parts) == 4 and parts[:2] == ["api", "donors"] and parts[2].isdigit() and parts[3] == "ranking":
            return 200, self.service.donor_ranking(int(parts[2]), role, hospital)
        if len(parts) == 3 and parts[:2] == ["api", "donors"] and parts[2].isdigit():
            return 200, self.service.get_donor(int(parts[2]), role, hospital)
        if len(parts) == 4 and parts[:2] == ["api", "donors"] and parts[2].isdigit() and parts[3] == "audit":
            return 200, self.service.donor_audit(int(parts[2]), role)
        if path == "/api/donors":
            return 200, self.service.list_donors(role, hospital)
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
        if len(parts) == 4 and parts[:2] == ["api", "donors"] and parts[2].isdigit() and parts[3] == "organs":
            return 201, self.service.add_organ(int(parts[2]), actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "allocations"] and parts[2].isdigit():
            aid, action = int(parts[2]), parts[3]
            routes = {
                "accept": lambda: self.service.accept(aid, actor, role, hospital, body),
                "withdraw": lambda: self.service.withdraw(aid, actor, role, hospital, body),
                "transit": lambda: self.service.mark_transit(aid, actor, role, hospital, body),
                "delay": lambda: self.service.report_delay(aid, actor, role, hospital, body),
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
