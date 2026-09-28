import sys, tempfile, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, OrganAllocationService, iso, utcnow


class OrganFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.svc = OrganAllocationService(Path(self.tmp.name) / "test.db"); self.now = utcnow()

    def tearDown(self): self.tmp.cleanup()

    def donor(self, expires_days=2):
        return self.svc.register_donor("coord", "coordinator", {"blood_type": "O", "organ": "kidney", "hospital": "H1", "region": "East", "available_at": iso(self.now - timedelta(days=3)), "expires_at": iso(self.now + timedelta(days=expires_days)), "clinical_match": 8})

    def candidate(self, name="患者甲", hospital="H2", urgency=5, wait=500):
        return self.svc.register_candidate("coord", "coordinator", {"patient_name": name, "blood_type": "B", "organ": "kidney", "hospital": hospital, "region": "East", "urgency": urgency, "wait_days": wait, "willing": True, "clinical_match": 9})

    def test_complete_allocation_and_cold_chain_flow(self):
        donor, candidate = self.donor(), self.candidate()
        rank = self.svc.ranking(donor["id"], "allocation_officer", "")
        self.assertEqual(rank["candidates"][0]["id"], candidate["id"])
        allocation = self.svc.propose("allocator", "allocation_officer", {"donor_id": donor["id"], "candidate_id": candidate["id"]})
        accepted = self.svc.accept(allocation["id"], "hospital-h2", "hospital", "H2", {"expected_revision": 1})
        self.assertEqual(accepted["status"], "accepted")
        transit = self.svc.mark_transit(allocation["id"], "allocator", "allocation_officer", {"cold_chain_temp": 3.5})
        self.assertEqual(transit["status"], "in_transit")
        handoff = self.svc.initiate_handoff(allocation["id"], "hospital-h1", "hospital", "H1", {"expected_revision": transit["revision"], "to_hospital": "H2", "cold_chain_temp": 3.0})
        self.assertEqual(handoff["handoff"]["status"], "initiated")
        received = self.svc.accept_handoff(allocation["id"], "hospital-h2", "hospital", "H2", {})
        self.assertEqual(received["status"], "handed_off")
        implanted = self.svc.implant(allocation["id"], "allocator", "allocation_officer", {})
        self.assertEqual(implanted["status"], "implanted")
        audit = self.svc.audit(allocation["id"], "auditor")
        self.assertEqual([item["action"] for item in audit], ["allocation_proposed", "allocation_accepted", "transfer_started", "handoff_initiated", "handoff_accepted", "organ_implanted"])

    def test_expiry_privacy_and_single_allocation(self):
        expired = self.donor(expires_days=-1); candidate = self.candidate()
        with self.assertRaises(ApiError) as ctx:
            self.svc.propose("allocator", "allocation_officer", {"donor_id": expired["id"], "candidate_id": candidate["id"]})
        self.assertEqual(ctx.exception.code, "organ_expired")
        donor2 = self.donor(); allocation = self.svc.propose("allocator", "allocation_officer", {"donor_id": donor2["id"], "candidate_id": candidate["id"]})
        with self.assertRaises(ApiError) as ctx:
            self.svc.accept(allocation["id"], "wrong", "hospital", "H1", {"expected_revision": 1})
        self.assertEqual(ctx.exception.status, 403)
        masked = self.svc.get_allocation(allocation["id"], "hospital", "H1")
        self.assertEqual(masked["patient_name"], "***")
        with self.assertRaises(ApiError) as ctx:
            self.svc.propose("allocator", "allocation_officer", {"donor_id": donor2["id"], "candidate_id": candidate["id"]})
        self.assertEqual(ctx.exception.code, "donor_unavailable")
        other = self.candidate("患者乙", "H2", 4, 300)
        self.assertNotEqual(other["id"], candidate["id"])
        with self.assertRaises(ApiError) as ctx:
            self.svc.mark_transit(allocation["id"], "allocator", "allocation_officer", {"cold_chain_temp": 12})
        self.assertEqual(ctx.exception.code, "cold_chain_violation")

    def multi_donor(self):
        return self.svc.register_donor("coord", "coordinator", {
            "blood_type": "O", "hospital": "H1", "region": "East",
            "available_at": iso(self.now - timedelta(days=3)),
            "organs": [
                {"organ": "kidney", "expires_at": iso(self.now + timedelta(days=2))},
                {"organ": "liver", "expires_at": iso(self.now + timedelta(days=1))},
            ], "clinical_match": 8})

    def organ_candidate(self, organ, name, hospital="H2", urgency=4):
        return self.svc.register_candidate("coord", "coordinator", {"patient_name": name, "blood_type": "B", "organ": organ,
                                                                    "hospital": hospital, "region": "East", "urgency": urgency,
                                                                    "wait_days": 200, "willing": True, "clinical_match": 9})

    def test_multi_organs_allocate_and_flow_independently(self):
        donor = self.multi_donor()
        kidney_id, liver_id = [o["id"] for o in donor["organs"]]
        self.assertEqual([o["organ"] for o in donor["organs"]], ["kidney", "liver"])
        kidney_c = self.organ_candidate("kidney", "肾患者", urgency=5)
        liver_c = self.organ_candidate("liver", "肝患者", urgency=5)

        k_alloc = self.svc.propose("allocator", "allocation_officer", {"organ_id": kidney_id, "candidate_id": kidney_c["id"]})
        l_alloc = self.svc.propose("allocator", "allocation_officer", {"organ_id": liver_id, "candidate_id": liver_c["id"]})
        self.assertNotEqual(k_alloc["id"], l_alloc["id"])
        detail = self.svc.get_donor(donor["id"], "allocation_officer", "")
        self.assertEqual(detail["status"], "allocated")
        self.assertEqual({o["organ"]: o["stage"] for o in detail["organs"]},
                         {"kidney": "waiting_accept", "liver": "waiting_accept"})
        blocked = {o["organ"]: o["blocked_reason"] for o in detail["organs"]}
        self.assertIn("接收医院", blocked["kidney"])

        # 肾走完完整流程；肝停在已接受，二者互不干扰
        self.svc.accept(k_alloc["id"], "h2", "hospital", "H2", {"expected_revision": 1})
        self.svc.accept(l_alloc["id"], "h2", "hospital", "H2", {"expected_revision": 1})
        self.svc.mark_transit(k_alloc["id"], "allocator", "allocation_officer", {"cold_chain_temp": 3.5})
        self.svc.initiate_handoff(k_alloc["id"], "h1", "hospital", "H1",
                                  {"expected_revision": 3, "to_hospital": "H2", "cold_chain_temp": 3.0})
        self.svc.accept_handoff(k_alloc["id"], "h2", "hospital", "H2", {})
        implanted = self.svc.implant(k_alloc["id"], "allocator", "allocation_officer", {})
        self.assertEqual(implanted["status"], "implanted")
        detail = self.svc.get_donor(donor["id"], "allocation_officer", "")
        by_organ = {o["organ"]: o for o in detail["organs"]}
        self.assertEqual(by_organ["kidney"]["effective_status"], "used")
        self.assertEqual(by_organ["kidney"]["stage"], "done")
        self.assertEqual(by_organ["liver"]["effective_status"], "allocated")
        self.assertEqual(by_organ["liver"]["stage"], "waiting_transit")
        self.assertEqual(detail["status"], "allocated")  # 肝仍在流转，捐献者未用完

    def test_withdraw_one_organ_frees_only_that_organ(self):
        donor = self.multi_donor()
        kidney_id, liver_id = [o["id"] for o in donor["organs"]]
        kidney_c = self.organ_candidate("kidney", "肾患者")
        liver_c = self.organ_candidate("liver", "肝患者")
        k_alloc = self.svc.propose("allocator", "allocation_officer", {"organ_id": kidney_id, "candidate_id": kidney_c["id"]})
        l_alloc = self.svc.propose("allocator", "allocation_officer", {"organ_id": liver_id, "candidate_id": liver_c["id"]})

        withdrawn = self.svc.withdraw(l_alloc["id"], "h2", "hospital", "H2", {"reason": "患者临时不宜手术"})
        self.assertEqual(withdrawn["status"], "withdrawn")
        detail = self.svc.get_donor(donor["id"], "allocation_officer", "")
        by_organ = {o["organ"]: o for o in detail["organs"]}
        self.assertEqual(by_organ["liver"]["effective_status"], "available")
        self.assertEqual(by_organ["liver"]["stage"], "waiting_reproposal")
        self.assertEqual(by_organ["kidney"]["effective_status"], "allocated")
        self.assertEqual(detail["status"], "allocated")

        # 肝可重新提出分配，肾的分配不受撤回影响
        liver_c2 = self.organ_candidate("liver", "肝患者乙")
        l_alloc2 = self.svc.propose("allocator", "allocation_officer", {"organ_id": liver_id, "candidate_id": liver_c2["id"]})
        k_now = self.svc.get_allocation(k_alloc["id"], "allocation_officer", "")
        self.assertEqual(k_now["status"], "proposed")
        self.assertNotEqual(l_alloc2["id"], l_alloc["id"])

    def test_expiry_of_one_organ_does_not_block_others_and_donor_used_last(self):
        donor = self.multi_donor()
        kidney_id, liver_id = [o["id"] for o in donor["organs"]]
        kidney_c = self.organ_candidate("kidney", "肾患者")
        liver_c = self.organ_candidate("liver", "肝患者")
        k_alloc = self.svc.propose("allocator", "allocation_officer", {"organ_id": kidney_id, "candidate_id": kidney_c["id"]})
        l_alloc = self.svc.propose("allocator", "allocation_officer", {"organ_id": liver_id, "candidate_id": liver_c["id"]})
        self.svc.accept(k_alloc["id"], "h2", "hospital", "H2", {"expected_revision": 1})

        # 肝过期：只终止肝及其分配
        self.svc.repo.conn.execute("UPDATE organs SET expires_at=? WHERE id=?", (iso(self.now - timedelta(minutes=1)), liver_id))
        with self.assertRaises(ApiError) as ctx:
            self.svc.accept(l_alloc["id"], "h2", "hospital", "H2", {"expected_revision": 1})
        self.assertEqual(ctx.exception.code, "organ_expired")
        detail = self.svc.get_donor(donor["id"], "allocation_officer", "")
        by_organ = {o["organ"]: o for o in detail["organs"]}
        self.assertEqual(by_organ["liver"]["stage"], "expired")
        self.assertEqual(by_organ["kidney"]["effective_status"], "allocated")

        # 肾照常推进直到植入
        self.svc.mark_transit(k_alloc["id"], "allocator", "allocation_officer", {"cold_chain_temp": 2.0})
        self.svc.initiate_handoff(k_alloc["id"], "h1", "hospital", "H1",
                                  {"expected_revision": 3, "to_hospital": "H2", "cold_chain_temp": 2.5})
        self.svc.accept_handoff(k_alloc["id"], "h2", "hospital", "H2", {})
        self.svc.implant(k_alloc["id"], "allocator", "allocation_officer", {})
        detail = self.svc.get_donor(donor["id"], "allocation_officer", "")
        self.assertEqual(detail["status"], "used")  # 肾植入、肝过期，全部结束才用完
        self.assertEqual(detail["organ_summary"], {"used": 1, "expired": 1})

    def test_legacy_v1_database_migrates_to_per_organ_model(self):
        import sqlite3
        db = Path(self.tmp.name) / "legacy.db"
        raw = sqlite3.connect(db)
        t0, t1, t2, t3, t4 = (iso(self.now - timedelta(days=3)), iso(self.now + timedelta(days=2)),
                              iso(self.now), iso(self.now), iso(self.now))
        raw.executescript(f"""
        CREATE TABLE donors(id INTEGER PRIMARY KEY AUTOINCREMENT,blood_type TEXT,organ TEXT,hospital TEXT,region TEXT,
            available_at TEXT,expires_at TEXT,clinical_match INTEGER DEFAULT 0,status TEXT DEFAULT 'available',
            revision INTEGER DEFAULT 1,created_by TEXT,created_at TEXT);
        CREATE TABLE candidates(id INTEGER PRIMARY KEY AUTOINCREMENT,patient_name TEXT,blood_type TEXT,organ TEXT,
            hospital TEXT,region TEXT,urgency INTEGER,wait_days INTEGER,willing INTEGER DEFAULT 1,
            clinical_match INTEGER DEFAULT 0,status TEXT DEFAULT 'active',created_by TEXT,created_at TEXT);
        CREATE TABLE allocations(id INTEGER PRIMARY KEY AUTOINCREMENT,donor_id INTEGER NOT NULL UNIQUE,candidate_id INTEGER,
            score REAL,status TEXT DEFAULT 'proposed',revision INTEGER DEFAULT 1,cold_chain_temp REAL,
            delayed_minutes INTEGER DEFAULT 0,created_by TEXT,created_at TEXT,updated_at TEXT,accepted_at TEXT,implanted_at TEXT);
        CREATE TABLE handoffs(id INTEGER PRIMARY KEY AUTOINCREMENT,allocation_id INTEGER,from_hospital TEXT,to_hospital TEXT,
            cold_chain_temp REAL,status TEXT DEFAULT 'initiated',initiated_by TEXT,accepted_by TEXT,
            initiated_at TEXT,accepted_at TEXT,UNIQUE(allocation_id));
        CREATE TABLE audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT,allocation_id INTEGER,donor_id INTEGER,actor TEXT,
            role TEXT,action TEXT,detail_json TEXT,created_at TEXT);
        INSERT INTO donors VALUES(1,'O','kidney','H1','East','{t0}','{t1}',8,'allocated',1,'c','{t2}');
        INSERT INTO candidates VALUES(1,'旧患者','B','kidney','H2','East',5,100,1,9,'active','c','{t2}');
        INSERT INTO allocations VALUES(1,1,1,5000.0,'accepted',2,NULL,0,'a','{t2}','{t3}','{t4}',NULL);
        """)
        raw.commit(); raw.close()

        svc = OrganAllocationService(db)
        detail = svc.get_donor(1, "allocation_officer", "")
        self.assertEqual(detail["organ_count"], 1)
        self.assertEqual(detail["organs"][0]["organ"], "kidney")
        self.assertEqual(detail["organs"][0]["effective_status"], "allocated")
        allocation = svc.get_allocation(1, "allocation_officer", "")
        self.assertEqual(allocation["organ_id"], detail["organs"][0]["id"])
        self.assertEqual(allocation["status"], "accepted")


if __name__ == "__main__": unittest.main()
