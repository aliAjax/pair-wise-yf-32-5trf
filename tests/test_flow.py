import sys, tempfile, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, OrganAllocationService, iso, utcnow


class OrganFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.svc = OrganAllocationService(Path(self.tmp.name) / "test.db"); self.now = utcnow()

    def tearDown(self): self.tmp.cleanup()

    def donor(self, expires_days=2, organ="kidney"):
        return self.svc.register_donor("coord", "coordinator", {"blood_type": "O", "organ": organ, "hospital": "H1", "region": "East", "available_at": iso(self.now - timedelta(days=3)), "expires_at": iso(self.now + timedelta(days=expires_days)), "clinical_match": 8})

    def multi_donor(self, expires_days=2, organs=("kidney", "liver")):
        return self.svc.register_donor("coord", "coordinator", {
            "blood_type": "O", "hospital": "H1", "region": "East",
            "organs": [{"organ": name, "available_at": iso(self.now - timedelta(days=3)),
                        "expires_at": iso(self.now + timedelta(days=expires_days)), "clinical_match": 8} for name in organs]})

    def candidate(self, name="患者甲", hospital="H2", urgency=5, wait=500, organ="kidney"):
        return self.svc.register_candidate("coord", "coordinator", {"patient_name": name, "blood_type": "B", "organ": organ, "hospital": hospital, "region": "East", "urgency": urgency, "wait_days": wait, "willing": True, "clinical_match": 9})

    def organ_id(self, donor, index=0):
        return donor["organs"][index]["id"]

    def test_complete_allocation_and_cold_chain_flow(self):
        donor, candidate = self.donor(), self.candidate()
        organ_id = self.organ_id(donor)
        rank = self.svc.organ_ranking(organ_id, "allocation_officer", "")
        self.assertEqual(rank["candidates"][0]["id"], candidate["id"])
        allocation = self.svc.propose("allocator", "allocation_officer", {"organ_id": organ_id, "candidate_id": candidate["id"]})
        accepted = self.svc.accept(allocation["id"], "hospital-h2", "hospital", "H2", {"expected_revision": 1})
        self.assertEqual(accepted["status"], "accepted")
        transit = self.svc.mark_transit(allocation["id"], "allocator", "allocation_officer", "", {"cold_chain_temp": 3.5})
        self.assertEqual(transit["status"], "in_transit")
        handoff = self.svc.initiate_handoff(allocation["id"], "hospital-h1", "hospital", "H1", {"expected_revision": transit["revision"], "to_hospital": "H2", "cold_chain_temp": 3.0})
        self.assertEqual(handoff["handoff"]["status"], "initiated")
        received = self.svc.accept_handoff(allocation["id"], "hospital-h2", "hospital", "H2", {})
        self.assertEqual(received["status"], "handed_off")
        implanted = self.svc.implant(allocation["id"], "allocator", "allocation_officer", {})
        self.assertEqual(implanted["status"], "implanted")
        audit = self.svc.audit(allocation["id"], "auditor")
        self.assertEqual([item["action"] for item in audit], ["allocation_proposed", "allocation_accepted", "transfer_started", "handoff_initiated", "handoff_accepted", "organ_implanted"])
        final = self.svc.get_donor(donor["id"], "allocation_officer", "")
        self.assertEqual(final["status"], "used")
        self.assertEqual(final["organs"][0]["effective_status"], "implanted")

    def test_expiry_privacy_and_single_allocation(self):
        expired = self.donor(expires_days=-1); candidate = self.candidate()
        with self.assertRaises(ApiError) as ctx:
            self.svc.propose("allocator", "allocation_officer", {"organ_id": self.organ_id(expired), "candidate_id": candidate["id"]})
        self.assertEqual(ctx.exception.code, "organ_expired")
        donor2 = self.donor(); organ_id = self.organ_id(donor2)
        allocation = self.svc.propose("allocator", "allocation_officer", {"organ_id": organ_id, "candidate_id": candidate["id"]})
        with self.assertRaises(ApiError) as ctx:
            self.svc.accept(allocation["id"], "wrong", "hospital", "H1", {"expected_revision": 1})
        self.assertEqual(ctx.exception.status, 403)
        masked = self.svc.get_allocation(allocation["id"], "hospital", "H1")
        self.assertEqual(masked["patient_name"], "***")
        with self.assertRaises(ApiError) as ctx:
            self.svc.propose("allocator", "allocation_officer", {"organ_id": organ_id, "candidate_id": candidate["id"]})
        self.assertEqual(ctx.exception.code, "organ_unavailable")
        other = self.candidate("患者乙", "H2", 4, 300)
        self.assertNotEqual(other["id"], candidate["id"])
        with self.assertRaises(ApiError) as ctx:
            self.svc.mark_transit(allocation["id"], "allocator", "allocation_officer", "", {"cold_chain_temp": 12})
        self.assertEqual(ctx.exception.code, "cold_chain_violation")

    def test_parallel_organs_progress_independently(self):
        donor = self.multi_donor()
        kidney_id, liver_id = self.organ_id(donor, 0), self.organ_id(donor, 1)
        kidney_patient = self.candidate("肾衰患者", "H2", 5, 500, "kidney")
        liver_patient = self.candidate("肝衰患者", "H3", 4, 200, "liver")
        self.assertEqual(self.svc.get_donor(donor["id"], "coordinator", "")["status"], "available")

        a_kidney = self.svc.propose("allocator", "allocation_officer", {"organ_id": kidney_id, "candidate_id": kidney_patient["id"]})
        a_liver = self.svc.propose("allocator", "allocation_officer", {"organ_id": liver_id, "candidate_id": liver_patient["id"]})
        view = self.svc.get_donor(donor["id"], "coordinator", "")
        self.assertEqual(view["status"], "allocating")
        statuses = {o["organ"]: o["effective_status"] for o in view["organs"]}
        self.assertEqual(statuses, {"kidney": "allocated", "liver": "allocated"})

        # 肾走完接受→转运→交接→植入，肝始终停在 proposed，互不干扰
        self.svc.accept(a_kidney["id"], "h2", "hospital", "H2", {"expected_revision": 1})
        self.svc.mark_transit(a_kidney["id"], "allocator", "allocation_officer", "", {"cold_chain_temp": 3.0})
        self.svc.initiate_handoff(a_kidney["id"], "h1", "hospital", "H1", {"expected_revision": 3, "to_hospital": "H2", "cold_chain_temp": 3.0})
        self.svc.accept_handoff(a_kidney["id"], "h2", "hospital", "H2", {})
        self.svc.implant(a_kidney["id"], "allocator", "allocation_officer", {})

        view = self.svc.get_donor(donor["id"], "coordinator", "")
        self.assertEqual(view["status"], "allocating", "一个器官植入后，另一个仍在推进，捐献者不能显示已用完")
        organs = {o["organ"]: o for o in view["organs"]}
        self.assertEqual(organs["kidney"]["effective_status"], "implanted")
        self.assertEqual(organs["liver"]["allocation"]["status"], "proposed")
        self.assertEqual(organs["liver"]["progress"]["blocked"], "等待候选患者医院 H3 接受")

        # 肝继续走完
        self.svc.accept(a_liver["id"], "h3", "hospital", "H3", {"expected_revision": 1})
        self.svc.mark_transit(a_liver["id"], "allocator", "allocation_officer", "", {"cold_chain_temp": 2.0})
        self.svc.initiate_handoff(a_liver["id"], "h1", "hospital", "H1", {"expected_revision": 3, "to_hospital": "H3", "cold_chain_temp": 2.0})
        self.svc.accept_handoff(a_liver["id"], "h3", "hospital", "H3", {})
        self.svc.implant(a_liver["id"], "allocator", "allocation_officer", {})
        view = self.svc.get_donor(donor["id"], "coordinator", "")
        self.assertEqual(view["status"], "used", "所有器官结束后捐献者才显示已用完")
        self.assertTrue(all(o["effective_status"] == "implanted" for o in view["organs"]))

    def test_withdraw_one_organ_frees_only_that_organ(self):
        donor = self.multi_donor()
        kidney_id, liver_id = self.organ_id(donor, 0), self.organ_id(donor, 1)
        kidney1 = self.candidate("肾甲", "H2", 5, 500, "kidney")
        kidney2 = self.candidate("肾乙", "H4", 3, 100, "kidney")
        liver_patient = self.candidate("肝衰患者", "H3", 4, 200, "liver")
        a_kidney = self.svc.propose("allocator", "allocation_officer", {"organ_id": kidney_id, "candidate_id": kidney1["id"]})
        a_liver = self.svc.propose("allocator", "allocation_officer", {"organ_id": liver_id, "candidate_id": liver_patient["id"]})

        # H2 撤回肾；肝的 proposed 分配不受牵动
        self.svc.withdraw(a_kidney["id"], "h2", "hospital", "H2", {"reason": "患者临时不宜手术"})
        view = self.svc.get_donor(donor["id"], "coordinator", "")
        organs = {o["organ"]: o for o in view["organs"]}
        self.assertEqual(organs["kidney"]["effective_status"], "available", "撤回后该器官应回到可分配")
        self.assertEqual(organs["liver"]["allocation"]["status"], "proposed", "撤回不得牵动另一器官")
        self.assertEqual(view["status"], "allocating")

        # 撤回的肾可重新提出给另一位患者；肝照常接受推进
        a_kidney2 = self.svc.propose("allocator", "allocation_officer", {"organ_id": kidney_id, "candidate_id": kidney2["id"]})
        self.assertEqual(a_kidney2["status"], "proposed")
        accepted_liver = self.svc.accept(a_liver["id"], "h3", "hospital", "H3", {"expected_revision": 1})
        self.assertEqual(accepted_liver["status"], "accepted")

        # 第二版肾分配的审计与第一版互不覆盖
        audit = self.svc.donor_audit(donor["id"], "auditor")["audit"]
        self.assertEqual(sum(1 for item in audit if item["action"] == "allocation_proposed"), 3)
        self.assertEqual(sum(1 for item in audit if item["action"] == "allocation_withdrawn"), 1)

    def test_expired_organ_does_not_block_others(self):
        donor = self.svc.register_donor("coord", "coordinator", {
            "blood_type": "O", "hospital": "H1", "region": "East",
            "organs": [
                {"organ": "kidney", "available_at": iso(self.now - timedelta(days=3)), "expires_at": iso(self.now - timedelta(hours=1)), "clinical_match": 8},
                {"organ": "liver", "available_at": iso(self.now - timedelta(days=3)), "expires_at": iso(self.now + timedelta(days=1)), "clinical_match": 8},
            ]})
        kidney_id, liver_id = self.organ_id(donor, 0), self.organ_id(donor, 1)
        liver_patient = self.candidate("肝衰患者", "H3", 4, 200, "liver")

        a_liver = self.svc.propose("allocator", "allocation_officer", {"organ_id": liver_id, "candidate_id": liver_patient["id"]})
        view = self.svc.get_donor(donor["id"], "coordinator", "")
        organs = {o["organ"]: o for o in view["organs"]}
        self.assertEqual(organs["kidney"]["effective_status"], "expired", "过期在读取视图时即可体现")
        self.assertEqual(organs["liver"]["effective_status"], "allocated")
        self.assertEqual(view["status"], "allocating", "一个器官失效不影响另一个推进")

        self.svc.accept(a_liver["id"], "h3", "hospital", "H3", {"expected_revision": 1})
        self.svc.mark_transit(a_liver["id"], "allocator", "allocation_officer", "", {"cold_chain_temp": 2.0})
        self.svc.initiate_handoff(a_liver["id"], "h1", "hospital", "H1", {"expected_revision": 3, "to_hospital": "H3", "cold_chain_temp": 2.0})
        self.svc.accept_handoff(a_liver["id"], "h3", "hospital", "H3", {})
        self.svc.implant(a_liver["id"], "allocator", "allocation_officer", {})
        view = self.svc.get_donor(donor["id"], "coordinator", "")
        self.assertEqual(view["status"], "used", "器官全部结束或失效后捐献者才已用完")

        # 过期器官不能再提出分配
        kidney_patient = self.candidate("肾患者", "H2", 5, 500, "kidney")
        with self.assertRaises(ApiError) as ctx:
            self.svc.propose("allocator", "allocation_officer", {"organ_id": kidney_id, "candidate_id": kidney_patient["id"]})
        self.assertEqual(ctx.exception.code, "organ_expired")

    def test_add_organ_revives_used_donor(self):
        donor = self.donor()
        patient = self.candidate()
        organ_id = self.organ_id(donor)
        allocation = self.svc.propose("allocator", "allocation_officer", {"organ_id": organ_id, "candidate_id": patient["id"]})
        self.svc.accept(allocation["id"], "h2", "hospital", "H2", {"expected_revision": 1})
        self.svc.mark_transit(allocation["id"], "allocator", "allocation_officer", "", {"cold_chain_temp": 3.0})
        self.svc.initiate_handoff(allocation["id"], "h1", "hospital", "H1", {"expected_revision": 3, "to_hospital": "H2", "cold_chain_temp": 3.0})
        self.svc.accept_handoff(allocation["id"], "h2", "hospital", "H2", {})
        self.svc.implant(allocation["id"], "allocator", "allocation_officer", {})
        self.assertEqual(self.svc.get_donor(donor["id"], "coordinator", "")["status"], "used")

        # 多器官捐献场景：事后补登新器官，捐献者重新可用
        view = self.svc.add_organ(donor["id"], "coord", "coordinator",
                                  {"organ": "liver", "available_at": iso(self.now - timedelta(hours=1)),
                                   "expires_at": iso(self.now + timedelta(days=1)), "clinical_match": 6})
        self.assertEqual(view["status"], "available", "已植入器官结束、新器官待分配且无在途处置，捐献者回到可分配")
        self.assertEqual(view["organ_count"], 2)
        self.assertEqual([o["organ"] for o in view["organs"]], ["kidney", "liver"])

    def test_progress_describes_each_stage(self):
        donor = self.multi_donor()
        kidney_id, liver_id = self.organ_id(donor, 0), self.organ_id(donor, 1)
        view = self.svc.get_donor(donor["id"], "coordinator", "")
        for organ in view["organs"]:
            self.assertEqual([s["state"] for s in organ["progress"]["steps"]], ["pending"] * 5)
            self.assertEqual(organ["progress"]["blocked"], "等待分配员提出分配")

        patient = self.candidate("肾甲", "H2", 5, 500, "kidney")
        allocation = self.svc.propose("allocator", "allocation_officer", {"organ_id": kidney_id, "candidate_id": patient["id"]})
        view = self.svc.get_donor(donor["id"], "coordinator", "")
        kidney = next(o for o in view["organs"] if o["organ"] == "kidney")
        self.assertEqual(kidney["progress"]["stage"], "proposed")
        self.assertEqual([s["state"] for s in kidney["progress"]["steps"]], ["current", "pending", "pending", "pending", "pending"])

        self.svc.accept(allocation["id"], "h2", "hospital", "H2", {"expected_revision": 1})
        view = self.svc.get_donor(donor["id"], "coordinator", "")
        kidney = next(o for o in view["organs"] if o["organ"] == "kidney")
        self.assertEqual(kidney["progress"]["blocked"], "等待分配员登记冷链转运")
        self.assertEqual([s["state"] for s in kidney["progress"]["steps"]], ["done", "current", "pending", "pending", "pending"])

        # 未被触碰的肝进度保持在第一步之前
        liver = next(o for o in view["organs"] if o["organ"] == "liver")
        self.assertEqual(liver["progress"]["stage"], None)
        self.assertTrue(all(s["state"] == "pending" for s in liver["progress"]["steps"]))

    def test_multi_organ_ranking_requires_organ_id(self):
        donor = self.multi_donor()
        self.candidate("肾甲", "H2", 5, 500, "kidney")
        self.candidate("肝甲", "H3", 4, 200, "liver")
        with self.assertRaises(ApiError) as ctx:
            self.svc.donor_ranking(donor["id"], "allocation_officer", "")
        self.assertEqual(ctx.exception.code, "organ_required")
        with self.assertRaises(ApiError) as ctx:
            self.svc.propose("allocator", "allocation_officer", {"donor_id": donor["id"], "candidate_id": 1})
        self.assertEqual(ctx.exception.code, "organ_required")
        # 单器官捐献者仍可按 donor_id 便捷操作
        single = self.donor()
        rank = self.svc.donor_ranking(single["id"], "allocation_officer", "")
        self.assertEqual({c["organ"] for c in rank["candidates"]}, {"kidney"})


if __name__ == "__main__": unittest.main()
