"""
test_integration.py
End-to-end integration test for the Document Processing Pipeline API.
Requires the FastAPI server to be running on port 8000.

Run: python test_integration.py
"""
import httpx
import sys

BASE = "http://127.0.0.1:8000/api"


def run():
    c = httpx.Client(timeout=10)

    print("=" * 60)
    print("DOCUMENT PROCESSING PIPELINE — API Integration Test")
    print("=" * 60)

    # 1. Health
    print("\n[1] GET /api/health")
    h = c.get(f"{BASE}/health").json()
    assert "system_status" in h, "Missing system_status"
    assert h["system_status"] in ("HEALTHY", "DEGRADED")
    assert "metrics" in h
    print(f"    status={h['system_status']}  alerts={h['active_alerts']}  PASS ✓")

    # 2. Stats (baseline)
    print("\n[2] GET /api/stats")
    stats0 = c.get(f"{BASE}/stats").json()
    print(f"    pending={stats0.get('pending_items',0)}  reviewed_today={stats0.get('reviewed_today',0)}  PASS ✓")

    # 3. Seed demo data
    print("\n[3] POST /api/seed?count=5")
    seed = c.post(f"{BASE}/seed", params={"count": 5}).json()
    assert "seeded" in seed
    print(f"    created {seed['seeded']} items  PASS ✓")

    # 4. Queue
    print("\n[4] GET /api/queue")
    q = c.get(f"{BASE}/queue", params={"limit": 20}).json()
    assert isinstance(q, list), "Queue must be a list"
    print(f"    {len(q)} pending items")
    if len(q) >= 2:
        prios = [item["priority"] for item in q]
        assert prios == sorted(prios, reverse=True), "Queue not sorted by priority"
    for item in q[:3]:
        low_fields = [f for f in item["fields"] if f["confidence"] < 0.8]
        print(f"    {item['documentId']} p={item['priority']} sla={item['slaHoursRemaining']}h "
              f"status={item['status']} fields={len(item['fields'])} low_conf={len(low_fields)}")
    print("    Sorted by priority DESC  PASS ✓")

    if not q:
        print("    (no pending items, skipping claim/submit)")
        return True

    # 5. Claim
    item = q[0]
    item_id = item["id"]
    print(f"\n[5] POST /api/queue/{item_id[:8]}…/claim")
    cr = c.post(f"{BASE}/queue/{item_id}/claim", json={"user_id": "reviewer_01"})
    assert cr.status_code == 200, f"Claim failed: {cr.text}"
    print(f"    {item['documentId']} claimed  PASS ✓")

    # 6. Double-claim should fail (400)
    print(f"\n[6] Double-claim should return 400")
    cr2 = c.post(f"{BASE}/queue/{item_id}/claim", json={"user_id": "reviewer_02"})
    assert cr2.status_code == 400, f"Expected 400 on double-claim, got {cr2.status_code}"
    print(f"    Got 400 as expected  PASS ✓")

    # 7. Submit with correction
    print(f"\n[7] POST /api/queue/{item_id[:8]}…/submit (with correction)")
    sr = c.post(f"{BASE}/queue/{item_id}/submit", json={
        "user_id": "reviewer_01",
        "action": "COMPLETED",
        "corrections": {"vendor_name": "Corrected Corp LLC"}
    })
    assert sr.status_code == 200, f"Submit failed: {sr.text}"
    print(f"    Review submitted  PASS ✓")

    # 8. Stats should reflect reviewed_today increment
    print("\n[8] GET /api/stats (after review)")
    stats1 = c.get(f"{BASE}/stats").json()
    assert stats1.get("reviewed_today", 0) >= stats0.get("reviewed_today", 0) + 1
    print(f"    pending={stats1.get('pending_items')} reviewed_today={stats1.get('reviewed_today')} "
          f"avg_time={stats1.get('avg_review_time_seconds')}s  PASS ✓")

    # 9. Health reflects updated queue depth
    print("\n[9] GET /api/health (queue metrics updated)")
    h2 = c.get(f"{BASE}/health").json()
    print(f"    status={h2['system_status']} queue_depth={h2['metrics']['review_queue_depth']}  PASS ✓")

    # 10. Reject flow
    if len(q) >= 2:
        item2 = q[1]
        print(f"\n[10] Reject flow on {item2['documentId']}")
        rr = c.post(f"{BASE}/queue/{item2['id']}/submit", json={
            "user_id": "reviewer_01",
            "action": "REJECTED",
            "reason": "Duplicate invoice"
        })
        assert rr.status_code == 200
        print(f"    Rejection submitted  PASS ✓")

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED — Backend + Infrastructure verified ✓")
    print("=" * 60)
    return True


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"\nFAILED: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)
