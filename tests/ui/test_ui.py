"""API integration tests for the Review Dashboard UI backend.

Uses FastAPI's synchronous TestClient so no running server is needed.
These tests validate all endpoints consumed by ui/src/App.tsx.
"""
import pytest
from fastapi.testclient import TestClient
from src.api import app

client = TestClient(app)


def test_api_queue_returns_list():
    """GET /api/queue must return a JSON array."""
    response = client.get("/api/queue")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


def test_api_queue_pagination():
    """limit/offset parameters are accepted and respected."""
    response = client.get("/api/queue?limit=5&offset=0")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) <= 5


def test_api_queue_item_shape():
    """If the queue has items each must contain required UI keys."""
    response = client.get("/api/queue?limit=100")
    assert response.status_code == 200
    items = response.json()
    for item in items:
        assert "id" in item
        assert "documentId" in item
        assert "priority" in item
        assert "slaHoursRemaining" in item
        assert "status" in item
        assert item["status"] in ("urgent", "warning", "good")
        assert "fields" in item
        assert isinstance(item["fields"], list)


def test_api_queue_sorted_by_priority():
    """Items must be returned highest priority first (matches UI queue sidebar sort)."""
    response = client.get("/api/queue?limit=100")
    assert response.status_code == 200
    items = response.json()
    if len(items) >= 2:
        priorities = [item["priority"] for item in items]
        assert priorities == sorted(priorities, reverse=True), (
            "Queue items must be sorted by priority DESC"
        )


def test_api_stats_returns_metrics():
    """GET /api/stats returns the expected keys (or empty dict if DB is offline)."""
    response = client.get("/api/stats")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    if data:  # if DB is connected
        assert "pending_items" in data
        assert "reviewed_today" in data


def test_api_health_returns_status():
    """GET /api/health must return system_status, active_alerts, and metrics."""
    response = client.get("/api/health")
    assert response.status_code == 200
    data = response.json()
    assert "system_status" in data
    assert data["system_status"] in ("HEALTHY", "DEGRADED")
    assert "active_alerts" in data
    assert "metrics" in data


def test_claim_nonexistent_returns_400():
    """Claiming an item that does not exist must return HTTP 400."""
    response = client.post(
        "/api/queue/nonexistent-uuid-xxxx/claim",
        json={"user_id": "test_reviewer"},
    )
    assert response.status_code == 400


def test_release_nonexistent_returns_400():
    """Releasing an item that is not claimed must return HTTP 400."""
    response = client.post(
        "/api/queue/nonexistent-uuid-xxxx/release",
        json={"user_id": "test_reviewer"},
    )
    assert response.status_code == 400


def test_submit_nonexistent_returns_400():
    """Submitting a review for a non-existent item must return HTTP 400."""
    response = client.post(
        "/api/queue/nonexistent-uuid-xxxx/submit",
        json={"user_id": "test_reviewer", "action": "COMPLETED"},
    )
    assert response.status_code == 400


def test_api_seed_endpoint():
    """POST /api/seed creates items when DB is available and returns a count."""
    response = client.post("/api/seed?count=3")
    # If DB is offline the endpoint raises 503 — that is acceptable
    assert response.status_code in (200, 503)
    if response.status_code == 200:
        data = response.json()
        assert "seeded" in data
        assert "item_ids" in data
        assert isinstance(data["item_ids"], list)
        # After seeding, queue list endpoint must still succeed
        q_response = client.get("/api/queue?limit=100")
        assert q_response.status_code == 200


def test_full_review_cycle():
    """Seed → claim → submit COMPLETED — full happy path with real DB."""
    seed_resp = client.post("/api/seed?count=1")
    if seed_resp.status_code != 200 or seed_resp.json()["seeded"] == 0:
        pytest.skip("DB not available or seed returned 0 items")

    # Fetch queue and pick the first item
    queue_resp = client.get("/api/queue?limit=100")
    assert queue_resp.status_code == 200
    queue = queue_resp.json()
    assert len(queue) >= 1
    item_id = queue[0]["id"]

    # Claim it
    claim_resp = client.post(
        f"/api/queue/{item_id}/claim",
        json={"user_id": "test_reviewer"},
    )
    assert claim_resp.status_code == 200

    # Submit approval
    submit_resp = client.post(
        f"/api/queue/{item_id}/submit",
        json={"user_id": "test_reviewer", "action": "COMPLETED"},
    )
    assert submit_resp.status_code == 200

    # Stats should reflect at least 1 reviewed today
    stats_resp = client.get("/api/stats")
    assert stats_resp.status_code == 200
    stats = stats_resp.json()
    if stats:
        assert stats.get("reviewed_today", 0) >= 1
