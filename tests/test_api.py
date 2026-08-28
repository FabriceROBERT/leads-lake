from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_root():
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json()["version"]


def test_health_ok(sample_lake):
    body = client.get("/health").json()
    assert body["service"] == "leads-lake-api"
    assert body["status"] == "healthy"


def test_leads_seeded_excludes_clients_and_ranks_by_score(sample_lake):
    body = client.get("/leads").json()
    assert body["available"] is True
    assert body["total"] == 2  # the notaire row is is_client=True
    scores = [item["score"] for item in body["items"]]
    assert scores == sorted(scores, reverse=True)


def test_leads_include_clients(sample_lake):
    body = client.get("/leads", params={"include_clients": True}).json()
    assert body["total"] == 3


def test_leads_filter_segment(sample_lake):
    body = client.get("/leads", params={"segment": "avocat"}).json()
    assert [item["siren"] for item in body["items"]] == ["812345678"]


def test_leads_filter_has_recent_offer(sample_lake):
    body = client.get("/leads", params={"has_recent_offer": True}).json()
    assert [item["siren"] for item in body["items"]] == ["812345678"]


def test_get_lead(sample_lake):
    resp = client.get("/leads/902345671")
    assert resp.status_code == 200
    assert resp.json()["raison_sociale"] == "EC LOIRE"


def test_get_lead_404(sample_lake):
    assert client.get("/leads/000000000").status_code == 404


def test_kpi_seeded(sample_lake):
    body = client.get("/kpis/kpi_marche").json()
    assert body["available"] is True
    assert len(body["rows"]) == 2


def test_kpi_not_produced_yet_is_available_false(empty_lake):
    resp = client.get("/kpis/kpi_couverture")
    assert resp.status_code == 200
    assert resp.json()["available"] is False


def test_leads_empty_lake_is_available_false(empty_lake):
    body = client.get("/leads").json()
    assert body["available"] is False
    assert body["items"] == []


def test_unknown_kpi_is_404(sample_lake):
    assert client.get("/kpis/nope").status_code == 404
