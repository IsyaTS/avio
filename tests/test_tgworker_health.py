def test_health_counts(tgworker_client):
    client, stub = tgworker_client
    stub.stats = {"authorized": 5, "waiting": 2, "needs_2fa": 1}

    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "ok": True,
        "authorized_count": 5,
        "waiting_count": 2,
        "needs_2fa": 1,
    }


def test_health_handles_stats_failure(tgworker_client):
    client, stub = tgworker_client
    stub.raise_stats = True

    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["authorized_count"] == 0
    assert payload["waiting_count"] == 0
    assert payload["needs_2fa"] == 0
