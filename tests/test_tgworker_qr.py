import time


def test_qr_start_returns_metadata(tgworker_client):
    client, stub = tgworker_client
    expiry_ms = int(time.time() * 1000) + 90_000
    stub.snapshot.qr_id = "stub-qr"
    stub.snapshot.qr_valid_until = expiry_ms
    stub.flow.qr_id = "stub-qr"
    stub.flow.qr_expires_at = expiry_ms / 1000.0
    stub.qr_png = b"qr-bytes"

    response = client.post("/qr/start", json={"tenant": 1})
    assert response.status_code == 200
    payload = response.json()
    assert payload["qr_id"] == "stub-qr"
    assert payload["expires_at"] == expiry_ms
    assert payload["state"] == "need_qr"

    png_response = client.get("/qr/png", params={"tenant": 1, "qr_id": "stub-qr"})
    assert png_response.status_code == 200
    assert png_response.headers["content-type"] == "image/png"
    assert png_response.content == b"qr-bytes"
