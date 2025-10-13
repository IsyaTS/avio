from __future__ import annotations

import base64
import json
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict

_PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGMAAQAABQABDQottAAAAABJRU5ErkJggg=="
)


class MockTGHandler(BaseHTTPRequestHandler):
    server_version = "MockTGWorker/1.0"

    def _send_json(self, status_code: int, body: Dict[str, object]) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802 - required by BaseHTTPRequestHandler
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/rpc/qr.png":
            params = urllib.parse.parse_qs(parsed.query)
            qr_id = params.get("qr_id", [""])[0]
            if qr_id == "expired":
                self._send_json(410, {"error": "qr_expired"})
                return
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(_PNG_BYTES)
            return

        if parsed.path == "/rpc/status":
            self._send_json(
                200,
                {
                    "status": "waiting_qr",
                    "qr_id": "mock-qr",
                    "qr_valid_until": 4_102_444_800,
                    "twofa_pending": False,
                    "needs_2fa": False,
                    "twofa_since": None,
                    "stats": {"authorized": 0, "waiting": 1, "needs_2fa": 0},
                },
            )
            return

        if parsed.path == "/health":
            self._send_json(200, {"ok": True})
            return

        self._send_json(404, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802 - required by BaseHTTPRequestHandler
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length:
            _ = self.rfile.read(content_length)

        if self.path == "/rpc/start":
            self._send_json(
                200,
                {
                    "status": "waiting_qr",
                    "qr_id": "mock-qr",
                    "qr_valid_until": 4_102_444_800,
                },
            )
            return

        if self.path == "/rpc/twofa.submit":
            self._send_json(
                200,
                {
                    "status": "authorized",
                    "qr_id": None,
                    "qr_valid_until": None,
                    "twofa_pending": False,
                    "needs_2fa": False,
                    "twofa_backoff_until": None,
                },
            )
            return

        self._send_json(404, {"error": "not_found"})


def run() -> None:
    server = HTTPServer(("0.0.0.0", 8085), MockTGHandler)
    server.serve_forever()


if __name__ == "__main__":
    run()
