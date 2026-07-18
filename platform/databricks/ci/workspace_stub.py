"""Minimal offline stand-in for a Databricks workspace, used only by CI.

``databricks bundle validate`` always resolves the current user through
``/api/2.0/preview/scim/v2/Me`` before running its schema and interpolation
checks — even when nothing is deployed. CI has no workspace, so the bundle
job points ``DATABRICKS_HOST`` at this stub and every GET receives a canned
SCIM identity, which is all validate needs to complete offline.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer

HOST = "127.0.0.1"
PORT = 8787


class _StubHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        # Only the identity endpoint exists; everything else behaves like a
        # real empty workspace (404), so validate treats bundle paths as
        # not-yet-deployed instead of misparsing a bogus object.
        if self.path.split("?")[0].endswith("/scim/v2/Me"):
            self._reply(
                200,
                {"id": "0", "userName": "ci-stub@example.com", "active": True},
            )
        else:
            self._reply(
                404,
                {"error_code": "RESOURCE_DOES_NOT_EXIST", "message": "Not found"},
            )

    def _reply(self, code: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        """Silence per-request logging; CI output stays readable."""


def main() -> None:
    HTTPServer((HOST, PORT), _StubHandler).serve_forever()


if __name__ == "__main__":
    main()
