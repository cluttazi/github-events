"""Minimal offline stand-in for a Databricks workspace, used only by CI.

``databricks bundle validate`` talks to the workspace even for pure schema
checks: it resolves the current user via ``/api/2.0/preview/scim/v2/Me``,
stats the bundle paths via ``/api/2.0/workspace/get-status`` and creates the
bundle files directory via ``/api/2.0/workspace/mkdirs``. CI has no
workspace, so the bundle job points ``DATABRICKS_HOST`` at this stub, which
plays an empty workspace: the identity endpoint answers, ``mkdirs`` records
the path, and ``get-status`` reports recorded paths (and their ancestors) as
directories — everything else is a 404. That is all validate needs to
complete offline.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

HOST = "127.0.0.1"
PORT = 8787

_created_dirs: set[str] = set()


def _is_directory(path: str) -> bool:
    """True when `path` was mkdirs'd, or is an ancestor of such a path."""
    return any(p == path or p.startswith(path + "/") for p in _created_dirs)


class _StubHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path.endswith("/scim/v2/Me"):
            self._reply(
                200,
                {"id": "0", "userName": "ci-stub@example.com", "active": True},
            )
            return
        if url.path.endswith("/workspace/get-status"):
            queried = parse_qs(url.query).get("path", [""])[0]
            if queried and _is_directory(queried):
                self._reply(200, {"object_type": "DIRECTORY", "path": queried})
                return
        self._reply(
            404,
            {"error_code": "RESOURCE_DOES_NOT_EXIST", "message": "Not found"},
        )

    def do_POST(self) -> None:
        url = urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        if url.path.endswith("/workspace/mkdirs"):
            try:
                target = json.loads(raw).get("path", "")
            except json.JSONDecodeError:
                target = ""
            if target:
                _created_dirs.add(target)
        self._reply(200, {})

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
