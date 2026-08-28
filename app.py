from __future__ import annotations

import json
import os
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from backend import (
    LiveFusionDisabledError,
    PROBLEM_LABELS,
    build_problem_payload,
    build_real_fusion_payload,
    validate_request_parameters,
)


WEB_ROOT = os.path.join(os.path.dirname(__file__), "web")
FUSION_CONFIRM_HEADER = "X-QFusion-Confirm"
FUSION_CONFIRM_VALUE = "run-fusion"


def live_qpu_enabled() -> bool:
    """Return true only for the explicit, documented live-QPU opt in."""
    return os.environ.get("QFUSION_ENABLE_QPU", "") == "1"


def _int_param(params, name, default):
    raw = params.get(name, [default])[0]
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise ValueError("parameter %r must be an integer" % name)


def _request_parameters(parsed):
    query = parse_qs(parsed.query)
    return {
        "problem": query.get("problem", ["join_order"])[0],
        "scale": _int_param(query, "scale", "4"),
        "partitions": _int_param(query, "partitions", "3"),
        "merge_strategy": query.get("merge_strategy", ["direct_fusion"])[0],
        "merge_order": query.get("merge_order", ["left_deep"])[0],
        "planner_mode": query.get("planner_mode", ["default"])[0],
    }


def _is_qpu_service_error(error: Exception) -> bool:
    module = type(error).__module__
    if module.startswith(("dwave", "requests", "urllib3")):
        return True
    if isinstance(error, (ConnectionError, OSError, TimeoutError)):
        return True
    message = str(error).lower()
    return any(
        marker in message
        for marker in (
            "api token",
            "authentication",
            "credential",
            "dwave",
            "d-wave",
            "endpoint",
            "solver",
        )
    )


class DemoRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_ROOT, **kwargs)

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/catalog":
            self.send_json(
                {
                    "problems": [
                        {"id": problem_id, "label": label}
                        for problem_id, label in PROBLEM_LABELS.items()
                    ],
                    "merge_strategies": [
                        "direct_fusion",
                        "top2_merge",
                        "conditioned_fusion",
                    ],
                    "merge_orders": ["left_deep", "bushy"],
                    "live_qpu_enabled": live_qpu_enabled(),
                }
            )
            return
        if parsed.path == "/api/fusion":
            self.send_error_json(
                405,
                "Fusion execution requires POST.",
                "method_not_allowed",
                {"Allow": "POST"},
            )
            return
        if parsed.path == "/api/problem":
            try:
                parameters = _request_parameters(parsed)
                payload = build_problem_payload(**parameters)
            except (KeyError, ValueError) as error:
                self.send_error_json(400, str(error).strip("'\""), "invalid_request")
                return
            self.send_json(payload)
            return
        return super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/api/fusion":
            self.send_error_json(404, "Unknown API endpoint.", "not_found")
            return
        if not self._is_same_origin_request():
            self.send_error_json(
                403,
                "Cross-origin fusion requests are not allowed.",
                "cross_origin_forbidden",
            )
            return
        if self.headers.get(FUSION_CONFIRM_HEADER) != FUSION_CONFIRM_VALUE:
            self.send_error_json(
                403,
                f"Fusion requests require {FUSION_CONFIRM_HEADER}: {FUSION_CONFIRM_VALUE}.",
                "fusion_confirmation_required",
            )
            return

        try:
            parameters = _request_parameters(parsed)
            validate_request_parameters(**parameters)
        except (KeyError, ValueError) as error:
            self.send_error_json(400, str(error).strip("'\""), "invalid_request")
            return

        try:
            payload = build_real_fusion_payload(
                **parameters,
                allow_live=live_qpu_enabled(),
            )
        except LiveFusionDisabledError as error:
            self.send_error_json(403, str(error), "live_qpu_disabled")
            return
        except ModuleNotFoundError:
            self.send_error_json(
                503,
                "D-Wave Ocean dependencies are unavailable for live execution.",
                "qpu_dependencies_unavailable",
            )
            return
        except Exception as error:
            if _is_qpu_service_error(error):
                self.log_error("QPU service error: %s", type(error).__name__)
                self.send_error_json(
                    503,
                    "D-Wave credentials, solver access, or the remote service are unavailable.",
                    "qpu_service_unavailable",
                )
            else:
                self.log_error("Fusion execution error: %s", type(error).__name__)
                self.send_error_json(
                    502,
                    "Fusion execution failed before a valid result was produced.",
                    "fusion_execution_failed",
                )
            return
        self.send_json(payload)

    def do_OPTIONS(self) -> None:
        if urlparse(self.path).path == "/api/fusion":
            self.send_error_json(
                403,
                "Cross-origin fusion requests are not allowed.",
                "cross_origin_forbidden",
            )
            return
        self.send_error_json(404, "Unknown API endpoint.", "not_found")

    def _is_same_origin_request(self) -> bool:
        fetch_site = self.headers.get("Sec-Fetch-Site")
        if fetch_site not in (None, "none", "same-origin"):
            return False
        origin = self.headers.get("Origin")
        if origin is None:
            return True
        parsed_origin = urlparse(origin)
        request_host = self.headers.get("Host", "")
        return (
            parsed_origin.scheme in ("http", "https")
            and parsed_origin.netloc.lower() == request_host.lower()
        )

    def send_json(
        self,
        payload: object,
        status: int = 200,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (extra_headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def send_error_json(
        self,
        status: int,
        message: str,
        error_code: str,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self.send_json(
            {"error": message, "error_code": error_code},
            status=status,
            extra_headers=extra_headers,
        )


def main() -> None:
    host = os.environ.get("VLDB_DEMO_HOST", "127.0.0.1")
    port = int(os.environ.get("VLDB_DEMO_PORT", "8000"))
    server = ThreadingHTTPServer((host, port), DemoRequestHandler)
    print(f"Serving VLDB demo at http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
