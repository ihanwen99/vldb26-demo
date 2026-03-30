from __future__ import annotations

import json
import os
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from demo_backend import PROBLEM_LABELS, build_problem_payload, build_real_fusion_payload


WEB_ROOT = os.path.join(os.path.dirname(__file__), "web")


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
                }
            )
            return
        if parsed.path == "/api/problem":
            query = parse_qs(parsed.query)
            problem = query.get("problem", ["join_order"])[0]
            scale = int(query.get("scale", ["4"])[0])
            partitions = int(query.get("partitions", ["3"])[0])
            merge_strategy = query.get("merge_strategy", ["top2_merge"])[0]
            merge_order = query.get("merge_order", ["left_deep"])[0]
            planner_mode = query.get("planner_mode", ["default"])[0]
            payload = build_problem_payload(problem, scale, partitions, merge_strategy, merge_order, planner_mode)
            self.send_json(payload)
            return
        if parsed.path == "/api/fusion":
            query = parse_qs(parsed.query)
            problem = query.get("problem", ["join_order"])[0]
            scale = int(query.get("scale", ["4"])[0])
            partitions = int(query.get("partitions", ["3"])[0])
            merge_strategy = query.get("merge_strategy", ["top2_merge"])[0]
            merge_order = query.get("merge_order", ["left_deep"])[0]
            planner_mode = query.get("planner_mode", ["default"])[0]
            payload = build_real_fusion_payload(problem, scale, partitions, merge_strategy, merge_order, planner_mode)
            self.send_json(payload)
            return
        return super().do_GET()

    def send_json(self, payload: object) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    host = os.environ.get("VLDB_DEMO_HOST", "0.0.0.0")
    port = int(os.environ.get("VLDB_DEMO_PORT", "8000"))
    server = ThreadingHTTPServer((host, port), DemoRequestHandler)
    print(f"Serving VLDB demo at http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
