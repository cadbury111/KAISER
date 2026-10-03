"""KAISER — Portfolio Analytics Dashboard Local HTTP Server.

Provides a lightweight, zero-dependency read-only HTTP server powered strictly
by Python's standard library `http.server`.

SAFETY & RESEARCH INVARIANTS:
1. Purely informational, research-only dashboard.
2. Strictly read-only: all POST, PUT, DELETE, and PATCH methods are rejected with HTTP 405.
3. Binds strictly to local interfaces (default: 127.0.0.1), preventing external network exposure.
4. Preserves Settings.execution_locked = True; zero trading, order, or broker paths exist.
5. Strict path validation and defense-in-depth security headers.
"""

from __future__ import annotations

import json
import logging
import re
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional, Sequence
from urllib.parse import parse_qs, urlparse

from app.core.exceptions import ValidationError
from app.portfolio.dashboard.template import get_dashboard_html
from app.portfolio.diagnostics import (
    DiagnosticEventType,
    DiagnosticOutcome,
    PortfolioDiagnosticsService,
    sanitize_diagnostic_error_category,
)
from app.portfolio.report_models import sanitize_dict_for_export
from app.portfolio.reporting import PortfolioReportingService

logger = logging.getLogger("kaiser.portfolio.dashboard")

ALLOWED_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
SAFE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-\.]+$")
MAX_URI_LENGTH = 2048


class DashboardHTTPRequestHandler(BaseHTTPRequestHandler):
    """Read-only HTTP request handler for the Portfolio Analytics Command Center."""

    server: ThreadingHTTPServer  # Attached server instance

    def do_HEAD(self) -> None:
        """Handle read-only HEAD requests safely without sending body."""
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        if path in ("/", "/index.html"):
            html = get_dashboard_html()
            encoded = html.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self._add_security_headers()
            self.end_headers()
        else:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self._add_security_headers()
            self.end_headers()

    def do_GET(self) -> None:
        """Handle incoming read-only GET requests."""
        if len(self.path) > MAX_URI_LENGTH:
            self._send_json_response(
                {
                    "error": "URI Too Long",
                    "message": f"Request URI exceeds maximum allowable limit of {MAX_URI_LENGTH} characters.",
                },
                status=HTTPStatus.REQUEST_URI_TOO_LONG,
            )
            return

        parsed_url = urlparse(self.path)
        path = parsed_url.path
        query = parse_qs(parsed_url.query)

        # Path traversal and injection checks
        if "\x00" in self.path or "/../" in path or path.endswith("/..") or "\\.." in path:
            self._send_json_response(
                {"error": "Bad Request", "message": "Invalid request path containing prohibited traversal sequences."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        service: PortfolioReportingService = getattr(self.server, "reporting_service", None)
        if service is None:
            self._send_json_response(
                {"error": "Internal Error", "message": "Reporting service not attached."},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return

        try:
            # 1. Main Dashboard Single-Page Application
            if path in ("/", "/index.html"):
                html = get_dashboard_html()
                self._send_html_response(html)
                return

            # 2. Latest Report Endpoint
            if path == "/api/report/latest":
                report = service.get_latest_unified_report()
                if report is None:
                    self._send_json_response(
                        {
                            "status": "not_found",
                            "message": "No portfolio reports generated yet. Run a simulation or backtest first.",
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                self._send_json_response(report.to_dict())
                return

            # 3. Report by ID Endpoint (/api/report/<report_id>)
            if path.startswith("/api/report/"):
                report_id = path.split("/api/report/", 1)[1].strip()
                if not report_id or not SAFE_ID_PATTERN.match(report_id):
                    self._send_json_response(
                        {
                            "error": "Bad Request",
                            "message": "Invalid report_id format. Must contain only alphanumeric characters, dashes, underscores, and dots.",
                        },
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                report = service.get_report_by_id(report_id) or service.get_report_by_run_id(report_id)
                if report is None:
                    self._send_json_response(
                        {"status": "not_found", "message": f"Report '{report_id}' not found."},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                self._send_json_response(report.to_dict())
                return

            # 4. Historical Reports List Endpoint (/api/reports)
            if path == "/api/reports":
                limit_val = int(query.get("limit", ["20"])[0])
                offset_val = int(query.get("offset", ["0"])[0])
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)

                summaries = service.list_historical_reports(limit=limit_clamped, offset=offset_clamped)
                self._send_json_response({
                    "status": "success",
                    "count": len(summaries),
                    "limit": limit_clamped,
                    "offset": offset_clamped,
                    "reports": [s.to_dict() for s in summaries],
                })
                return

            # 5. Timeline Series Endpoint (/api/series/<report_id>)
            if path.startswith("/api/series/"):
                report_id = path.split("/api/series/", 1)[1].strip()
                if not report_id or not SAFE_ID_PATTERN.match(report_id):
                    self._send_json_response(
                        {
                            "error": "Bad Request",
                            "message": "Invalid report_id format for series.",
                        },
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                limit_str = query.get("limit", [None])[0]
                limit_val = int(limit_str) if limit_str is not None else None
                offset_val = int(query.get("offset", ["0"])[0])

                series = service.get_equity_and_drawdown_series(report_id, limit=limit_val, offset=offset_val)
                if series is None:
                    self._send_json_response(
                        {"status": "not_found", "message": f"Series for report '{report_id}' not found."},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                self._send_json_response({"status": "success", "series": series.to_dict()})
                return

            # 6. Multi-Run Comparison Endpoint (/api/compare?runs=run1,run2)
            if path == "/api/compare":
                runs_param = query.get("runs", [""])[0]
                run_ids = [r.strip() for r in runs_param.split(",") if r.strip()]
                if len(run_ids) < 2:
                    self._send_json_response(
                        {"error": "Bad Request", "message": "At least two run IDs are required for comparison (e.g. ?runs=id1,id2)."},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                for rid in run_ids:
                    if not SAFE_ID_PATTERN.match(rid):
                        self._send_json_response(
                            {"error": "Bad Request", "message": f"Invalid run ID format: '{rid}'."},
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return
                comp = service.compare_simulation_runs(run_ids)
                self._send_json_response({"status": "success", "comparison": comp.to_dict()})
                return

            # 7. Health and Status Endpoint (/api/status)
            if path == "/api/status":
                self._send_json_response({
                    "status": "healthy",
                    "app": "KAISER",
                    "dashboard": "Portfolio Analytics Command Center",
                    "execution_locked": True,
                    "read_only": True,
                    "cached_reports_count": len(service._reports),
                })
                return

            # 8. Operational Diagnostics Snapshot & Bounded History Endpoint (/api/diagnostics)
            if path == "/api/diagnostics":
                diagnostics: Optional[PortfolioDiagnosticsService] = getattr(self.server, "diagnostics_service", None)
                if diagnostics is None and service is not None:
                    diagnostics = getattr(service, "diagnostics_service", None)

                if diagnostics is None:
                    self._send_json_response(
                        {"error": "Internal Error", "message": "Diagnostics service not attached."},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return

                try:
                    limit_val = int(query.get("limit", ["20"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    self._send_json_response(
                        {"error": "Bad Request", "message": "Limit and offset query parameters must be integers."},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                ev_type = query.get("type", [None])[0]
                ev_outcome = query.get("outcome", [None])[0]

                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)

                snapshot = diagnostics.get_snapshot()
                events = diagnostics.get_history(
                    limit=limit_clamped,
                    offset=offset_clamped,
                    event_type=ev_type,
                    outcome=ev_outcome,
                )
                self._send_json_response({
                    "status": "success",
                    "snapshot": snapshot,
                    "count": len(events),
                    "limit": limit_clamped,
                    "offset": offset_clamped,
                    "events": [e.to_dict() for e in events],
                })
                return

            # Unrecognized path: 404
            self._send_json_response({"error": "Not Found", "path": path}, status=HTTPStatus.NOT_FOUND)

        except ValidationError as val_err:
            self._send_json_response(
                {"error": "Validation Error", "message": str(val_err)},
                status=HTTPStatus.BAD_REQUEST,
            )
        except Exception as exc:
            logger.error("Unhandled error processing dashboard GET request '%s': %s", self.path, exc, exc_info=True)
            self._send_json_response(
                {"error": "Internal Server Error", "message": "An error occurred generating dashboard data."},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    def do_POST(self) -> None:
        """Reject POST requests with 405 Method Not Allowed (read-only safety constraint)."""
        self._send_method_not_allowed()

    def do_PUT(self) -> None:
        """Reject PUT requests with 405 Method Not Allowed."""
        self._send_method_not_allowed()

    def do_DELETE(self) -> None:
        """Reject DELETE requests with 405 Method Not Allowed."""
        self._send_method_not_allowed()

    def do_PATCH(self) -> None:
        """Reject PATCH requests with 405 Method Not Allowed."""
        self._send_method_not_allowed()

    def _send_method_not_allowed(self) -> None:
        """Helper to return read-only 405 status with Allow header."""
        self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
        self.send_header("Allow", "GET, HEAD")
        clean_data = sanitize_dict_for_export(
            {
                "error": "Method Not Allowed",
                "message": "The KAISER Portfolio Analytics Dashboard is strictly read-only. Modification operations are prohibited.",
            }
        )
        encoded = json.dumps(clean_data, indent=2).encode("utf-8")
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self._add_security_headers()
        self.end_headers()
        self.wfile.write(encoded)

    def _add_security_headers(self) -> None:
        """Inject defense-in-depth security headers into all HTTP responses."""
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'",
        )
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Permissions-Policy", "geolocation=(), microphone=(), camera=(), payment=()")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate, private")
        self.send_header("Pragma", "no-cache")

    def _send_html_response(self, html_content: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        """Send an HTML HTTP response with security headers."""
        encoded = html_content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self._add_security_headers()
        self.end_headers()
        self.wfile.write(encoded)

    def _send_json_response(self, data: Dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        """Send a JSON HTTP response with security headers and sanitized numeric values."""
        clean_data = sanitize_dict_for_export(data)
        encoded = json.dumps(clean_data, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self._add_security_headers()
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        """Route HTTP server logs through standard logging rather than raw stderr."""
        logger.debug("Dashboard HTTP [%s]: %s", self.address_string(), format % args)


class PortfolioDashboardServer:
    """Manages the lifecycle of the local read-only Portfolio Analytics Dashboard HTTP server."""

    def __init__(
        self,
        reporting_service: PortfolioReportingService,
        host: str = "127.0.0.1",
        port: int = 8080,
        diagnostics_service: Optional[PortfolioDiagnosticsService] = None,
    ) -> None:
        """Initialize the dashboard server.

        Args:
            reporting_service: PortfolioReportingService instance providing data.
            host: Network interface host to bind to (strictly loopback by default).
            port: Port number to bind to (0 for dynamic free port).
            diagnostics_service: Optional diagnostics service for auditing server lifecycle.
        """
        clean_host = (host or "").strip().lower()
        if clean_host not in ALLOWED_LOOPBACK_HOSTS:
            raise ValidationError(
                f"Security constraint violation: The Portfolio Analytics Dashboard must bind strictly "
                f"to loopback interfaces ({', '.join(sorted(ALLOWED_LOOPBACK_HOSTS))}). "
                f"Host '{host}' is prohibited."
            )

        if not isinstance(port, int) or port < 0 or port > 65535:
            raise ValidationError(f"Invalid port: {port}. Port must be an integer between 0 and 65535.")

        self.reporting_service = reporting_service
        self.host = clean_host
        self.port = port
        self.diagnostics_service = (
            diagnostics_service
            or getattr(reporting_service, "diagnostics_service", None)
            or PortfolioDiagnosticsService()
        )

        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._is_running = False

    @property
    def is_running(self) -> bool:
        """True if the HTTP server is currently listening."""
        return self._is_running and self._server is not None

    @property
    def url(self) -> str:
        """Get the full URL address to access the dashboard."""
        return f"http://{self.host}:{self.port}"

    def start(self, blocking: bool = False) -> None:
        """Start the dashboard HTTP server.

        Args:
            blocking: If True, blocks on serve_forever(); if False, runs in a daemon thread.
        """
        if self.is_running:
            logger.warning("Portfolio dashboard server is already running at %s", self.url)
            return

        try:
            # Bind server to host and port
            self._server = ThreadingHTTPServer((self.host, self.port), DashboardHTTPRequestHandler)
            self._server.daemon_threads = True
            self._server.allow_reuse_address = True

            # Attach reporting service and diagnostics to server instance for handler access
            setattr(self._server, "reporting_service", self.reporting_service)
            setattr(self._server, "diagnostics_service", self.diagnostics_service)

            # In case dynamic port 0 was passed, update to bound port
            actual_port = self._server.server_port
            self.port = actual_port

            self._is_running = True
            logger.info("Portfolio Analytics Dashboard started at %s", self.url)

            self.diagnostics_service.record_event(
                event_type=DiagnosticEventType.DASHBOARD_SERVER_STARTED,
                outcome=DiagnosticOutcome.SUCCESS,
                details={"host": self.host, "port": self.port, "url": self.url},
            )

        except Exception as exc:
            err_cat = sanitize_diagnostic_error_category(exc)
            logger.error("Failed to start Portfolio Analytics Dashboard server: %s", exc)
            self.diagnostics_service.record_event(
                event_type=DiagnosticEventType.DASHBOARD_SERVER_START_FAILED,
                outcome=DiagnosticOutcome.FAILURE,
                error_category=err_cat,
                details={"host": self.host, "port": self.port},
            )
            raise

        if blocking:
            try:
                self._server.serve_forever()
            finally:
                self.stop()
        else:
            self._thread = threading.Thread(
                target=self._server.serve_forever,
                name="kaiser_portfolio_dashboard_server",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        """Stop the dashboard HTTP server cleanly."""
        if not self._is_running and self._server is None:
            return

        logger.info("Stopping Portfolio Analytics Dashboard server...")
        self._is_running = False
        if self._server is not None:
            try:
                self._server.shutdown()
            except Exception as exc:
                logger.debug("Server shutdown exception (ignored): %s", exc)
            try:
                self._server.server_close()
            except Exception as exc:
                logger.debug("Server close exception (ignored): %s", exc)

        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=3.0)

        self._server = None
        self._thread = None

        self.diagnostics_service.record_event(
            event_type=DiagnosticEventType.DASHBOARD_SERVER_STOPPED,
            outcome=DiagnosticOutcome.INFO,
            details={"host": self.host, "port": self.port},
        )
        logger.info("Portfolio Analytics Dashboard stopped.")
