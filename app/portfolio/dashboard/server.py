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
import math
import re
import secrets
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Sequence
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

    server_version = "KAISER-Dashboard"
    sys_version = ""
    server: ThreadingHTTPServer  # pyright: ignore[reportIncompatibleVariableOverride]

    def version_string(self) -> str:
        """Override version_string to suppress runtime/OS version and trailing spaces."""
        return self.server_version

    def send_error(self, code: int, message: Optional[str] = None, explain: Optional[str] = None) -> None:
        """Override default HTML send_error to return sanitized JSON without leaking server info."""
        self.send_response(code, message)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self._add_security_headers()
        self.end_headers()
        err_body = json.dumps({
            "error": message or "Error",
            "status": code,
        }).encode("utf-8")
        self.wfile.write(err_body)

    def _send_unauthorized_response(self, message: str = "Valid authentication credentials required.") -> None:
        """Send a 401 Unauthorized response with WWW-Authenticate header and sanitized message."""
        clean_data = sanitize_dict_for_export({
            "error": "Unauthorized",
            "message": message,
        })
        encoded = json.dumps(clean_data, indent=2).encode("utf-8")
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("WWW-Authenticate", 'Bearer realm="KAISER Dashboard"')
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self._add_security_headers()
        self.end_headers()
        if not getattr(self, "_is_head", False):
            self.wfile.write(encoded)

    def _check_authorization(self) -> bool:
        """Validate authorization credentials for protected endpoints.

        Supports:
        - Authorization: Bearer <token>
        - X-API-Key: <token>

        When both headers are provided, both must match the server token.
        Token comparison is strictly performed in constant time using secrets.compare_digest.
        Tokens passed via URL query parameters are rejected.

        Returns:
            bool: True if authorized (or authentication not required), False if unauthorized.
        """
        server_auth_token: Optional[str] = getattr(self.server, "auth_token", None)
        require_auth: bool = bool(getattr(self.server, "require_auth", False))

        # If no server auth token is configured and auth is not strictly required, allow local access
        if not server_auth_token and not require_auth:
            return True

        # If auth is required but no server token was configured, fail closed with 403 Forbidden
        if require_auth and not server_auth_token:
            self._send_json_response(
                {
                    "error": "Forbidden",
                    "message": "Dashboard authentication is required, but no server authentication token is configured.",
                },
                status=HTTPStatus.FORBIDDEN,
            )
            return False

        # Extract credentials from Authorization and X-API-Key headers
        bearer_token: Optional[str] = None
        auth_header = self.headers.get("Authorization")
        if auth_header is not None:
            parts = auth_header.strip().split(None, 1)
            if len(parts) == 2 and parts[0].lower() == "bearer":
                bearer_token = parts[1].strip()
                if not bearer_token:
                    self._send_unauthorized_response(message="Empty Bearer token provided.")
                    return False
            else:
                self._send_unauthorized_response(message="Invalid Authorization header format. Expected 'Bearer <token>'.")
                return False

        api_key_token: Optional[str] = None
        api_key_header = self.headers.get("X-API-Key")
        if api_key_header is not None:
            clean_api_key = api_key_header.strip()
            if not clean_api_key:
                self._send_unauthorized_response(message="Empty X-API-Key header provided.")
                return False
            api_key_token = clean_api_key

        if bearer_token is None and api_key_token is None:
            self._send_unauthorized_response(message="Missing authentication credentials.")
            return False

        if server_auth_token is None:
            self._send_unauthorized_response(message="Invalid authentication credentials.")
            return False

        # When both headers are provided, both must match the server token
        if bearer_token is not None and api_key_token is not None:
            bearer_match = secrets.compare_digest(bearer_token, server_auth_token)
            api_key_match = secrets.compare_digest(api_key_token, server_auth_token)
            if not (bearer_match and api_key_match):
                self._send_unauthorized_response(message="Conflicting or invalid authentication credentials.")
                return False
            return True

        # Single header provided
        token_to_check = bearer_token if bearer_token is not None else api_key_token
        if token_to_check is None or not secrets.compare_digest(token_to_check, server_auth_token):
            self._send_unauthorized_response(message="Invalid authentication credentials.")
            return False

        return True

    def do_GET(self) -> None:  # pyright: ignore[reportGeneralTypeIssues]
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
            # Check authorization for protected endpoints
            require_auth_flag = bool(getattr(self.server, "require_auth", False))
            server_token = getattr(self.server, "auth_token", None)
            auth_enforced = bool(server_token or require_auth_flag)

            # Enforce authorization: all sensitive data APIs (/api/* except public health /api/status)
            if auth_enforced and path.startswith("/api/") and path != "/api/status":
                if not self._check_authorization():
                    return

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

            # Paper Trading Workflow Summary Endpoint (/api/paper/summary and /api/paper/status)
            if path in ("/api/paper/summary", "/api/paper/status"):
                coordinator = getattr(self.server, "paper_coordinator", None)
                if coordinator is not None:
                    summary = coordinator.get_summary()
                    self._send_json_response(summary)
                    return

                engine = getattr(self.server, "paper_execution_engine", None)
                if engine is not None:
                    summary = engine.get_status()
                    self._send_json_response({
                        "status": "success",
                        "label": "PAPER ONLY",
                        "paper_execution_enabled": engine.is_enabled,
                        "execution_locked": True,
                        "broker_provider": "none",
                        "trading_engine": "not initialized",
                        "engine_status": summary,
                    })
                    return

                state_store = getattr(self.server, "state_store", None)
                paper_status = state_store.get("paper_status", "disabled") if state_store else "disabled"
                paper_portfolio = state_store.get("paper_portfolio") if state_store else None
                self._send_json_response({
                    "status": "success",
                    "label": "PAPER ONLY",
                    "paper_execution_enabled": paper_status == "active",
                    "execution_locked": True,
                    "broker_provider": "none",
                    "trading_engine": "not initialized",
                    "paper_portfolio": paper_portfolio,
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

            # 9. Market Data Quality Endpoint (/api/data-quality)
            if path == "/api/data-quality":
                monitor = getattr(self.server, "quality_monitor", None)
                if monitor is None:
                    # Fallback to state store if available
                    state_store = getattr(service, "state_store", None)
                    if state_store is not None:
                        summary = state_store.get("data_quality_summary", {})
                        assessments = state_store.get("market_data_quality", {})
                        anomalies = state_store.get("market_data_anomalies", [])
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "summary": summary,
                            "assessments": assessments,
                            "anomalies": anomalies,
                        })
                        return

                if monitor is None:
                    self._send_json_response(
                        {
                            "status": "unavailable",
                            "message": "Market data quality monitor not attached.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

                symbol_param = query.get("symbol", [None])[0]
                timeframe_param = query.get("timeframe", [None])[0]
                limit_val = int(query.get("limit", ["50"])[0])
                limit_clamped = max(1, min(100, limit_val))

                if symbol_param:
                    assessment = monitor.get_latest_assessment(symbol_param, timeframe_param)
                    anomalies = monitor.get_anomalies(symbol=symbol_param, limit=limit_clamped)
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "symbol": symbol_param,
                        "assessment": assessment.to_dict() if assessment else None,
                        "anomalies": [a.to_dict() for a in anomalies],
                        "summary": monitor.get_quality_summary(),
                    })
                    return

                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "summary": monitor.get_quality_summary(),
                    "assessments": monitor.get_all_latest_assessments(),
                    "anomalies": [a.to_dict() for a in monitor.get_anomalies(limit=limit_clamped)],
                })
                return

            # 10. Market Data Provenance Endpoint (/api/data-provenance)
            if path == "/api/data-provenance":
                tracker = getattr(self.server, "provenance_tracker", None)
                if tracker is None:
                    # Fallback to state store if available
                    state_store = getattr(service, "state_store", None)
                    if state_store is not None:
                        summary = state_store.get("market_provenance_summary", {})
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "summary": summary,
                            "records": [],
                        })
                        return

                    self._send_json_response(
                        {
                            "status": "unavailable",
                            "message": "Market data provenance tracker not attached.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

                prov_id = query.get("provenance_id", [None])[0]
                if prov_id:
                    if ".." in prov_id or not SAFE_ID_PATTERN.match(prov_id.strip()):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Valid 'provenance_id' query parameter is required.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return
                    prov = tracker.get_provenance(prov_id)
                    if not prov:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Provenance record '{prov_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "provenance": prov.to_dict(),
                    })
                    return

                symbol_param = query.get("symbol", [None])[0]
                timeframe_param = query.get("timeframe", [None])[0]
                source_type_param = query.get("source_type", [None])[0]
                limit_val = int(query.get("limit", ["50"])[0])
                offset_val = int(query.get("offset", ["0"])[0])
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)

                records = tracker.get_audit_history(
                    limit=limit_clamped,
                    offset=offset_clamped,
                    symbol=symbol_param,
                    timeframe=timeframe_param,
                    source_type=source_type_param,
                )
                summary = tracker.get_summary()
                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "summary": summary,
                    "count": len(records),
                    "limit": limit_clamped,
                    "offset": offset_clamped,
                    "records": [r.to_dict() for r in records],
                })
                return

            # 11. Market Data Lineage Endpoint (/api/data-lineage or /api/data-lineage/<provenance_id>)
            if path == "/api/data-lineage" or path.startswith("/api/data-lineage/"):
                tracker = getattr(self.server, "provenance_tracker", None)
                if tracker is None:
                    self._send_json_response(
                        {
                            "status": "unavailable",
                            "message": "Market data provenance tracker not attached.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

                prov_id = None
                if path.startswith("/api/data-lineage/"):
                    prov_id = path[len("/api/data-lineage/"):].strip()
                if not prov_id:
                    prov_id = query.get("provenance_id", [None])[0]

                if not prov_id or ".." in prov_id or not SAFE_ID_PATTERN.match(prov_id.strip()):
                    self._send_json_response(
                        {
                            "status": "invalid_request",
                            "message": "Valid 'provenance_id' query parameter is required.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                direction = query.get("direction", ["upstream"])[0]
                depth_val = int(query.get("max_depth", ["10"])[0])
                depth_clamped = max(1, min(20, depth_val))

                graph = tracker.get_lineage(
                    provenance_id=prov_id.strip(),
                    max_depth=depth_clamped,
                    direction=direction,
                )
                if not graph.nodes:
                    self._send_json_response(
                        {
                            "status": "not_found",
                            "message": f"Provenance record '{prov_id}' not found.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "provenance_id": prov_id.strip(),
                    "lineage": graph.to_dict(),
                })
                return

            # 12. Analysis Run Registry Endpoint (/api/analysis-runs or /api/analysis-runs/<run_id>)
            if path == "/api/analysis-runs" or path.startswith("/api/analysis-runs/"):
                registry = getattr(self.server, "run_registry", None)
                if registry is None:
                    # Fallback to state store if available
                    state_store = getattr(service, "state_store", None)
                    if state_store is not None:
                        latest = state_store.get("analysis_runs.latest", [])
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "runs": latest if isinstance(latest, list) else [latest] if latest else [],
                        })
                        return

                    self._send_json_response(
                        {
                            "status": "unavailable",
                            "message": "Analysis run registry not attached.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

                run_id = None
                if path.startswith("/api/analysis-runs/"):
                    run_id = path[len("/api/analysis-runs/"):].strip()

                if run_id:
                    if ".." in run_id or not SAFE_ID_PATTERN.match(run_id):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid run_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return
                    run_rec = registry.get_run(run_id)
                    if not run_rec:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Analysis run '{run_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "run": run_rec.to_dict(),
                    })
                    return

                # Query listing
                repro_id = query.get("reproducibility_id", [None])[0]
                limit_val = int(query.get("limit", ["50"])[0])
                offset_val = int(query.get("offset", ["0"])[0])
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)

                if repro_id:
                    if ".." in repro_id or not SAFE_ID_PATTERN.match(repro_id.strip()):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid reproducibility_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return
                    matched = registry.get_runs_by_reproducibility(
                        reproducibility_id=repro_id.strip(),
                        limit=limit_clamped,
                        offset=offset_clamped,
                    )
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "reproducibility_id": repro_id.strip(),
                        "count": len(matched),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "runs": [r.to_dict() for r in matched],
                    })
                    return

                runs = registry.query_runs(
                    operation_type=query.get("operation_type", [None])[0],
                    status=query.get("status", [None])[0],
                    symbol=query.get("symbol", [None])[0],
                    strategy_id=query.get("strategy_id", [None])[0],
                    limit=limit_clamped,
                    offset=offset_clamped,
                )
                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "count": len(runs),
                    "limit": limit_clamped,
                    "offset": offset_clamped,
                    "runs": [r.to_dict() for r in runs],
                })
                return

            # 14. Analysis Comparisons Endpoint (/api/analysis-comparisons)
            if path == "/api/analysis-comparisons" or path.startswith("/api/analysis-comparisons/"):
                comp_engine = getattr(self.server, "comparison_engine", None)
                if comp_engine is None:
                    self._send_json_response(
                        {
                            "status": "not_available",
                            "message": "Analysis comparison engine not initialized.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

                comp_id = None
                if path.startswith("/api/analysis-comparisons/"):
                    comp_id = path[len("/api/analysis-comparisons/"):].strip()

                if comp_id:
                    if ".." in comp_id or not SAFE_ID_PATTERN.match(comp_id):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid comparison_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    comparison = comp_engine.get_comparison(comp_id)
                    if not comparison:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Comparison '{comp_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    c_dict = comparison.to_dict() if hasattr(comparison, "to_dict") else comparison
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "comparison": c_dict,
                    })
                    return

                # Listing endpoint: /api/analysis-comparisons
                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)

                comps: List[Dict[str, Any]] = []
                if comp_engine.state_store:
                    comps = comp_engine.state_store.get("analysis_comparisons:history", [])
                if not comps and hasattr(comp_engine, "_cache"):
                    with comp_engine._lock:
                        comps = [c.to_dict() for c in comp_engine._cache.values()]

                sliced = comps[offset_clamped : offset_clamped + limit_clamped]
                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "count": len(sliced),
                    "limit": limit_clamped,
                    "offset": offset_clamped,
                    "comparisons": sliced,
                })
                return

            # 15. Evidence Syntheses Endpoint (/api/evidence-syntheses or /api/evidence-syntheses/<report_id>)
            if path == "/api/evidence-syntheses" or path.startswith("/api/evidence-syntheses/"):
                evidence_engine = getattr(self.server, "evidence_engine", None)
                if evidence_engine is None:
                    # Fallback to state store if available
                    state_store = getattr(service, "state_store", None)
                    if state_store is not None:
                        latest = state_store.get("evidence_synthesis:latest", None)
                        history = state_store.get("evidence_synthesis:history", [])
                        if path.startswith("/api/evidence-syntheses/"):
                            req_id = path[len("/api/evidence-syntheses/"):].strip()
                            if latest and latest.get("report_id") == req_id:
                                self._send_json_response({
                                    "status": "success",
                                    "read_only": True,
                                    "execution_locked": True,
                                    "report": latest,
                                })
                                return
                            self._send_json_response(
                                {
                                    "status": "not_found",
                                    "message": f"Evidence synthesis '{req_id}' not found.",
                                    "read_only": True,
                                    "execution_locked": True,
                                },
                                status=HTTPStatus.NOT_FOUND,
                            )
                            return
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "count": len(history),
                            "reports": history,
                        })
                        return

                    self._send_json_response(
                        {
                            "status": "not_available",
                            "message": "Evidence synthesis engine not initialized.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

                report_id = None
                if path.startswith("/api/evidence-syntheses/"):
                    report_id = path[len("/api/evidence-syntheses/"):].strip()

                if report_id:
                    if ".." in report_id or not SAFE_ID_PATTERN.match(report_id):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid report_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    report = evidence_engine.get_synthesis(report_id)
                    if not report:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Evidence synthesis '{report_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    r_dict = report.to_dict() if hasattr(report, "to_dict") else report
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "report": r_dict,
                    })
                    return

                # Listing endpoint: /api/evidence-syntheses
                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)

                subject_param = query.get("subject", [None])[0]
                reports = evidence_engine.list_syntheses(
                    limit=limit_clamped,
                    offset=offset_clamped,
                    subject=subject_param,
                )
                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "count": len(reports),
                    "limit": limit_clamped,
                    "offset": offset_clamped,
                    "reports": [r.to_dict() for r in reports],
                })
                return

            # 16. Opportunity Assessments Endpoint (/api/opportunity-assessments or /api/opportunity-assessments/<candidate_id>)
            if path == "/api/opportunity-assessments" or path.startswith("/api/opportunity-assessments/"):
                opp_engine = getattr(self.server, "opportunity_engine", None)
                if opp_engine is None:
                    state_store = getattr(service, "state_store", None)
                    if state_store is not None:
                        latest = state_store.get("opportunity_assessment:latest", None)
                        history = state_store.get("opportunity_assessment:history", [])
                        if path.startswith("/api/opportunity-assessments/"):
                            req_id = path[len("/api/opportunity-assessments/"):].strip()
                            if latest and latest.get("candidate_id") == req_id:
                                self._send_json_response({
                                    "status": "success",
                                    "read_only": True,
                                    "execution_locked": True,
                                    "assessment": latest,
                                })
                                return
                            self._send_json_response(
                                {
                                    "status": "not_found",
                                    "message": f"Opportunity assessment '{req_id}' not found.",
                                    "read_only": True,
                                    "execution_locked": True,
                                },
                                status=HTTPStatus.NOT_FOUND,
                            )
                            return
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "count": len(history),
                            "assessments": history,
                        })
                        return

                    self._send_json_response(
                        {
                            "status": "not_available",
                            "message": "Opportunity assessment engine not initialized.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

                candidate_id = None
                if path.startswith("/api/opportunity-assessments/"):
                    candidate_id = path[len("/api/opportunity-assessments/"):].strip()

                if candidate_id:
                    if ".." in candidate_id or not SAFE_ID_PATTERN.match(candidate_id):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid candidate_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    cand = opp_engine.get_assessment(candidate_id)
                    if not cand:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Opportunity candidate '{candidate_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    c_dict = cand.to_dict() if hasattr(cand, "to_dict") else cand
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "assessment": c_dict,
                    })
                    return

                # Listing endpoint: /api/opportunity-assessments
                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)

                assessments = opp_engine.list_assessments(
                    limit=limit_clamped,
                    offset=offset_clamped,
                )
                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "count": len(assessments),
                    "limit": limit_clamped,
                    "offset": offset_clamped,
                    "assessments": [a.to_dict() if hasattr(a, "to_dict") else a for a in assessments],
                })
                return

            # 17. Opportunity Priorities Endpoint (/api/opportunity-priorities or /api/opportunity-priorities/<priority_id>)
            if path == "/api/opportunity-priorities" or path.startswith("/api/opportunity-priorities/"):
                prio_engine = getattr(self.server, "prioritization_engine", None)
                if prio_engine is None:
                    state_store = getattr(service, "state_store", None)
                    if state_store is not None:
                        latest = state_store.get("opportunity_priority:latest", None)
                        history = state_store.get("opportunity_priority:history", [])
                        if path.startswith("/api/opportunity-priorities/"):
                            req_id = path[len("/api/opportunity-priorities/"):].strip()
                            if latest and (latest.get("priority_id") == req_id or latest.get("candidate_id") == req_id):
                                self._send_json_response({
                                    "status": "success",
                                    "read_only": True,
                                    "execution_locked": True,
                                    "priority": latest,
                                })
                                return
                            self._send_json_response(
                                {
                                    "status": "not_found",
                                    "message": f"Opportunity priority '{req_id}' not found.",
                                    "read_only": True,
                                    "execution_locked": True,
                                },
                                status=HTTPStatus.NOT_FOUND,
                            )
                            return
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "count": len(history),
                            "priorities": history,
                        })
                        return

                    self._send_json_response(
                        {
                            "status": "not_available",
                            "message": "Opportunity prioritization engine not initialized.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

                priority_id = None
                if path.startswith("/api/opportunity-priorities/"):
                    priority_id = path[len("/api/opportunity-priorities/"):].strip()

                if priority_id:
                    if ".." in priority_id or not SAFE_ID_PATTERN.match(priority_id):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid priority_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    rec = prio_engine.get_priority(priority_id)
                    if not rec:
                        queue_obj = getattr(self.server, "investigation_queue", None)
                        if queue_obj:
                            rec = queue_obj.get(priority_id)

                    if not rec:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Opportunity priority record '{priority_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    r_dict = rec.to_dict() if hasattr(rec, "to_dict") else rec
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "priority": r_dict,
                    })
                    return

                # Listing endpoint: /api/opportunity-priorities
                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)

                state_param = query.get("state", [None])[0]
                records = prio_engine.list_priorities(
                    limit=limit_clamped,
                    offset=offset_clamped,
                    state=state_param,
                )
                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "count": len(records),
                    "limit": limit_clamped,
                    "offset": offset_clamped,
                    "priorities": [r.to_dict() if hasattr(r, "to_dict") else r for r in records],
                })
                return

            # 18. Opportunity Investigation Queue Endpoint (/api/opportunity-queue)
            if path == "/api/opportunity-queue":
                queue_obj = getattr(self.server, "investigation_queue", None)
                if queue_obj is None:
                    state_store = getattr(service, "state_store", None)
                    if state_store is not None:
                        queued = state_store.get("opportunity_queue:latest", [])
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "queue_size": len(queued),
                            "count": len(queued),
                            "queue": queued,
                        })
                        return

                    self._send_json_response(
                        {
                            "status": "not_available",
                            "message": "Opportunity investigation queue not initialized.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)

                items = queue_obj.list_queue(limit=limit_clamped, offset=offset_clamped)
                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "queue_size": queue_obj.size(),
                    "count": len(items),
                    "limit": limit_clamped,
                    "offset": offset_clamped,
                    "queue": [i.to_dict() if hasattr(i, "to_dict") else i for i in items],
                })
                return

            # 19. Human Investigation Sessions Endpoint (/api/investigation-sessions, /api/investigation-sessions/<session_id>, /api/investigation-sessions/<session_id>/events)
            if path == "/api/investigation-sessions" or path.startswith("/api/investigation-sessions/"):
                inv_manager = getattr(self.server, "investigation_manager", None)
                if inv_manager is None:
                    state_store = getattr(service, "state_store", None)
                    if state_store is not None:
                        latest = state_store.get("investigation_session:latest", None)
                        history = state_store.get("investigation_session:history", [])
                        if path.startswith("/api/investigation-sessions/"):
                            rest = path[len("/api/investigation-sessions/"):].strip()
                            if rest.endswith("/events"):
                                sess_id = rest[:-len("/events")].strip()
                                self._send_json_response({
                                    "status": "success",
                                    "read_only": True,
                                    "execution_locked": True,
                                    "session_id": sess_id,
                                    "count": 0,
                                    "audit_trail": [],
                                })
                                return
                            req_id = rest
                            if latest and latest.get("session_id") == req_id:
                                self._send_json_response({
                                    "status": "success",
                                    "read_only": True,
                                    "execution_locked": True,
                                    "session": latest,
                                })
                                return
                            for h in history:
                                if isinstance(h, dict) and h.get("session_id") == req_id:
                                    self._send_json_response({
                                        "status": "success",
                                        "read_only": True,
                                        "execution_locked": True,
                                        "session": h,
                                    })
                                    return
                            self._send_json_response(
                                {
                                    "status": "not_found",
                                    "message": f"Investigation session '{req_id}' not found.",
                                    "read_only": True,
                                    "execution_locked": True,
                                },
                                status=HTTPStatus.NOT_FOUND,
                            )
                            return
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "count": len(history),
                            "sessions": history,
                        })
                        return

                    self._send_json_response(
                        {
                            "status": "not_available",
                            "message": "Investigation session manager not initialized.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

                # Manager is attached
                if path.startswith("/api/investigation-sessions/"):
                    rest = path[len("/api/investigation-sessions/"):].strip()
                    is_events = rest.endswith("/events")
                    if is_events:
                        session_id = rest[:-len("/events")].strip()
                    else:
                        session_id = rest

                    if not session_id or ".." in session_id or not SAFE_ID_PATTERN.match(session_id):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid session_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    sess = inv_manager.get_session(session_id)
                    if not sess:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Investigation session '{session_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    if is_events:
                        audits = inv_manager.get_audit_trail(session_id)
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "session_id": session_id,
                            "count": len(audits),
                            "audit_trail": [a.to_dict() for a in audits],
                        })
                        return

                    s_dict = sess.to_dict() if hasattr(sess, "to_dict") else sess
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "session": s_dict,
                    })
                    return

                # Listing
                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)
                status_param = query.get("status", [None])[0]
                candidate_param = query.get("candidate_id", [None])[0]

                sessions = inv_manager.list_sessions(
                    limit=limit_clamped,
                    offset=offset_clamped,
                    status=status_param,
                    candidate_id=candidate_param,
                )
                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "count": len(sessions),
                    "limit": limit_clamped,
                    "offset": offset_clamped,
                    "sessions": [s.to_dict() if hasattr(s, "to_dict") else s for s in sessions],
                })
                return

            # 20. Analytical Investigation Reports Endpoint (/api/investigation-reports, /api/investigation-reports/<report_id>, /api/investigation-reports/<report_id>/evidence, /api/investigation-reports/<report_id>/audit)
            if path == "/api/investigation-reports" or path.startswith("/api/investigation-reports/"):
                inv_rep_service = getattr(self.server, "investigation_reporting_service", None)
                state_store = getattr(service, "state_store", None)

                if path.startswith("/api/investigation-reports/"):
                    rest = path[len("/api/investigation-reports/"):].strip()
                    sub_resource = None
                    if rest.endswith("/evidence"):
                        report_id = rest[:-len("/evidence")].strip()
                        sub_resource = "evidence"
                    elif rest.endswith("/audit"):
                        report_id = rest[:-len("/audit")].strip()
                        sub_resource = "audit"
                    else:
                        report_id = rest

                    # Traversal, length, null byte, format protections
                    if (
                        not report_id
                        or ".." in report_id
                        or "/" in report_id
                        or "\\" in report_id
                        or "\0" in report_id
                        or len(report_id) > 128
                        or not SAFE_ID_PATTERN.match(report_id)
                    ):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid report_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    r_dict = None
                    if inv_rep_service is not None:
                        rep = inv_rep_service.get_report(report_id)
                        if rep is not None:
                            r_dict = rep.to_dict()

                    if r_dict is None and state_store is not None:
                        # Fallback to state store
                        latest = state_store.get("investigation_report:latest", None)
                        if isinstance(latest, dict) and latest.get("report_id") == report_id:
                            r_dict = latest
                        else:
                            item = state_store.get(f"investigation_report:{report_id}", None)
                            if isinstance(item, dict):
                                r_dict = item
                            else:
                                history = state_store.get("investigation_report:history", [])
                                if isinstance(history, list):
                                    for h in history:
                                        if isinstance(h, dict) and h.get("report_id") == report_id:
                                            r_dict = h
                                            break

                    if r_dict is None:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Investigation report '{report_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    if sub_resource == "evidence":
                        ev_matrix = r_dict.get("evidence_matrix", [])
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "report_id": report_id,
                            "count": len(ev_matrix),
                            "evidence_matrix": ev_matrix,
                        })
                        return

                    if sub_resource == "audit":
                        audit_summary = r_dict.get("audit_summary", None)
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "report_id": report_id,
                            "audit_summary": audit_summary,
                        })
                        return

                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "report": r_dict,
                    })
                    return

                # Listing endpoint: /api/investigation-reports
                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)
                cand_param = query.get("candidate_id", [None])[0]
                sess_param = query.get("session_id", [None])[0]
                status_param = query.get("status", [None])[0]

                if inv_rep_service is not None:
                    reports = inv_rep_service.list_reports(
                        candidate_id=cand_param,
                        session_id=sess_param,
                        status=status_param,
                        limit=limit_clamped,
                        offset=offset_clamped,
                    )
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(reports),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "reports": [r.to_dict() if hasattr(r, "to_dict") else r for r in reports],
                    })
                    return
                elif state_store is not None:
                    history = state_store.get("investigation_report:history", [])
                    if not isinstance(history, list):
                        history = []
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(history),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "reports": history[offset_clamped : offset_clamped + limit_clamped],
                    })
                    return
                else:
                    self._send_json_response(
                        {
                            "status": "not_available",
                            "message": "Investigation reporting service not initialized.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

            # 21. Cross-Investigation Comparisons Endpoint (/api/investigation-comparisons, /api/investigation-comparisons/<comparison_id>, /api/investigation-comparisons/<comparison_id>/discrepancies)
            if path == "/api/investigation-comparisons" or path.startswith("/api/investigation-comparisons/"):
                comp_engine = getattr(self.server, "investigation_comparison_engine", None)
                state_store = getattr(service, "state_store", None)

                if path.startswith("/api/investigation-comparisons/"):
                    rest = path[len("/api/investigation-comparisons/"):].strip()
                    sub_resource = None
                    if rest.endswith("/discrepancies"):
                        comp_id = rest[:-len("/discrepancies")].strip()
                        sub_resource = "discrepancies"
                    else:
                        comp_id = rest

                    # Traversal, length, null byte, format protections
                    if (
                        not comp_id
                        or ".." in comp_id
                        or "/" in comp_id
                        or "\\" in comp_id
                        or "\0" in comp_id
                        or len(comp_id) > 128
                        or not SAFE_ID_PATTERN.match(comp_id)
                    ):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid comparison_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    c_dict = None
                    if comp_engine is not None:
                        cmp_obj = comp_engine.get_comparison(comp_id)
                        if cmp_obj is not None:
                            c_dict = cmp_obj.to_dict()

                    if c_dict is None and state_store is not None:
                        latest = state_store.get("investigation_comparison:latest", None)
                        if isinstance(latest, dict) and latest.get("comparison_id") == comp_id:
                            c_dict = latest
                        else:
                            item = state_store.get(f"investigation_comparison:{comp_id}", None)
                            if isinstance(item, dict):
                                c_dict = item

                    if c_dict is None:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Investigation comparison '{comp_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    if sub_resource == "discrepancies":
                        discs = c_dict.get("discrepancy_summary", [])
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "comparison_id": comp_id,
                            "count": len(discs),
                            "discrepancies": discs,
                        })
                        return

                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "comparison": c_dict,
                    })
                    return

                # Listing endpoint: /api/investigation-comparisons
                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)
                rep_param = query.get("report_id", [None])[0]
                state_param = query.get("comparability_state", [None])[0]

                if comp_engine is not None:
                    comparisons = comp_engine.list_comparisons(
                        report_id=rep_param,
                        comparability_state=state_param,
                        limit=limit_clamped,
                        offset=offset_clamped,
                    )
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(comparisons),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "comparisons": [c.to_dict() if hasattr(c, "to_dict") else c for c in comparisons],
                    })
                    return
                elif state_store is not None:
                    history = state_store.get("investigation_comparison:history", [])
                    if not isinstance(history, list):
                        history = []
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(history),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "comparisons": history[offset_clamped : offset_clamped + limit_clamped],
                    })
                    return
                else:
                    self._send_json_response(
                        {
                            "status": "not_available",
                            "message": "Investigation comparison engine not initialized.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

            # 22. Investigation Discrepancy Resolutions Endpoint (/api/discrepancy-resolutions, /api/discrepancy-resolutions/<resolution_case_id>, /api/discrepancy-resolutions/<resolution_case_id>/evidence, /api/discrepancy-resolutions/<resolution_case_id>/audit)
            if path == "/api/discrepancy-resolutions" or path.startswith("/api/discrepancy-resolutions/"):
                res_service = getattr(self.server, "discrepancy_resolution_service", None)
                state_store = getattr(service, "state_store", None)

                if path.startswith("/api/discrepancy-resolutions/"):
                    rest = path[len("/api/discrepancy-resolutions/"):].strip()
                    sub_resource = None
                    if rest.endswith("/evidence"):
                        case_id = rest[:-len("/evidence")].strip()
                        sub_resource = "evidence"
                    elif rest.endswith("/audit"):
                        case_id = rest[:-len("/audit")].strip()
                        sub_resource = "audit"
                    else:
                        case_id = rest

                    # Traversal, length, null byte, format protections
                    if (
                        not case_id
                        or ".." in case_id
                        or "/" in case_id
                        or "\\" in case_id
                        or "\0" in case_id
                        or len(case_id) > 128
                        or not SAFE_ID_PATTERN.match(case_id)
                    ):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid resolution_case_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    c_dict = None
                    if res_service is not None:
                        case_obj = res_service.get_case(case_id)
                        if case_obj is not None:
                            c_dict = case_obj.to_dict()

                    if c_dict is None and state_store is not None:
                        latest = state_store.get("discrepancy_resolution:latest", None)
                        if isinstance(latest, dict) and latest.get("resolution_case_id") == case_id:
                            c_dict = latest
                        else:
                            item = state_store.get(f"discrepancy_resolution:{case_id}", None)
                            if isinstance(item, dict):
                                c_dict = item

                    if c_dict is None:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Resolution case '{case_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    if sub_resource == "evidence":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "resolution_case_id": case_id,
                            "supporting_evidence_ids": c_dict.get("supporting_evidence_ids", []),
                            "reconciliation_records": c_dict.get("reconciliation_records", []),
                        })
                        return

                    if sub_resource == "audit":
                        audits = c_dict.get("audit_trail", [])
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "resolution_case_id": case_id,
                            "count": len(audits),
                            "audit_trail": audits,
                        })
                        return

                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "case": c_dict,
                    })
                    return

                # Listing endpoint: /api/discrepancy-resolutions
                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)
                comp_param = query.get("comparison_id", [None])[0]
                disc_param = query.get("discrepancy_id", [None])[0]
                state_param = query.get("state", [None])[0]

                if res_service is not None:
                    cases = res_service.list_cases(
                        comparison_id=comp_param,
                        discrepancy_id=disc_param,
                        state=state_param,
                        limit=limit_clamped,
                        offset=offset_clamped,
                    )
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(cases),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "cases": [c.to_dict() if hasattr(c, "to_dict") else c for c in cases],
                    })
                    return
                elif state_store is not None:
                    history = state_store.get("discrepancy_resolution:history", [])
                    if not isinstance(history, list):
                        history = []
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(history),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "cases": history[offset_clamped : offset_clamped + limit_clamped],
                    })
                    return
                else:
                    self._send_json_response(
                        {
                            "status": "not_available",
                            "message": "Discrepancy resolution service not initialized.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

            # 23. Consolidated Investigation Outcomes Endpoint (/api/investigation-outcomes, /api/investigation-outcomes/<id>, /api/investigation-outcomes/<id>/evidence, /api/investigation-outcomes/<id>/conflicts, /api/investigation-outcomes/<id>/limitations, /api/investigation-outcomes/<id>/provenance)
            if path == "/api/investigation-outcomes" or path.startswith("/api/investigation-outcomes/"):
                cons_service = getattr(self.server, "consolidation_service", None)
                state_store = getattr(service, "state_store", None)

                if path.startswith("/api/investigation-outcomes/"):
                    rest = path[len("/api/investigation-outcomes/"):].strip()
                    sub_resource = None
                    if rest.endswith("/evidence"):
                        outcome_id = rest[:-len("/evidence")].strip()
                        sub_resource = "evidence"
                    elif rest.endswith("/conflicts"):
                        outcome_id = rest[:-len("/conflicts")].strip()
                        sub_resource = "conflicts"
                    elif rest.endswith("/limitations"):
                        outcome_id = rest[:-len("/limitations")].strip()
                        sub_resource = "limitations"
                    elif rest.endswith("/provenance"):
                        outcome_id = rest[:-len("/provenance")].strip()
                        sub_resource = "provenance"
                    else:
                        outcome_id = rest

                    # Traversal, length, null byte, format protections
                    if (
                        not outcome_id
                        or ".." in outcome_id
                        or "/" in outcome_id
                        or "\\" in outcome_id
                        or "\0" in outcome_id
                        or len(outcome_id) > 128
                        or not SAFE_ID_PATTERN.match(outcome_id)
                    ):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid outcome_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    o_dict = None
                    if cons_service is not None:
                        out_obj = cons_service.get_outcome(outcome_id)
                        if out_obj is not None:
                            o_dict = out_obj.to_dict()

                    if o_dict is None and state_store is not None:
                        latest = state_store.get("investigation_outcome:latest", None)
                        if isinstance(latest, dict) and latest.get("outcome_id") == outcome_id:
                            o_dict = latest
                        else:
                            item = state_store.get(f"investigation_outcome:{outcome_id}", None)
                            if isinstance(item, dict):
                                o_dict = item

                    if o_dict is None:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Consolidated outcome '{outcome_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    if sub_resource == "evidence":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "outcome_id": outcome_id,
                            "facts": o_dict.get("facts", []),
                            "interpretations": o_dict.get("interpretations", []),
                            "human_review_decisions": o_dict.get("human_review_decisions", []),
                            "evidence_summary": o_dict.get("evidence_summary", {}),
                        })
                        return

                    if sub_resource == "conflicts":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "outcome_id": outcome_id,
                            "conflicts": o_dict.get("conflicts", []),
                            "unresolved_items": o_dict.get("unresolved_items", []),
                            "resolved_items": o_dict.get("resolved_items", []),
                        })
                        return

                    if sub_resource == "limitations":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "outcome_id": outcome_id,
                            "limitations": o_dict.get("limitations", []),
                            "quality_summary": o_dict.get("quality_summary", {}),
                        })
                        return

                    if sub_resource == "provenance":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "outcome_id": outcome_id,
                            "traceability": o_dict.get("traceability", {}),
                            "provenance_summary": o_dict.get("provenance_summary", {}),
                            "reproducibility_summary": o_dict.get("reproducibility_summary", {}),
                        })
                        return

                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "outcome": o_dict,
                    })
                    return

                # Listing endpoint: /api/investigation-outcomes
                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)
                inv_param = query.get("investigation_id", [None])[0]
                state_param = query.get("state", [None])[0]

                if cons_service is not None:
                    outcomes = cons_service.list_outcomes(
                        investigation_id=inv_param,
                        state=state_param,
                        limit=limit_clamped,
                        offset=offset_clamped,
                    )
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(outcomes),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "outcomes": [o.to_dict() if hasattr(o, "to_dict") else o for o in outcomes],
                    })
                    return
                elif state_store is not None:
                    history = state_store.get("investigation_outcome:history", [])
                    if not isinstance(history, list):
                        history = []
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(history),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "outcomes": history[offset_clamped : offset_clamped + limit_clamped],
                    })
                    return
                else:
                    self._send_json_response(
                        {
                            "status": "not_available",
                            "message": "Investigation outcome consolidation service not initialized.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

            # 24. Consolidated Outcome Comparisons Endpoint (/api/consolidated-outcome-comparisons, /api/consolidated-outcome-comparisons/<id>, /api/consolidated-outcome-comparisons/<id>/dimensions, /api/consolidated-outcome-comparisons/<id>/evidence, /api/consolidated-outcome-comparisons/<id>/coverage, /api/consolidated-outcome-comparisons/<id>/divergences, /api/consolidated-outcome-comparisons/<id>/provenance)
            if path == "/api/consolidated-outcome-comparisons" or path.startswith("/api/consolidated-outcome-comparisons/"):
                c_service = getattr(self.server, "consistency_service", None)
                state_store = getattr(service, "state_store", None)

                if path.startswith("/api/consolidated-outcome-comparisons/"):
                    rest = path[len("/api/consolidated-outcome-comparisons/"):].strip()
                    sub_resource = None
                    if rest.endswith("/dimensions"):
                        comp_id = rest[:-len("/dimensions")].strip()
                        sub_resource = "dimensions"
                    elif rest.endswith("/evidence"):
                        comp_id = rest[:-len("/evidence")].strip()
                        sub_resource = "evidence"
                    elif rest.endswith("/coverage"):
                        comp_id = rest[:-len("/coverage")].strip()
                        sub_resource = "coverage"
                    elif rest.endswith("/divergences"):
                        comp_id = rest[:-len("/divergences")].strip()
                        sub_resource = "divergences"
                    elif rest.endswith("/provenance"):
                        comp_id = rest[:-len("/provenance")].strip()
                        sub_resource = "provenance"
                    else:
                        comp_id = rest

                    # Traversal, length, null byte, format protections
                    if (
                        not comp_id
                        or ".." in comp_id
                        or "/" in comp_id
                        or "\\" in comp_id
                        or "\0" in comp_id
                        or len(comp_id) > 128
                        or not SAFE_ID_PATTERN.match(comp_id)
                    ):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid comparison_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    c_dict = None
                    if c_service is not None:
                        comp_obj = c_service.get_comparison(comp_id)
                        if comp_obj is not None:
                            c_dict = comp_obj.to_dict()

                    if c_dict is None and state_store is not None:
                        latest = state_store.get("consolidated_outcome_comparison:latest", None)
                        if isinstance(latest, dict) and latest.get("comparison_id") == comp_id:
                            c_dict = latest
                        else:
                            item = state_store.get(f"consolidated_outcome_comparison:{comp_id}", None)
                            if isinstance(item, dict):
                                c_dict = item

                    if c_dict is None:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Consolidated outcome comparison '{comp_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    if sub_resource == "dimensions":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "comparison_id": comp_id,
                            "dimensions": c_dict.get("dimensions", []),
                        })
                        return

                    if sub_resource == "evidence":
                        ev = c_dict.get("evidence_consistency", [])
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "comparison_id": comp_id,
                            "count": len(ev),
                            "evidence_consistency": ev,
                        })
                        return

                    if sub_resource == "coverage":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "comparison_id": comp_id,
                            "review_coverage": c_dict.get("review_coverage", {}),
                        })
                        return

                    if sub_resource == "divergences":
                        divs = c_dict.get("divergence_items", [])
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "comparison_id": comp_id,
                            "count": len(divs),
                            "divergences": divs,
                        })
                        return

                    if sub_resource == "provenance":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "comparison_id": comp_id,
                            "provenance_comparison": c_dict.get("provenance_comparison", {}),
                            "reproducibility_comparison": c_dict.get("reproducibility_comparison", {}),
                            "traceability": c_dict.get("traceability", {}),
                        })
                        return

                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "comparison": c_dict,
                    })
                    return

                # Listing endpoint: /api/consolidated-outcome-comparisons
                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)
                state_param = query.get("comparability_state", [None])[0]

                if c_service is not None:
                    comparisons = c_service.list_comparisons(
                        comparability_state=state_param,
                        limit=limit_clamped,
                        offset=offset_clamped,
                    )
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(comparisons),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "comparisons": [c.to_dict() if hasattr(c, "to_dict") else c for c in comparisons],
                    })
                    return
                elif state_store is not None:
                    history = state_store.get("consolidated_outcome_comparison:history", [])
                    if not isinstance(history, list):
                        history = []
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(history),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "comparisons": history[offset_clamped : offset_clamped + limit_clamped],
                    })
                    return
                else:
                    self._send_json_response(
                        {
                            "status": "not_available",
                            "message": "Consolidated outcome consistency service not initialized.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

            # 25. Historical Review Snapshot Endpoint (/api/historical-review, /api/historical-review/<id>, /api/historical-review/<id>/evolution, /api/historical-review/<id>/coverage, /api/historical-review/<id>/conflicts, /api/historical-review/<id>/provenance)
            if path == "/api/historical-review" or path.startswith("/api/historical-review/"):
                ev_service = getattr(self.server, "evolution_service", None)
                state_store = getattr(ev_service, "state_store", None) or getattr(self.server, "state_store", None) or getattr(service, "state_store", None)

                if path.startswith("/api/historical-review/"):
                    rest = path[len("/api/historical-review/"):].strip()
                    sub_resource = None
                    if rest.endswith("/evolution"):
                        snap_id = rest[:-len("/evolution")].strip()
                        sub_resource = "evolution"
                    elif rest.endswith("/coverage"):
                        snap_id = rest[:-len("/coverage")].strip()
                        sub_resource = "coverage"
                    elif rest.endswith("/conflicts"):
                        snap_id = rest[:-len("/conflicts")].strip()
                        sub_resource = "conflicts"
                    elif rest.endswith("/provenance"):
                        snap_id = rest[:-len("/provenance")].strip()
                        sub_resource = "provenance"
                    else:
                        snap_id = rest

                    # Traversal, length, null byte, format protections
                    if (
                        not snap_id
                        or ".." in snap_id
                        or "/" in snap_id
                        or "\\" in snap_id
                        or "\0" in snap_id
                        or len(snap_id) > 128
                        or not SAFE_ID_PATTERN.match(snap_id)
                    ):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid snapshot_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    snap_obj = None
                    if ev_service is not None:
                        snap_obj = ev_service.get_snapshot(snap_id)

                    snap_dict = None
                    if snap_obj is not None:
                        snap_dict = snap_obj.to_dict()
                    elif state_store is not None:
                        latest = state_store.get("historical_review:latest", None)
                        if isinstance(latest, dict) and latest.get("snapshot_id") == snap_id:
                            snap_dict = latest
                        else:
                            item = state_store.get(f"historical_review:{snap_id}", None)
                            if isinstance(item, dict):
                                snap_dict = item

                    if snap_dict is None:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Historical review snapshot '{snap_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    if sub_resource == "evolution":
                        inv_id = snap_dict.get("investigation_id")
                        evolutions = []
                        if ev_service is not None and inv_id:
                            ev_list = ev_service.list_evolution_history(investigation_id=inv_id, limit=50)
                            evolutions = [e.to_dict() for e in ev_list]
                        self._send_json_response({
                            "status": "success",
                            "snapshot_id": snap_id,
                            "investigation_id": inv_id,
                            "evolutions": evolutions,
                            "read_only": True,
                            "execution_locked": True,
                        })
                        return

                    if sub_resource == "coverage":
                        self._send_json_response({
                            "status": "success",
                            "snapshot_id": snap_id,
                            "coverage_state": snap_dict.get("coverage_state", {}),
                            "read_only": True,
                            "execution_locked": True,
                        })
                        return

                    if sub_resource == "conflicts":
                        self._send_json_response({
                            "status": "success",
                            "snapshot_id": snap_id,
                            "conflict_state": snap_dict.get("conflict_state", {}),
                            "read_only": True,
                            "execution_locked": True,
                        })
                        return

                    if sub_resource == "provenance":
                        self._send_json_response({
                            "status": "success",
                            "snapshot_id": snap_id,
                            "provenance_state": snap_dict.get("provenance_state", {}),
                            "read_only": True,
                            "execution_locked": True,
                        })
                        return

                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "snapshot": snap_dict,
                    })
                    return

                # Listing endpoint: /api/historical-review
                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)
                inv_id_param = query.get("investigation_id", [None])[0]

                if ev_service is not None:
                    snapshots = ev_service.list_snapshots(
                        investigation_id=inv_id_param,
                        limit=limit_clamped,
                        offset=offset_clamped,
                    )
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(snapshots),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "snapshots": [s.to_dict() if hasattr(s, "to_dict") else s for s in snapshots],
                    })
                    return
                elif state_store is not None:
                    history = state_store.get("historical_review:history", [])
                    if not isinstance(history, list):
                        history = []
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(history),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "snapshots": history[offset_clamped : offset_clamped + limit_clamped],
                    })
                    return
                else:
                    self._send_json_response(
                        {
                            "status": "not_available",
                            "message": "Historical review evolution service not initialized.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

            # 26. Review Evolution Endpoint (/api/review-evolution, /api/review-evolution/<id>)
            if path == "/api/review-evolution" or path.startswith("/api/review-evolution/"):
                ev_service = getattr(self.server, "evolution_service", None)
                state_store = getattr(ev_service, "state_store", None) or getattr(self.server, "state_store", None) or getattr(service, "state_store", None)

                if path.startswith("/api/review-evolution/"):
                    rest = path[len("/api/review-evolution/"):].strip()
                    sub_resource = None
                    if rest.endswith("/dimensions"):
                        evol_id = rest[:-len("/dimensions")].strip()
                        sub_resource = "dimensions"
                    elif rest.endswith("/summary"):
                        evol_id = rest[:-len("/summary")].strip()
                        sub_resource = "summary"
                    elif rest.endswith("/traceability"):
                        evol_id = rest[:-len("/traceability")].strip()
                        sub_resource = "traceability"
                    else:
                        evol_id = rest

                    # Traversal, length, null byte, format protections
                    if (
                        not evol_id
                        or ".." in evol_id
                        or "/" in evol_id
                        or "\\" in evol_id
                        or "\0" in evol_id
                        or len(evol_id) > 128
                        or not SAFE_ID_PATTERN.match(evol_id)
                    ):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid evolution_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    evol_dict = None
                    if ev_service is not None:
                        evol_obj = ev_service.get_evolution(evol_id)
                        if evol_obj is not None:
                            evol_dict = evol_obj.to_dict()

                    if evol_dict is None and state_store is not None:
                        latest = state_store.get("historical_review_evolution:latest", None)
                        if isinstance(latest, dict) and latest.get("evolution_id") == evol_id:
                            evol_dict = latest
                        else:
                            item = state_store.get(f"historical_review_evolution:{evol_id}", None)
                            if isinstance(item, dict):
                                evol_dict = item

                    if evol_dict is None:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Review evolution record '{evol_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    if sub_resource == "dimensions":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "evolution_id": evol_id,
                            "changed_dimensions": evol_dict.get("changed_dimensions", []),
                        })
                        return

                    if sub_resource == "summary":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "evolution_id": evol_id,
                            "voice_summary": evol_dict.get("voice_summary", None),
                        })
                        return

                    if sub_resource == "traceability":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "evolution_id": evol_id,
                            "traceability": evol_dict.get("traceability", {}),
                        })
                        return

                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "evolution": evol_dict,
                    })
                    return

                # Listing endpoint: /api/review-evolution
                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)
                inv_id_param = query.get("investigation_id", [None])[0]

                if ev_service is not None:
                    evolutions = ev_service.list_evolution_history(
                        investigation_id=inv_id_param,
                        limit=limit_clamped,
                        offset=offset_clamped,
                    )
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(evolutions),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "evolutions": [e.to_dict() if hasattr(e, "to_dict") else e for e in evolutions],
                    })
                    return
                elif state_store is not None:
                    history = state_store.get("historical_review_evolution:history", [])
                    if not isinstance(history, list):
                        history = []
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(history),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "evolutions": history[offset_clamped : offset_clamped + limit_clamped],
                    })
                    return
                else:
                    self._send_json_response(
                        {
                            "status": "not_available",
                            "message": "Historical review evolution service not initialized.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

            # Endpoint 26: Historical Review Evolution Audit & Validation: /api/historical-review-audit
            if path == "/api/historical-review-audit" or path.startswith("/api/historical-review-audit/"):
                audit_service = getattr(self.server, "audit_service", None)
                state_store = getattr(self.server, "state_store", None) or getattr(
                    getattr(self.server, "reporting_service", None), "state_store", None
                )

                sub_path = path[len("/api/historical-review-audit"):]
                if sub_path.startswith("/"):
                    sub_path = sub_path[1:]
                parts = [p for p in sub_path.split("/") if p]

                if parts:
                    if len(parts) > 2:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Resource path '{path}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    report_id = parts[0]
                    sub_resource = parts[1] if len(parts) > 1 else None

                    if sub_resource is not None and sub_resource not in ("findings", "chain", "reproducibility", "summary"):
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Sub-resource '{sub_resource}' not found for report '{report_id}'.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    # Strict path traversal and safe ID verification
                    if (
                        ".." in report_id
                        or "/" in report_id
                        or "\\" in report_id
                        or "\0" in report_id
                        or len(report_id) > 128
                        or not SAFE_ID_PATTERN.match(report_id)
                    ):
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid report_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    report_dict = None
                    if audit_service is not None:
                        report_obj = audit_service.get_audit_report(report_id)
                        if report_obj is not None:
                            report_dict = report_obj.to_dict()

                    if report_dict is None and state_store is not None:
                        latest = state_store.get("historical_review_audit:latest", None)
                        if isinstance(latest, dict) and latest.get("report_id") == report_id:
                            report_dict = latest
                        else:
                            item = state_store.get(f"historical_review_audit:{report_id}", None)
                            if isinstance(item, dict):
                                report_dict = item

                    if report_dict is None:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Historical review audit report '{report_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    if sub_resource == "findings":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "report_id": report_id,
                            "findings": report_dict.get("findings", []),
                        })
                        return

                    if sub_resource == "chain":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "report_id": report_id,
                            "chain_result": report_dict.get("chain_result", None),
                        })
                        return

                    if sub_resource == "reproducibility":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "report_id": report_id,
                            "reproducibility_result": report_dict.get("reproducibility_result", None),
                        })
                        return

                    if sub_resource == "summary":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "report_id": report_id,
                            "voice_summary": report_dict.get("voice_summary", None),
                        })
                        return

                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "report": report_dict,
                    })
                    return

                # Listing endpoint: /api/historical-review-audit
                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)
                inv_id_param = query.get("investigation_id", [None])[0]
                if inv_id_param is not None:
                    inv_id_param = inv_id_param.strip()
                    if not SAFE_ID_PATTERN.match(inv_id_param) or ".." in inv_id_param:
                        self._send_json_response(
                            {
                                "status": "invalid_request",
                                "message": "Invalid investigation_id format.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                if audit_service is not None:
                    reports = audit_service.list_audit_reports(
                        investigation_id=inv_id_param,
                        limit=limit_clamped,
                        offset=offset_clamped,
                    )
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(reports),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "reports": [r.to_dict() if hasattr(r, "to_dict") else r for r in reports],
                    })
                    return
                elif state_store is not None:
                    history = state_store.get("historical_review_audit:history", [])
                    if not isinstance(history, list):
                        history = []
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(history),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "reports": history[offset_clamped : offset_clamped + limit_clamped],
                    })
                    return
                else:
                    self._send_json_response(
                        {
                            "status": "not_available",
                            "message": "Historical review audit service not initialized.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

            # Endpoint 27: Historical Review Audit Reliability & Coverage API (Phase 2, Step 34)
            if path == "/api/historical-review-reliability" or path.startswith("/api/historical-review-reliability/"):
                reliability_service = getattr(self.server, "reliability_service", None)
                state_store = getattr(self.server, "state_store", None)

                rel_sub = path[len("/api/historical-review-reliability"):]
                if rel_sub.startswith("/"):
                    rel_sub = rel_sub[1:]
                parts = [p for p in rel_sub.split("/") if p]

                if parts:
                    if len(parts) > 2:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Resource path '{path}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    raw_eval_id = parts[0]
                    sub_resource = parts[1] if len(parts) > 1 else None

                    if sub_resource is not None and sub_resource not in ("coverage", "repeatability", "regression", "summary"):
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Sub-resource '{sub_resource}' not found for evaluation '{raw_eval_id}'.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    # Sanitize and validate ID
                    eval_id = (raw_eval_id or "").strip()
                    if not SAFE_ID_PATTERN.match(eval_id) or ".." in eval_id or "/" in eval_id or "\\" in eval_id or len(eval_id) > 128:
                        self._send_json_response(
                            {
                                "status": "invalid_parameter",
                                "message": f"Invalid evaluation_id '{eval_id}'. Must be a safe identifier.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    eval_dict = None
                    if reliability_service is not None:
                        eval_obj = reliability_service.get_reliability_result(eval_id)
                        if eval_obj is not None:
                            eval_dict = eval_obj.to_dict()

                    if eval_dict is None and state_store is not None:
                        latest = state_store.get("historical_review_reliability:latest", None)
                        if isinstance(latest, dict) and latest.get("evaluation_id") == eval_id:
                            eval_dict = latest
                        else:
                            item = state_store.get(f"historical_review_reliability:{eval_id}", None)
                            if isinstance(item, dict):
                                eval_dict = item

                    if eval_dict is None:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Historical review reliability evaluation '{eval_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    if sub_resource == "coverage":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "evaluation_id": eval_id,
                            "coverage_summary": eval_dict.get("coverage_summary", None),
                        })
                        return

                    if sub_resource == "repeatability":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "evaluation_id": eval_id,
                            "repeatability_result": eval_dict.get("repeatability_result", None),
                        })
                        return

                    if sub_resource == "regression":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "evaluation_id": eval_id,
                            "regression_result": eval_dict.get("regression_result", None),
                        })
                        return

                    if sub_resource == "summary":
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "evaluation_id": eval_id,
                            "voice_summary": eval_dict.get("voice_summary", None),
                        })
                        return

                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "evaluation": eval_dict,
                    })
                    return

                # Listing endpoint: /api/historical-review-reliability
                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)
                inv_id_param = query.get("investigation_id", [None])[0]
                if inv_id_param is not None:
                    inv_id_param = inv_id_param.strip()
                    if not SAFE_ID_PATTERN.match(inv_id_param) or ".." in inv_id_param:
                        self._send_json_response(
                            {
                                "status": "invalid_parameter",
                                "message": f"Invalid investigation_id '{inv_id_param}'. Must be a safe identifier.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                if reliability_service is not None:
                    evaluations = reliability_service.list_reliability_results(
                        investigation_id=inv_id_param,
                        limit=limit_clamped,
                        offset=offset_clamped,
                    )
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(evaluations),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "evaluations": [e.to_dict() if hasattr(e, "to_dict") else e for e in evaluations],
                    })
                    return
                elif state_store is not None:
                    history = state_store.get("historical_review_reliability:history", [])
                    if not isinstance(history, list):
                        history = []
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(history),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "evaluations": history[offset_clamped : offset_clamped + limit_clamped],
                    })
                    return
                else:
                    self._send_json_response(
                        {
                            "status": "not_available",
                            "message": "Historical review reliability service not initialized.",
                            "read_only": True,
                            "execution_locked": True,
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return

            # Endpoint 28: Historical Review Audit Diagnostics & Observability API (Phase 2, Step 36)
            if path == "/api/historical-review-diagnostics" or path.startswith("/api/historical-review-diagnostics/"):
                diagnostics_service = getattr(self.server, "audit_diagnostics_service", None)
                state_store = getattr(self.server, "state_store", None)

                diag_sub = path[len("/api/historical-review-diagnostics"):]
                if diag_sub.startswith("/"):
                    diag_sub = diag_sub[1:]
                parts = [p for p in diag_sub.split("/") if p]

                # Case 1: Sub-resource /alerts
                if parts and parts[0] == "alerts":
                    if len(parts) > 2:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Resource path '{path}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    if len(parts) == 2:
                        alert_id = parts[1].strip()
                        if not SAFE_ID_PATTERN.match(alert_id) or ".." in alert_id or "/" in alert_id or "\\" in alert_id or len(alert_id) > 128:
                            self._send_json_response(
                                {
                                    "status": "invalid_parameter",
                                    "message": f"Invalid alert_id '{alert_id}'. Must be a safe identifier.",
                                    "read_only": True,
                                    "execution_locked": True,
                                },
                                status=HTTPStatus.BAD_REQUEST,
                            )
                            return

                        alert_obj = diagnostics_service.get_alert(alert_id) if diagnostics_service is not None else None
                        if alert_obj is None and state_store is not None:
                            stored = state_store.get(f"historical_review_alerts:{alert_id}", None)
                            if isinstance(stored, dict):
                                alert_obj = stored

                        if alert_obj is None:
                            self._send_json_response(
                                {
                                    "status": "not_found",
                                    "message": f"Operational alert '{alert_id}' not found.",
                                    "read_only": True,
                                    "execution_locked": True,
                                },
                                status=HTTPStatus.NOT_FOUND,
                            )
                            return

                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "alert": alert_obj.to_dict() if hasattr(alert_obj, "to_dict") else alert_obj,
                        })
                        return

                    # Listing alerts
                    try:
                        limit_val = int(query.get("limit", ["50"])[0])
                        offset_val = int(query.get("offset", ["0"])[0])
                    except (ValueError, TypeError):
                        limit_val = 50
                        offset_val = 0
                    limit_clamped = max(1, min(100, limit_val))
                    offset_clamped = max(0, offset_val)

                    inv_id_param = query.get("investigation_id", [None])[0]
                    if inv_id_param is not None:
                        inv_id_param = inv_id_param.strip()
                        if not SAFE_ID_PATTERN.match(inv_id_param) or ".." in inv_id_param:
                            self._send_json_response(
                                {
                                    "status": "invalid_parameter",
                                    "message": f"Invalid investigation_id '{inv_id_param}'. Must be a safe identifier.",
                                    "read_only": True,
                                    "execution_locked": True,
                                },
                                status=HTTPStatus.BAD_REQUEST,
                            )
                            return

                    cat_param = query.get("category", [None])[0]
                    sev_param = query.get("severity", [None])[0]
                    active_only_param = query.get("active_only", ["false"])[0].lower() in ("true", "1")
                    include_suppressed_param = query.get("include_suppressed", ["false"])[0].lower() in ("true", "1")

                    if diagnostics_service is not None:
                        alerts_list = diagnostics_service.list_alerts(
                            investigation_id=inv_id_param,
                            category=cat_param,
                            severity=sev_param,
                            active_only=active_only_param,
                            limit=limit_clamped,
                            offset=offset_clamped,
                            include_suppressed=include_suppressed_param,
                        )
                        alerts_data = [a.to_dict() if hasattr(a, "to_dict") else a for a in alerts_list]
                    else:
                        alerts_data = []

                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(alerts_data),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "alerts": alerts_data,
                    })
                    return

                # Case 2: Sub-resource /summary
                if parts and parts[0] == "summary":
                    inv_id_param = query.get("investigation_id", [None])[0]
                    if not inv_id_param:
                        self._send_json_response(
                            {
                                "status": "invalid_parameter",
                                "message": "Missing required query parameter 'investigation_id'.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return
                    inv_id_param = inv_id_param.strip()
                    if not SAFE_ID_PATTERN.match(inv_id_param) or ".." in inv_id_param:
                        self._send_json_response(
                            {
                                "status": "invalid_parameter",
                                "message": f"Invalid investigation_id '{inv_id_param}'. Must be a safe identifier.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    if diagnostics_service is not None:
                        summary = diagnostics_service.get_diagnostic_summary(inv_id_param)
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "summary": summary.to_dict(),
                        })
                        return
                    else:
                        self._send_json_response(
                            {
                                "status": "not_available",
                                "message": "Historical review diagnostics service not initialized.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                # Case 2b: Sub-resource /retention
                if parts and parts[0] == "retention":
                    if len(parts) > 1:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Resource path '{path}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    if diagnostics_service is not None:
                        ret_data = diagnostics_service.get_retention_status()
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "retention": ret_data,
                        })
                        return
                    else:
                        self._send_json_response(
                            {
                                "status": "not_available",
                                "message": "Historical review diagnostics service not initialized.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                # Case 2c: Sub-resource /exports
                if parts and parts[0] == "exports":
                    if len(parts) > 2:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Resource path '{path}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    if len(parts) == 2:
                        export_id = parts[1].strip()
                        if not SAFE_ID_PATTERN.match(export_id) or ".." in export_id or "/" in export_id or "\\" in export_id or len(export_id) > 128:
                            self._send_json_response(
                                {
                                    "status": "invalid_parameter",
                                    "message": f"Invalid export_id '{export_id}'. Must be a safe identifier.",
                                    "read_only": True,
                                    "execution_locked": True,
                                },
                                status=HTTPStatus.BAD_REQUEST,
                            )
                            return

                        export_obj = diagnostics_service.get_export(export_id) if diagnostics_service is not None else None
                        if export_obj is None and state_store is not None:
                            stored = state_store.get(f"historical_review_exports:{export_id}", None)
                            if isinstance(stored, dict):
                                export_obj = stored

                        if export_obj is None:
                            self._send_json_response(
                                {
                                    "status": "not_found",
                                    "message": f"Diagnostics export '{export_id}' not found.",
                                    "read_only": True,
                                    "execution_locked": True,
                                },
                                status=HTTPStatus.NOT_FOUND,
                            )
                            return

                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "export": export_obj.to_dict() if hasattr(export_obj, "to_dict") else export_obj,
                        })
                        return

                    # Listing exports
                    try:
                        limit_val = int(query.get("limit", ["50"])[0])
                        offset_val = int(query.get("offset", ["0"])[0])
                    except (ValueError, TypeError):
                        limit_val = 50
                        offset_val = 0
                    limit_clamped = max(1, min(100, limit_val))
                    offset_clamped = max(0, offset_val)

                    inv_id_param = query.get("investigation_id", [None])[0]
                    if inv_id_param is not None:
                        inv_id_param = inv_id_param.strip()
                        if not SAFE_ID_PATTERN.match(inv_id_param) or ".." in inv_id_param:
                            self._send_json_response(
                                {
                                    "status": "invalid_parameter",
                                    "message": f"Invalid investigation_id '{inv_id_param}'. Must be a safe identifier.",
                                    "read_only": True,
                                    "execution_locked": True,
                                },
                                status=HTTPStatus.BAD_REQUEST,
                            )
                            return

                    exports_data = []
                    if diagnostics_service is not None:
                        exports_list = diagnostics_service.list_exports(
                            investigation_id=inv_id_param,
                            limit=limit_clamped,
                            offset=offset_clamped,
                        )
                        exports_data = [e.to_dict() if hasattr(e, "to_dict") else e for e in exports_list]

                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(exports_data),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "exports": exports_data,
                    })
                    return

                # Case 2d: Sub-resource /reconciliations
                if parts and parts[0] == "reconciliations":
                    if len(parts) > 2:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Resource path '{path}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    if len(parts) == 2:
                        recon_id = parts[1].strip()
                        if not SAFE_ID_PATTERN.match(recon_id) or ".." in recon_id or "/" in recon_id or "\\" in recon_id or len(recon_id) > 128:
                            self._send_json_response(
                                {
                                    "status": "invalid_parameter",
                                    "message": f"Invalid reconciliation_id '{recon_id}'. Must be a safe identifier.",
                                    "read_only": True,
                                    "execution_locked": True,
                                },
                                status=HTTPStatus.BAD_REQUEST,
                            )
                            return

                        recon_obj = diagnostics_service.get_reconciliation(recon_id) if diagnostics_service is not None else None
                        if recon_obj is None and state_store is not None:
                            stored = state_store.get(f"historical_review_reconciliations:{recon_id}", None)
                            if isinstance(stored, dict):
                                recon_obj = stored

                        if recon_obj is None:
                            self._send_json_response(
                                {
                                    "status": "not_found",
                                    "message": f"Diagnostics reconciliation report '{recon_id}' not found.",
                                    "read_only": True,
                                    "execution_locked": True,
                                },
                                status=HTTPStatus.NOT_FOUND,
                            )
                            return

                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "reconciliation": recon_obj.to_dict() if hasattr(recon_obj, "to_dict") else recon_obj,
                        })
                        return

                    # Listing reconciliations
                    try:
                        limit_val = int(query.get("limit", ["50"])[0])
                        offset_val = int(query.get("offset", ["0"])[0])
                    except (ValueError, TypeError):
                        limit_val = 50
                        offset_val = 0
                    limit_clamped = max(1, min(100, limit_val))
                    offset_clamped = max(0, offset_val)

                    export_id_param = query.get("export_id", [None])[0]
                    if export_id_param is not None:
                        export_id_param = export_id_param.strip()
                        if not SAFE_ID_PATTERN.match(export_id_param) or ".." in export_id_param:
                            self._send_json_response(
                                {
                                    "status": "invalid_parameter",
                                    "message": f"Invalid export_id '{export_id_param}'. Must be a safe identifier.",
                                    "read_only": True,
                                    "execution_locked": True,
                                },
                                status=HTTPStatus.BAD_REQUEST,
                            )
                            return

                    recons_data = []
                    if diagnostics_service is not None:
                        recons_list = diagnostics_service.list_reconciliations(
                            export_id=export_id_param,
                            limit=limit_clamped,
                            offset=offset_clamped,
                        )
                        recons_data = [r.to_dict() if hasattr(r, "to_dict") else r for r in recons_list]

                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "count": len(recons_data),
                        "limit": limit_clamped,
                        "offset": offset_clamped,
                        "reconciliations": recons_data,
                    })
                    return

                # Case 3: Specific diagnostic record by ID: /api/historical-review-diagnostics/<diag_id>
                if parts:
                    if len(parts) > 1:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Resource path '{path}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    diag_id = parts[0].strip()
                    if not SAFE_ID_PATTERN.match(diag_id) or ".." in diag_id or "/" in diag_id or "\\" in diag_id or len(diag_id) > 128:
                        self._send_json_response(
                            {
                                "status": "invalid_parameter",
                                "message": f"Invalid diagnostic_id '{diag_id}'. Must be a safe identifier.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                    rec_obj = diagnostics_service.get_diagnostic(diag_id) if diagnostics_service is not None else None
                    if rec_obj is None and state_store is not None:
                        stored = state_store.get(f"historical_review_diagnostics:{diag_id}", None)
                        if isinstance(stored, dict):
                            rec_obj = stored

                    if rec_obj is None:
                        self._send_json_response(
                            {
                                "status": "not_found",
                                "message": f"Diagnostic record '{diag_id}' not found.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "diagnostic": rec_obj.to_dict() if hasattr(rec_obj, "to_dict") else rec_obj,
                    })
                    return

                # Case 4: Listing diagnostics: /api/historical-review-diagnostics
                try:
                    limit_val = int(query.get("limit", ["50"])[0])
                    offset_val = int(query.get("offset", ["0"])[0])
                except (ValueError, TypeError):
                    limit_val = 50
                    offset_val = 0
                limit_clamped = max(1, min(100, limit_val))
                offset_clamped = max(0, offset_val)

                inv_id_param = query.get("investigation_id", [None])[0]
                if inv_id_param is not None:
                    inv_id_param = inv_id_param.strip()
                    if not SAFE_ID_PATTERN.match(inv_id_param) or ".." in inv_id_param:
                        self._send_json_response(
                            {
                                "status": "invalid_parameter",
                                "message": f"Invalid investigation_id '{inv_id_param}'. Must be a safe identifier.",
                                "read_only": True,
                                "execution_locked": True,
                            },
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                cat_param = query.get("category", [None])[0]
                sev_param = query.get("severity", [None])[0]

                records_data = []
                if diagnostics_service is not None:
                    records = diagnostics_service.list_diagnostics(
                        investigation_id=inv_id_param,
                        category=cat_param,
                        severity=sev_param,
                        limit=limit_clamped,
                        offset=offset_clamped,
                    )
                    records_data = [r.to_dict() if hasattr(r, "to_dict") else r for r in records]

                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "count": len(records_data),
                    "limit": limit_clamped,
                    "offset": offset_clamped,
                    "diagnostics": records_data,
                })
            # 13. Paper Order Lifecycle Metrics & Latency Profiling Endpoint (/api/paper/metrics)
            if path == "/api/paper/metrics":
                coordinator = getattr(self.server, "paper_coordinator", None)
                if coordinator is not None and hasattr(coordinator, "metrics_tracker"):
                    tracker = coordinator.metrics_tracker
                    summary = tracker.get_summary()
                    profile = tracker.profiler.get_profile_summary()
                else:
                    summary = {
                        "total_events_recorded": 0,
                        "distinct_client_orders": 0,
                        "distinct_order_ids": 0,
                        "distinct_orders_tracked": 0,
                        "active_orders": 0,
                        "terminal_orders": {"completed": 0, "rejected": 0, "cancelled": 0, "failed": 0},
                        "stage_counts": {},
                        "status_counts": {},
                        "duplicate_events_ignored": 0,
                        "recent_events": [],
                        "deterministic_digest": "",
                    }
                    profile = {"unit": "milliseconds", "stages": {}, "fingerprint": ""}

                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "broker_provider": "none",
                    "lifecycle": summary,
                    "latency": profile,
                })
                return

            # 14. Historical-Spread-Based Slippage Calibration Endpoint (/api/paper/calibration)
            if path == "/api/paper/calibration":
                symbol_param = query.get("symbol", [None])[0]
                as_of_param = query.get("as_of", [None])[0]
                as_of_time: Optional[float] = None
                if as_of_param is not None:
                    try:
                        as_of_time = float(as_of_param)
                        if not math.isfinite(as_of_time) or as_of_time < 0:
                            raise ValueError()
                    except ValueError:
                        self._send_json_response(
                            {"error": "Bad Request", "message": f"Invalid as_of timestamp '{as_of_param}'. Must be a positive finite number."},
                            status=HTTPStatus.BAD_REQUEST,
                        )
                        return

                coordinator = getattr(self.server, "paper_coordinator", None)
                calibrator = getattr(coordinator, "calibrator", None) if coordinator is not None else None

                if calibrator is None:
                    self._send_json_response({
                        "status": "no_data",
                        "read_only": True,
                        "execution_locked": True,
                        "message": "Paper workflow coordinator or calibrator not available.",
                    })
                    return

                if symbol_param:
                    clean_sym = symbol_param.strip().upper()
                    rep = calibrator.calibrate(clean_sym, as_of_time=as_of_time)
                    rep_dict = rep.to_dict()
                    self._send_json_response(rep_dict)
                    return
                else:
                    all_reports = calibrator.calibrate_all(as_of_time=as_of_time)
                    data = {sym: r.to_dict() for sym, r in all_reports.items()}
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "reports": data,
                    })
                    return

            # 15. Paper Trading Multi-Scenario Stress Testing & Parameter Sensitivity Endpoint (/api/paper/stress-test)
            if path in ("/api/paper/stress-test", "/api/paper/stress-test/report"):
                scenario_id = query.get("scenario_id", [None])[0]
                coordinator = getattr(self.server, "paper_coordinator", None)

                report_dict: Optional[Dict[str, Any]] = None
                if coordinator is not None and hasattr(coordinator, "get_stress_test_report"):
                    report_dict = coordinator.get_stress_test_report()

                if report_dict is None:
                    stress_svc = getattr(self.server, "stress_service", None)
                    if stress_svc is not None and hasattr(stress_svc, "to_dict"):
                        report_dict = stress_svc.to_dict()
                    elif isinstance(stress_svc, dict):
                        report_dict = stress_svc

                if report_dict is None:
                    self._send_json_response({
                        "status": "no_data",
                        "read_only": True,
                        "execution_locked": True,
                        "broker_provider": "none",
                        "message": "Paper trading stress test report not available or not yet executed.",
                    })
                    return

                if scenario_id:
                    clean_id = scenario_id.strip()
                    scenarios = report_dict.get("scenario_results", {})
                    if clean_id in scenarios:
                        self._send_json_response({
                            "status": "success",
                            "read_only": True,
                            "execution_locked": True,
                            "scenario": scenarios[clean_id],
                        })
                        return
                    else:
                        self._send_json_response(
                            {"error": "Not Found", "message": f"Scenario ID '{clean_id}' not found in stress test report."},
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return

                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "broker_provider": "none",
                    "report": report_dict,
                })
                return

            # 16. Paper Trading Multi-Asset Portfolio Allocation & Sleeves (/api/paper/allocations, /api/paper/sleeves, /api/paper/portfolio-exposure)
            if path in ("/api/paper/allocations", "/api/paper/allocation"):
                coordinator = getattr(self.server, "paper_coordinator", None)
                alloc_mgr = getattr(self.server, "allocation_manager", None)
                if alloc_mgr is None and coordinator is not None and hasattr(coordinator, "get_allocation_manager"):
                    alloc_mgr = coordinator.get_allocation_manager()

                if alloc_mgr is None or not hasattr(alloc_mgr, "list_sleeves"):
                    self._send_json_response({
                        "status": "no_data",
                        "read_only": True,
                        "execution_locked": True,
                        "broker_provider": "none",
                        "message": "Paper portfolio allocation service not initialized or no active allocations.",
                    })
                    return

                sleeves = alloc_mgr.list_sleeves()
                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "broker_provider": "none",
                    "total_allocated_capital": alloc_mgr.total_allocated_capital,
                    "unallocated_cash": alloc_mgr.unallocated_cash,
                    "sleeve_count": len(sleeves),
                    "sleeves": [s.to_dict() for s in sleeves],
                    "advisory_disclaimer": "SIMULATION ONLY: Capital partitions are offline simulation tools.",
                })
                return

            if path == "/api/paper/sleeves":
                sleeve_id = query.get("sleeve_id", [None])[0]
                coordinator = getattr(self.server, "paper_coordinator", None)
                alloc_mgr = getattr(self.server, "allocation_manager", None)
                if alloc_mgr is None and coordinator is not None and hasattr(coordinator, "get_allocation_manager"):
                    alloc_mgr = coordinator.get_allocation_manager()

                if alloc_mgr is None or not hasattr(alloc_mgr, "get_sleeve_exposure"):
                    self._send_json_response({
                        "status": "no_data",
                        "read_only": True,
                        "execution_locked": True,
                        "broker_provider": "none",
                        "message": "Paper portfolio allocation service not initialized.",
                    })
                    return

                if sleeve_id:
                    clean_sid = sleeve_id.strip()
                    snap = alloc_mgr.get_sleeve_exposure(clean_sid)
                    if snap is None:
                        self._send_json_response(
                            {"error": "Not Found", "message": f"Strategy sleeve '{clean_sid}' not found."},
                            status=HTTPStatus.NOT_FOUND,
                        )
                        return
                    self._send_json_response({
                        "status": "success",
                        "read_only": True,
                        "execution_locked": True,
                        "broker_provider": "none",
                        "sleeve": snap.to_dict(),
                    })
                    return

                sleeves_list = [s.to_dict() for s in alloc_mgr.list_sleeves()]
                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "broker_provider": "none",
                    "count": len(sleeves_list),
                    "sleeves": sleeves_list,
                })
                return

            if path in ("/api/paper/portfolio-exposure", "/api/paper/exposure"):
                coordinator = getattr(self.server, "paper_coordinator", None)
                alloc_mgr = getattr(self.server, "allocation_manager", None)
                if alloc_mgr is None and coordinator is not None and hasattr(coordinator, "get_allocation_manager"):
                    alloc_mgr = coordinator.get_allocation_manager()

                if alloc_mgr is None or not hasattr(alloc_mgr, "get_portfolio_exposure"):
                    self._send_json_response({
                        "status": "no_data",
                        "read_only": True,
                        "execution_locked": True,
                        "broker_provider": "none",
                        "message": "Paper portfolio exposure service not initialized.",
                    })
                    return

                snap = alloc_mgr.get_portfolio_exposure()
                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "broker_provider": "none",
                    "exposure": snap.to_dict(),
                })
                return

            if path in ("/api/paper/reconciliation", "/api/paper/reconcile"):
                coordinator = getattr(self.server, "paper_coordinator", None)
                alloc_mgr = getattr(self.server, "allocation_manager", None)
                if alloc_mgr is None and coordinator is not None and hasattr(coordinator, "get_allocation_manager"):
                    alloc_mgr = coordinator.get_allocation_manager()

                if alloc_mgr is None or not hasattr(alloc_mgr, "reconcile"):
                    self._send_json_response({
                        "status": "no_data",
                        "read_only": True,
                        "execution_locked": True,
                        "broker_provider": "none",
                        "message": "Paper portfolio reconciliation service not initialized.",
                    })
                    return

                recon_res = alloc_mgr.reconcile()
                self._send_json_response({
                    "status": "success",
                    "read_only": True,
                    "execution_locked": True,
                    "broker_provider": "none",
                    "reconciliation": recon_res.to_dict() if hasattr(recon_res, "to_dict") else recon_res,
                })
                return

            # Unrecognized path: 404
            self._send_json_response({"error": "Not Found", "path": path}, status=HTTPStatus.NOT_FOUND)

        except (ValueError, TypeError) as conv_err:
            self._send_json_response(
                {"error": "Bad Request", "message": f"Invalid query parameter format: {conv_err}"},
                status=HTTPStatus.BAD_REQUEST,
            )
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

    def do_HEAD(self) -> None:
        """Handle HEAD requests identically to GET with headers only (no body)."""
        self._is_head = True
        try:
            self.do_GET()
        finally:
            self._is_head = False

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

    def do_OPTIONS(self) -> None:
        """Reject OPTIONS requests with 405 Method Not Allowed."""
        self._send_method_not_allowed()

    def do_TRACE(self) -> None:
        """Reject TRACE requests with 405 Method Not Allowed."""
        self._send_method_not_allowed()

    def _send_method_not_allowed(self) -> None:
        """Helper to return read-only 405 status with Allow header."""
        try:
            cl = int(self.headers.get("Content-Length", 0))
            if cl > 0:
                self.rfile.read(min(cl, 65536))
        except Exception:
            pass
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
        if not getattr(self, "_is_head", False):
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
        if not getattr(self, "_is_head", False):
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
        if not getattr(self, "_is_head", False):
            self.wfile.write(encoded)


    def log_message(self, format: str, *args: Any) -> None:
        """Route HTTP server logs through standard logging with query parameter secret redaction."""
        try:
            msg = format % args
            sanitized = re.sub(
                r"([?&](?:token|auth_token|access_token|api_key|apikey|key|secret|bearer|bearer_token|password)=)[^&\s]*",
                r"\1[REDACTED]",
                msg,
                flags=re.IGNORECASE,
            )
        except Exception:
            sanitized = format
        logger.debug("Dashboard HTTP [%s]: %s", self.address_string(), sanitized)


class PortfolioDashboardServer:
    """Manages the lifecycle of the local read-only Portfolio Analytics Dashboard HTTP server."""

    def __init__(
        self,
        reporting_service: Optional[PortfolioReportingService] = None,
        host: str = "127.0.0.1",
        port: int = 8080,
        diagnostics_service: Optional[PortfolioDiagnosticsService] = None,
        quality_monitor: Optional[Any] = None,
        provenance_tracker: Optional[Any] = None,
        run_registry: Optional[Any] = None,
        comparison_engine: Optional[Any] = None,
        evidence_engine: Optional[Any] = None,
        opportunity_engine: Optional[Any] = None,
        prioritization_engine: Optional[Any] = None,
        investigation_queue: Optional[Any] = None,
        investigation_manager: Optional[Any] = None,
        investigation_reporting_service: Optional[Any] = None,
        investigation_comparison_engine: Optional[Any] = None,
        discrepancy_resolution_service: Optional[Any] = None,
        consolidation_service: Optional[Any] = None,
        consistency_service: Optional[Any] = None,
        evolution_service: Optional[Any] = None,
        audit_service: Optional[Any] = None,
        reliability_service: Optional[Any] = None,
        state_store: Optional[Any] = None,
        audit_diagnostics_service: Optional[Any] = None,
        paper_coordinator: Optional[Any] = None,
        paper_execution_engine: Optional[Any] = None,
        allocation_manager: Optional[Any] = None,
        auth_token: Optional[str] = None,
        require_auth: bool = False,
        workflow_coordinator: Optional[Any] = None,
    ) -> None:
        """Initialize the dashboard server.

        Args:
            reporting_service: Optional PortfolioReportingService instance providing data.
            host: Network interface host to bind to (strictly loopback by default).
            port: Port number to bind to (0 for dynamic free port).
            diagnostics_service: Optional diagnostics service for auditing server lifecycle.
            quality_monitor: Optional MarketDataQualityMonitor for data quality querying.
            provenance_tracker: Optional MarketDataProvenanceTracker for provenance/lineage queries.
            run_registry: Optional AnalysisRunRegistry for reproducible experiment run queries.
            comparison_engine: Optional RunComparisonEngine for experiment comparison queries.
            evidence_engine: Optional EvidenceSynthesisEngine for decision-support evidence queries.
            opportunity_engine: Optional OpportunityAssessmentEngine for opportunity candidate queries.
            prioritization_engine: Optional OpportunityPrioritizationEngine for priority record queries.
            investigation_queue: Optional OpportunityInvestigationQueue for queue inspection queries.
            investigation_manager: Optional InvestigationSessionManager for human investigation session queries.
            investigation_reporting_service: Optional InvestigationReportingService for analytical investigation report queries.
            investigation_comparison_engine: Optional InvestigationComparisonEngine for cross-investigation comparison queries.
            discrepancy_resolution_service: Optional DiscrepancyResolutionService for discrepancy resolution queries.
            consolidation_service: Optional InvestigationOutcomeConsolidationService for consolidated review queries.
            consistency_service: Optional ConsolidatedOutcomeConsistencyService for outcome consistency queries.
            evolution_service: Optional HistoricalReviewEvolutionService for review evolution queries.
            audit_service: Optional HistoricalReviewAuditService for review evolution audit queries.
            reliability_service: Optional HistoricalReviewAuditReliabilityService for audit reliability queries.
            state_store: Optional StateStore instance.
            audit_diagnostics_service: Optional HistoricalReviewAuditDiagnosticsService instance.
            paper_coordinator: Optional PaperTradingWorkflowCoordinator instance.
            paper_execution_engine: Optional PaperExecutionEngine instance.
            auth_token: Optional authentication secret token for dashboard access.
            require_auth: If True, enforce authentication across all data endpoints.
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

        self.reporting_service = reporting_service or PortfolioReportingService()
        self.host = clean_host
        self.port = port
        self.diagnostics_service = (
            diagnostics_service
            or getattr(self.reporting_service, "diagnostics_service", None)
            or PortfolioDiagnosticsService()
        )
        self.quality_monitor = quality_monitor
        self.provenance_tracker = provenance_tracker
        self.run_registry = run_registry
        self.comparison_engine = comparison_engine
        self.evidence_engine = evidence_engine
        self.opportunity_engine = opportunity_engine
        self.prioritization_engine = prioritization_engine
        self.investigation_queue = investigation_queue
        self.investigation_manager = investigation_manager
        self.investigation_reporting_service = investigation_reporting_service
        self.investigation_comparison_engine = investigation_comparison_engine
        self.discrepancy_resolution_service = discrepancy_resolution_service
        self.consolidation_service = consolidation_service
        self.consistency_service = consistency_service
        self.evolution_service = evolution_service
        self.audit_service = audit_service
        self.reliability_service = reliability_service
        self.state_store = state_store
        self.audit_diagnostics_service = audit_diagnostics_service
        self.paper_coordinator = workflow_coordinator or paper_coordinator
        self.paper_execution_engine = paper_execution_engine
        self.allocation_manager = allocation_manager

        if auth_token is not None:
            if not isinstance(auth_token, str):
                raise ValidationError(
                    f"Invalid auth_token: expected string or None, got {type(auth_token).__name__}."
                )
            clean_tok = auth_token.strip()
            self.auth_token = clean_tok if clean_tok else None
        else:
            self.auth_token = None

        if isinstance(require_auth, str):
            from app.core.config import parse_bool
            try:
                self.require_auth = parse_bool(require_auth, "require_auth")
            except Exception as err:
                raise ValidationError(str(err)) from err
        else:
            self.require_auth = bool(require_auth)

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

            # Attach services, monitors, trackers, registries, and engines to server instance
            setattr(self._server, "reporting_service", self.reporting_service)
            setattr(self._server, "diagnostics_service", self.diagnostics_service)
            setattr(self._server, "quality_monitor", self.quality_monitor)
            setattr(self._server, "provenance_tracker", self.provenance_tracker)
            setattr(self._server, "run_registry", self.run_registry)
            setattr(self._server, "comparison_engine", self.comparison_engine)
            setattr(self._server, "evidence_engine", self.evidence_engine)
            setattr(self._server, "opportunity_engine", self.opportunity_engine)
            setattr(self._server, "prioritization_engine", self.prioritization_engine)
            setattr(self._server, "investigation_queue", self.investigation_queue)
            setattr(self._server, "investigation_manager", self.investigation_manager)
            setattr(self._server, "investigation_reporting_service", self.investigation_reporting_service)
            setattr(self._server, "investigation_comparison_engine", self.investigation_comparison_engine)
            setattr(self._server, "discrepancy_resolution_service", self.discrepancy_resolution_service)
            setattr(self._server, "consolidation_service", self.consolidation_service)
            setattr(self._server, "consistency_service", self.consistency_service)
            setattr(self._server, "evolution_service", self.evolution_service)
            setattr(self._server, "audit_service", self.audit_service)
            setattr(self._server, "reliability_service", self.reliability_service)
            setattr(self._server, "state_store", self.state_store)
            setattr(self._server, "audit_diagnostics_service", self.audit_diagnostics_service)
            setattr(self._server, "paper_coordinator", self.paper_coordinator)
            setattr(self._server, "paper_execution_engine", self.paper_execution_engine)
            setattr(self._server, "allocation_manager", self.allocation_manager)
            setattr(self._server, "auth_token", self.auth_token)
            setattr(self._server, "require_auth", self.require_auth)



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
