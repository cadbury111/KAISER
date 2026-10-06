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
        reporting_service: Optional[PortfolioReportingService] = None,
        host: str = "127.0.0.1",
        port: int = 8080,
        diagnostics_service: Optional[PortfolioDiagnosticsService] = None,
        quality_monitor: Optional[Any] = None,
        provenance_tracker: Optional[Any] = None,
        run_registry: Optional[Any] = None,
        comparison_engine: Optional[Any] = None,
        evidence_engine: Optional[Any] = None,
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
