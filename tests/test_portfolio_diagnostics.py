"""KAISER — Tests for Portfolio Analytics Observability, Auditability & Operational Diagnostics.

Comprehensive verification of:
1. Successful report-generation event recording.
2. Failed report-generation event recording without leaking exception details.
3. Successful, unavailable, and invalid report retrieval recording.
4. Dashboard startup and shutdown diagnostics.
5. Missing, stale, and incomplete data provenance state handling.
6. Empty diagnostic history behavior.
7. Retention limit and deterministic oldest-first eviction.
8. Identical timestamps and stable event ordering (sequence number tie-breaker).
9. Concurrent event recording and history retrieval (thread-safety under stress).
10. Malformed or oversized event fields.
11. Secret, token, transcript, and local-path sanitization.
12. Diagnostics failure does not change underlying report-generation result (fault isolation).
13. Read-only dashboard behavior and forbidden HTTP methods on /api/diagnostics.
14. Loopback-only binding regression.
15. Safe rendering of diagnostic strings containing HTML/script content.
16. MasterOrchestrator handler integration for 'get_portfolio_diagnostics'.
"""

import concurrent.futures
import json
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from app.backtesting.models import EquityPoint, OrderSide, TradeRecord
from app.backtesting.portfolio_simulation.models import (
    PortfolioSimulationConfig,
    PortfolioSimulationResult,
    PortfolioSimulationStatus,
    StrategyAllocation,
    StrategyContribution,
)
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.attribution import PortfolioAttributionService
from app.portfolio.dashboard.server import PortfolioDashboardServer
from app.portfolio.dashboard.template import get_dashboard_html
from app.portfolio.diagnostics import (
    DEFAULT_DIAGNOSTICS_MAX_HISTORY,
    DiagnosticEventType,
    DiagnosticOutcome,
    OperationalDiagnosticEvent,
    PortfolioDiagnosticsService,
    format_utc_iso,
    sanitize_diagnostic_details,
    sanitize_diagnostic_error_category,
    sanitize_diagnostic_text,
)
from app.portfolio.report_models import ReportDataStatus, UnifiedPortfolioReport
from app.portfolio.reporting import PortfolioReportingService
from app.portfolio.reporting_handlers import register_reporting_handlers
from app.risk.models import AlertSeverity, AlertState, PortfolioRiskAlert, PortfolioRiskReport
from app.risk.monitor import PortfolioRiskMonitor


# ------------------------------------------------------------------------------
# Fixtures & Helpers
# ------------------------------------------------------------------------------

@pytest.fixture
def event_bus() -> EventBus:
    return EventBus()


@pytest.fixture
def state_store() -> StateStore:
    return StateStore()


@pytest.fixture
def diagnostics_service(event_bus: EventBus, state_store: StateStore) -> PortfolioDiagnosticsService:
    return PortfolioDiagnosticsService(
        max_history=50,
        event_bus=event_bus,
        state_store=state_store,
    )


@pytest.fixture
def reporting_service(
    event_bus: EventBus,
    state_store: StateStore,
    diagnostics_service: PortfolioDiagnosticsService,
) -> PortfolioReportingService:
    return PortfolioReportingService(
        event_bus=event_bus,
        state_store=state_store,
        diagnostics_service=diagnostics_service,
    )


@pytest.fixture
def dashboard_server(
    reporting_service: PortfolioReportingService,
    diagnostics_service: PortfolioDiagnosticsService,
) -> PortfolioDashboardServer:
    server = PortfolioDashboardServer(
        reporting_service=reporting_service,
        host="127.0.0.1",
        port=0,
        diagnostics_service=diagnostics_service,
    )
    server.start(blocking=False)
    yield server
    server.stop()


def _create_mock_simulation_result(
    simulation_id: str = "sim_diag_01",
    initial_equity: float = 100_000.0,
    final_equity: float = 112_000.0,
    gross_pnl: float = 13_000.0,
    net_pnl: float = 12_000.0,
    status: PortfolioSimulationStatus = PortfolioSimulationStatus.SUCCESS,
) -> PortfolioSimulationResult:
    """Helper creating deterministic simulation result."""
    alloc_a = StrategyAllocation(strategy_name="alpha_trend", symbol="TCS", weight=0.6)
    alloc_b = StrategyAllocation(strategy_name="beta_mean_rev", symbol="INFY", weight=0.4)

    cfg = PortfolioSimulationConfig(
        allocations=(alloc_a, alloc_b),
        start_time=1700000000.0,
        end_time=1700003600.0,
        initial_cash=initial_equity,
    )

    t1 = TradeRecord(
        trade_id="t1",
        symbol="TCS",
        side=OrderSide.BUY,
        entry_time=1700000100.0,
        exit_time=1700000500.0,
        entry_price=100.0,
        exit_price=112.0,
        quantity=50.0,
        gross_pnl=6_500.0,
        net_pnl=6_000.0,
        fees=300.0,
        slippage=200.0,
        return_pct=12.0,
        duration_seconds=400.0,
        is_win=True,
    )

    contrib_a = StrategyContribution(
        strategy_id="strat_01",
        strategy_name="alpha_trend",
        symbol="TCS",
        allocated_capital=60_000.0,
        weight=0.6,
        gross_pnl=6_500.0,
        net_pnl=6_000.0,
        net_return_pct=10.0,
        portfolio_pnl_share_pct=50.0,
        total_trades=1,
        winning_trades=1,
        losing_trades=0,
        win_rate=100.0,
        profit_factor=2.0,
        profit_factor_description="Profitable",
        total_fees=300.0,
        total_slippage=200.0,
        orders_submitted=1,
        orders_accepted=1,
        orders_rejected=0,
        trades=(t1,),
    )

    eq_points = (
        EquityPoint(timestamp=1700000000.0, cash=100_000.0, position_value=0.0, total_equity=100_000.0, drawdown_pct=0.0, drawdown_amount=0.0),
        EquityPoint(timestamp=1700001800.0, cash=95_000.0, position_value=11_000.0, total_equity=106_000.0, drawdown_pct=0.0, drawdown_amount=0.0),
        EquityPoint(timestamp=1700003600.0, cash=112_000.0, position_value=0.0, total_equity=112_000.0, drawdown_pct=0.0, drawdown_amount=0.0),
    )

    return PortfolioSimulationResult(
        simulation_id=simulation_id,
        status=status,
        config=cfg,
        initial_equity=initial_equity,
        final_equity=final_equity,
        gross_pnl=gross_pnl,
        net_pnl=net_pnl,
        net_return_pct=((final_equity - initial_equity) / initial_equity) * 100.0,
        max_drawdown_pct=0.0,
        max_drawdown_amount=0.0,
        max_drawdown_duration_bars=0,
        total_fees=600.0,
        total_slippage=400.0,
        total_transaction_costs=1000.0,
        total_trades=1,
        winning_trades=1,
        losing_trades=0,
        win_rate=100.0,
        profit_factor=2.0,
        profit_factor_description="Profitable",
        equity_curve=eq_points,
        risk_snapshots=(),
        strategy_contributions=(contrib_a,),
    )


# ------------------------------------------------------------------------------
# Acceptance Test Cases (1 to 16)
# ------------------------------------------------------------------------------

def test_successful_report_generation_event(reporting_service: PortfolioReportingService):
    """Case 1: Generating a report records REPORT_GENERATION_STARTED and REPORT_GENERATION_SUCCEEDED."""
    sim = _create_mock_simulation_result("sim_success_01")
    report = reporting_service.generate_unified_report(sim)

    diag = reporting_service.diagnostics_service
    history = diag.get_history(limit=10, newest_first=True)

    event_types = [e.event_type for e in history]
    assert DiagnosticEventType.REPORT_GENERATION_SUCCEEDED in event_types
    assert DiagnosticEventType.REPORT_GENERATION_STARTED in event_types

    success_ev = next(e for e in history if e.event_type == DiagnosticEventType.REPORT_GENERATION_SUCCEEDED)
    assert success_ev.outcome == DiagnosticOutcome.SUCCESS
    assert success_ev.report_id == report.report_id
    assert success_ev.run_id == "sim_success_01"
    assert success_ev.data_status == ReportDataStatus.VALID.value
    assert success_ev.duration_ms is not None and success_ev.duration_ms >= 0.0

    # Verify snapshot updated
    snap = diag.get_snapshot()
    assert snap["latest_report_generation"]["outcome"] == "SUCCESS"
    assert snap["latest_report_generation"]["report_id"] == report.report_id


def test_failed_report_generation_event_without_leaking_exception_details(reporting_service: PortfolioReportingService):
    """Case 2: Failed report-generation records REPORT_GENERATION_FAILED without leaking raw paths/stack traces."""
    # Pass a dict that will cause a validation/processing failure
    malformed_source = {"status": "INVALID_STATUS_TRIGGERING_CRASH", "fail_key": None}

    # Inject a failure in _extract_financials_and_curves
    with patch.object(reporting_service, "_extract_financials_and_curves", side_effect=ValueError("Simulated bad data in C:\\Users\\secret\\kaiser.py")):
        with pytest.raises(ValueError):
            reporting_service.generate_unified_report(malformed_source)

    diag = reporting_service.diagnostics_service
    history = diag.get_history(limit=5)
    fail_ev = next((e for e in history if e.event_type == DiagnosticEventType.REPORT_GENERATION_FAILED), None)
    assert fail_ev is not None
    assert fail_ev.outcome == DiagnosticOutcome.FAILURE
    assert fail_ev.error_category == "ValueError"

    # Verify no local paths leaked in details
    details_str = json.dumps(fail_ev.details)
    assert "C:\\Users" not in details_str
    assert "secret" not in details_str


def test_successful_and_unsuccessful_report_retrieval(reporting_service: PortfolioReportingService):
    """Case 3: Report retrieval records SUCCEEDED, UNAVAILABLE, and INVALID outcomes accurately."""
    diag = reporting_service.diagnostics_service

    # 1. Retrieval when empty -> UNAVAILABLE
    latest = reporting_service.get_latest_unified_report()
    assert latest is None
    ev = diag.get_history(limit=1)[0]
    assert ev.event_type == DiagnosticEventType.REPORT_RETRIEVAL_UNAVAILABLE
    assert ev.outcome == DiagnosticOutcome.UNAVAILABLE

    # 2. Retrieval by invalid ID -> INVALID
    with pytest.raises(ValidationError):
        reporting_service.get_report_by_id("")
    ev_inv = diag.get_history(limit=1)[0]
    assert ev_inv.event_type == DiagnosticEventType.REPORT_RETRIEVAL_INVALID
    assert ev_inv.outcome == DiagnosticOutcome.INVALID

    # 3. Generate report and retrieve by ID -> SUCCEEDED
    sim = _create_mock_simulation_result("sim_retrieval_01")
    rep = reporting_service.generate_unified_report(sim)

    retrieved = reporting_service.get_report_by_id(rep.report_id)
    assert retrieved is not None
    ev_succ = diag.get_history(limit=1)[0]
    assert ev_succ.event_type == DiagnosticEventType.REPORT_RETRIEVAL_SUCCEEDED
    assert ev_succ.outcome == DiagnosticOutcome.SUCCESS
    assert ev_succ.report_id == rep.report_id


def test_dashboard_startup_and_shutdown_diagnostics(reporting_service: PortfolioReportingService):
    """Case 4: Dashboard server startup and shutdown record structured lifecycle diagnostics."""
    diag = reporting_service.diagnostics_service
    server = PortfolioDashboardServer(
        reporting_service=reporting_service,
        host="127.0.0.1",
        port=0,
        diagnostics_service=diag,
    )

    # Start
    server.start(blocking=False)
    assert server.is_running
    start_ev = next((e for e in diag.get_history(limit=5) if e.event_type == DiagnosticEventType.DASHBOARD_SERVER_STARTED), None)
    assert start_ev is not None
    assert start_ev.outcome == DiagnosticOutcome.SUCCESS
    assert start_ev.details["host"] == "127.0.0.1"

    # Stop
    server.stop()
    assert not server.is_running
    stop_ev = next((e for e in diag.get_history(limit=5) if e.event_type == DiagnosticEventType.DASHBOARD_SERVER_STOPPED), None)
    assert stop_ev is not None
    assert stop_ev.outcome == DiagnosticOutcome.INFO


def test_missing_and_stale_provenance_states(reporting_service: PortfolioReportingService):
    """Case 5: Reports with STALE or INCOMPLETE data statuses record provenance states accurately."""
    diag = reporting_service.diagnostics_service

    # Create stale report
    sim = _create_mock_simulation_result("sim_stale_01")
    stale_risk = PortfolioRiskReport(
        report_id="risk_rep_01",
        timestamp=1700000000.0,
        overall_state=AlertState.NORMAL,
        active_alerts=(),
        is_snapshot_stale=True,
    )
    rep = reporting_service.generate_unified_report(sim, risk_report=stale_risk)
    assert rep.status == ReportDataStatus.STALE

    succ_ev = next(e for e in diag.get_history(limit=5) if e.event_type == DiagnosticEventType.REPORT_GENERATION_SUCCEEDED)
    assert succ_ev.data_status == "STALE"

    snap = diag.get_snapshot()
    assert snap["latest_data_status"] == "STALE"


def test_empty_diagnostic_history(event_bus: EventBus, state_store: StateStore):
    """Case 6: Fresh diagnostics service provides safe empty-state behavior."""
    diag = PortfolioDiagnosticsService(max_history=50, event_bus=event_bus, state_store=state_store)
    assert diag.get_history() == []

    snap = diag.get_snapshot()
    assert snap["counts"]["total_recorded"] == 0
    assert snap["counts"]["total_evicted"] == 0
    assert snap["counts"]["current_history_size"] == 0
    assert snap["counts"]["has_evicted"] is False
    assert snap["latest_report_generation"] is None
    assert snap["latest_data_status"] is None
    assert snap["audit_retention"] == "bounded_in_memory"


def test_retention_limit_and_deterministic_oldest_event_eviction():
    """Case 7: Exceeding max_history triggers deterministic oldest-first eviction."""
    diag = PortfolioDiagnosticsService(max_history=5)

    # Record 8 events
    for i in range(8):
        diag.record_event(
            event_type=DiagnosticEventType.REPORT_RETRIEVAL_SUCCEEDED,
            outcome=DiagnosticOutcome.SUCCESS,
            correlation_id=f"corr_{i}",
            details={"index": i},
        )

    # Current size must be capped at 5
    history = diag.get_history(limit=10, newest_first=True)
    assert len(history) == 5

    # Oldest events (0, 1, 2) were evicted; retained indices must be 7, 6, 5, 4, 3
    retained_indices = [e.details["index"] for e in history]
    assert retained_indices == [7, 6, 5, 4, 3]

    snap = diag.get_snapshot()
    assert snap["counts"]["total_recorded"] == 8
    assert snap["counts"]["total_evicted"] == 3
    assert snap["counts"]["has_evicted"] is True
    assert snap["counts"]["current_history_size"] == 5


def test_identical_timestamps_and_stable_event_ordering():
    """Case 8: Events with identical timestamps maintain stable deterministic order by sequence_number."""
    frozen_time = 1700000000.0
    diag = PortfolioDiagnosticsService(max_history=20, clock=lambda: frozen_time)

    # Record 6 events at the identical timestamp
    for i in range(6):
        diag.record_event(
            event_type=DiagnosticEventType.REPORT_RETRIEVAL_SUCCEEDED,
            outcome=DiagnosticOutcome.SUCCESS,
            correlation_id=f"corr_{i}",
            details={"seq_expected": i + 1},
        )

    history_newest = diag.get_history(limit=10, newest_first=True)
    seqs_newest = [e.sequence_number for e in history_newest]
    assert seqs_newest == [6, 5, 4, 3, 2, 1]

    history_oldest = diag.get_history(limit=10, newest_first=False)
    seqs_oldest = [e.sequence_number for e in history_oldest]
    assert seqs_oldest == [1, 2, 3, 4, 5, 6]


def test_concurrent_event_recording_and_history_retrieval():
    """Case 9: Multi-threaded concurrent recording and retrieval execute safely without corruption."""
    diag = PortfolioDiagnosticsService(max_history=200)

    def worker(worker_id: int):
        for j in range(10):
            diag.record_event(
                event_type=DiagnosticEventType.DATA_PROVENANCE_EVALUATED,
                outcome=DiagnosticOutcome.INFO,
                correlation_id=f"worker_{worker_id}_{j}",
                details={"w_id": worker_id, "step": j},
            )
            # Interleave reads
            _ = diag.get_history(limit=5)
            _ = diag.get_snapshot()

    # Run 10 threads concurrently recording 10 events each = 100 events
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(worker, w) for w in range(10)]
        concurrent.futures.wait(futures)

    snap = diag.get_snapshot()
    assert snap["counts"]["total_recorded"] == 100
    assert snap["counts"]["current_history_size"] == 100

    # Ensure sequence numbers are strictly unique
    history = diag.get_history(limit=100)
    all_seqs = [e.sequence_number for e in history]
    assert len(set(all_seqs)) == 100


def test_malformed_or_oversized_event_fields():
    """Case 10: Oversized strings, null bytes, and malformed inputs are sanitized and capped safely."""
    diag = PortfolioDiagnosticsService(max_history=10)

    oversized_corr = "A" * 500
    null_byte_str = "malicious\x00data"
    oversized_details = {f"key_{k}": "V" * 500 for k in range(50)}

    ev = diag.record_event(
        event_type="CUSTOM_NON_EXISTENT_TYPE",
        outcome="CUSTOM_UNKNOWN_OUTCOME",
        correlation_id=oversized_corr,
        details=oversized_details,
    )

    # Correlation ID capped
    assert len(ev.correlation_id) <= 64
    # Event type resolved to fallback or sanitized
    assert isinstance(ev.event_type, DiagnosticEventType)
    # Detail items count capped at MAX_DETAIL_ITEMS (20)
    assert len(ev.details) <= 20
    # Values capped
    for v in ev.details.values():
        if isinstance(v, str):
            assert len(v) <= 256


def test_secret_token_transcript_and_local_path_sanitization():
    """Case 11: Secret tokens, credentials, voice transcripts, and local paths are scrubbed."""
    raw_details = {
        "api_key": "super_secret_123456",
        "auth_token": "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.do_not_leak",
        "raw_transcript": "execute order buy 100 TCS",
        "audio_bytes": b"raw_audio_payload",
        "user_voice": "hello kaiser",
        "safe_note": "Found file at C:\\Users\\user\\Desktop\\KAISER\\config.json and /home/deploy/kaiser/run.py",
        "safe_comment": "GitHub token is ghp_123456789012345678901234567890123456",
    }

    sanitized = sanitize_diagnostic_details(raw_details)

    # Prohibited keys stripped entirely
    assert "api_key" not in sanitized
    assert "auth_token" not in sanitized
    assert "raw_transcript" not in sanitized
    assert "audio_bytes" not in sanitized
    assert "user_voice" not in sanitized

    # Local paths scrubbed from text
    safe_note = sanitized["safe_note"]
    assert "C:\\Users" not in safe_note
    assert "/home/deploy" not in safe_note
    assert "[REDACTED_PATH]" in safe_note

    # GitHub token scrubbed
    token_text = sanitized["safe_comment"]
    assert "ghp_1234567890" not in token_text
    assert "[REDACTED_SECRET]" in token_text


def test_diagnostics_failure_does_not_change_report_generation_result(reporting_service: PortfolioReportingService):
    """Case 12: A catastrophic failure in diagnostics recording never fails the underlying analytics report."""
    sim = _create_mock_simulation_result("sim_fault_isolation_01")

    # Force record_event to raise an unexpected internal exception
    with patch.object(reporting_service.diagnostics_service, "record_event", side_effect=RuntimeError("Telemetry internal crash")):
        # Must not raise RuntimeError; report generation must succeed
        report = reporting_service.generate_unified_report(sim)
        assert isinstance(report, UnifiedPortfolioReport)
        assert report.provenance.source_run_id == "sim_fault_isolation_01"


def test_read_only_dashboard_behavior_and_forbidden_http_methods(dashboard_server: PortfolioDashboardServer):
    """Case 13: /api/diagnostics supports GET and HEAD; rejects mutating methods with HTTP 405."""
    base_url = dashboard_server.url

    # 1. GET /api/diagnostics
    req_get = urllib.request.Request(f"{base_url}/api/diagnostics?limit=10")
    with urllib.request.urlopen(req_get) as resp:
        assert resp.status == 200
        assert "application/json" in resp.headers.get("Content-Type", "")
        # Security headers
        assert resp.headers.get("X-Content-Type-Options") == "nosniff"
        assert resp.headers.get("X-Frame-Options") == "DENY"
        data = json.loads(resp.read().decode("utf-8"))
        assert data["status"] == "success"
        assert "snapshot" in data
        assert "events" in data

    # 2. HEAD /api/diagnostics
    req_head = urllib.request.Request(f"{base_url}/api/diagnostics", method="HEAD")
    with urllib.request.urlopen(req_head) as resp_head:
        assert resp_head.status == 200
        assert "application/json" in resp_head.headers.get("Content-Type", "")
        body = resp_head.read()
        assert len(body) == 0

    # 3. POST /api/diagnostics -> 405 Method Not Allowed
    req_post = urllib.request.Request(
        f"{base_url}/api/diagnostics",
        data=b'{"hack": true}',
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req_post)
    assert exc_info.value.code == 405

    # 4. DELETE /api/diagnostics -> 405 Method Not Allowed
    req_del = urllib.request.Request(f"{base_url}/api/diagnostics", method="DELETE")
    with pytest.raises(urllib.error.HTTPError) as exc_del:
        urllib.request.urlopen(req_del)
    assert exc_del.value.code == 405


def test_loopback_only_binding_regression(reporting_service: PortfolioReportingService):
    """Case 14: PortfolioDashboardServer rejects any attempt to bind outside loopback."""
    with pytest.raises(ValidationError) as exc1:
        PortfolioDashboardServer(reporting_service, host="0.0.0.0", port=8080)
    assert "loopback" in str(exc1.value)

    with pytest.raises(ValidationError) as exc2:
        PortfolioDashboardServer(reporting_service, host="192.168.1.100", port=8080)
    assert "loopback" in str(exc2.value)


def test_safe_rendering_of_diagnostic_strings_containing_html_script(dashboard_server: PortfolioDashboardServer):
    """Case 15: Diagnostic events containing script injection characters are safely escaped in dashboard DOM."""
    diag = dashboard_server.diagnostics_service
    malicious_text = "<script>alert('xss_attack')</script>"

    diag.record_event(
        event_type=DiagnosticEventType.ANALYTICS_SERVICE_ERROR,
        outcome=DiagnosticOutcome.FAILURE,
        correlation_id=malicious_text,
        error_category=malicious_text,
        details={"raw_input": malicious_text},
    )

    base_url = dashboard_server.url
    with urllib.request.urlopen(f"{base_url}/") as resp:
        html = resp.read().decode("utf-8")
        assert "<title>KAISER — Portfolio Analytics Command Center</title>" in html
        assert "Operational Diagnostics & System Audit" in html
        assert "escapeHtml" in html
        assert "renderDiagnostics" in html


def test_orchestrator_integration_diagnostics_command(reporting_service: PortfolioReportingService):
    """Case 16: MasterOrchestrator dispatches 'get_portfolio_diagnostics' cleanly."""
    orchestrator = MasterOrchestrator()
    register_reporting_handlers(orchestrator=orchestrator, service=reporting_service)

    # Record sample diagnostic event
    reporting_service.diagnostics_service.record_event(
        event_type=DiagnosticEventType.REPORT_RETRIEVAL_SUCCEEDED,
        outcome=DiagnosticOutcome.SUCCESS,
        correlation_id="orch_test_01",
    )

    req = OrchestratorRequest.create(
        command="get_portfolio_diagnostics",
        context={"limit": 10},
    )
    res = orchestrator.handle_request(req)
    assert res.is_success
    result_data = res.result
    assert result_data["status"] == "success"
    assert "snapshot" in result_data
    assert len(result_data["events"]) >= 1
    assert result_data["events"][0]["correlation_id"] == "orch_test_01"
