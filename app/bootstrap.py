"""KAISER — System Bootstrap & Lifecycle Management.

Initializes configuration, logging, and foundational safety invariants.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.core.config import Settings, get_settings
from app.core.events import Event, EventBus, EventType
from app.core.logging_config import get_logger, setup_logging
from app.core.state import StateStore
from app.market import MarketDataService, MockMarketFeed
from app.orchestrator import MasterOrchestrator, create_system_status_handler
from app.voice import (
    AudioCaptureService,
    AudioPlaybackService,
    LocalSTTConfig,
    LocalWhisperSTTProvider,
    MockSTTProvider,
    MockTTSProvider,
    Pyttsx3Config,
    Pyttsx3TTSProvider,
    VoiceInteractionController,
    VoiceService,
)
from app.agents import (
    MarketRegimeAgent,
    MultiAgentCoordinationService,
    TechnicalAnalysisAgent,
    register_consensus_handlers,
    register_regime_agent_handlers,
    register_technical_agent_handlers,
)
from app.analysis import TechnicalAnalysisEngine, register_analysis_handlers
from app.market.historical import (
    HistoricalBarRepository,
    HistoricalDataCache,
    HistoricalDataLoader,
    HistoricalDataProvider,
    HistoricalReplayEngine,
    MockHistoricalDataProvider,
    SQLiteHistoricalBarRepository,
    register_historical_handlers,
)
from app.market.models import Bar
from app.backtesting import (
    BacktestEngine,
    PortfolioSimulationEngine,
    RobustnessTestEngine,
    StrategyRegistry,
    WalkForwardValidator,
    register_backtest_handlers,
)
from app.portfolio import (
    PortfolioAttributionService,
    PortfolioDashboardServer,
    PortfolioDiagnosticsService,
    PortfolioReportingService,
    register_portfolio_handlers,
    register_reporting_handlers,
)
from app.market.quality import MarketDataQualityMonitor, register_quality_handlers
from app.market.provenance import (
    MarketDataProvenanceTracker,
    register_provenance_handlers,
)
from app.analysis.runs import (
    AnalysisRunRegistry,
    register_run_handlers,
)
from app.analysis.comparison import (
    RunComparisonEngine,
    register_comparison_handlers,
)
from app.analysis.evidence import (
    EvidenceSynthesisEngine,
    register_evidence_handlers,
)
from app.analysis.opportunities import (
    OpportunityAssessmentEngine,
    OpportunityInvestigationQueue,
    OpportunityPrioritizationEngine,
    InvestigationSessionManager,
    InvestigationReportingService,
    InvestigationComparisonEngine,
    DiscrepancyResolutionService,
    InvestigationOutcomeConsolidationService,
    ConsolidatedOutcomeConsistencyService,
    HistoricalReviewEvolutionService,
    HistoricalReviewAuditService,
    HistoricalReviewAuditReliabilityService,
    HistoricalReviewAuditDiagnosticsService,
    register_opportunity_handlers,
    register_prioritization_handlers,
    register_investigation_handlers,
    register_investigation_reporting_handlers,
    register_investigation_comparison_handlers,
    register_discrepancy_resolution_handlers,
    register_consolidation_handlers,
    register_consistency_handlers,
    register_evolution_handlers,
    register_audit_handlers,
    register_reliability_handlers,
    register_audit_diagnostics_handlers,
)
from app.risk import PortfolioRiskMonitor, register_risk_handlers




@dataclass(frozen=True)
class SystemStatus:
    """Represents the operational status of KAISER subsystems."""

    app_name: str
    environment: str
    voice_architecture: str
    trading_engine: str
    execution_engine: str
    initialized: bool
    settings: Settings
    event_bus: Optional[EventBus] = None
    state_store: Optional[StateStore] = None
    market_service: Optional[MarketDataService] = None
    orchestrator: Optional[MasterOrchestrator] = None
    voice_service: Optional[VoiceService] = None
    voice_controller: Optional[VoiceInteractionController] = None
    analysis_engine: Optional[TechnicalAnalysisEngine] = None
    technical_agent: Optional[TechnicalAnalysisAgent] = None
    regime_agent: Optional[MarketRegimeAgent] = None
    coordination_service: Optional[MultiAgentCoordinationService] = None
    historical_loader: Optional[HistoricalDataLoader] = None
    historical_provider: Optional[HistoricalDataProvider] = None
    historical_repository: Optional[HistoricalBarRepository] = None
    historical_cache: Optional[HistoricalDataCache] = None
    historical_replay_engine: Optional[HistoricalReplayEngine] = None
    audio_capture: Optional[AudioCaptureService] = None
    audio_playback: Optional[AudioPlaybackService] = None
    backtest_engine: Optional[BacktestEngine] = None
    strategy_registry: Optional[StrategyRegistry] = None
    walk_forward_validator: Optional[WalkForwardValidator] = None
    robustness_test_engine: Optional[RobustnessTestEngine] = None
    portfolio_simulation_engine: Optional[PortfolioSimulationEngine] = None
    portfolio_risk_monitor: Optional[PortfolioRiskMonitor] = None
    portfolio_attribution_service: Optional[PortfolioAttributionService] = None
    portfolio_reporting_service: Optional[PortfolioReportingService] = None
    portfolio_dashboard_server: Optional[PortfolioDashboardServer] = None
    portfolio_diagnostics_service: Optional[PortfolioDiagnosticsService] = None
    quality_monitor: Optional[MarketDataQualityMonitor] = None
    provenance_tracker: Optional[MarketDataProvenanceTracker] = None
    run_registry: Optional[AnalysisRunRegistry] = None
    comparison_engine: Optional[RunComparisonEngine] = None
    evidence_engine: Optional[EvidenceSynthesisEngine] = None
    opportunity_engine: Optional[OpportunityAssessmentEngine] = None
    prioritization_engine: Optional[OpportunityPrioritizationEngine] = None
    investigation_queue: Optional[OpportunityInvestigationQueue] = None
    investigation_manager: Optional[InvestigationSessionManager] = None
    investigation_reporting_service: Optional[InvestigationReportingService] = None
    investigation_comparison_engine: Optional[InvestigationComparisonEngine] = None
    discrepancy_resolution_service: Optional[DiscrepancyResolutionService] = None
    consolidation_service: Optional[InvestigationOutcomeConsolidationService] = None
    consistency_service: Optional[ConsolidatedOutcomeConsistencyService] = None
    evolution_service: Optional[HistoricalReviewEvolutionService] = None
    audit_service: Optional[HistoricalReviewAuditService] = None
    reliability_service: Optional[HistoricalReviewAuditReliabilityService] = None
    audit_diagnostics_service: Optional[HistoricalReviewAuditDiagnosticsService] = None





def bootstrap_system(
    settings: Optional[Settings] = None,
    log_to_file: bool = True,
) -> SystemStatus:
    """Bootstrap the KAISER foundation layer.

    Initializes logging, validates system configuration, establishes the
    event bus and state store, initializes market data services, and verifies
    architectural safety constraints.

    Args:
        settings: Optional pre-loaded settings instance.
        log_to_file: Whether to attach file logging.

    Returns:
        SystemStatus: Subsystem readiness report.
    """
    active_settings = settings or get_settings()

    # Initialize centralized logging
    setup_logging(
        log_level=active_settings.log_level,
        log_to_file=log_to_file,
    )
    logger = get_logger("bootstrap")
    logger.info("Bootstrapping %s foundation layer...", active_settings.app_name)

    voice_status = "ready" if active_settings.voice_enabled else "disabled"
    trading_status = "not initialized"
    execution_status = "locked" if active_settings.execution_locked else "unlocked"

    # Initialize foundational communication and state infrastructure
    event_bus = EventBus(name="kaiser_main")
    state_store = StateStore(name="kaiser_main")

    state_store.set("system_status", "initialized")
    state_store.set("environment", active_settings.app_env)
    state_store.set("voice_enabled", active_settings.voice_enabled)
    state_store.set("trading_engine", trading_status)
    state_store.set("execution_engine", execution_status)

    # Initialize market data service (mock provider by default)
    market_service = MarketDataService(
        event_bus=event_bus,
        state_store=state_store,
        freshness_threshold_seconds=60.0,
    )
    if active_settings.market_data_provider == "mock":
        mock_feed = MockMarketFeed(name="default_mock_feed")
        market_service.register_feed(mock_feed)

    # Initialize Master Orchestrator and register safe foundational handlers
    orchestrator = MasterOrchestrator(
        event_bus=event_bus,
        state_store=state_store,
        settings=active_settings,
        name="kaiser_main_orchestrator",
    )

    # Initialize Voice Service foundation (mock providers in safe local mode)
    voice_service: Optional[VoiceService] = None
    voice_controller: Optional[VoiceInteractionController] = None
    audio_capture: Optional[AudioCaptureService] = None
    if active_settings.voice_enabled:
        if active_settings.stt_provider == "local_whisper":
            stt_config = LocalSTTConfig(
                model_path=active_settings.stt_model_path,
                model_id=active_settings.stt_model_id,
                device=active_settings.stt_device,
                compute_type=active_settings.stt_compute_type,
            )
            stt_provider = LocalWhisperSTTProvider(config=stt_config, name="local_whisper")
        else:
            stt_provider = MockSTTProvider(name="mock_stt")

        if active_settings.tts_provider in ("pyttsx3", "local_pyttsx3"):
            tts_config = Pyttsx3Config(
                voice_id=active_settings.tts_voice_id,
                rate=active_settings.tts_rate,
                volume=active_settings.tts_volume,
            )
            tts_provider = Pyttsx3TTSProvider(config=tts_config, name="pyttsx3")
        else:
            tts_provider = MockTTSProvider(name="mock_tts")

        audio_capture = AudioCaptureService(
            event_bus=event_bus,
            state_store=state_store,
        )
        audio_playback = AudioPlaybackService(
            event_bus=event_bus,
            state_store=state_store,
        )
        voice_service = VoiceService(
            orchestrator=orchestrator,
            stt_provider=stt_provider,
            tts_provider=tts_provider,
            audio_capture=audio_capture,
            audio_playback=audio_playback,
            event_bus=event_bus,
            state_store=state_store,
            name="kaiser_main_voice_service",
        )
        voice_controller = VoiceInteractionController(
            voice_service=voice_service,
            event_bus=event_bus,
            state_store=state_store,
            name="kaiser_main_voice_controller",
        )

    # Initialize Historical Repository & Bounded Cache
    db_path = "data/kaiser.db"
    if active_settings.database_url.startswith("sqlite:///"):
        db_path = active_settings.database_url.replace("sqlite:///", "")
    elif active_settings.database_url.startswith("sqlite://"):
        db_path = active_settings.database_url.replace("sqlite://", "")

    historical_repository = SQLiteHistoricalBarRepository(db_path=db_path, name="sqlite_historical_repo")
    historical_cache = HistoricalDataCache(max_size=200, ttl_seconds=300.0)

    # Initialize Reproducible Analysis Run Registry (Phase 2, Step 21)
    run_registry = AnalysisRunRegistry(
        event_bus=event_bus,
        state_store=state_store,
        db_path=db_path,
        name="kaiser_main_run_registry",
    )
    register_run_handlers(
        orchestrator=orchestrator,
        run_registry=run_registry,
    )

    # Initialize Technical & Quantitative Analysis Engine
    analysis_engine = TechnicalAnalysisEngine(
        event_bus=event_bus,
        state_store=state_store,
        run_registry=run_registry,
        name="kaiser_main_analysis_engine",
    )
    register_analysis_handlers(orchestrator=orchestrator, engine=analysis_engine)

    # Initialize Technical Analysis Specialist Agent
    technical_agent = TechnicalAnalysisAgent(
        analysis_engine=analysis_engine,
        quality_engine=market_service.quality_engine if market_service else None,
        event_bus=event_bus,
        state_store=state_store,
        name="kaiser_main_technical_agent",
    )
    register_technical_agent_handlers(orchestrator=orchestrator, agent=technical_agent)

    # Initialize Market Regime Specialist Agent (Phase 2, Step 49)
    regime_agent = MarketRegimeAgent(
        analysis_engine=analysis_engine,
        quality_engine=market_service.quality_engine if market_service else None,
        event_bus=event_bus,
        state_store=state_store,
        name="kaiser_main_regime_agent",
    )
    register_regime_agent_handlers(orchestrator=orchestrator, agent=regime_agent)

    # Initialize Multi-Agent Specialist Coordination Service (Phase 2, Step 49)
    coordination_service = MultiAgentCoordinationService(
        technical_agent=technical_agent,
        regime_agent=regime_agent,
        event_bus=event_bus,
        state_store=state_store,
        name="kaiser_main_coordination_service",
    )
    register_consensus_handlers(orchestrator=orchestrator, service=coordination_service)

    # Initialize Market Data Provenance & Lineage Tracker (Phase 2, Step 20)
    provenance_tracker = MarketDataProvenanceTracker(
        event_bus=event_bus,
        state_store=state_store,
        db_path=db_path,
        name="kaiser_main_provenance_tracker",
    )
    register_provenance_handlers(
        orchestrator=orchestrator,
        provenance_tracker=provenance_tracker,
    )

    # Initialize Historical Market Data Provider and Loader
    historical_provider = MockHistoricalDataProvider(name="mock_historical_provider")
    historical_loader = HistoricalDataLoader(
        provider=historical_provider,
        quality_engine=market_service.quality_engine,
        analysis_engine=analysis_engine,
        event_bus=event_bus,
        state_store=state_store,
        repository=historical_repository,
        cache=historical_cache,
        provenance_tracker=provenance_tracker,
        name="kaiser_main_historical_loader",
    )

    # Initialize Historical Replay Engine
    historical_replay_engine = HistoricalReplayEngine(
        repository=historical_repository,
        quality_engine=market_service.quality_engine,
        analysis_engine=analysis_engine,
        event_bus=event_bus,
        state_store=state_store,
        provenance_tracker=provenance_tracker,
        name="kaiser_main_replay_engine",
    )

    register_historical_handlers(
        orchestrator=orchestrator,
        loader=historical_loader,
        repository=historical_repository,
        replay_engine=historical_replay_engine,
    )

    # Initialize Backtesting & Strategy Evaluation Foundation
    strategy_registry = StrategyRegistry()
    backtest_engine = BacktestEngine(
        repository=historical_repository,
        quality_engine=market_service.quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
        event_bus=event_bus,
        state_store=state_store,
        provenance_tracker=provenance_tracker,
        run_registry=run_registry,
        name="kaiser_main_backtest_engine",
    )
    walk_forward_validator = WalkForwardValidator(
        engine=backtest_engine,
        repository=historical_repository,
        quality_engine=market_service.quality_engine,
        strategy_registry=strategy_registry,
        event_bus=event_bus,
        state_store=state_store,
        run_registry=run_registry,
        name="kaiser_main_walk_forward_validator",
    )
    robustness_test_engine = RobustnessTestEngine(
        engine=backtest_engine,
        repository=historical_repository,
        quality_engine=market_service.quality_engine,
        strategy_registry=strategy_registry,
        event_bus=event_bus,
        state_store=state_store,
        run_registry=run_registry,
        name="kaiser_main_robustness_test_engine",
    )
    portfolio_simulation_engine = PortfolioSimulationEngine(
        repository=historical_repository,
        quality_engine=market_service.quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
        event_bus=event_bus,
        state_store=state_store,
        provenance_tracker=provenance_tracker,
        run_registry=run_registry,
        name="kaiser_main_portfolio_simulation_engine",
    )
    register_backtest_handlers(
        orchestrator=orchestrator,
        engine=backtest_engine,
        strategy_registry=strategy_registry,
        validator=walk_forward_validator,
        robustness_engine=robustness_test_engine,
        portfolio_engine=portfolio_simulation_engine,
    )

    # Initialize Portfolio Risk Monitoring & Alerting Foundation
    portfolio_risk_monitor = PortfolioRiskMonitor(
        event_bus=event_bus,
        state_store=state_store,
        name="kaiser_main_portfolio_risk_monitor",
    )
    register_risk_handlers(orchestrator=orchestrator, monitor=portfolio_risk_monitor)

    # Initialize Portfolio Performance Attribution & Risk-Adjusted Metrics Foundation
    portfolio_attribution_service = PortfolioAttributionService(
        event_bus=event_bus,
        state_store=state_store,
        run_registry=run_registry,
    )
    register_portfolio_handlers(orchestrator=orchestrator, service=portfolio_attribution_service)

    # Initialize Portfolio Analytics Observability & Operational Diagnostics Foundation (Phase 2, Step 15)
    portfolio_diagnostics_service = PortfolioDiagnosticsService(
        max_history=100,
        event_bus=event_bus,
        state_store=state_store,
    )

    # Initialize Portfolio Analytics Reporting & Dashboard Data API Foundation
    portfolio_reporting_service = PortfolioReportingService(
        event_bus=event_bus,
        state_store=state_store,
        attribution_service=portfolio_attribution_service,
        risk_monitor=portfolio_risk_monitor,
        diagnostics_service=portfolio_diagnostics_service,
    )

    # Initialize Market Data Quality Monitor & Handlers (Phase 2, Step 19)
    quality_monitor = (
        market_service.quality_engine.monitor
        if market_service and hasattr(market_service.quality_engine, "monitor")
        else MarketDataQualityMonitor(
            event_bus=event_bus,
            state_store=state_store,
        )
    )
    register_quality_handlers(
        orchestrator=orchestrator,
        quality_monitor=quality_monitor,
        repository=historical_repository,
    )

    # Initialize Experiment Comparison & Run Comparison Foundation (Phase 2, Step 22)
    comparison_engine = RunComparisonEngine(
        run_registry=run_registry,
        provenance_tracker=provenance_tracker,
        quality_monitor=quality_monitor,
        event_bus=event_bus,
        state_store=state_store,
    )
    register_comparison_handlers(
        orchestrator=orchestrator,
        comparison_engine=comparison_engine,
    )

    # Initialize Evidence Synthesis & Decision-Support Reporting Engine (Phase 2, Step 23)
    evidence_engine = EvidenceSynthesisEngine(
        run_registry=run_registry,
        comparison_engine=comparison_engine,
        provenance_tracker=provenance_tracker,
        quality_monitor=quality_monitor,
        event_bus=event_bus,
        state_store=state_store,
    )
    register_evidence_handlers(
        orchestrator=orchestrator,
        evidence_engine=evidence_engine,
    )

    # Initialize Opportunity Assessment & Decision Support Engine (Phase 2, Step 24)
    opportunity_engine = OpportunityAssessmentEngine(
        evidence_engine=evidence_engine,
        run_registry=run_registry,
        event_bus=event_bus,
        state_store=state_store,
    )
    register_opportunity_handlers(
        orchestrator=orchestrator,
        opportunity_engine=opportunity_engine,
    )

    # Initialize Opportunity Prioritization & Investigation Queue (Phase 2, Step 25)
    prioritization_engine = OpportunityPrioritizationEngine(
        event_bus=event_bus,
        state_store=state_store,
    )
    investigation_queue = OpportunityInvestigationQueue(
        engine=prioritization_engine,
        event_bus=event_bus,
        state_store=state_store,
    )
    register_prioritization_handlers(
        orchestrator=orchestrator,
        prioritization_engine=prioritization_engine,
        investigation_queue=investigation_queue,
        opportunity_assessment_engine=opportunity_engine,
    )

    # Initialize Human Investigation Session Workflow (Phase 2, Step 26)
    investigation_manager = InvestigationSessionManager(
        event_bus=event_bus,
        state_store=state_store,
    )
    register_investigation_handlers(
        orchestrator=orchestrator,
        investigation_manager=investigation_manager,
    )

    # Initialize Investigation Reporting & Evidence Review Audit (Phase 2, Step 27)
    investigation_reporting_service = InvestigationReportingService(
        event_bus=event_bus,
        state_store=state_store,
    )
    register_investigation_reporting_handlers(
        orchestrator=orchestrator,
        reporting_service=investigation_reporting_service,
        session_manager=investigation_manager,
    )

    # Initialize Cross-Investigation Evidence Comparison & Review Consistency (Phase 2, Step 28)
    investigation_comparison_engine = InvestigationComparisonEngine(
        state_store=state_store,
        event_bus=event_bus,
        reporting_service=investigation_reporting_service,
    )
    register_investigation_comparison_handlers(
        orchestrator=orchestrator,
        comparison_engine=investigation_comparison_engine,
        reporting_service=investigation_reporting_service,
    )

    # Initialize Investigation Discrepancy Resolution & Evidence Reconciliation (Phase 2, Step 29)
    discrepancy_resolution_service = DiscrepancyResolutionService(
        state_store=state_store,
        event_bus=event_bus,
        comparison_engine=investigation_comparison_engine,
    )
    register_discrepancy_resolution_handlers(
        orchestrator=orchestrator,
        resolution_service=discrepancy_resolution_service,
    )

    # Initialize Resolved Investigation Evidence Consolidation & Review Outcome (Phase 2, Step 30)
    consolidation_service = InvestigationOutcomeConsolidationService(
        state_store=state_store,
        event_bus=event_bus,
        reporting_service=investigation_reporting_service,
        comparison_engine=investigation_comparison_engine,
        resolution_service=discrepancy_resolution_service,
    )
    register_consolidation_handlers(
        orchestrator=orchestrator,
        consolidation_service=consolidation_service,
    )

    # Initialize Cross-Investigation Consolidated Outcome Consistency (Phase 2, Step 31)
    consistency_service = ConsolidatedOutcomeConsistencyService(
        state_store=state_store,
        event_bus=event_bus,
        consolidation_service=consolidation_service,
    )
    register_consistency_handlers(
        orchestrator=orchestrator,
        consistency_service=consistency_service,
    )

    # Initialize Historical Review Evolution & Historical Consistency (Phase 2, Step 32)
    evolution_service = HistoricalReviewEvolutionService(
        state_store=state_store,
        event_bus=event_bus,
        consolidation_service=consolidation_service,
        consistency_service=consistency_service,
    )
    register_evolution_handlers(
        orchestrator=orchestrator,
        evolution_service=evolution_service,
    )

    # Initialize Historical Review Evolution Audit Service (Phase 2, Step 33)
    audit_service = HistoricalReviewAuditService(
        evolution_service=evolution_service,
        state_store=state_store,
        event_bus=event_bus,
        run_registry=run_registry,
    )
    register_audit_handlers(
        orchestrator=orchestrator,
        audit_service=audit_service,
    )

    # Initialize Historical Review Audit Reliability Service (Phase 2, Step 34)
    reliability_service = HistoricalReviewAuditReliabilityService(
        audit_service=audit_service,
        evolution_service=evolution_service,
        state_store=state_store,
        event_bus=event_bus,
    )
    register_reliability_handlers(
        orchestrator=orchestrator,
        reliability_service=reliability_service,
    )

    # Initialize Historical Review Audit Diagnostics & Observability (Phase 2, Step 36)
    audit_diagnostics_service = HistoricalReviewAuditDiagnosticsService(
        event_bus=event_bus,
        state_store=state_store,
    )
    register_audit_diagnostics_handlers(
        orchestrator=orchestrator,
        diagnostics_service=audit_diagnostics_service,
    )

    # Initialize Portfolio Analytics Dashboard UI Server (Phase 2, Step 12)
    portfolio_dashboard_server = PortfolioDashboardServer(
        reporting_service=portfolio_reporting_service,
        host=active_settings.dashboard_host,
        port=active_settings.dashboard_port,
        diagnostics_service=portfolio_diagnostics_service,
        quality_monitor=quality_monitor,
        provenance_tracker=provenance_tracker,
        run_registry=run_registry,
        comparison_engine=comparison_engine,
        evidence_engine=evidence_engine,
        opportunity_engine=opportunity_engine,
        prioritization_engine=prioritization_engine,
        investigation_queue=investigation_queue,
        investigation_manager=investigation_manager,
        investigation_reporting_service=investigation_reporting_service,
        investigation_comparison_engine=investigation_comparison_engine,
        discrepancy_resolution_service=discrepancy_resolution_service,
        consolidation_service=consolidation_service,
        consistency_service=consistency_service,
        evolution_service=evolution_service,
        audit_service=audit_service,
        reliability_service=reliability_service,
        state_store=state_store,
        audit_diagnostics_service=audit_diagnostics_service,
    )
    if active_settings.dashboard_enabled:
        portfolio_dashboard_server.start(blocking=False)

    register_reporting_handlers(
        orchestrator=orchestrator,
        service=portfolio_reporting_service,
        server=portfolio_dashboard_server,
    )




    def _on_market_bar_updated(event: Event) -> None:
        try:
            p = event.payload
            bar = Bar(
                symbol=str(p["symbol"]),
                exchange=str(p["exchange"]),
                timeframe=str(p["timeframe"]),
                timestamp=float(p["timestamp"]),
                open=float(p["open"]),
                high=float(p["high"]),
                low=float(p["low"]),
                close=float(p["close"]),
                volume=float(p.get("volume", 0.0)),
                source=str(p.get("source", "mock")),
            )
            safe = bool(event.metadata.get("safe_to_consume", True))
            analysis_engine.add_bar(bar, safe_to_consume=safe, correlation_id=event.correlation_id)
        except Exception as exc:
            logger.error("Failed to process market bar in analysis engine: %s", exc)

    event_bus.subscribe(EventType.MARKET_BAR_UPDATED, _on_market_bar_updated)

    # Publish initialization fact on the event bus
    event_bus.publish(
        Event.create(
            event_type=EventType.SYSTEM_INITIALIZED,
            source="bootstrap",
            payload={
                "app_name": active_settings.app_name,
                "environment": active_settings.app_env,
                "execution_locked": active_settings.execution_locked,
                "market_provider": active_settings.market_data_provider,
            },
        )
    )

    status = SystemStatus(
        app_name=active_settings.app_name,
        environment=active_settings.app_env,
        voice_architecture=voice_status,
        trading_engine=trading_status,
        execution_engine=execution_status,
        initialized=True,
        settings=active_settings,
        event_bus=event_bus,
        state_store=state_store,
        market_service=market_service,
        orchestrator=orchestrator,
        voice_service=voice_service,
        voice_controller=voice_controller,
        analysis_engine=analysis_engine,
        technical_agent=technical_agent,
        regime_agent=regime_agent,
        coordination_service=coordination_service,
        historical_loader=historical_loader,
        historical_provider=historical_provider,
        historical_repository=historical_repository,
        historical_cache=historical_cache,
        historical_replay_engine=historical_replay_engine,
        audio_capture=voice_service.audio_capture if voice_service else None,
        audio_playback=voice_service.audio_playback if voice_service else None,
        backtest_engine=backtest_engine,
        strategy_registry=strategy_registry,
        walk_forward_validator=walk_forward_validator,
        robustness_test_engine=robustness_test_engine,
        portfolio_simulation_engine=portfolio_simulation_engine,
        portfolio_risk_monitor=portfolio_risk_monitor,
        portfolio_attribution_service=portfolio_attribution_service,
        portfolio_reporting_service=portfolio_reporting_service,
        portfolio_dashboard_server=portfolio_dashboard_server,
        portfolio_diagnostics_service=portfolio_diagnostics_service,
        quality_monitor=quality_monitor,
        provenance_tracker=provenance_tracker,
        run_registry=run_registry,
        comparison_engine=comparison_engine,
        evidence_engine=evidence_engine,
        opportunity_engine=opportunity_engine,
        prioritization_engine=prioritization_engine,
        investigation_queue=investigation_queue,
        investigation_manager=investigation_manager,
        investigation_reporting_service=investigation_reporting_service,
        investigation_comparison_engine=investigation_comparison_engine,
        discrepancy_resolution_service=discrepancy_resolution_service,
        consolidation_service=consolidation_service,
        consistency_service=consistency_service,
        evolution_service=evolution_service,
        audit_service=audit_service,
        reliability_service=reliability_service,
        audit_diagnostics_service=audit_diagnostics_service,
    )

    logger.info("System initialized successfully in '%s' mode.", status.environment)
    logger.info("Voice-first architecture: %s", status.voice_architecture)
    logger.info("Market data quality monitor: ready")
    logger.info("Market data provenance & lineage tracker: ready")
    logger.info("Reproducible analysis run registry: ready (max_history=1000)")
    logger.info("Run comparison & experiment evaluation engine: ready (cache=%s)", comparison_engine.max_cache_size)
    logger.info("Evidence synthesis & decision-support engine: ready (max_history=%s)", evidence_engine.max_history)
    logger.info("Opportunity assessment & decision-support engine: ready (max_history=%s)", opportunity_engine.max_history)
    logger.info("Opportunity prioritization engine: ready (max_history=%s)", prioritization_engine.max_history)
    logger.info("Opportunity investigation queue: ready (capacity=%s)", investigation_queue.max_capacity)
    logger.info("Human investigation session manager: ready (max_history=%s)", investigation_manager.max_history)
    logger.info("Investigation reporting service: ready (max_history=%s)", investigation_reporting_service.max_history)
    logger.info("Cross-investigation comparison engine: ready (max_history=%s)", investigation_comparison_engine.max_history)
    logger.info("Investigation discrepancy resolution service: ready (max_history=%s)", discrepancy_resolution_service.max_history)
    logger.info("Investigation outcome consolidation service: ready (max_history=%s)", consolidation_service.max_history)
    logger.info("Consolidated outcome consistency service: ready (max_history=%s)", consistency_service.max_history)
    logger.info(
        "Historical review evolution service: ready (max_snapshots=%s, max_evolution=%s)",
        evolution_service.max_snapshots_per_investigation,
        evolution_service.max_evolution_per_investigation,
    )
    logger.info("Historical review audit service: ready (max_reports=%s)", audit_service.max_reports)
    logger.info("Historical review audit reliability service: ready (max_evaluations=%s)", reliability_service.max_evaluations)
    logger.info("Historical review audit diagnostics service: ready (max_diagnostics=%s)", audit_diagnostics_service._max_diagnostics)

    logger.info("Technical analysis engine: ready")
    logger.info("Technical analysis agent: ready")
    logger.info("Market regime specialist agent: ready")
    logger.info("Multi-agent specialist coordination & consensus service: ready")
    logger.info("Historical data loader: ready (provider=%s)", historical_provider.name)
    logger.info("Historical repository: ready (db=%s)", db_path)
    logger.info("Historical cache: ready (max_size=200)")
    logger.info("Historical replay engine: ready")
    logger.info("Backtest engine: ready (strategies=%s)", [s["name"] for s in strategy_registry.list_strategies()])
    logger.info("Walk-forward validator: ready")
    logger.info("Robustness test engine: ready")
    logger.info("Portfolio simulation engine: ready")
    logger.info("Portfolio risk monitor: ready")
    logger.info("Portfolio attribution engine: ready")
    logger.info("Portfolio reporting & dashboard API: ready")
    logger.info("Portfolio diagnostics & audit: ready (max_history=100, retention=bounded_in_memory)")
    dash_status = f"running at {portfolio_dashboard_server.url}" if portfolio_dashboard_server.is_running else "ready (offline)"
    logger.info("Portfolio analytics dashboard UI: %s", dash_status)
    logger.info("Trading engine: %s", status.trading_engine)
    logger.info("Execution engine: %s", status.execution_engine)



    return status


def format_banner(status: SystemStatus) -> str:
    """Format the standard KAISER console initialization banner.

    Args:
        status: The bootstrapped system status.

    Returns:
        str: Human-readable startup banner.
    """
    return (
        f"{status.app_name}\n"
        f"System initialized successfully.\n"
        f"Environment: {status.environment}\n"
        f"Voice-first architecture: {status.voice_architecture}\n"
        f"Trading engine: {status.trading_engine}\n"
        f"Execution engine: {status.execution_engine}"
    )
