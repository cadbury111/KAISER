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
from app.agents import TechnicalAnalysisAgent, register_technical_agent_handlers
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

    # Initialize Technical & Quantitative Analysis Engine
    analysis_engine = TechnicalAnalysisEngine(
        event_bus=event_bus,
        state_store=state_store,
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

    # Initialize Historical Repository & Bounded Cache
    db_path = "data/kaiser.db"
    if active_settings.database_url.startswith("sqlite:///"):
        db_path = active_settings.database_url.replace("sqlite:///", "")
    elif active_settings.database_url.startswith("sqlite://"):
        db_path = active_settings.database_url.replace("sqlite://", "")

    historical_repository = SQLiteHistoricalBarRepository(db_path=db_path, name="sqlite_historical_repo")
    historical_cache = HistoricalDataCache(max_size=200, ttl_seconds=300.0)

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
        name="kaiser_main_historical_loader",
    )

    # Initialize Historical Replay Engine
    historical_replay_engine = HistoricalReplayEngine(
        repository=historical_repository,
        quality_engine=market_service.quality_engine,
        analysis_engine=analysis_engine,
        event_bus=event_bus,
        state_store=state_store,
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
        name="kaiser_main_backtest_engine",
    )
    walk_forward_validator = WalkForwardValidator(
        engine=backtest_engine,
        repository=historical_repository,
        quality_engine=market_service.quality_engine,
        strategy_registry=strategy_registry,
        event_bus=event_bus,
        state_store=state_store,
        name="kaiser_main_walk_forward_validator",
    )
    robustness_test_engine = RobustnessTestEngine(
        engine=backtest_engine,
        repository=historical_repository,
        quality_engine=market_service.quality_engine,
        strategy_registry=strategy_registry,
        event_bus=event_bus,
        state_store=state_store,
        name="kaiser_main_robustness_test_engine",
    )
    portfolio_simulation_engine = PortfolioSimulationEngine(
        repository=historical_repository,
        quality_engine=market_service.quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
        event_bus=event_bus,
        state_store=state_store,
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
    )

    logger.info("System initialized successfully in '%s' mode.", status.environment)
    logger.info("Voice-first architecture: %s", status.voice_architecture)
    logger.info("Technical analysis engine: ready")
    logger.info("Technical analysis agent: ready")
    logger.info("Historical data loader: ready (provider=%s)", historical_provider.name)
    logger.info("Historical repository: ready (db=%s)", db_path)
    logger.info("Historical cache: ready (max_size=200)")
    logger.info("Historical replay engine: ready")
    logger.info("Backtest engine: ready (strategies=%s)", [s["name"] for s in strategy_registry.list_strategies()])
    logger.info("Walk-forward validator: ready")
    logger.info("Robustness test engine: ready")
    logger.info("Portfolio simulation engine: ready")
    logger.info("Portfolio risk monitor: ready")
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
