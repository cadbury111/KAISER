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
    audio_capture: Optional[AudioCaptureService] = None
    audio_playback: Optional[AudioPlaybackService] = None


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
        audio_capture=voice_service.audio_capture if voice_service else None,
        audio_playback=voice_service.audio_playback if voice_service else None,
    )

    logger.info("System initialized successfully in '%s' mode.", status.environment)
    logger.info("Voice-first architecture: %s", status.voice_architecture)
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
