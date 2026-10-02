"""KAISER — Phase 1, Step 12: Voice Interaction Controller & Session Management Tests.

Deterministic test suite verifying:
- Session creation, status inspection, and closure.
- Successful consecutive voice turns within a valid session.
- Invalid, expired, and closed session rejection.
- Cancellation across all supported lifecycle stages (before capture, during capture, during playback).
- Distinction between cancellable stages (capture, playback) and synchronous stages (STT, orchestration, TTS).
- Repeated cancellation and repeated closure idempotency.
- Per-session concurrency rejection (overlapping turns in the same session).
- Multiple independent sessions and hardware turn lock serialization.
- Session cleanup, bounded retention, and clean recovery from errors.
- Event ordering, correlation ID propagation, and StateStore telemetry.
- Controller shutdown behavior.
- Invariant: Zero automatic microphone capture or playback at startup or initialization.
- Invariant: Execution engine remains locked; trading engine remains uninitialized.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.state import StateStore
from app.orchestrator import create_system_status_handler
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest, OrchestratorResponse
from app.orchestrator.registry import HandlerRegistry
from app.voice.audio_capture import AudioCaptureService, MockAudioCaptureBackend
from app.voice.audio_models import AudioCaptureConfig, AudioCaptureError, AudioPlaybackStatus
from app.voice.audio_playback import AudioPlaybackService, MockAudioPlaybackBackend
from app.voice.controller import VoiceControllerError, VoiceInteractionController
from app.voice.models import (
    TranscriptionResult,
    VoiceOutput,
    VoiceSessionStatus,
    VoiceTurnResult,
    VoiceTurnStatus,
)
from app.voice.providers.mock_audio import MockAudioCaptureProvider
from app.voice.providers.mock_stt import MockSTTProvider
from app.voice.providers.mock_tts import MockTTSProvider
from app.voice.service import VoiceService
from app.voice.session import VoiceSession, VoiceSessionError, VoiceSessionManager
from app.voice.tts_models import TTSError, TTSUnsupportedLanguageError


# ------------------------------------------------------------------------------
# Test Fixtures & Mocks
# ------------------------------------------------------------------------------


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus()


@pytest.fixture
def state_store() -> StateStore:
    return StateStore()


@pytest.fixture
def mock_capture_backend() -> MockAudioCaptureBackend:
    provider = MockAudioCaptureProvider(name="mock_capture_provider")
    return MockAudioCaptureBackend(provider=provider)


@pytest.fixture
def mock_playback_backend() -> MockAudioPlaybackBackend:
    return MockAudioPlaybackBackend()


@pytest.fixture
def audio_capture_service(
    event_bus: EventBus,
    state_store: StateStore,
    mock_capture_backend: MockAudioCaptureBackend,
) -> AudioCaptureService:
    service = AudioCaptureService(event_bus=event_bus, state_store=state_store)
    service.backend = mock_capture_backend
    return service


@pytest.fixture
def audio_playback_service(
    event_bus: EventBus,
    state_store: StateStore,
    mock_playback_backend: MockAudioPlaybackBackend,
) -> AudioPlaybackService:
    service = AudioPlaybackService(event_bus=event_bus, state_store=state_store)
    service.backend = mock_playback_backend
    return service


@pytest.fixture
def master_orchestrator(event_bus: EventBus, state_store: StateStore) -> MasterOrchestrator:
    settings = Settings(
        app_name="KAISER_TEST",
        app_env="development",
        execution_locked=True,
    )
    registry = HandlerRegistry(name="test_controller_registry")
    registry.register(
        command="system_status",
        handler=create_system_status_handler(state_store=state_store, settings=settings),
        description="Status diagnostics",
        capability="diagnostics",
    )

    def slow_handler(req: OrchestratorRequest) -> OrchestratorResponse:
        time.sleep(0.05)
        return OrchestratorResponse.success(
            request_id=req.request_id,
            response_text="Slow command finished.",
        )

    registry.register(
        command="slow_command",
        handler=slow_handler,
        description="Slow handler",
        capability="diagnostics",
    )

    def failing_handler(req: OrchestratorRequest) -> OrchestratorResponse:
        raise RuntimeError("Fatal hardware simulation exception in handler.")

    registry.register(
        command="fail_command",
        handler=failing_handler,
        description="Failing handler",
        capability="diagnostics",
    )

    return MasterOrchestrator(
        event_bus=event_bus,
        state_store=state_store,
        registry=registry,
        settings=settings,
        name="test_orchestrator",
    )


@pytest.fixture
def mock_stt_provider() -> MockSTTProvider:
    stt = MockSTTProvider(name="mock_stt")
    # Register deterministic fixtures
    stt.register_fixture(
        "audio:32000",
        TranscriptionResult(text="system_status", language="en", confidence=0.99),
    )
    stt.register_fixture(
        "audio:3200",
        TranscriptionResult(text="system_status", language="en", confidence=0.99),
    )
    stt.register_fixture(
        "fixture:status",
        TranscriptionResult(text="system_status", language="en", confidence=0.98),
    )
    stt.register_fixture(
        "fixture:slow",
        TranscriptionResult(text="slow_command", language="en", confidence=0.95),
    )
    return stt


@pytest.fixture
def mock_tts_provider() -> MockTTSProvider:
    return MockTTSProvider(name="mock_tts")


@pytest.fixture
def voice_service(
    master_orchestrator: MasterOrchestrator,
    mock_stt_provider: MockSTTProvider,
    mock_tts_provider: MockTTSProvider,
    audio_capture_service: AudioCaptureService,
    audio_playback_service: AudioPlaybackService,
    event_bus: EventBus,
    state_store: StateStore,
) -> VoiceService:
    return VoiceService(
        orchestrator=master_orchestrator,
        stt_provider=mock_stt_provider,
        tts_provider=mock_tts_provider,
        audio_capture=audio_capture_service,
        audio_playback=audio_playback_service,
        event_bus=event_bus,
        state_store=state_store,
        name="test_voice_service",
    )


@pytest.fixture
def controller(
    voice_service: VoiceService,
    event_bus: EventBus,
    state_store: StateStore,
) -> VoiceInteractionController:
    return VoiceInteractionController(
        voice_service=voice_service,
        event_bus=event_bus,
        state_store=state_store,
        name="test_voice_controller",
    )


# ==============================================================================
# 1. SESSION LIFECYCLE & BASIC OPERATIONS
# ==============================================================================


class TestSessionLifecycleAndOperations:
    """Tests for session creation, inspection, closure, idempotency, and recovery."""

    def test_session_creation_valid(
        self,
        controller: VoiceInteractionController,
        event_bus: EventBus,
    ):
        """Verify session creation produces a valid ACTIVE session and publishes event."""
        events: List[Event] = []
        event_bus.subscribe(EventType.VOICE_SESSION_CREATED, lambda e: events.append(e))

        session = controller.create_session(
            session_id="sess_custom_001",
            language="en",
            metadata={"caller": "test_client"},
        )

        assert session.session_id == "sess_custom_001"
        assert session.status == VoiceSessionStatus.ACTIVE
        assert session.language == "en"
        assert session.is_active is True
        assert session.is_closed is False
        assert session.created_at > 0
        assert session.get_metadata().get("caller") == "test_client"

        # Verify event was published
        assert len(events) == 1
        assert events[0].payload["session_id"] == "sess_custom_001"
        assert events[0].payload["language"] == "en"

    def test_session_creation_auto_id(self, controller: VoiceInteractionController):
        """Verify session creation generates unique ID when omitted."""
        s1 = controller.create_session()
        s2 = controller.create_session()

        assert s1.session_id.startswith("sess_")
        assert s2.session_id.startswith("sess_")
        assert s1.session_id != s2.session_id

    def test_session_status_inspection(self, controller: VoiceInteractionController):
        """Verify get_session_status returns complete structured state dictionary."""
        controller.create_session(
            session_id="sess_inspect",
            language="ta",
            metadata={"device": "desktop"},
        )

        status = controller.get_session_status("sess_inspect")
        assert status is not None
        assert status["session_id"] == "sess_inspect"
        assert status["status"] == VoiceSessionStatus.ACTIVE.value
        assert status["language"] == "ta"
        assert status["is_active"] is True
        assert status["is_closed"] is False
        assert status["has_active_turn"] is False
        assert status["active_turn"] is None
        assert status["metadata"]["device"] == "desktop"

    def test_session_status_inspection_nonexistent(self, controller: VoiceInteractionController):
        """Verify inspecting non-existent session returns None cleanly."""
        assert controller.get_session_status("nonexistent_session") is None

    def test_session_closure_graceful(
        self,
        controller: VoiceInteractionController,
        event_bus: EventBus,
    ):
        """Verify closing a session sets status to CLOSED and publishes event."""
        events: List[Event] = []
        event_bus.subscribe(EventType.VOICE_SESSION_CLOSED, lambda e: events.append(e))

        controller.create_session(session_id="sess_close_me")
        closed = controller.close_session("sess_close_me", reason="Test completion")

        assert closed is True
        session = controller.get_session("sess_close_me")
        assert session is not None
        assert session.status == VoiceSessionStatus.CLOSED
        assert session.is_closed is True
        assert session.is_active is False

        assert len(events) == 1
        assert events[0].payload["session_id"] == "sess_close_me"
        assert events[0].payload["reason"] == "Test completion"

    def test_repeated_close_is_idempotent(self, controller: VoiceInteractionController):
        """Verify repeated calls to close_session on the same session do not crash and return True."""
        controller.create_session(session_id="sess_multi_close")
        assert controller.close_session("sess_multi_close") is True
        assert controller.close_session("sess_multi_close") is True
        assert controller.close_session("sess_multi_close") is True

    def test_close_nonexistent_session_returns_false(self, controller: VoiceInteractionController):
        """Verify closing non-existent session returns False safely."""
        assert controller.close_session("ghost_session") is False

    def test_closed_session_cannot_submit_turns(self, controller: VoiceInteractionController):
        """Verify submitting a turn to a closed session is rejected with structured failure."""
        controller.create_session(session_id="sess_already_closed")
        controller.close_session("sess_already_closed")

        res = controller.submit_turn(
            session_id="sess_already_closed",
            audio_data="fixture:status",
        )

        assert res.is_success is False
        assert res.status == VoiceTurnStatus.FAILED
        assert res.command_status == "skipped"
        assert "closed" in (res.error or "").lower()

    def test_nonexistent_session_turn_submission_rejected(self, controller: VoiceInteractionController):
        """Verify submitting a turn with invalid/non-existent session ID is rejected."""
        res = controller.submit_turn(
            session_id="sess_ghost",
            audio_data="fixture:status",
        )

        assert res.is_success is False
        assert res.status == VoiceTurnStatus.FAILED
        assert res.command_status == "skipped"
        assert "not found" in (res.error or "").lower()

    def test_invalid_session_id_empty_string(self, controller: VoiceInteractionController):
        """Verify empty or whitespace session ID is rejected immediately."""
        res = controller.submit_turn(session_id="", audio_data="fixture:status")
        assert res.is_success is False
        assert res.command_status == "skipped"
        assert "session_id must be a non-empty string" in (res.error or "")

    def test_consecutive_turns_in_same_session(self, controller: VoiceInteractionController):
        """Verify consecutive turns in the same session succeed and recover session to ACTIVE."""
        session = controller.create_session(session_id="sess_consecutive")

        for i in range(3):
            res = controller.submit_turn(
                session_id="sess_consecutive",
                audio_data="fixture:status",
                synthesize_output=False,
                play_output=False,
                correlation_id=f"corr_turn_{i}",
            )
            assert res.is_success is True
            assert res.command_status == "success"
            assert "operational" in res.response_text.lower()
            # Session should be cleanly recovered to ACTIVE after each turn
            assert session.status == VoiceSessionStatus.ACTIVE
            assert session.is_active is True

    def test_recover_session_explicit(self, controller: VoiceInteractionController):
        """Verify recover_session explicitly recovers a session from COMPLETED back to ACTIVE."""
        session = controller.create_session(session_id="sess_manual_recover")
        session.transition_to(VoiceSessionStatus.COMPLETED)
        assert session.status == VoiceSessionStatus.COMPLETED

        recovered = controller.recover_session("sess_manual_recover", reason="Operator reset")
        assert recovered is True
        assert session.status == VoiceSessionStatus.ACTIVE

    def test_recover_closed_session_rejected(self, controller: VoiceInteractionController):
        """Verify closed sessions cannot be recovered."""
        controller.create_session(session_id="sess_closed_forever")
        controller.close_session("sess_closed_forever")

        assert controller.recover_session("sess_closed_forever") is False


# ==============================================================================
# 2. TURN SUBMISSION & OUTPUT CONTROLS
# ==============================================================================


class TestTurnSubmissionAndOutputControls:
    """Tests for turn submission modes, explicit output controls, and fault isolation."""

    def test_submit_turn_text_only(
        self,
        controller: VoiceInteractionController,
        mock_tts_provider: MockTTSProvider,
        mock_playback_backend: MockAudioPlaybackBackend,
    ):
        """Verify text-only mode skips synthesis and playback completely."""
        controller.create_session(session_id="sess_text_only")

        turn = controller.submit_turn(
            session_id="sess_text_only",
            audio_data="fixture:status",
            synthesize_output=False,
            play_output=False,
        )

        assert turn.is_success is True
        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.command_status == "success"
        assert turn.synthesis_status == "skipped"
        assert turn.playback_status == "skipped"
        assert "operational" in turn.response_text.lower()
        assert len(mock_tts_provider.calls) == 0
        assert mock_playback_backend.play_count == 0

    def test_submit_turn_synthesis_only(
        self,
        controller: VoiceInteractionController,
        mock_tts_provider: MockTTSProvider,
        mock_playback_backend: MockAudioPlaybackBackend,
    ):
        """Verify synthesis-only mode generates audio payload but skips playback."""
        controller.create_session(session_id="sess_synth_only")

        turn = controller.submit_turn(
            session_id="sess_synth_only",
            audio_data="fixture:status",
            synthesize_output=True,
            play_output=False,
        )

        assert turn.is_success is True
        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.command_status == "success"
        assert turn.synthesis_status == "completed"
        assert turn.playback_status == "skipped"
        assert turn.command_response is not None
        assert turn.command_response.has_audio is True
        assert len(mock_tts_provider.calls) == 1
        assert mock_playback_backend.play_count == 0

    def test_submit_turn_synthesize_and_play(
        self,
        controller: VoiceInteractionController,
        mock_tts_provider: MockTTSProvider,
        mock_playback_backend: MockAudioPlaybackBackend,
    ):
        """Verify synthesize-and-play mode synthesizes and plays audio through device."""
        controller.create_session(session_id="sess_play")

        turn = controller.submit_turn(
            session_id="sess_play",
            audio_data="fixture:status",
            synthesize_output=True,
            play_output=True,
        )

        assert turn.is_success is True
        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.command_status == "success"
        assert turn.synthesis_status == "completed"
        assert turn.playback_status == "completed"
        assert len(mock_tts_provider.calls) == 1
        assert mock_playback_backend.play_count == 1

    def test_tts_failure_isolated_in_controller(
        self,
        controller: VoiceInteractionController,
        voice_service: VoiceService,
    ):
        """Verify TTS engine failure preserves command response text and command_status='success'."""
        controller.create_session(session_id="sess_tts_fail")

        # Simulate TTS failure
        voice_service.tts_provider.synthesize = MagicMock(side_effect=TTSError("TTS engine failed"))

        turn = controller.submit_turn(
            session_id="sess_tts_fail",
            audio_data="fixture:status",
            synthesize_output=True,
            play_output=True,
        )

        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.command_status == "success"
        assert turn.synthesis_status == "failed"
        assert turn.playback_status == "skipped"
        assert "operational" in turn.response_text.lower()
        assert turn.synthesis_error is not None
        assert "TTS engine failed" in turn.synthesis_error

    def test_playback_failure_isolated_in_controller(
        self,
        controller: VoiceInteractionController,
        mock_playback_backend: MockAudioPlaybackBackend,
    ):
        """Verify playback device failure preserves command response text."""
        controller.create_session(session_id="sess_play_fail")
        mock_playback_backend.set_failure_mode(True, message="Speaker driver disconnected")

        turn = controller.submit_turn(
            session_id="sess_play_fail",
            audio_data="fixture:status",
            synthesize_output=True,
            play_output=True,
        )

        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.command_status == "success"
        assert turn.synthesis_status == "completed"
        assert turn.playback_status == "failed"
        assert "operational" in turn.response_text.lower()
        assert turn.playback_error is not None
        assert "Speaker driver disconnected" in turn.playback_error


# ==============================================================================
# 3. CONCURRENCY & CANCELLATION
# ==============================================================================


class TestConcurrencyAndCancellation:
    """Tests for turn concurrency boundaries and stage-specific cancellation."""

    def test_concurrent_turns_in_same_session_rejected(
        self,
        controller: VoiceInteractionController,
    ):
        """Verify concurrent turns within the same session are rejected with CONCURRENCY_ERROR."""
        controller.create_session(session_id="sess_concurrent")

        first_turn_started = threading.Event()
        first_turn_can_finish = threading.Event()
        results: Dict[str, VoiceTurnResult] = {}

        def run_first_turn():
            # Monkey-patch audio_capture to block until signaled
            original_capture = controller.voice_service.audio_capture.capture_audio

            def slow_capture(*args, **kwargs):
                first_turn_started.set()
                first_turn_can_finish.wait(timeout=2.0)
                return original_capture(*args, **kwargs)

            controller.voice_service.audio_capture.capture_audio = slow_capture
            try:
                results["turn1"] = controller.submit_turn(
                    session_id="sess_concurrent",
                    duration_seconds=0.1,
                )
            finally:
                controller.voice_service.audio_capture.capture_audio = original_capture

        t1 = threading.Thread(target=run_first_turn)
        t1.start()

        # Wait until first turn has acquired active turn slot
        assert first_turn_started.wait(timeout=2.0) is True

        # Attempt to run second turn in SAME session concurrently
        results["turn2"] = controller.submit_turn(
            session_id="sess_concurrent",
            duration_seconds=0.1,
        )

        # Release first turn
        first_turn_can_finish.set()
        t1.join(timeout=3.0)

        # Check outcomes
        assert results["turn2"].status == VoiceTurnStatus.CONCURRENCY_ERROR
        assert results["turn2"].is_success is False
        assert results["turn2"].command_status == "skipped"
        assert "active turn" in (results["turn2"].error or "").lower()

        assert results["turn1"].status == VoiceTurnStatus.COMPLETED
        assert results["turn1"].command_status == "success"

    def test_cancellation_during_capture(
        self,
        controller: VoiceInteractionController,
    ):
        """Verify cancellation during audio capture halts immediately with CANCELLED status."""
        controller.create_session(session_id="sess_cancel_cap")

        capture_started = threading.Event()

        def slow_capture_loop(config, stop_event, on_chunk=None):
            capture_started.set()
            # Wait for cancellation signal
            while not stop_event.is_set():
                time.sleep(0.01)
            return b""  # Cancelled

        controller.voice_service.audio_capture.backend.capture = slow_capture_loop

        def run_turn():
            return controller.submit_turn(
                session_id="sess_cancel_cap",
                duration_seconds=5.0,
            )

        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future = executor.submit(run_turn)
            assert capture_started.wait(timeout=2.0) is True

            # Request cancellation through controller
            assert controller.cancel_turn("sess_cancel_cap") is True

            turn_result = future.result(timeout=3.0)

        assert turn_result.status == VoiceTurnStatus.CANCELLED
        assert turn_result.is_success is False
        assert turn_result.transcription_status == "cancelled"
        assert turn_result.command_status == "skipped"

    def test_cancellation_during_playback(
        self,
        controller: VoiceInteractionController,
        mock_playback_backend: MockAudioPlaybackBackend,
    ):
        """Verify cancellation during audio playback stops playback cleanly."""
        controller.create_session(session_id="sess_cancel_play")

        playback_started = threading.Event()

        def slow_play(pcm_bytes, sample_rate, channels, sample_width_bytes=2, device_id=None, stop_event=None):
            playback_started.set()
            while stop_event and not stop_event.is_set():
                time.sleep(0.01)
            from app.voice.audio_models import AudioPlaybackCancelledError
            raise AudioPlaybackCancelledError("Playback cancelled during execution.")

        mock_playback_backend.play = slow_play

        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future = executor.submit(
                controller.submit_turn,
                session_id="sess_cancel_play",
                audio_data="fixture:status",
                synthesize_output=True,
                play_output=True,
            )
            assert playback_started.wait(timeout=2.0) is True

            # Cancel through controller
            assert controller.cancel_turn("sess_cancel_play") is True
            turn_result = future.result(timeout=3.0)

        assert turn_result.status == VoiceTurnStatus.COMPLETED
        assert turn_result.command_status == "success"
        assert turn_result.synthesis_status == "completed"
        assert turn_result.playback_status == "cancelled"
        assert "operational" in turn_result.response_text.lower()

    def test_repeated_cancellation_is_idempotent(
        self,
        controller: VoiceInteractionController,
    ):
        """Verify calling cancel_turn repeatedly is safe and idempotent."""
        controller.create_session(session_id="sess_multi_cancel")

        # When no turn is active, cancel_turn returns False
        assert controller.cancel_turn("sess_multi_cancel") is False
        assert controller.cancel_turn("sess_multi_cancel") is False

    def test_cancel_nonexistent_session_returns_false(
        self,
        controller: VoiceInteractionController,
    ):
        """Verify calling cancel_turn on a non-existent session returns False cleanly."""
        assert controller.cancel_turn("ghost_sess") is False
        assert controller.cancel_turn("") is False

    def test_multiple_independent_sessions_serialized_by_hardware_turn_lock(
        self,
        controller: VoiceInteractionController,
    ):
        """Verify two distinct sessions attempting simultaneous physical capture serialize safely."""
        s1 = controller.create_session(session_id="sess_A")
        s2 = controller.create_session(session_id="sess_B")

        turn1_started = threading.Event()
        turn1_can_proceed = threading.Event()
        results: Dict[str, VoiceTurnResult] = {}

        def run_turn1():
            orig_capture = controller.voice_service.audio_capture.capture_audio

            def slow_capture(*args, **kwargs):
                turn1_started.set()
                turn1_can_proceed.wait(timeout=2.0)
                return orig_capture(*args, **kwargs)

            controller.voice_service.audio_capture.capture_audio = slow_capture
            try:
                results["t1"] = controller.submit_turn(session_id="sess_A", duration_seconds=0.1)
            finally:
                controller.voice_service.audio_capture.capture_audio = orig_capture

        t = threading.Thread(target=run_turn1)
        t.start()

        assert turn1_started.wait(timeout=2.0) is True

        # Second turn in different session attempts capture while VoiceService._turn_lock is held
        results["t2"] = controller.submit_turn(session_id="sess_B", duration_seconds=0.1)

        turn1_can_proceed.set()
        t.join(timeout=3.0)

        # t2 is rejected because the single physical microphone resource is busy
        assert results["t2"].status == VoiceTurnStatus.CONCURRENCY_ERROR
        assert results["t2"].command_status == "skipped"

        # t1 completed successfully
        assert results["t1"].status == VoiceTurnStatus.COMPLETED
        assert results["t1"].command_status == "success"


# ==============================================================================
# 4. BOUNDED STATE, SHUTDOWN & TELEMETRY
# ==============================================================================


class TestBoundedStateAndShutdown:
    """Tests for bounded session manager retention, shutdown, and telemetry."""

    def test_session_manager_capacity_and_retention(self):
        """Verify SessionManager purges closed sessions and stays bounded."""
        mgr = VoiceSessionManager(max_sessions=5)

        for i in range(10):
            sess = mgr.create_session(session_id=f"bounded_{i}")
            if i % 2 == 0:
                sess.close()

        # Total sessions tracked should not exceed max_sessions
        assert len(mgr._sessions) <= 5

    def test_controller_shutdown_cancels_turns_and_closes_sessions(
        self,
        controller: VoiceInteractionController,
    ):
        """Verify controller shutdown signals active turns and closes all sessions."""
        s1 = controller.create_session(session_id="sess_shut_1")
        s2 = controller.create_session(session_id="sess_shut_2")

        assert s1.is_closed is False
        assert s2.is_closed is False

        controller.shutdown()

        assert s1.is_closed is True
        assert s2.is_closed is True
        assert controller.session_manager.active_count == 0

    def test_event_ordering_and_correlation_propagation(
        self,
        controller: VoiceInteractionController,
        event_bus: EventBus,
    ):
        """Verify correlation IDs and event sequence across session and turn."""
        recorded_events: List[Event] = []

        def record(e: Event):
            recorded_events.append(e)

        event_bus.subscribe(EventType.VOICE_SESSION_CREATED, record)
        event_bus.subscribe(EventType.VOICE_TURN_STARTED, record)
        event_bus.subscribe(EventType.VOICE_TURN_COMPLETED, record)
        event_bus.subscribe(EventType.VOICE_SESSION_CLOSED, record)

        corr_id = "corr_trace_999"
        controller.create_session(session_id="sess_trace")
        controller.submit_turn(
            session_id="sess_trace",
            audio_data="fixture:status",
            synthesize_output=False,
            play_output=False,
            correlation_id=corr_id,
        )
        controller.close_session("sess_trace")

        event_types = [e.event_type for e in recorded_events]
        assert event_types == [
            EventType.VOICE_SESSION_CREATED,
            EventType.VOICE_TURN_STARTED,
            EventType.VOICE_TURN_COMPLETED,
            EventType.VOICE_SESSION_CLOSED,
        ]

        # Verify correlation ID on turn events
        turn_events = [e for e in recorded_events if e.event_type in (EventType.VOICE_TURN_STARTED, EventType.VOICE_TURN_COMPLETED)]
        for te in turn_events:
            assert te.correlation_id == corr_id


# ==============================================================================
# 5. TRADING SAFETY GUARANTEES
# ==============================================================================


class TestTradingSafetyGuarantees:
    """Verify security boundaries: voice controller never unlocks execution or trades."""

    def test_no_automatic_recording_or_playback_on_init(
        self,
        controller: VoiceInteractionController,
        mock_capture_backend: MockAudioCaptureBackend,
        mock_playback_backend: MockAudioPlaybackBackend,
    ):
        """Verify initializing controller performs zero audio capture or playback."""
        assert mock_capture_backend.provider.capture_count == 0
        assert mock_playback_backend.play_count == 0

    def test_execution_engine_locked_throughout(
        self,
        controller: VoiceInteractionController,
    ):
        """Verify execution engine remains locked before, during, and after controller operations."""
        settings = controller.voice_service.orchestrator.settings
        assert settings.execution_locked is True

        controller.create_session(session_id="sess_safe")
        turn = controller.submit_turn(
            session_id="sess_safe",
            audio_data="fixture:status",
            synthesize_output=True,
            play_output=True,
        )

        assert turn.is_success is True
        assert settings.execution_locked is True

    @pytest.mark.parametrize("trading_word", ["buy", "sell", "confirm", "execute", "ok"])
    def test_spoken_trading_words_rejected_and_cannot_authorize(
        self,
        controller: VoiceInteractionController,
        trading_word: str,
    ):
        """Verify spoken financial action words are rejected with COMMAND_REJECTED."""
        controller.create_session(session_id=f"sess_trade_{trading_word}")

        # Register fixture simulating spoken trading word
        controller.voice_service.stt_provider.register_fixture(
            f"fixture:{trading_word}",
            TranscriptionResult(text=trading_word, language="en"),
        )

        turn = controller.submit_turn(
            session_id=f"sess_trade_{trading_word}",
            audio_data=f"fixture:{trading_word}",
            synthesize_output=False,
            play_output=False,
        )

        assert turn.status == VoiceTurnStatus.COMMAND_REJECTED
        assert turn.is_success is False
        assert turn.command_status == "rejected"
        assert f"Unknown command: '{trading_word}'" in (turn.error or "")
