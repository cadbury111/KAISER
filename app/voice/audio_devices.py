"""KAISER — Audio Device Discovery & Management.

Provides an extensible, failure-isolated interface for querying and discovering
audio input devices (microphones). Supports:
- Hardware-backed local device discovery via `sounddevice`
- Fully deterministic mock device discovery for automated tests
- Graceful degradation when audio drivers, libraries, or microphones are missing
- Publication of discovery events and bounded StateStore device metrics
"""

from __future__ import annotations

import copy
import threading
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Mapping, Optional

from app.core.events import Event, EventBus, EventType
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.orchestrator.lifecycle import sanitize_container
from app.voice.audio_models import (
    AudioDeviceError,
    AudioDeviceInfo,
    AudioDeviceNotFoundError,
)

logger = get_logger("voice.devices")


# ---------------------------------------------------------------------------
# Backend Interface
# ---------------------------------------------------------------------------


class AudioDeviceBackend(ABC):
    """Abstract interface for audio device discovery backends."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the device discovery backend."""

    @property
    @abstractmethod
    def is_available(self) -> bool:
        """Return True if backend library and audio subsystem are functional."""

    @abstractmethod
    def query_devices(self) -> List[AudioDeviceInfo]:
        """Query and return all audio devices reported by the underlying subsystem."""

    @abstractmethod
    def get_default_input_index(self) -> Optional[int]:
        """Return the device index of the system default input device, if any."""

    @abstractmethod
    def get_default_output_index(self) -> Optional[int]:
        """Return the device index of the system default output device, if any."""


# ---------------------------------------------------------------------------
# SoundDevice Adapter (Local Hardware / PortAudio)
# ---------------------------------------------------------------------------


class SoundDeviceBackend(AudioDeviceBackend):
    """Local device discovery backend utilizing `sounddevice` and PortAudio."""

    def __init__(self) -> None:
        self._available: bool = False
        self._sd_module: Optional[Any] = None
        self._init_backend()

    def _init_backend(self) -> None:
        """Attempt to safely import sounddevice without raising uncaught errors."""
        try:
            import sounddevice as sd  # type: ignore

            self._sd_module = sd
            self._available = True
            logger.debug("SoundDeviceBackend successfully initialized.")
        except Exception as exc:
            self._available = False
            self._sd_module = None
            logger.warning(
                "SoundDeviceBackend is unavailable (audio driver or library missing): %s", exc
            )

    @property
    def name(self) -> str:
        return "sounddevice"

    @property
    def is_available(self) -> bool:
        return self._available and (self._sd_module is not None)

    def get_default_input_index(self) -> Optional[int]:
        """Return system default input device index, or None if unavailable/unset."""
        if not self.is_available:
            return None
        try:
            default_tuple = self._sd_module.default.device
            if isinstance(default_tuple, (list, tuple)) and len(default_tuple) >= 1:
                idx = default_tuple[0]
                return int(idx) if idx >= 0 else None
            elif isinstance(default_tuple, int) and default_tuple >= 0:
                return int(default_tuple)
        except Exception as exc:
            logger.debug("Failed to query default sounddevice input: %s", exc)
        return None

    def get_default_output_index(self) -> Optional[int]:
        """Return system default output device index, or None if unavailable/unset."""
        if not self.is_available:
            return None
        try:
            default_tuple = self._sd_module.default.device
            if isinstance(default_tuple, (list, tuple)) and len(default_tuple) >= 2:
                idx = default_tuple[1]
                return int(idx) if idx >= 0 else None
            elif isinstance(default_tuple, int) and default_tuple >= 0:
                return int(default_tuple)
        except Exception as exc:
            logger.debug("Failed to query default sounddevice output: %s", exc)
        return None

    def query_devices(self) -> List[AudioDeviceInfo]:
        """Query device list from sounddevice, mapping each to AudioDeviceInfo."""
        if not self.is_available:
            return []

        try:
            raw_devices = self._sd_module.query_devices()
            hostapis = []
            try:
                raw_apis = self._sd_module.query_hostapis()
                hostapis = [api.get("name", "Unknown") for api in raw_apis]
            except Exception:
                pass

            default_in_idx = self.get_default_input_index()
            device_list: List[AudioDeviceInfo] = []

            for idx, dev in enumerate(raw_devices):
                dev_name = str(dev.get("name", f"Audio Device {idx}")).strip() or f"Device {idx}"
                max_in = int(dev.get("max_input_channels", 0))
                default_rate = float(dev.get("default_samplerate", 16000.0))
                api_idx = dev.get("hostapi", -1)
                hostapi_name = hostapis[api_idx] if 0 <= api_idx < len(hostapis) else None

                device_info = AudioDeviceInfo(
                    device_id=idx,
                    name=dev_name,
                    max_input_channels=max_in,
                    default_sample_rate=default_rate if default_rate > 0 else 16000.0,
                    is_default=(idx == default_in_idx),
                    is_available=True,
                    hostapi=hostapi_name,
                    extra_info={
                        "max_output_channels": int(dev.get("max_output_channels", 0)),
                        "default_low_input_latency": float(dev.get("default_low_input_latency", 0.0)),
                    },
                )
                device_list.append(device_info)

            return device_list

        except Exception as exc:
            logger.error("SoundDevice query_devices failed: %s", exc)
            return []


# ---------------------------------------------------------------------------
# Mock Device Backend (Deterministic Test Fixtures)
# ---------------------------------------------------------------------------


class MockAudioDeviceBackend(AudioDeviceBackend):
    """Deterministic in-memory device discovery backend for testing."""

    def __init__(
        self,
        devices: Optional[List[AudioDeviceInfo]] = None,
        default_input_id: Optional[int] = None,
        default_output_id: Optional[int] = None,
        available: bool = True,
    ) -> None:
        self._lock = threading.RLock()
        self._available = available
        self._should_fail = False
        self._failure_message = "Simulated device discovery failure"
        self._default_input_id = default_input_id
        self._default_output_id = default_output_id

        if devices is not None:
            self._devices = list(devices)
        else:
            self._devices = self._create_default_fixtures()

    def _create_default_fixtures(self) -> List[AudioDeviceInfo]:
        """Create standard test fixtures."""
        return [
            AudioDeviceInfo(
                device_id=0,
                name="Mock Studio Microphone",
                max_input_channels=1,
                default_sample_rate=16000.0,
                is_default=True,
                hostapi="MockAPI",
                extra_info={"kind": "usb_mic"},
            ),
            AudioDeviceInfo(
                device_id=1,
                name="Mock Stereo Array",
                max_input_channels=2,
                default_sample_rate=44100.0,
                is_default=False,
                hostapi="MockAPI",
                extra_info={"kind": "array_mic"},
            ),
            AudioDeviceInfo(
                device_id=2,
                name="Mock Output Speakers (No Input)",
                max_input_channels=0,
                default_sample_rate=48000.0,
                is_default=False,
                hostapi="MockAPI",
                extra_info={"kind": "speaker"},
            ),
        ]

    @property
    def name(self) -> str:
        return "mock_device_backend"

    @property
    def is_available(self) -> bool:
        with self._lock:
            return self._available and not self._should_fail

    def set_available(self, available: bool) -> None:
        """Toggle backend availability."""
        with self._lock:
            self._available = available

    def set_failure_mode(self, should_fail: bool, message: str = "Simulated failure") -> None:
        """Enable or disable simulated query failure."""
        with self._lock:
            self._should_fail = should_fail
            self._failure_message = message

    def set_devices(
        self,
        devices: List[AudioDeviceInfo],
        default_input_id: Optional[int] = None,
        default_output_id: Optional[int] = None,
    ) -> None:
        """Replace device list."""
        with self._lock:
            self._devices = list(devices)
            self._default_input_id = default_input_id
            self._default_output_id = default_output_id

    def get_default_input_index(self) -> Optional[int]:
        with self._lock:
            if not self.is_available:
                return None
            if self._default_input_id is not None:
                return self._default_input_id
            for dev in self._devices:
                if dev.is_default and dev.is_input_device:
                    return dev.device_id
            # Fallback to first input device
            for dev in self._devices:
                if dev.is_input_device:
                    return dev.device_id
            return None

    def get_default_output_index(self) -> Optional[int]:
        with self._lock:
            if not self.is_available:
                return None
            if self._default_output_id is not None:
                return self._default_output_id
            for dev in self._devices:
                if dev.is_output_device:
                    return dev.device_id
            return None

    def query_devices(self) -> List[AudioDeviceInfo]:
        with self._lock:
            if not self.is_available:
                return []
            if self._should_fail:
                raise AudioDeviceError(self._failure_message)
            return list(self._devices)


# ---------------------------------------------------------------------------
# Audio Device Manager
# ---------------------------------------------------------------------------


class AudioDeviceManager:
    """Coordinator for discovering, selecting, and validating audio input hardware.

    Guarantees:
    - Never crashes on missing libraries, audio-driver failures, or absent microphones.
    - Never assumes a fixed microphone name or system device index.
    - Safely reports device availability and input capabilities.
    - Synchronizes bounded summaries to StateStore and publishes lifecycle events.
    """

    def __init__(
        self,
        backend: Optional[AudioDeviceBackend] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
    ) -> None:
        self.event_bus = event_bus
        self.state_store = state_store
        self._lock = threading.RLock()

        if backend is not None:
            self._backend = backend
        else:
            # Default to sounddevice if available, else fallback to mock backend
            sd_backend = SoundDeviceBackend()
            if sd_backend.is_available:
                self._backend = sd_backend
            else:
                logger.info(
                    "SoundDevice backend is unavailable; initializing empty device backend."
                )
                self._backend = MockAudioDeviceBackend(devices=[], available=True)

        self._cached_devices: List[AudioDeviceInfo] = []
        self._last_refresh_time: float = 0.0

        # Perform initial non-crashing discovery
        self.refresh_devices()

    @property
    def backend_name(self) -> str:
        """Name of active device backend."""
        return self._backend.name

    @property
    def is_backend_available(self) -> bool:
        """Check if active backend is operational."""
        return self._backend.is_available

    def set_backend(self, backend: AudioDeviceBackend) -> None:
        """Swap device backend implementation."""
        with self._lock:
            self._backend = backend
            self.refresh_devices()

    def refresh_devices(self) -> List[AudioDeviceInfo]:
        """Query devices from backend, refresh cache, update StateStore, and emit events."""
        with self._lock:
            try:
                devices = self._backend.query_devices()
            except Exception as exc:
                logger.error("Failed to refresh audio devices: %s", exc)
                devices = []

            self._cached_devices = devices
            self._sync_state_store()
            self._publish_discovery_events(devices)
            return list(self._cached_devices)

    def get_all_devices(self) -> List[AudioDeviceInfo]:
        """Return list of all cached audio devices (input and output)."""
        with self._lock:
            return list(self._cached_devices)

    def get_input_devices(self) -> List[AudioDeviceInfo]:
        """Return list of all devices that support audio input (microphones)."""
        with self._lock:
            return [d for d in self._cached_devices if d.is_input_device and d.is_available]

    def has_input_device(self) -> bool:
        """Check if at least one valid audio input device is available."""
        return len(self.get_input_devices()) > 0

    def get_default_input_device(self) -> Optional[AudioDeviceInfo]:
        """Retrieve system default input device, or fallback to first input device."""
        with self._lock:
            input_devices = self.get_input_devices()
            if not input_devices:
                return None

            default_idx = self._backend.get_default_input_index()
            if default_idx is not None:
                for dev in input_devices:
                    if dev.device_id == default_idx:
                        return dev

            # Check if any device has is_default marked
            for dev in input_devices:
                if dev.is_default:
                    return dev

            # Fallback to first available input device
            return input_devices[0]

    def get_device_by_id(self, device_id: int) -> Optional[AudioDeviceInfo]:
        """Retrieve device by ID if present in cached devices."""
        with self._lock:
            for dev in self._cached_devices:
                if dev.device_id == device_id:
                    return dev
            return None

    def validate_input_device(self, device_id: Optional[int] = None) -> AudioDeviceInfo:
        """Validate and return an active AudioDeviceInfo suitable for recording.

        Args:
            device_id: Optional specific device ID. If None, resolves default device.

        Returns:
            AudioDeviceInfo: Validated input device.

        Raises:
            AudioDeviceNotFoundError: If device is not found or no microphone is connected.
            AudioDeviceError: If device exists but has 0 input channels or is unavailable.
        """
        with self._lock:
            if device_id is None:
                dev = self.get_default_input_device()
                if dev is None:
                    raise AudioDeviceNotFoundError(
                        "No audio input device (microphone) was found on the system."
                    )
                return dev

            dev = self.get_device_by_id(device_id)
            if dev is None:
                raise AudioDeviceNotFoundError(
                    f"Audio device ID {device_id} was not found among available devices."
                )

            if not dev.is_input_device:
                raise AudioDeviceError(
                    f"Audio device ID {device_id} ('{dev.name}') does not support audio input "
                    f"(max_input_channels={dev.max_input_channels})."
                )

            if not dev.is_available:
                raise AudioDeviceError(
                    f"Audio device ID {device_id} ('{dev.name}') is currently unavailable."
                )

            return dev

    def get_output_devices(self) -> List[AudioDeviceInfo]:
        """Return list of all devices that support audio output (speakers/headphones)."""
        with self._lock:
            return [d for d in self._cached_devices if d.is_output_device and d.is_available]

    def has_output_device(self) -> bool:
        """Check if at least one valid audio output device is available."""
        return len(self.get_output_devices()) > 0

    def get_default_output_device(self) -> Optional[AudioDeviceInfo]:
        """Retrieve system default output device, or fallback to first output device."""
        with self._lock:
            output_devices = self.get_output_devices()
            if not output_devices:
                return None

            default_idx = self._backend.get_default_output_index()
            if default_idx is not None:
                for dev in output_devices:
                    if dev.device_id == default_idx:
                        return dev

            for dev in output_devices:
                if dev.is_default:
                    return dev

            return output_devices[0]

    def validate_output_device(self, device_id: Optional[int] = None) -> AudioDeviceInfo:
        """Validate and return an active AudioDeviceInfo suitable for audio playback.

        Args:
            device_id: Optional specific device ID. If None, resolves default output device.

        Returns:
            AudioDeviceInfo: Validated output device.

        Raises:
            AudioDeviceNotFoundError: If device is not found or no speaker/output device is connected.
            AudioDeviceError: If device exists but has 0 output channels or is unavailable.
        """
        with self._lock:
            if device_id is None:
                dev = self.get_default_output_device()
                if dev is None:
                    raise AudioDeviceNotFoundError(
                        "No audio output device (speaker/headphones) was found on the system."
                    )
                return dev

            dev = self.get_device_by_id(device_id)
            if dev is None:
                raise AudioDeviceNotFoundError(
                    f"Audio device ID {device_id} was not found among available devices."
                )

            if not dev.is_output_device:
                raise AudioDeviceError(
                    f"Audio device ID {device_id} ('{dev.name}') does not support audio output "
                    f"(max_output_channels={dev.max_output_channels})."
                )

            if not dev.is_available:
                raise AudioDeviceError(
                    f"Audio device ID {device_id} ('{dev.name}') is currently unavailable."
                )

            return dev

    # --------------------------------------------------------------------------
    # Telemetry and StateStore Integration
    # --------------------------------------------------------------------------

    def _sync_state_store(self) -> None:
        """Persist bounded summary of audio devices to StateStore."""
        if not self.state_store:
            return

        with self._lock:
            input_devices = [d for d in self._cached_devices if d.is_input_device]
            default_dev = self.get_default_input_device()

            summary = {
                "backend": self.backend_name,
                "backend_available": self.is_backend_available,
                "total_devices": len(self._cached_devices),
                "input_devices_count": len(input_devices),
                "has_microphone": len(input_devices) > 0,
                "default_input_device": default_dev.name if default_dev else None,
                "default_input_id": default_dev.device_id if default_dev else None,
            }

            try:
                self.state_store.set("audio.device_summary", sanitize_container(summary))
            except Exception as exc:
                logger.debug("StateStore update failed in AudioDeviceManager: %s", exc)

    def _publish_discovery_events(self, devices: List[AudioDeviceInfo]) -> None:
        """Publish AUDIO_DEVICE_DISCOVERED events for discovered input devices."""
        if not self.event_bus:
            return

        for dev in devices:
            if dev.is_input_device:
                try:
                    event = Event.create(
                        event_type=EventType.AUDIO_DEVICE_DISCOVERED,
                        source="audio.devices",
                        payload={
                            "device_id": dev.device_id,
                            "name": dev.name,
                            "max_input_channels": dev.max_input_channels,
                            "default_sample_rate": dev.default_sample_rate,
                            "is_default": dev.is_default,
                            "hostapi": dev.hostapi,
                        },
                    )
                    self.event_bus.publish(event)
                except Exception as exc:
                    logger.debug("Failed to publish AUDIO_DEVICE_DISCOVERED: %s", exc)
