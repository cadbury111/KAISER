"""KAISER — Step 51: Production Dependency & Environment Foundation Tests.

Deterministic verification suite covering:
1. Manifest files existence, structure, and consistency:
   - requirements.txt (direct production runtime dependencies)
   - requirements-dev.txt (development and testing dependencies)
   - requirements-stt.txt (optional offline speech-to-text dependencies)
   - pyproject.toml (PEP 517/518/621 packaging specification)
2. Direct runtime dependency importability and version constraints in current environment.
3. Offline STT dependency status and model weight error boundaries.
4. Mandatory architectural safety invariants (execution lock, mock defaults).
"""

from __future__ import annotations

import os
import platform
import sys
from pathlib import Path
from typing import Dict, List, Set

import pytest

from app.core.config import Settings
from app.voice.stt_models import (
    LocalSTTConfig,
    STTDependencyError,
    STTModelNotFoundError,
)
from app.voice.providers.local_whisper import LocalWhisperSTTProvider

WORKSPACE_ROOT = Path(__file__).resolve().parent.parent


# ==============================================================================
# 1. MANIFEST SPECIFICATION TESTS
# ==============================================================================

class TestDependencyManifests:
    """Verify that all dependency manifests exist, are parseable, and have consistent declarations."""

    def test_requirements_txt_exists_and_contains_runtime_deps(self) -> None:
        """Verify requirements.txt exists and specifies verified core runtime packages."""
        req_file = WORKSPACE_ROOT / "requirements.txt"
        assert req_file.is_file(), "requirements.txt must exist at workspace root"
        content = req_file.read_text(encoding="utf-8")
        lines = [line.strip() for line in content.splitlines() if line.strip() and not line.startswith("#")]

        # Expected direct runtime dependencies
        expected_packages = ["numpy", "scipy", "sounddevice", "pyttsx3"]
        for pkg in expected_packages:
            assert any(line.startswith(pkg) for line in lines), f"{pkg} must be declared in requirements.txt"

        # Windows-specific conditional dependency
        assert any("pywin32" in line for line in lines), "pywin32 must be declared for Windows platform"

    def test_requirements_dev_txt_exists_and_references_base(self) -> None:
        """Verify requirements-dev.txt references requirements.txt and includes pytest."""
        dev_file = WORKSPACE_ROOT / "requirements-dev.txt"
        assert dev_file.is_file(), "requirements-dev.txt must exist at workspace root"
        content = dev_file.read_text(encoding="utf-8")
        lines = [line.strip() for line in content.splitlines() if line.strip() and not line.startswith("#")]

        assert "-r requirements.txt" in lines, "requirements-dev.txt must reference -r requirements.txt"
        assert any(line.startswith("pytest") for line in lines), "pytest must be declared in requirements-dev.txt"

    def test_requirements_stt_txt_exists_and_specifies_whisper(self) -> None:
        """Verify requirements-stt.txt specifies faster-whisper without conflicting declarations."""
        stt_file = WORKSPACE_ROOT / "requirements-stt.txt"
        assert stt_file.is_file(), "requirements-stt.txt must exist at workspace root"
        content = stt_file.read_text(encoding="utf-8")
        lines = [line.strip() for line in content.splitlines() if line.strip() and not line.startswith("#")]

        assert any(line.startswith("faster-whisper") for line in lines), "faster-whisper must be declared in requirements-stt.txt"

    def test_pyproject_toml_exists_and_matches_spec(self) -> None:
        """Verify pyproject.toml defines valid project metadata and optional-dependencies."""
        pyproject_file = WORKSPACE_ROOT / "pyproject.toml"
        assert pyproject_file.is_file(), "pyproject.toml must exist at workspace root"
        content = pyproject_file.read_text(encoding="utf-8")

        assert '[project]' in content
        assert 'name = "kaiser"' in content
        assert 'requires-python = ">=3.11"' in content
        assert '[project.optional-dependencies]' in content
        assert 'stt = [' in content
        assert 'test = [' in content

    def test_gitignore_protects_local_model_weights(self) -> None:
        """Verify .gitignore excludes model weight binaries while preserving .gitkeep."""
        gitignore_file = WORKSPACE_ROOT / ".gitignore"
        assert gitignore_file.is_file()
        content = gitignore_file.read_text(encoding="utf-8")

        assert "data/models/" in content or "data/models/**" in content, ".gitignore must ignore data/models/"
        assert "!data/models/**/.gitkeep" in content or "!data/models/.gitkeep" in content, ".gitignore must keep .gitkeep"



# ==============================================================================
# 2. RUNTIME DEPENDENCY IMPORT & VERSION BOUNDARY TESTS
# ==============================================================================

class TestRuntimeDependencyImports:
    """Verify that all direct runtime packages import cleanly and meet compatibility thresholds."""

    def test_numpy_available_and_compatible(self) -> None:
        """Verify numpy is available and satisfies version >=1.24.0."""
        import numpy as np  # type: ignore
        version_parts = [int(p) for p in np.__version__.split(".")[:2] if p.isdigit()]
        assert len(version_parts) >= 2
        major, minor = version_parts[0], version_parts[1]
        assert (major == 1 and minor >= 24) or major >= 2, f"numpy version {np.__version__} must be >=1.24.0"

    def test_scipy_available_and_compatible(self) -> None:
        """Verify scipy is available and satisfies version >=1.11.0."""
        import scipy  # type: ignore
        version_parts = [int(p) for p in scipy.__version__.split(".")[:2] if p.isdigit()]
        assert len(version_parts) >= 2
        major, minor = version_parts[0], version_parts[1]
        assert (major == 1 and minor >= 11) or major >= 2, f"scipy version {scipy.__version__} must be >=1.11.0"
        # Verify signal processing submodule is functional
        import scipy.signal  # type: ignore
        assert hasattr(scipy.signal, "resample_poly")

    def test_sounddevice_available(self) -> None:
        """Verify sounddevice imports cleanly."""
        import sounddevice as sd  # type: ignore
        assert hasattr(sd, "query_devices")

    def test_pyttsx3_available(self) -> None:
        """Verify pyttsx3 imports cleanly."""
        import pyttsx3  # type: ignore
        assert hasattr(pyttsx3, "init")

    @pytest.mark.skipif(platform.system() != "Windows", reason="Windows-only COM test")
    def test_pywin32_available_on_windows(self) -> None:
        """Verify pythoncom from pywin32 is importable on Windows platform."""
        import pythoncom  # type: ignore
        assert hasattr(pythoncom, "CoInitialize")


# ==============================================================================
# 3. OFFLINE STT READINESS & ERROR BOUNDARY TESTS
# ==============================================================================

class TestOfflineSTTReadiness:
    """Verify LocalWhisperSTTProvider error handling and offline safety bounds."""

    def test_provider_defaults_to_unavailable_without_model(self) -> None:
        """Verify LocalWhisperSTTProvider reports is_available=False when model path is unset."""
        config = LocalSTTConfig(model_path=None, model_id="whisper-base")
        provider = LocalWhisperSTTProvider(config=config)
        assert provider.name == "local_whisper"
        assert provider.is_available is False

    def test_verify_environment_raises_model_not_found(self) -> None:
        """Verify _verify_environment raises STTModelNotFoundError when model_path is None or missing."""
        config = LocalSTTConfig(model_path=None, model_id="whisper-base")
        provider = LocalWhisperSTTProvider(config=config)

        # If faster_whisper is installed, it checks model_path; if not, STTDependencyError is raised
        with pytest.raises((STTModelNotFoundError, STTDependencyError)):
            provider._verify_environment()

    def test_nonexistent_model_path_rejected(self, tmp_path: Path) -> None:
        """Verify that pointing to a nonexistent model path explicitly fails without downloading."""
        fake_path = str(tmp_path / "nonexistent_whisper_weights")
        config = LocalSTTConfig(model_path=fake_path, model_id="whisper-base")
        provider = LocalWhisperSTTProvider(config=config)

        with pytest.raises((STTModelNotFoundError, STTDependencyError)):
            provider._verify_environment()


# ==============================================================================
# 4. SAFETY INVARIANT TESTS
# ==============================================================================

class TestSafetyInvariants:
    """Verify that dependency definitions preserve all foundational safety rules."""

    def test_execution_locked_by_default(self) -> None:
        """Verify execution lock remains enabled in default settings."""
        settings = Settings()
        assert settings.execution_locked is True
        assert settings.broker_provider == "none"
        assert settings.paper_trading_only is True

    def test_mock_defaults_preserved(self) -> None:
        """Verify mock providers remain default across all subsystems."""
        settings = Settings()
        assert settings.market_data_provider == "mock"
        assert settings.stt_provider == "mock"
        assert settings.tts_provider == "mock"
        assert settings.llm_provider == "mock"
