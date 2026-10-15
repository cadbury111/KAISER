"""KAISER — Deterministic Multilingual Voice Command Intent Resolution & Safety Boundary.

Provides pure mathematical, deterministic natural language intent resolution
and parameter extraction for English, Tamil, and Tanglish spoken queries.

SAFETY INVARIANTS:
1. Non-Prescriptive & Non-Executable: Spoken trading directives (e.g. buy, sell,
   order placement, leverage, position sizing) are intercepted and rejected
   immediately with structured failure at the voice boundary.
2. Zero External NLP/Cloud Dependencies: Uses pure Python regex and deterministic
   pattern matching for 100% offline reproducibility without cloud speech/NLP APIs.
3. Complete Backward Compatibility: Direct command names (e.g. 'system_status')
   and legacy snake_case strings continue to resolve with 1.0 confidence.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import re
import threading
from typing import Any, Dict, List, Mapping, Optional, Pattern, Sequence, Tuple, Union

from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger

logger = get_logger("voice.intent")


class VoiceIntentType(str, Enum):
    """Categorical intent definitions for voice command routing."""

    SYSTEM_STATUS = "SYSTEM_STATUS"
    PORTFOLIO_RISK_STATUS = "PORTFOLIO_RISK_STATUS"
    MARKET_DATA_QUALITY = "MARKET_DATA_QUALITY"
    TECHNICAL_ANALYSIS = "TECHNICAL_ANALYSIS"
    OPPORTUNITY_PRIORITIES = "OPPORTUNITY_PRIORITIES"
    INVESTIGATION_QUEUE = "INVESTIGATION_QUEUE"
    LATEST_PORTFOLIO_REPORT = "LATEST_PORTFOLIO_REPORT"
    HISTORICAL_REVIEW_SUMMARY = "HISTORICAL_REVIEW_SUMMARY"
    PROHIBITED_TRADING_COMMAND = "PROHIBITED_TRADING_COMMAND"
    DIRECT_COMMAND = "DIRECT_COMMAND"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class VoiceCommandIntent:
    """Immutable, strongly typed resolution of a spoken voice utterance into a command intent."""

    intent_type: VoiceIntentType
    command: str
    confidence: float
    parameters: Dict[str, Any] = field(default_factory=dict)
    language: str = "en"
    raw_text: str = ""
    normalized_text: str = ""
    is_prohibited_trading: bool = False
    rejection_reason: Optional[str] = None
    matched_pattern: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize intent to a clean dictionary."""
        return {
            "intent_type": self.intent_type.value,
            "command": self.command,
            "confidence": round(self.confidence, 4),
            "parameters": dict(self.parameters),
            "language": self.language,
            "raw_text": self.raw_text,
            "normalized_text": self.normalized_text,
            "is_prohibited_trading": self.is_prohibited_trading,
            "rejection_reason": self.rejection_reason,
            "matched_pattern": self.matched_pattern,
        }

    @classmethod
    def prohibited(
        cls,
        raw_text: str,
        normalized_text: str,
        language: str = "en",
        reason: str = "Unknown command: Voice trade execution and trading recommendations are strictly prohibited by KAISER safety invariants.",
        matched_pattern: Optional[str] = None,
    ) -> VoiceCommandIntent:
        """Create a prohibited trading command intent."""
        return cls(
            intent_type=VoiceIntentType.PROHIBITED_TRADING_COMMAND,
            command="prohibited_trading_command",
            confidence=1.0,
            parameters={},
            language=language,
            raw_text=raw_text,
            normalized_text=normalized_text,
            is_prohibited_trading=True,
            rejection_reason=reason,
            matched_pattern=matched_pattern,
        )

    @classmethod
    def unknown(
        cls,
        raw_text: str,
        normalized_text: str,
        language: str = "en",
        fallback_command: Optional[str] = None,
    ) -> VoiceCommandIntent:
        """Create an unknown intent."""
        return cls(
            intent_type=VoiceIntentType.UNKNOWN,
            command=fallback_command or normalized_text.replace(" ", "_"),
            confidence=0.0,
            parameters={},
            language=language,
            raw_text=raw_text,
            normalized_text=normalized_text,
            is_prohibited_trading=False,
            rejection_reason="No registered command matched spoken utterance.",
        )


class VoiceCommandIntentResolver:
    """Thread-safe, deterministic multilingual intent resolver for English, Tamil, and Tanglish."""

    def __init__(self, orchestrator: Optional[Any] = None) -> None:
        """Initialize intent resolver with optional orchestrator reference for command validation."""
        self.orchestrator = orchestrator
        self._lock = threading.RLock()

        # Prohibited trading patterns: detect attempts to place trades, buy/sell, position sizing, leverage
        self._prohibited_patterns: List[Tuple[Pattern[str], str]] = [
            (
                re.compile(
                    r"\b(buy|purchase|go\s+long)\s+([0-9]+\s+)?([a-zA-Z0-9_\.\-]+)\b",
                    re.IGNORECASE,
                ),
                "buy_directive",
            ),
            (
                re.compile(
                    r"\b(sell|short|go\s+short|liquidate)\s+([0-9]+\s+)?([a-zA-Z0-9_\.\-]+)\b",
                    re.IGNORECASE,
                ),
                "sell_directive",
            ),
            (
                re.compile(
                    r"\b(place\s+order|execute\s+trade|send\s+order|market\s+order|limit\s+order)\b",
                    re.IGNORECASE,
                ),
                "order_placement",
            ),
            (
                re.compile(
                    r"\b(set\s+leverage|apply\s+leverage|position\s+size|set\s+stop\s*loss|set\s+take\s*profit)\b",
                    re.IGNORECASE,
                ),
                "position_management",
            ),
            (
                re.compile(
                    r"\b(vaangu|vithuru|order\s+podu|trade\s+execute\s+pannu)\b",
                    re.IGNORECASE,
                ),
                "tanglish_trade_directive",
            ),
        ]

        # Canonical English intent patterns
        self._english_patterns: List[Tuple[VoiceIntentType, str, Pattern[str], float]] = [
            (
                VoiceIntentType.SYSTEM_STATUS,
                "system_status",
                re.compile(
                    r"\b(system\s+status|how\s+is\s+the\s+system|system\s+health|health\s+check|system\s+readiness|status\s+check|system\s+check)\b",
                    re.IGNORECASE,
                ),
                0.98,
            ),
            (
                VoiceIntentType.PORTFOLIO_RISK_STATUS,
                "get_portfolio_risk_status",
                re.compile(
                    r"\b(portfolio\s+risk(\s+status)?|risk\s+status|check\s+risk|how\s+is\s+the\s+risk|portfolio\s+risk\s+report|risk\s+monitor)\b",
                    re.IGNORECASE,
                ),
                0.95,
            ),
            (
                VoiceIntentType.MARKET_DATA_QUALITY,
                "get_market_data_quality",
                re.compile(
                    r"\b(market\s+(data\s+)?quality|data\s+quality|data\s+health|market\s+anomalies|quality\s+status)\b",
                    re.IGNORECASE,
                ),
                0.95,
            ),
            (
                VoiceIntentType.OPPORTUNITY_PRIORITIES,
                "list_opportunity_priorities",
                re.compile(
                    r"\b(opportunity\s+priorit(ies|y)|prioritized\s+opportunities|top\s+opportunities|show\s+opportunities|list\s+opportunities)\b",
                    re.IGNORECASE,
                ),
                0.95,
            ),
            (
                VoiceIntentType.INVESTIGATION_QUEUE,
                "get_investigation_queue",
                re.compile(
                    r"\b(investigation\s+queue|pending\s+investigations|show\s+investigation\s+queue|review\s+queue)\b",
                    re.IGNORECASE,
                ),
                0.95,
            ),
            (
                VoiceIntentType.LATEST_PORTFOLIO_REPORT,
                "get_latest_portfolio_report",
                re.compile(
                    r"\b(latest\s+portfolio\s+report|portfolio\s+report|portfolio\s+summary|unified\s+portfolio\s+report)\b",
                    re.IGNORECASE,
                ),
                0.95,
            ),
            (
                VoiceIntentType.HISTORICAL_REVIEW_SUMMARY,
                "get_historical_review_migration_audit_summary",
                re.compile(
                    r"\b(historical\s+review(\s+summary)?|audit\s+summary|migration\s+audit(\s+summary)?|diagnostics\s+audit|review\s+audit)\b",
                    re.IGNORECASE,
                ),
                0.95,
            ),
        ]

        # Tanglish (Tamil-English) conversational patterns
        self._tanglish_patterns: List[Tuple[VoiceIntentType, str, Pattern[str], float]] = [
            (
                VoiceIntentType.SYSTEM_STATUS,
                "system_status",
                re.compile(
                    r"\b(system\s+eppadi\s+irukku|system\s+status\s+sollunga|system\s+status\s+enna|status\s+enna|system\s+epdi\s+irukku|system\s+nallaa\s+irukkaa|system\s+working\s+aa)\b",
                    re.IGNORECASE,
                ),
                0.98,
            ),
            (
                VoiceIntentType.PORTFOLIO_RISK_STATUS,
                "get_portfolio_risk_status",
                re.compile(
                    r"\b(portfolio\s+risk\s+eppadi\s+irukku|risk\s+status\s+sollunga|risk\s+enna|risk\s+pathi\s+sollu|risk\s+epdi\s+irukku|risk\s+check\s+pannu)\b",
                    re.IGNORECASE,
                ),
                0.95,
            ),
            (
                VoiceIntentType.MARKET_DATA_QUALITY,
                "get_market_data_quality",
                re.compile(
                    r"\b(market\s+quality\s+eppadi\s+irukku|data\s+quality\s+sollunga|data\s+health\s+eppadi\s+irukku|market\s+data\s+eppadi\s+irukku|data\s+correct\s+aa\s+irukkaa)\b",
                    re.IGNORECASE,
                ),
                0.95,
            ),
            (
                VoiceIntentType.INVESTIGATION_QUEUE,
                "get_investigation_queue",
                re.compile(
                    r"\b(queue\s+eppadi\s+irukku|investigation\s+queue\s+sollunga|investigation\s+queue\s+enna|queue\s+la\s+enna\s+irukku)\b",
                    re.IGNORECASE,
                ),
                0.95,
            ),
            (
                VoiceIntentType.LATEST_PORTFOLIO_REPORT,
                "get_latest_portfolio_report",
                re.compile(
                    r"\b(portfolio\s+report\s+sollunga|portfolio\s+summary\s+enna|portfolio\s+eppadi\s+irukku)\b",
                    re.IGNORECASE,
                ),
                0.95,
            ),
            (
                VoiceIntentType.HISTORICAL_REVIEW_SUMMARY,
                "get_historical_review_migration_audit_summary",
                re.compile(
                    r"\b(audit\s+report\s+sollunga|migration\s+audit\s+eppadi\s+irukku|audit\s+summary\s+enna|audit\s+status\s+sollu)\b",
                    re.IGNORECASE,
                ),
                0.95,
            ),
        ]

        # Parameterized technical analysis patterns (English & Tanglish)
        self._param_technical_patterns: List[Pattern[str]] = [
            re.compile(
                r"\b(?P<symbol>[a-zA-Z0-9_\.\-]+)\s+(?:technical\s+analysis(?:\s+pannu)?|chart\s+eppadi\s+irukku|indicators\s+sollu|analysis\s+pannunga)\b",
                re.IGNORECASE,
            ),
            re.compile(
                r"\b(?:technical\s+analysis(?:\s+for)?|analyze|technical\s+indicators?(?:\s+for)?|chart\s+analysis(?:\s+for)?)\s+(?P<symbol>[a-zA-Z0-9_\.\-]+)\b",
                re.IGNORECASE,
            ),
        ]
        self._reserved_words = frozenset({
            "PANNU", "SOLLU", "EPPADI", "IRUKKU", "ENNA", "STATUS", "FOR", "NOW", "PLEASE",
            "THE", "AND", "REPORT", "SUMMARY", "QUEUE", "CHECK", "ANALYSIS",
        })

    def normalize_utterance(self, text: str) -> str:
        """Clean and normalize spoken text for deterministic matching."""
        if not text:
            return ""
        # Strip trailing punctuation (. , ? ! ;)
        cleaned = re.sub(r"[,\.?!;:]+", " ", text)
        # Collapse multiple spaces into single space and lowercase
        return " ".join(cleaned.strip().lower().split())

    def resolve(
        self,
        text: str,
        language: str = "en",
    ) -> VoiceCommandIntent:
        """Resolve a spoken voice utterance into a strongly typed VoiceCommandIntent.

        Args:
            text: Raw speech transcription string.
            language: Language hint ('en', 'ta', 'tanglish', 'mixed').

        Returns:
            VoiceCommandIntent: Deterministic resolution containing intent type,
            command, confidence, and extracted parameters.
        """
        raw_text = (text or "").strip()
        normalized = self.normalize_utterance(raw_text)

        if not normalized:
            return VoiceCommandIntent.unknown(
                raw_text=raw_text,
                normalized_text="",
                language=language,
            )

        with self._lock:
            # 1. Prohibited Trading Directives Check (Strict Safety Invariant)
            for pattern, pattern_name in self._prohibited_patterns:
                if pattern.search(normalized):
                    logger.warning(
                        "Voice trade execution attempt detected and blocked: '%s' (matched=%s)",
                        raw_text,
                        pattern_name,
                    )
                    return VoiceCommandIntent.prohibited(
                        raw_text=raw_text,
                        normalized_text=normalized,
                        language=language,
                        matched_pattern=pattern_name,
                    )

            # 2. Parameterized Technical Analysis Intent (English & Tanglish)
            for pattern in self._param_technical_patterns:
                match = pattern.search(normalized)
                if match:
                    symbol_raw = match.group("symbol").strip().upper()
                    # Validate symbol format (alphanumeric, 1-12 chars) and not a reserved word
                    if (
                        re.match(r"^[A-Z0-9_\.\-]{1,12}$", symbol_raw)
                        and symbol_raw not in self._reserved_words
                    ):
                        return VoiceCommandIntent(
                            intent_type=VoiceIntentType.TECHNICAL_ANALYSIS,
                            command="technical_analysis",
                            confidence=0.96,
                            parameters={"symbol": symbol_raw},
                            language=language,
                            raw_text=raw_text,
                            normalized_text=normalized,
                            matched_pattern="technical_analysis_param",
                        )

            # 3. English Conversational Patterns
            for intent_type, cmd, pattern, conf in self._english_patterns:
                if pattern.search(normalized):
                    return VoiceCommandIntent(
                        intent_type=intent_type,
                        command=cmd,
                        confidence=conf,
                        parameters={},
                        language=language,
                        raw_text=raw_text,
                        normalized_text=normalized,
                        matched_pattern=f"en_{intent_type.value.lower()}",
                    )

            # 4. Tanglish Conversational Patterns
            for intent_type, cmd, pattern, conf in self._tanglish_patterns:
                if pattern.search(normalized):
                    return VoiceCommandIntent(
                        intent_type=intent_type,
                        command=cmd,
                        confidence=conf,
                        parameters={},
                        language="tanglish" if language == "en" else language,
                        raw_text=raw_text,
                        normalized_text=normalized,
                        matched_pattern=f"tanglish_{intent_type.value.lower()}",
                    )

            # 5. Direct Command Match (Exact orchestrator command name in registry)
            if self.orchestrator and hasattr(self.orchestrator, "registry"):
                if self.orchestrator.registry.has_command(raw_text):
                    return VoiceCommandIntent(
                        intent_type=VoiceIntentType.DIRECT_COMMAND,
                        command=raw_text,
                        confidence=1.0,
                        parameters={},
                        language=language,
                        raw_text=raw_text,
                        normalized_text=normalized,
                        matched_pattern="exact_command",
                    )
                # Snake case conversion of normalized string
                snake_candidate = normalized.replace(" ", "_")
                if self.orchestrator.registry.has_command(snake_candidate):
                    return VoiceCommandIntent(
                        intent_type=VoiceIntentType.DIRECT_COMMAND,
                        command=snake_candidate,
                        confidence=0.98,
                        parameters={},
                        language=language,
                        raw_text=raw_text,
                        normalized_text=normalized,
                        matched_pattern="snake_case_command",
                    )

            # 6. Fallback: Unknown Command (preserves legacy snake_case candidate)
            return VoiceCommandIntent.unknown(
                raw_text=raw_text,
                normalized_text=normalized,
                language=language,
                fallback_command=normalized.replace(" ", "_"),
            )
