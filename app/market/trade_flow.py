"""KAISER — Trade Flow Imbalance (TFI), Aggressor Volume Analytics & Microstructure Engine.

Provides deterministic, thread-safe, read-only trade flow analysis:
1. Trade Flow Imbalance (TFI): Evaluates aggressor-initiated trade volume asymmetry
   bounded strictly in [-1.0, 1.0] with zero-volume protection and UNKNOWN aggressor preservation.
2. Rolling Window Analytics: Rolling buy/sell/unknown volume, rolling TFI, and
   Exponential Weighted Moving Average (EWMA) of TFI.
3. Resilient Flow Ingestion: Deduplication of trade IDs, sequence gap detection, and
   out-of-order trade accounting without mutating historical state.
4. Combined Diagnostic Microstructure Features: Synthesizes Order Book Imbalance (OBI)
   with Trade Flow Imbalance (TFI) to identify liquidity absorption vs. breakout momentum.
5. Multi-Symbol Isolation: Thread-safe state tracking per instrument.

STRICT INVARIANTS:
- Analytical only: Zero trade recommendations, order placement, or execution fields.
- Non-prescriptive: Never infers aggressor side when unknown.
- Bounded math: TFI strictly in [-1.0, 1.0], never produces NaN, Inf, or ZeroDivisionError.
"""

from __future__ import annotations

import collections
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import math
import threading
import time
from typing import Any, Deque, Dict, List, Optional, Sequence, Tuple, Union

from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.market.models import AggressorSide, TradeTick

logger = get_logger("market.trade_flow")


class TradeFlowInterpretation(str, Enum):
    """Categorical classification of trade flow pressure."""

    BUY_PRESSURE = "BUY_PRESSURE"
    SELL_PRESSURE = "SELL_PRESSURE"
    NEUTRAL = "NEUTRAL"


class CombinedPressureDiagnostic(str, Enum):
    """Diagnostic synthesis of resting depth pressure and executed trade flow pressure."""

    AGREEMENT_BUY = "AGREEMENT_BUY"          # Both OBI > 0 and TFI > 0
    AGREEMENT_SELL = "AGREEMENT_SELL"        # Both OBI < 0 and TFI < 0
    DIVERGENCE_ABSORPTION = "DIVERGENCE"     # OBI and TFI have opposing signs
    NEUTRAL = "NEUTRAL"                      # Either OBI or TFI is neutral


@dataclass(frozen=True)
class TradeFlowAnalytics:
    """Immutable point-in-time trade flow analytics snapshot for a symbol.

    Attributes:
        symbol: Ticker symbol.
        exchange: Exchange identifier.
        timestamp: POSIX UTC calculation timestamp.
        trade_flow_imbalance: Cumulative TFI in [-1.0, 1.0].
        tfi_ewma: Exponential Weighted Moving Average of TFI in [-1.0, 1.0].
        rolling_tfi: Rolling window TFI in [-1.0, 1.0].
        total_traded_volume: Total volume across all processed trades.
        buy_aggressor_volume: Total volume initiated by buyers.
        sell_aggressor_volume: Total volume initiated by sellers.
        unknown_volume: Total volume with unknown aggressor side.
        trade_count: Total valid trades processed.
        rolling_buy_volume: Rolling window buy aggressor volume.
        rolling_sell_volume: Rolling window sell aggressor volume.
        rolling_total_volume: Rolling window total volume.
        rolling_trade_count: Rolling window trade count.
        interpretation: Categorical classification of cumulative TFI.
        rolling_interpretation: Categorical classification of rolling TFI.
        duplicates_rejected: Count of duplicate trades rejected.
        sequence_gaps_detected: Count of sequence gaps detected.
        out_of_order_count: Count of out-of-order trades observed.
        last_trade_price: Most recent execution price.
        vwap: Volume-Weighted Average Price across processed trades.
        combined_diagnostic: Diagnostic synthesis with depth imbalance if available.
        is_valid: False if calculation failed or unhandled corruption occurred.
        error: Diagnostic error message if any.
    """

    symbol: str
    exchange: str
    timestamp: float
    trade_flow_imbalance: float
    tfi_ewma: float
    rolling_tfi: float
    total_traded_volume: float
    buy_aggressor_volume: float
    sell_aggressor_volume: float
    unknown_volume: float
    trade_count: int
    rolling_buy_volume: float
    rolling_sell_volume: float
    rolling_total_volume: float
    rolling_trade_count: int
    interpretation: TradeFlowInterpretation
    rolling_interpretation: TradeFlowInterpretation
    duplicates_rejected: int = 0
    sequence_gaps_detected: int = 0
    out_of_order_count: int = 0
    last_trade_price: Optional[float] = None
    vwap: Optional[float] = None
    combined_diagnostic: Optional[CombinedPressureDiagnostic] = None
    is_valid: bool = True
    error: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate bounds and invariants fail-closed."""
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("TradeFlowAnalytics symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        if not math.isfinite(self.trade_flow_imbalance) or self.trade_flow_imbalance < -1.0 or self.trade_flow_imbalance > 1.0:
            raise ValidationError(
                f"trade_flow_imbalance must be in [-1.0, 1.0], got {self.trade_flow_imbalance}"
            )
        if not math.isfinite(self.tfi_ewma) or self.tfi_ewma < -1.0 or self.tfi_ewma > 1.0:
            raise ValidationError(f"tfi_ewma must be in [-1.0, 1.0], got {self.tfi_ewma}")
        if not math.isfinite(self.rolling_tfi) or self.rolling_tfi < -1.0 or self.rolling_tfi > 1.0:
            raise ValidationError(f"rolling_tfi must be in [-1.0, 1.0], got {self.rolling_tfi}")

    @property
    def is_buy_pressure(self) -> bool:
        """True if cumulative trade flow indicates net buyer aggression."""
        return self.interpretation == TradeFlowInterpretation.BUY_PRESSURE

    @property
    def is_sell_pressure(self) -> bool:
        """True if cumulative trade flow indicates net seller aggression."""
        return self.interpretation == TradeFlowInterpretation.SELL_PRESSURE

    @property
    def is_neutral(self) -> bool:
        """True if cumulative trade flow is balanced or unclassified."""
        return self.interpretation == TradeFlowInterpretation.NEUTRAL

    @property
    def iso_timestamp(self) -> str:
        """Return ISO-8601 UTC timestamp string."""
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert into a JSON-serializable dictionary with safe keys (no prohibited recommendation words)."""
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
            "trade_flow_imbalance": self.trade_flow_imbalance,
            "tfi_ewma": self.tfi_ewma,
            "rolling_tfi": self.rolling_tfi,
            "total_traded_volume": self.total_traded_volume,
            "buy_aggressor_volume": self.buy_aggressor_volume,
            "sell_aggressor_volume": self.sell_aggressor_volume,
            "unknown_volume": self.unknown_volume,
            "trade_count": self.trade_count,
            "rolling_buy_volume": self.rolling_buy_volume,
            "rolling_sell_volume": self.rolling_sell_volume,
            "rolling_total_volume": self.rolling_total_volume,
            "rolling_trade_count": self.rolling_trade_count,
            "interpretation": self.interpretation.value,
            "rolling_interpretation": self.rolling_interpretation.value,
            "duplicates_rejected": self.duplicates_rejected,
            "sequence_gaps_detected": self.sequence_gaps_detected,
            "out_of_order_count": self.out_of_order_count,
            "last_trade_price": self.last_trade_price,
            "vwap": self.vwap,
            "combined_diagnostic": self.combined_diagnostic.value if self.combined_diagnostic else None,
            "is_valid": self.is_valid,
            "error": self.error,
        }


# =============================================================================
# Functional Mathematical Calculations (Pure & Deterministic)
# =============================================================================

def calculate_trade_flow_imbalance(
    buy_volume: float,
    sell_volume: float,
    threshold: float = 0.05,
) -> Tuple[float, TradeFlowInterpretation]:
    """Calculate Trade Flow Imbalance (TFI) strictly bounded in [-1.0, 1.0].

    Formula:
        TFI = (buy_volume - sell_volume) / (buy_volume + sell_volume)

    Guarantees:
    - Zero classified volume returns 0.0 and NEUTRAL (no ZeroDivisionError).
    - Unclassified / UNKNOWN volume is excluded from classified denominator.
    - Strictly bounded in [-1.0, 1.0].

    Args:
        buy_volume: Cumulative or window buyer-initiated volume.
        sell_volume: Cumulative or window seller-initiated volume.
        threshold: Sensitivity threshold for categorical classification (default 0.05).

    Returns:
        Tuple[float, TradeFlowInterpretation]: Rounded TFI and pressure category.
    """
    if not math.isfinite(buy_volume) or buy_volume < 0:
        buy_volume = 0.0
    if not math.isfinite(sell_volume) or sell_volume < 0:
        sell_volume = 0.0

    classified_vol = buy_volume + sell_volume
    if classified_vol <= 0.0:
        return 0.0, TradeFlowInterpretation.NEUTRAL

    raw_tfi = (buy_volume - sell_volume) / classified_vol
    bounded_tfi = round(max(-1.0, min(1.0, raw_tfi)), 6)

    if bounded_tfi > threshold:
        interp = TradeFlowInterpretation.BUY_PRESSURE
    elif bounded_tfi < -threshold:
        interp = TradeFlowInterpretation.SELL_PRESSURE
    else:
        interp = TradeFlowInterpretation.NEUTRAL

    return bounded_tfi, interp


def calculate_ewma(
    current_value: float,
    prior_ewma: Optional[float],
    alpha: float = 0.2,
) -> float:
    """Calculate Exponential Weighted Moving Average (EWMA) of a scalar.

    Formula:
        EWMA_t = alpha * X_t + (1 - alpha) * EWMA_{t-1}

    Args:
        current_value: New point value.
        prior_ewma: Prior EWMA state (seeded with current_value if None).
        alpha: Smoothing factor in (0.0, 1.0].

    Returns:
        float: Updated EWMA value rounded to 6 decimals.
    """
    if not math.isfinite(current_value):
        return prior_ewma if (prior_ewma is not None and math.isfinite(prior_ewma)) else 0.0
    if prior_ewma is None or not math.isfinite(prior_ewma):
        return round(current_value, 6)

    safe_alpha = max(0.001, min(1.0, alpha))
    updated = safe_alpha * current_value + (1.0 - safe_alpha) * prior_ewma
    return round(updated, 6)


def evaluate_combined_microstructure_pressure(
    obi_value: Optional[float],
    tfi_value: Optional[float],
    threshold: float = 0.05,
) -> CombinedPressureDiagnostic:
    """Synthesize Order Book Imbalance (resting depth) and Trade Flow Imbalance (active execution).

    Classification:
    - Both > threshold: AGREEMENT_BUY (strong buying conviction).
    - Both < -threshold: AGREEMENT_SELL (strong selling conviction).
    - Opposing signs (one > threshold and one < -threshold): DIVERGENCE (liquidity absorption).
    - Either within threshold: NEUTRAL.

    Args:
        obi_value: Order Book Imbalance in [-1.0, 1.0] or None.
        tfi_value: Trade Flow Imbalance in [-1.0, 1.0] or None.
        threshold: Sensitivity threshold (default 0.05).

    Returns:
        CombinedPressureDiagnostic: Categorical diagnostic state.
    """
    if obi_value is None or tfi_value is None or not math.isfinite(obi_value) or not math.isfinite(tfi_value):
        return CombinedPressureDiagnostic.NEUTRAL

    obi_buy = obi_value > threshold
    obi_sell = obi_value < -threshold
    tfi_buy = tfi_value > threshold
    tfi_sell = tfi_value < -threshold

    if obi_buy and tfi_buy:
        return CombinedPressureDiagnostic.AGREEMENT_BUY
    if obi_sell and tfi_sell:
        return CombinedPressureDiagnostic.AGREEMENT_SELL
    if (obi_buy and tfi_sell) or (obi_sell and tfi_buy):
        return CombinedPressureDiagnostic.DIVERGENCE_ABSORPTION
    return CombinedPressureDiagnostic.NEUTRAL


# =============================================================================
# Stateful & Thread-Safe Trade Flow Engine
# =============================================================================

class _SymbolTradeFlowState:
    """Internal mutable tracking container for a single symbol under engine lock."""

    def __init__(self, symbol: str, rolling_window_size: int = 50, ewma_alpha: float = 0.2) -> None:
        self.symbol = symbol
        self.rolling_window_size = max(1, rolling_window_size)
        self.ewma_alpha = max(0.001, min(1.0, ewma_alpha))

        # Cumulative counters
        self.total_volume = 0.0
        self.buy_volume = 0.0
        self.sell_volume = 0.0
        self.unknown_volume = 0.0
        self.total_traded_value = 0.0
        self.trade_count = 0

        # State tracking
        self.tfi_ewma: Optional[float] = None
        self.last_trade_price: Optional[float] = None
        self.last_timestamp: float = 0.0
        self.last_sequence_number: Optional[int] = None

        # Fault tracking
        self.duplicates_rejected = 0
        self.sequence_gaps_detected = 0
        self.out_of_order_count = 0

        # Rolling ring buffer
        self.rolling_window: Deque[TradeTick] = collections.deque(maxlen=self.rolling_window_size)
        self.seen_trade_ids: collections.OrderedDict[str, None] = collections.OrderedDict()
        self._max_seen_ids = 5000

        # External depth cache for combined diagnostic
        self.latest_obi: Optional[float] = None


class TradeFlowEngine:
    """Thread-safe, deterministic, read-only Trade Flow Analytics coordinator.

    Maintains per-symbol execution volume, computes cumulative and rolling TFI,
    tracks EWMA smoothing, detects duplicates/discontinuities, and optionally
    publishes events over the KAISER EventBus.
    """

    def __init__(
        self,
        rolling_window_size: int = 50,
        ewma_alpha: float = 0.2,
        imbalance_threshold: float = 0.05,
        event_bus: Optional[Any] = None,
        name: str = "trade_flow_engine",
    ) -> None:
        self.name = name
        if rolling_window_size <= 0:
            raise ValidationError(f"rolling_window_size must be positive, got {rolling_window_size}")
        self.rolling_window_size = rolling_window_size
        self.ewma_alpha = ewma_alpha
        self.imbalance_threshold = imbalance_threshold
        self.event_bus = event_bus

        self._lock = threading.RLock()
        self._states: Dict[str, _SymbolTradeFlowState] = {}
        self._latest_analytics: Dict[str, TradeFlowAnalytics] = {}
        self._total_ticks_processed = 0

    @property
    def total_ticks_processed(self) -> int:
        """Count of valid non-duplicate ticks processed."""
        with self._lock:
            return self._total_ticks_processed

    def _get_or_create_state(self, symbol: str) -> _SymbolTradeFlowState:
        """Internal helper to retrieve or allocate symbol state under lock."""
        sym = symbol.strip().upper()
        if sym not in self._states:
            self._states[sym] = _SymbolTradeFlowState(
                symbol=sym,
                rolling_window_size=self.rolling_window_size,
                ewma_alpha=self.ewma_alpha,
            )
        return self._states[sym]

    def set_depth_imbalance(self, symbol: str, obi_value: float) -> None:
        """Supply external Order Book Imbalance to update combined diagnostic features.

        Args:
            symbol: Ticker symbol.
            obi_value: Evaluated Order Book Imbalance.
        """
        sym = symbol.strip().upper()
        with self._lock:
            state = self._get_or_create_state(sym)
            state.latest_obi = obi_value
            # Refresh cached analytics if already present
            if sym in self._latest_analytics:
                curr = self._latest_analytics[sym]
                combined = evaluate_combined_microstructure_pressure(
                    obi_value=obi_value,
                    tfi_value=curr.trade_flow_imbalance,
                    threshold=self.imbalance_threshold,
                )
                self._latest_analytics[sym] = TradeFlowAnalytics(
                    symbol=curr.symbol,
                    exchange=curr.exchange,
                    timestamp=curr.timestamp,
                    trade_flow_imbalance=curr.trade_flow_imbalance,
                    tfi_ewma=curr.tfi_ewma,
                    rolling_tfi=curr.rolling_tfi,
                    total_traded_volume=curr.total_traded_volume,
                    buy_aggressor_volume=curr.buy_aggressor_volume,
                    sell_aggressor_volume=curr.sell_aggressor_volume,
                    unknown_volume=curr.unknown_volume,
                    trade_count=curr.trade_count,
                    rolling_buy_volume=curr.rolling_buy_volume,
                    rolling_sell_volume=curr.rolling_sell_volume,
                    rolling_total_volume=curr.rolling_total_volume,
                    rolling_trade_count=curr.rolling_trade_count,
                    interpretation=curr.interpretation,
                    rolling_interpretation=curr.rolling_interpretation,
                    duplicates_rejected=curr.duplicates_rejected,
                    sequence_gaps_detected=curr.sequence_gaps_detected,
                    out_of_order_count=curr.out_of_order_count,
                    last_trade_price=curr.last_trade_price,
                    vwap=curr.vwap,
                    combined_diagnostic=combined,
                    is_valid=curr.is_valid,
                    error=curr.error,
                )

    def process_tick(self, tick: TradeTick) -> TradeFlowAnalytics:
        """Process an incoming TradeTick deterministically.

        Guarantees:
        - Rejects duplicate trade IDs without updating cumulative volume.
        - Preserves UNKNOWN aggressor volume without classifying as buy or sell.
        - Calculates cumulative TFI, rolling window TFI, and EWMA.
        - Thread-safe and publishes EventBus event if configured.

        Args:
            tick: Validated TradeTick instance.

        Returns:
            TradeFlowAnalytics: Updated analytics for the instrument.
        """
        with self._lock:
            state = self._get_or_create_state(tick.symbol)

            # 1. Deduplication Guard
            if tick.trade_id in state.seen_trade_ids:
                state.duplicates_rejected += 1
                logger.debug("Duplicate trade tick rejected for %s: id=%s", tick.symbol, tick.trade_id)
                analytics = self._build_analytics_snapshot(state, state.last_timestamp or tick.timestamp, tick.exchange)
                self._latest_analytics[tick.symbol] = analytics
                return analytics

            # Record seen trade_id
            state.seen_trade_ids[tick.trade_id] = None
            if len(state.seen_trade_ids) > state._max_seen_ids:
                state.seen_trade_ids.popitem(last=False)

            # 2. Sequence Continuity Check
            if tick.sequence_number is not None:
                if state.last_sequence_number is not None:
                    expected = state.last_sequence_number + 1
                    if tick.sequence_number > expected:
                        state.sequence_gaps_detected += 1
                        logger.warning(
                            "Sequence gap for %s: expected %d, got %d",
                            tick.symbol,
                            expected,
                            tick.sequence_number,
                        )
                    elif tick.sequence_number < state.last_sequence_number:
                        state.out_of_order_count += 1
                        logger.warning(
                            "Out-of-order sequence for %s: prior %d, got %d",
                            tick.symbol,
                            state.last_sequence_number,
                            tick.sequence_number,
                        )
                state.last_sequence_number = tick.sequence_number

            # 3. Timestamp Ordering Check
            if state.last_timestamp > 0 and tick.timestamp < state.last_timestamp:
                state.out_of_order_count += 1
            state.last_timestamp = max(state.last_timestamp, tick.timestamp)

            # 4. Volume and Price Accumulation
            vol = tick.volume
            state.total_volume = round(state.total_volume + vol, 4)
            state.trade_count += 1
            state.last_trade_price = tick.price
            state.total_traded_value = round(state.total_traded_value + (tick.price * vol), 4)

            # Route by aggressor side — UNKNOWN is strictly preserved and never inferred
            if tick.aggressor_side == AggressorSide.BUY_INITIATED:
                state.buy_volume = round(state.buy_volume + vol, 4)
            elif tick.aggressor_side == AggressorSide.SELL_INITIATED:
                state.sell_volume = round(state.sell_volume + vol, 4)
            else:
                state.unknown_volume = round(state.unknown_volume + vol, 4)

            # 5. Append to Rolling Window
            state.rolling_window.append(tick)

            # 6. Build Immutable Analytics Snapshot
            analytics = self._build_analytics_snapshot(state, tick.timestamp, tick.exchange)
            self._latest_analytics[tick.symbol] = analytics
            self._total_ticks_processed += 1

            # 7. Dispatch Event if EventBus attached
            if self.event_bus is not None:
                try:
                    from app.core.events import Event, EventType
                    evt_type = getattr(EventType, "TRADE_FLOW_ANALYTICS_UPDATED", "TRADE_FLOW_ANALYTICS_UPDATED")
                    event = Event.create(
                        event_type=evt_type,
                        payload=analytics.to_dict(),
                        source=self.name,
                    )
                    self.event_bus.publish(event)
                except Exception as err:
                    logger.error("Failed to publish trade flow analytics event: %s", err)

            return analytics

    def _build_analytics_snapshot(
        self,
        state: _SymbolTradeFlowState,
        timestamp: float,
        exchange: str,
    ) -> TradeFlowAnalytics:
        """Internal helper to calculate snapshot metrics from state."""
        # Cumulative TFI
        cum_tfi, cum_interp = calculate_trade_flow_imbalance(
            state.buy_volume,
            state.sell_volume,
            threshold=self.imbalance_threshold,
        )

        # EWMA of TFI
        state.tfi_ewma = calculate_ewma(
            current_value=cum_tfi,
            prior_ewma=state.tfi_ewma,
            alpha=state.ewma_alpha,
        )

        # Rolling window metrics
        roll_buy = sum(t.volume for t in state.rolling_window if t.aggressor_side == AggressorSide.BUY_INITIATED)
        roll_sell = sum(t.volume for t in state.rolling_window if t.aggressor_side == AggressorSide.SELL_INITIATED)
        roll_total = sum(t.volume for t in state.rolling_window)
        roll_count = len(state.rolling_window)

        roll_tfi, roll_interp = calculate_trade_flow_imbalance(
            roll_buy,
            roll_sell,
            threshold=self.imbalance_threshold,
        )

        # VWAP
        vwap = round(state.total_traded_value / state.total_volume, 4) if state.total_volume > 0 else None

        # Combined pressure diagnostic
        combined_diag = evaluate_combined_microstructure_pressure(
            obi_value=state.latest_obi,
            tfi_value=cum_tfi,
            threshold=self.imbalance_threshold,
        )

        return TradeFlowAnalytics(
            symbol=state.symbol,
            exchange=exchange,
            timestamp=timestamp,
            trade_flow_imbalance=cum_tfi,
            tfi_ewma=state.tfi_ewma,
            rolling_tfi=roll_tfi,
            total_traded_volume=round(state.total_volume, 4),
            buy_aggressor_volume=round(state.buy_volume, 4),
            sell_aggressor_volume=round(state.sell_volume, 4),
            unknown_volume=round(state.unknown_volume, 4),
            trade_count=state.trade_count,
            rolling_buy_volume=round(roll_buy, 4),
            rolling_sell_volume=round(roll_sell, 4),
            rolling_total_volume=round(roll_total, 4),
            rolling_trade_count=roll_count,
            interpretation=cum_interp,
            rolling_interpretation=roll_interp,
            duplicates_rejected=state.duplicates_rejected,
            sequence_gaps_detected=state.sequence_gaps_detected,
            out_of_order_count=state.out_of_order_count,
            last_trade_price=state.last_trade_price,
            vwap=vwap,
            combined_diagnostic=combined_diag,
            is_valid=True,
            error=None,
        )

    def get_latest(self, symbol: str) -> Optional[TradeFlowAnalytics]:
        """Retrieve most recent TradeFlowAnalytics for symbol."""
        with self._lock:
            return self._latest_analytics.get(symbol.strip().upper())

    def clear(self) -> None:
        """Clear all in-memory trade flow state."""
        with self._lock:
            self._states.clear()
            self._latest_analytics.clear()
            self._total_ticks_processed = 0
