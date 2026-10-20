"""KAISER — Historical-Spread-Based Slippage Calibration.

Provides an offline, deterministic calibration component analyzing bid-ask spreads
from historical or recorded synthetic market replay observations without any network calls.

SAFETY & INTEGRITY INVARIANTS:
1. PURELY ADVISORY & OFFLINE: Zero network calls; calibration never mutates production
   slippage settings, execution engine parameters, or risk limits.
2. STRICT PROVENANCE: Data is labeled with its true origin (e.g. "replay", "historical",
   "synthetic_test"); never claims data is historical unless provenance supports it.
3. FAIL CLOSED: Invalid, non-finite, non-positive, stale, or inverted quotes are rejected.
4. NO LOOK-AHEAD BIAS: Point-in-time calibration strictly filters out observations
   exceeding the specified evaluation timestamp.
5. DETERMINISTIC FINGERPRINTING: Emits a canonical SHA-256 fingerprint for reproducible auditability.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import math
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.execution.metrics import calculate_quantiles
from app.execution.models import PaperRiskConfig
from app.market.models import Quote

logger = get_logger("execution.calibration")


class ObservationRejectionReason(str, Enum):
    """Categorical reasons for discarding an ineligible market observation."""

    MISSING_BBO = "MISSING_BBO"
    NON_POSITIVE = "NON_POSITIVE"
    INVERTED_SPREAD = "INVERTED_SPREAD"
    NON_FINITE = "NON_FINITE"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    STALE = "STALE"
    DUPLICATE = "DUPLICATE"


@dataclass(frozen=True)
class SpreadObservation:
    """Strongly-typed, validated record of a single bid-ask quote observation."""

    symbol: str
    timestamp: float
    bid: float
    ask: float
    last_price: float
    price_spread: float
    mid_price: float
    relative_spread_bps: float
    half_spread_bps: float
    source: str = "replay"

    def to_dict(self) -> Dict[str, Any]:
        """Convert observation to dictionary."""
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp,
            "bid": self.bid,
            "ask": self.ask,
            "last_price": self.last_price,
            "price_spread": self.price_spread,
            "mid_price": self.mid_price,
            "relative_spread_bps": self.relative_spread_bps,
            "half_spread_bps": self.half_spread_bps,
            "source": self.source,
        }


@dataclass(frozen=True)
class SpreadStats:
    """Descriptive statistics for a numeric spread distribution."""

    count: int
    min: Optional[float]
    max: Optional[float]
    mean: Optional[float]
    median: Optional[float]
    p95: Optional[float]
    unit: str

    @property
    def sample_count(self) -> int:
        """Alias for count."""
        return self.count

    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary representation."""
        return {
            "count": self.count,
            "sample_count": self.count,
            "min": self.min,
            "max": self.max,
            "mean": self.mean,
            "median": self.median,
            "p95": self.p95,
            "unit": self.unit,
        }


@dataclass(frozen=True)
class SpreadCalibrationReport:
    """Comprehensive, advisory calibration report summarizing spread conditions for a symbol."""

    symbol: str
    provenance: str
    period_start: Optional[float]
    period_end: Optional[float]
    elapsed_period_seconds: Optional[float]
    total_observations: int
    valid_observations: int
    rejected_observations: int
    rejection_breakdown: Dict[str, int]
    price_spread: SpreadStats
    relative_spread_bps: SpreadStats
    half_spread_bps: SpreadStats
    config_comparison: Dict[str, Any]
    calibration_status: str
    warnings: Tuple[str, ...]
    advisory_notice: str
    digest: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary representation."""
        return {
            "symbol": self.symbol,
            "provenance": self.provenance,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "elapsed_period_seconds": self.elapsed_period_seconds,
            "total_observations": self.total_observations,
            "valid_observations": self.valid_observations,
            "rejected_observations": self.rejected_observations,
            "rejection_breakdown": dict(self.rejection_breakdown),
            "price_spread": self.price_spread.to_dict(),
            "relative_spread_bps": self.relative_spread_bps.to_dict(),
            "half_spread_bps": self.half_spread_bps.to_dict(),
            "config_comparison": dict(self.config_comparison),
            "calibration_status": self.calibration_status,
            "warnings": list(self.warnings),
            "advisory_notice": self.advisory_notice,
            "digest": self.digest,
            "stats": self.relative_spread_bps.to_dict(),
            "deterministic_digest": self.digest,
        }

    @property
    def stats(self) -> SpreadStats:
        return self.relative_spread_bps

    @property
    def is_statistically_reliable(self) -> bool:
        return self.calibration_status == "CALIBRATED"

    @property
    def configured_slippage_bps(self) -> Optional[float]:
        return self.config_comparison.get("configured_slippage_bps")

    @property
    def is_slippage_adequate(self) -> bool:
        ratio = self.config_comparison.get("half_spread_to_slippage_ratio")
        return ratio is not None and ratio <= 1.0

    @property
    def recommended_slippage_bps(self) -> Optional[float]:
        val = self.config_comparison.get("advisory_recommended_slippage_rate")
        return round(val * 10000.0, 4) if val is not None else None

    @property
    def deterministic_digest(self) -> str:
        return self.digest

    @property
    def as_of_time(self) -> Optional[float]:
        return self.period_end


class HistoricalSpreadCalibrator:
    """Deterministic, offline bid-ask spread analyzer and slippage calibration model.

    Safety:
    - Entirely offline; zero network calls.
    - Advisory only; never mutates execution engine risk or slippage settings.
    - Prevents look-ahead bias by supporting strictly bounded as-of timestamps.
    """

    def __init__(
        self,
        risk_config: Optional[PaperRiskConfig] = None,
        min_sample_size: int = 30,
        freshness_threshold_seconds: float = 86400.0,
        strict_chronological: bool = False,
    ) -> None:
        self.risk_config = risk_config or PaperRiskConfig()
        self.min_sample_size = max(1, min_sample_size)
        self.freshness_threshold = freshness_threshold_seconds
        self.strict_chronological = strict_chronological
        self._lock = threading.RLock()

        # Storage organized by normalized symbol
        self._observations: Dict[str, List[SpreadObservation]] = {}
        self._last_timestamps: Dict[str, float] = {}
        self._seen_signatures: Dict[str, Set[Tuple[float, float, float]]] = {}
        self._rejections: Dict[str, Dict[ObservationRejectionReason, int]] = {}
        self._total_received: Dict[str, int] = {}
        self._provenance_sources: Set[str] = set()

    def add_observation(
        self,
        quote_or_record: Quote | Dict[str, Any],
        reference_time: Optional[float] = None,
    ) -> Tuple[bool, Optional[ObservationRejectionReason], Optional[SpreadObservation]]:
        """Validate and record an individual market observation.

        Returns:
            Tuple[bool, Optional[ObservationRejectionReason], Optional[SpreadObservation]]:
            (is_accepted, rejection_reason_if_any, observation_record_if_accepted)
        """
        with self._lock:
            # 1. Parse raw fields
            if isinstance(quote_or_record, Quote):
                symbol = quote_or_record.symbol
                timestamp = quote_or_record.timestamp
                last_price = quote_or_record.last_price
                bid = quote_or_record.bid
                ask = quote_or_record.ask
                source = quote_or_record.source or "replay"
            elif isinstance(quote_or_record, dict):
                symbol = str(quote_or_record.get("symbol", ""))
                timestamp = float(quote_or_record.get("timestamp", 0.0))
                last_price = float(quote_or_record.get("last_price", quote_or_record.get("close", 0.0)))
                bid = quote_or_record.get("bid")
                ask = quote_or_record.get("ask")
                source = str(quote_or_record.get("source", "replay"))
            else:
                return False, ObservationRejectionReason.NON_FINITE, None

            norm_sym = symbol.strip().upper()
            if not norm_sym:
                return False, ObservationRejectionReason.NON_POSITIVE, None

            # Initialize counters for symbol
            if norm_sym not in self._observations:
                self._observations[norm_sym] = []
                self._seen_signatures[norm_sym] = set()
                self._rejections[norm_sym] = {r: 0 for r in ObservationRejectionReason}
                self._total_received[norm_sym] = 0

            self._total_received[norm_sym] += 1
            self._provenance_sources.add(source)

            # 2. Check for missing BBO
            if bid is None or ask is None:
                self._rejections[norm_sym][ObservationRejectionReason.MISSING_BBO] += 1
                return False, ObservationRejectionReason.MISSING_BBO, None

            try:
                bid_f = float(bid)
                ask_f = float(ask)
                last_f = float(last_price)
                ts_f = float(timestamp)
            except (ValueError, TypeError):
                self._rejections[norm_sym][ObservationRejectionReason.NON_FINITE] += 1
                return False, ObservationRejectionReason.NON_FINITE, None

            # 3. Check for non-finite values
            if not (math.isfinite(bid_f) and math.isfinite(ask_f) and math.isfinite(last_f) and math.isfinite(ts_f)):
                self._rejections[norm_sym][ObservationRejectionReason.NON_FINITE] += 1
                return False, ObservationRejectionReason.NON_FINITE, None

            # 4. Check for non-positive values
            if bid_f <= 0.0 or ask_f <= 0.0 or last_f <= 0.0 or ts_f <= 0.0:
                self._rejections[norm_sym][ObservationRejectionReason.NON_POSITIVE] += 1
                return False, ObservationRejectionReason.NON_POSITIVE, None

            # 5. Check for inverted bid-ask spread
            if ask_f < bid_f:
                self._rejections[norm_sym][ObservationRejectionReason.INVERTED_SPREAD] += 1
                return False, ObservationRejectionReason.INVERTED_SPREAD, None

            # 6. Check chronological monotonicity if required
            if self.strict_chronological and norm_sym in self._last_timestamps:
                last_ts = self._last_timestamps[norm_sym]
                if ts_f < last_ts:
                    self._rejections[norm_sym][ObservationRejectionReason.OUT_OF_ORDER] += 1
                    return False, ObservationRejectionReason.OUT_OF_ORDER, None

            # 7. Check freshness threshold if reference time is supplied
            if reference_time is not None:
                age = reference_time - ts_f
                if age > self.freshness_threshold or age < 0.0:
                    self._rejections[norm_sym][ObservationRejectionReason.STALE] += 1
                    return False, ObservationRejectionReason.STALE, None

            # 8. Check duplicate signature
            sig = (ts_f, round(bid_f, 6), round(ask_f, 6))
            if sig in self._seen_signatures[norm_sym]:
                self._rejections[norm_sym][ObservationRejectionReason.DUPLICATE] += 1
                return False, ObservationRejectionReason.DUPLICATE, None

            # 9. Compute spread metrics
            price_spread = round(ask_f - bid_f, 6)
            mid_price = round((bid_f + ask_f) / 2.0, 6)
            if mid_price <= 0.0:
                self._rejections[norm_sym][ObservationRejectionReason.NON_POSITIVE] += 1
                return False, ObservationRejectionReason.NON_POSITIVE, None

            rel_spread_bps = round((price_spread / mid_price) * 10000.0, 4)
            half_spread_bps = round(rel_spread_bps / 2.0, 4)

            obs = SpreadObservation(
                symbol=norm_sym,
                timestamp=ts_f,
                bid=round(bid_f, 4),
                ask=round(ask_f, 4),
                last_price=round(last_f, 4),
                price_spread=price_spread,
                mid_price=mid_price,
                relative_spread_bps=rel_spread_bps,
                half_spread_bps=half_spread_bps,
                source=source,
            )

            self._observations[norm_sym].append(obs)
            self._seen_signatures[norm_sym].add(sig)
            self._last_timestamps[norm_sym] = ts_f
            return True, None, obs

    def add_observations(
        self,
        records: Sequence[Quote | Dict[str, Any]],
        reference_time: Optional[float] = None,
    ) -> int:
        """Batch-process multiple market observations. Returns count of accepted observations."""
        accepted = 0
        for rec in records:
            ok, _, _ = self.add_observation(rec, reference_time=reference_time)
            if ok:
                accepted += 1
        return accepted

    def calibrate(
        self,
        symbol: str,
        as_of_time: Optional[float] = None,
    ) -> SpreadCalibrationReport:
        """Generate an advisory spread calibration report for a symbol.

        Look-Ahead Safeguard:
            If as_of_time is provided, observations with timestamp > as_of_time
            are strictly excluded from statistical calculation.
        """
        norm_sym = symbol.strip().upper()
        with self._lock:
            all_obs = self._observations.get(norm_sym, [])
            rejections = self._rejections.get(norm_sym, {r: 0 for r in ObservationRejectionReason})
            total_rcv = self._total_received.get(norm_sym, 0)

            # Apply look-ahead time filter if specified
            if as_of_time is not None:
                filtered_obs = [obs for obs in all_obs if obs.timestamp <= as_of_time]
            else:
                filtered_obs = list(all_obs)

            n_valid = len(filtered_obs)
            n_rejected = total_rcv - n_valid
            rejection_tallies = {reason.value: count for reason, count in rejections.items() if count > 0}

            # Observation period
            if n_valid > 0:
                period_start = min(obs.timestamp for obs in filtered_obs)
                period_end = max(obs.timestamp for obs in filtered_obs)
                elapsed_sec = round(period_end - period_start, 4)
            else:
                period_start = None
                period_end = None
                elapsed_sec = None

            # Spread statistics
            price_spreads = [obs.price_spread for obs in filtered_obs]
            rel_spreads_bps = [obs.relative_spread_bps for obs in filtered_obs]
            half_spreads_bps = [obs.half_spread_bps for obs in filtered_obs]

            p_min, p_max, p_mean, p_med, p_p95 = calculate_quantiles(price_spreads)
            r_min, r_max, r_mean, r_med, r_p95 = calculate_quantiles(rel_spreads_bps)
            h_min, h_max, h_mean, h_med, h_p95 = calculate_quantiles(half_spreads_bps)

            price_stats = SpreadStats(count=n_valid, min=p_min, max=p_max, mean=p_mean, median=p_med, p95=p_p95, unit="price_units")
            rel_stats = SpreadStats(count=n_valid, min=r_min, max=r_max, mean=r_mean, median=r_med, p95=r_p95, unit="bps")
            half_stats = SpreadStats(count=n_valid, min=h_min, max=h_max, mean=h_mean, median=h_med, p95=h_p95, unit="bps")

            # Configuration comparison
            sim_slippage_rate = self.risk_config.slippage_rate
            sim_slippage_bps = round(sim_slippage_rate * 10000.0, 4)
            sim_half_spread_rate = self.risk_config.half_spread_rate
            sim_half_spread_bps = round(sim_half_spread_rate * 10000.0, 4)

            spread_ratio: Optional[float] = None
            rec_slippage_rate: Optional[float] = None
            rec_half_spread_rate: Optional[float] = None

            if h_med is not None and sim_slippage_bps > 0:
                spread_ratio = round(h_med / sim_slippage_bps, 4)
                rec_slippage_rate = round(h_med / 10000.0, 6)
            if h_med is not None:
                rec_half_spread_rate = round(h_med / 10000.0, 6)

            config_comp = {
                "configured_slippage_rate": sim_slippage_rate,
                "configured_slippage_bps": sim_slippage_bps,
                "configured_half_spread_rate": sim_half_spread_rate,
                "configured_half_spread_bps": sim_half_spread_bps,
                "observed_median_half_spread_bps": h_med,
                "observed_p95_half_spread_bps": h_p95,
                "half_spread_to_slippage_ratio": spread_ratio,
                "advisory_recommended_slippage_rate": rec_slippage_rate,
                "advisory_recommended_half_spread_rate": rec_half_spread_rate,
            }

            warnings: List[str] = []
            if n_valid == 0:
                status = "NO_VALID_DATA"
                warnings.append("Zero valid market data observations available for calibration.")
            elif n_valid < self.min_sample_size:
                status = "INSUFFICIENT_DATA"
                warnings.append(
                    f"Insufficient sample size ({n_valid}) is below minimum threshold ({self.min_sample_size}). "
                    "Calibration results are advisory and statistically unrepresentative."
                )
            else:
                status = "CALIBRATED"

            # Determine provenance label
            if not self._provenance_sources:
                prov_label = "none"
            elif self._provenance_sources == {"historical"}:
                prov_label = "verified_historical"
            elif "historical" in self._provenance_sources:
                prov_label = "mixed_historical_and_replay"
            else:
                prov_label = "replayed_or_synthetic"

            # Compute reproducible SHA-256 digest
            hasher = hashlib.sha256()
            line = (
                f"CALIB:{norm_sym}:valid={n_valid}:rej={n_rejected}:"
                f"p_med={p_med}:p_p95={p_p95}:r_med={r_med}:r_p95={r_p95}:"
                f"cfg_slip_bps={sim_slippage_bps}:ratio={spread_ratio}\n"
            )
            hasher.update(line.encode("utf-8"))
            digest = hasher.hexdigest()

            return SpreadCalibrationReport(
                symbol=norm_sym,
                provenance=prov_label,
                period_start=period_start,
                period_end=period_end,
                elapsed_period_seconds=elapsed_sec,
                total_observations=total_rcv,
                valid_observations=n_valid,
                rejected_observations=n_rejected,
                rejection_breakdown=rejection_tallies,
                price_spread=price_stats,
                relative_spread_bps=rel_stats,
                half_spread_bps=half_stats,
                config_comparison=config_comp,
                calibration_status=status,
                warnings=tuple(warnings),
                advisory_notice=(
                    "ADVISORY ONLY: Spread calibration is strictly observational and does "
                    "NOT automatically alter simulation parameters or risk limits."
                ),
                digest=digest,
            )

    def calibrate_all(self, as_of_time: Optional[float] = None) -> Dict[str, SpreadCalibrationReport]:
        """Generate calibration reports for all recorded symbols."""
        with self._lock:
            return {sym: self.calibrate(sym, as_of_time=as_of_time) for sym in sorted(self._observations.keys())}

    def add_quote(
        self,
        quote: Quote,
        reference_time: Optional[float] = None,
    ) -> Optional[SpreadObservation]:
        """Convenience wrapper adding Quote. Returns SpreadObservation if accepted, else None."""
        ok, _, obs = self.add_observation(quote, reference_time=reference_time)
        return obs if ok else None

    def generate_report(
        self,
        symbol: str,
        as_of_time: Optional[float] = None,
        risk_config: Optional[PaperRiskConfig] = None,
    ) -> SpreadCalibrationReport:
        """Alias for calibrate() with optional temporary risk_config override."""
        old_cfg = self.risk_config
        try:
            if risk_config is not None:
                self.risk_config = risk_config
            return self.calibrate(symbol, as_of_time=as_of_time)
        finally:
            self.risk_config = old_cfg

    def get_symbols(self) -> List[str]:
        """Return list of all recorded symbols."""
        with self._lock:
            return sorted(self._observations.keys())

    def get_observations(self, symbol: str) -> List[SpreadObservation]:
        """Return list of accepted SpreadObservation records for symbol."""
        norm = symbol.strip().upper()
        with self._lock:
            return list(self._observations.get(norm, []))

    def get_rejected_counts(self) -> Dict[str, Dict[ObservationRejectionReason, int]]:
        """Return copy of rejection counts by symbol and reason."""
        with self._lock:
            return copy.deepcopy(self._rejections)

    def reset(self) -> None:
        """Clear all stored observations and calibration state."""
        with self._lock:
            self._observations.clear()
            self._seen_signatures.clear()
            self._last_timestamps.clear()
            self._rejections.clear()
            self._total_received.clear()
            self._provenance_sources.clear()
