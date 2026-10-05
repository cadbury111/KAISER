"""KAISER — Market Data Quality & Anomaly Detection Package.

Exports:
- MarketDataQualityEngine & MarketDataQualityMonitor
- DataQualityStatus, DataQualityAssessment, DatasetQualityAssessment
- QualitySeverity, AnomalyType, MarketAnomaly, QualityFinding, max_severity
- MarketQualityConfig, calculate_quality_score
- SourceHealthTracker, SourceHealth
- Quality policies and completeness evaluators
- Orchestrator command handlers
"""

from app.market.quality.engine import MarketDataQualityEngine
from app.market.quality.handlers import register_quality_handlers
from app.market.quality.models import (
    AnomalyType,
    MarketAnomaly,
    QualityFinding,
    QualitySeverity,
    max_severity,
)
from app.market.quality.monitor import MarketDataQualityMonitor
from app.market.quality.policy import (
    MarketQualityConfig,
    calculate_quality_score,
    evaluate_bar_completeness,
    evaluate_order_book_completeness,
    evaluate_quote_completeness,
    is_safe_for_downstream,
    resolve_quality_status,
)
from app.market.quality.status import (
    DataQualityAssessment,
    DataQualityStatus,
    DatasetQualityAssessment,
)
from app.market.quality.tracker import SourceHealth, SourceHealthTracker

__all__ = [
    "MarketDataQualityEngine",
    "MarketDataQualityMonitor",
    "DataQualityStatus",
    "DataQualityAssessment",
    "DatasetQualityAssessment",
    "QualitySeverity",
    "AnomalyType",
    "MarketAnomaly",
    "QualityFinding",
    "max_severity",
    "MarketQualityConfig",
    "calculate_quality_score",
    "SourceHealth",
    "SourceHealthTracker",
    "evaluate_quote_completeness",
    "evaluate_bar_completeness",
    "evaluate_order_book_completeness",
    "resolve_quality_status",
    "is_safe_for_downstream",
    "register_quality_handlers",
]
