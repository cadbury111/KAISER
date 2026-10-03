"""KAISER — Portfolio Analytics Dashboard UI Package.

Provides local, read-only dashboard server and interactive command center interface.
"""

from app.portfolio.dashboard.server import PortfolioDashboardServer
from app.portfolio.dashboard.template import get_dashboard_html

__all__ = [
    "PortfolioDashboardServer",
    "get_dashboard_html",
]
