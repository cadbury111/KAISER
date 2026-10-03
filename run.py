#!/usr/bin/env python3
"""KAISER — Voice-First AI Trading Desk / Intelligence System.

Entry point for bootstrapping the KAISER foundation layer.
"""

from __future__ import annotations

import sys

from app.bootstrap import bootstrap_system, format_banner
from app.core.exceptions import KaiserError


def main() -> int:
    """Run the KAISER foundation bootstrap sequence.

    Returns:
        int: Process exit code (0 for success, non-zero for error).
    """
    try:
        launch_dashboard = "--dashboard" in sys.argv
        custom_settings = None
        if launch_dashboard:
            from app.core.config import Settings, get_settings
            base_s = get_settings()
            custom_settings = Settings(
                **{**base_s.to_dict(), "dashboard_enabled": True}
            )

        status = bootstrap_system(settings=custom_settings)
        print(format_banner(status))

        if launch_dashboard and status.portfolio_dashboard_server:
            print(f"\nPortfolio Analytics Command Center active at {status.portfolio_dashboard_server.url}")
            print("Press Ctrl+C to stop dashboard.")
            try:
                import time
                while status.portfolio_dashboard_server.is_running:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\nStopping dashboard server...")
                status.portfolio_dashboard_server.stop()

        return 0
    except KaiserError as err:
        print(f"KAISER Configuration/Initialization Error: {err}", file=sys.stderr)
        return 1
    except Exception as err:
        print(f"Unexpected error during KAISER startup: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
