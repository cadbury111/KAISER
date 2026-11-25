#!/usr/bin/env python3
"""KAISER — Voice-First AI Trading Desk / Intelligence System.

Entry point for bootstrapping the KAISER foundation layer.
"""

from __future__ import annotations

import os
import sys

from app.bootstrap import bootstrap_system, format_banner, shutdown_system
from app.core.exceptions import KaiserError


def main() -> int:
    """Run the KAISER foundation bootstrap sequence.

    Returns:
        int: Process exit code (0 for success, non-zero for error).
    """
    try:
        # Guarantee test database override cannot leak into normal application startup
        os.environ.pop("KAISER_TEST_DB_PATH", None)

        if "--help" in sys.argv or "-h" in sys.argv:
            print("KAISER — Voice-First AI Trading Desk / Intelligence System\n")
            print("Usage: python run.py [OPTIONS]\n")
            print("Options:")
            print("  --dashboard    Launch the Portfolio Analytics Command Center web UI")
            print("  --help, -h     Show this message and exit")
            return 0

        launch_dashboard = "--dashboard" in sys.argv
        custom_settings = None
        if launch_dashboard:
            import dataclasses
            from app.core.config import get_settings
            base_s = get_settings()
            custom_settings = dataclasses.replace(base_s, dashboard_enabled=True)

        status = bootstrap_system(settings=custom_settings)
        print(format_banner(status))

        try:
            if launch_dashboard and status.portfolio_dashboard_server:
                print(f"\nPortfolio Analytics Command Center active at {status.portfolio_dashboard_server.url}")
                print("Press Ctrl+C to stop dashboard.")
                while status.portfolio_dashboard_server.is_running:
                    time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down KAISER gracefully...")
        finally:
            shutdown_system(status)

        return 0
    except KaiserError as err:
        print(f"KAISER Configuration/Initialization Error: {err}", file=sys.stderr)
        return 1
    except Exception as err:
        print(f"Unexpected error during KAISER startup: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
