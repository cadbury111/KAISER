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
        status = bootstrap_system()
        print(format_banner(status))
        return 0
    except KaiserError as err:
        print(f"KAISER Configuration/Initialization Error: {err}", file=sys.stderr)
        return 1
    except Exception as err:
        print(f"Unexpected error during KAISER startup: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
