"""KAISER Risk Management Package.

Enforces deterministic risk controls, position limits, drawdown guardrails,
and pre-trade verification gates.

SAFETY INVARIANT:
KAISER analysis must never directly execute a trade without passing through
an independent deterministic risk gate.
"""
