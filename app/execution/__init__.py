"""KAISER Execution Engine Package.

Manages order routing, broker integrations, and paper trading simulation.

SAFETY INVARIANTS:
1. The execution engine is locked by default.
2. Real-money execution strictly requires explicit human approval.
3. No live broker execution is enabled during foundation phase.
"""
