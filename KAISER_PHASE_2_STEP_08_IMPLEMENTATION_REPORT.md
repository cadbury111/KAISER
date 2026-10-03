# KAISER — Phase 2, Step 08 Implementation Report
## Portfolio-Level Risk Analytics & Multi-Strategy Simulation Foundation

**Status:** Completed & Fully Validated  
**Date:** 2026-10-03  
**System Invariants:**
* `Settings.execution_locked = True` (strictly enforced, no live broker connections)
* External JUJU repository remains completely untouched
* Virtual accounting only: all balances, positions, orders, and fills are simulated
* Research-only disclaimer: backtested multi-strategy results never guarantee future profitability or diversification

---

### 1. Executive Summary

Phase 2, Step 08 introduces a deterministic, research-only **Multi-Strategy Virtual Portfolio Simulation and Risk Analytics Foundation** to the KAISER platform. The framework enables multiple registered quantitative strategies to execute concurrently within a shared virtual portfolio, enforcing centralized capital allocation, exposure bounds, cash reserve buffers, instrument concentration limits, drawdown circuit breakers, and next-bar execution rules.

The framework provides:
1. **Centralized Multi-Strategy Portfolio Coordination**: An orchestrating engine (`PortfolioSimulationEngine`) that runs multiple strategy instances across diverse instruments over synchronized historical timelines.
2. **Deterministic Simultaneous-Signal Resolution**: Strict order-priority rules (`SELL` before `BUY`, capital weight priority, signal strength priority, alphabetical tie-breakers).
3. **Multi-Tier Risk Gates**: Centralized checks for free cash reserves, portfolio gross exposure, per-strategy allocation limits, single-instrument concentration limits, and peak-to-trough drawdown circuit breakers.
4. **Distinguishable Standalone vs. Portfolio Analytics**: Separate tracking of individual strategy contributions (PnL, win rate, profit factor, costs, share of portfolio net PnL) alongside portfolio-level metrics (net return, equity curve, HHI concentration, maximum drawdown duration).
5. **Historical Diversification & Correlation Engine**: Aligned pairwise Pearson return correlation calculations across instruments and strategies, with strict safe handling of zero-variance series, missing observations, and clear research disclaimers.

---

### 2. Files Created and Modified

#### A. Files Created
1. `app/backtesting/portfolio_simulation/models.py`:
   - Strongly typed, validated domain models:
     - `PortfolioSimulationStatus`: Execution lifecycle states (`PENDING`, `RUNNING`, `SUCCESS`, `FAILED`, `CANCELLED`).
     - `StrategyAllocation`: Configuration for an individual strategy instance (registered strategy name, symbol, capital weight, custom parameters, sizing model, exposure cap).
     - `PortfolioPosition`: Virtual holding owned by a strategy instance (`market_value`, `cost_basis`, `unrealized_pnl`, `unrealized_return_pct`).
     - `PortfolioRiskSnapshot`: Instantaneous risk record (`gross_exposure`, `net_exposure`, `cash_reserve_pct`, `instrument_exposures`, `herfindahl_index`, `drawdown_pct`).
     - `PortfolioRiskWarning`: Structured risk audit warning log (`CASH_RESERVE_VIOLATION`, `MAX_PORTFOLIO_EXPOSURE_EXCEEDED`, `STRATEGY_EXPOSURE_LIMIT_EXCEEDED`, `INSTRUMENT_CONCENTRATION_EXCEEDED`, `CIRCUIT_BREAKER_TRIGGERED`, `DATA_QUALITY_REJECTION`).
     - `StrategyContribution`: Granular performance attribution for each allocated strategy.
     - `DiversificationMetrics`: Pairwise correlation matrices for instruments and strategies, HHI concentration index, effective number of uncorrelated strategies.
     - `PortfolioSimulationConfig`: Validated multi-strategy simulation configuration.
     - `PortfolioSimulationResult`: Complete immutable result model with audit trail and research disclaimers.
2. `app/backtesting/portfolio_simulation/analytics.py`:
   - `calculate_pearson_correlation`: Safe Pearson correlation coefficient calculator with zero-variance protection, division-by-zero prevention, and minimum observation thresholds.
   - `compute_diversification_metrics`: Computes aligned pairwise return correlations, Herfindahl-Hirschman capital concentration ($HHI = \sum w_i^2$), and effective strategy counts ($N_{eff} = 1 / HHI$).
3. `app/backtesting/portfolio_simulation/engine.py`:
   - `PortfolioSimulationEngine`: Coordinates chronological multi-symbol replay, next-bar execution, deterministic signal resolution, central risk controls, mark-to-market accounting, position liquidation on simulation close, cooperative cancellation (`CancellationToken`), EventBus publishing, and StateStore persistence.
4. `app/backtesting/portfolio_simulation/__init__.py`:
   - Package re-exports for all models, analytics, and engine components.
5. `tests/test_portfolio_simulation.py`:
   - 21 deterministic focused unit and integration tests covering allocation validation, shared portfolio accounting, simultaneous signals, risk gates, transaction costs, diversification metrics, anti-look-ahead protection, state isolation, cancellation, and orchestrator commands.

#### B. Files Modified
1. `app/core/events/event.py`:
   - Added EventType entries: `PORTFOLIO_SIMULATION_STARTED`, `PORTFOLIO_SIMULATION_PROGRESS`, `PORTFOLIO_SIMULATION_COMPLETED`, `PORTFOLIO_SIMULATION_CANCELLED`, `PORTFOLIO_SIMULATION_FAILED`.
2. `app/backtesting/__init__.py`:
   - Re-exported all portfolio simulation symbols and `PortfolioSimulationEngine`.
3. `app/backtesting/handlers.py`:
   - Updated `register_backtest_handlers` signature to accept `portfolio_engine`.
   - Added and registered `run_portfolio_simulation` command handler with input parsing, validation, fallback equal-weight allocation generation, and structured return format.
4. `app/bootstrap.py`:
   - Bootstrapped `PortfolioSimulationEngine`, passed to `register_backtest_handlers` and `SystemStatus`, added startup log `Portfolio simulation engine: ready`.

---

### 3. Shared Accounting and Allocation Methodology

#### A. Virtual Capital & Balance Integrity
* The simulation starts with a central virtual cash balance (`initial_cash`, default $100,000.00).
* **Balance Invariant**: At every simulation step $T$:
  $$\text{Total Equity}_T = \text{Cash}_T + \sum_{p \in \text{Positions}} \text{Market Value}(p)_T$$
* Positions are tracked uniquely by `(strategy_allocation_id, symbol)`. This prevents Strategy A from mutating or selling Strategy B's position in the same or different instruments, while aggregating cleanly into total portfolio holdings.

#### B. Allocation & Sizing Models
* Each strategy instance defines a fractional capital allocation weight ($w_i \in (0.0, 1.0]$) such that:
  $$\sum_{i=1}^K w_i \le 1.0001$$
* **Sizing Rules**:
  - `percent_equity`: Target position value is derived from the strategy's allocated share of portfolio equity ($w_i \times \text{Total Equity} \times \text{size\_value}$).
  - `fixed_cash`: Target position dollar value is fixed.
  - `fixed_quantity`: Target position unit quantity is fixed.
* Sizing is rounded to 4 decimal places and validated against available free cash before order generation.

---

### 4. Order Priority and Execution Timing Rules

#### A. Deterministic Simultaneous-Signal Resolution
When multiple strategies emit signals on the close of bar $T$, signals are sorted deterministically before evaluation against risk gates:
1. **Side Priority**: `SELL` orders take absolute precedence over `BUY` orders.
   * *Rationale*: Liquidating or trimming existing positions frees virtual cash and relieves portfolio exposure *before* evaluating capital availability for new long entries.
2. **Allocation Weight Priority**: Strategies with higher capital allocation weights ($w_i$) take precedence over lower-weight strategies when competing for limited virtual cash.
3. **Signal Strength Priority**: Higher numerical signal strength (`StrategySignal.strength`) takes precedence.
4. **Deterministic Tie-Breakers**: Alphabetical ordering on `allocation_id` followed by `symbol`.

#### B. Next-Bar Execution & Anti-Look-Ahead Guarantees
* **Signal Generation**: Evaluated strictly at candle close on timestamp $T$. StrategyContext exposes only bars and indicators with timestamps $\le T$.
* **Execution Timing**: Accepted orders enter `pending_orders` status.
* **Fill Execution**: Pending orders are filled on the arrival of timestamp $T+1$ at the **OPEN** price of bar $T+1$:
  $$\text{Exec Price}_{\text{BUY}} = \text{Open}_{T+1} \times (1 + \text{slippage\_pct} + \text{half\_spread\_pct})$$
  $$\text{Exec Price}_{\text{SELL}} = \text{Open}_{T+1} \times (1 - \text{slippage\_pct} - \text{half\_spread\_pct})$$
* Strategies are completely forbidden from accessing bar $T+1$ open or close prices when generating signals at bar $T$.

---

### 5. Portfolio Risk Controls & Metrics

#### A. Central Risk Gates
1. **Cash Reserve Constraint (`min_cash_reserve_pct`)**:
   - Requires the portfolio to maintain a minimum uncommitted cash buffer ($\text{initial\_cash} \times \text{min\_cash\_reserve\_pct}$).
   - Orders requiring cash that would breach this threshold are rejected with `CASH_RESERVE_VIOLATION`.
2. **Maximum Portfolio Gross Exposure (`max_portfolio_exposure_pct`)**:
   - Caps total gross market exposure ($\sum |\text{market\_val}_i|$) as a percentage of total equity (e.g. 100%).
   - Orders breaching the limit are rejected with `MAX_PORTFOLIO_EXPOSURE_EXCEEDED`.
3. **Per-Strategy Exposure Cap (`max_strategy_exposure_pct`)**:
   - Restricts an individual strategy instance's market value relative to total portfolio equity.
   - Breaches are rejected with `STRATEGY_EXPOSURE_LIMIT_EXCEEDED`.
4. **Single-Instrument Concentration Cap (`max_instrument_concentration_pct`)**:
   - Caps total market exposure in any single instrument (e.g. max 30% in TCS).
   - Breaches are rejected with `INSTRUMENT_CONCENTRATION_EXCEEDED`.
5. **Drawdown Circuit Breaker (`max_drawdown_limit_pct`)**:
   - Tracks peak-to-trough portfolio equity drawdown:
     $$\text{Drawdown \%}_T = \frac{\text{Peak Equity}_T - \text{Total Equity}_T}{\text{Peak Equity}_T} \times 100$$
   - If drawdown reaches or exceeds `max_drawdown_limit_pct`, the circuit breaker activates, a `CIRCUIT_BREAKER_TRIGGERED` warning is recorded, and all subsequent `BUY` orders are rejected with `CIRCUIT_BREAKER_ACTIVE`.

#### B. Transaction Cost Accounting
* Transaction costs (commission fee and slippage) are calculated explicitly per fill:
  $$\text{Fee} = (\text{Quantity} \times \text{Exec Price} \times \text{fee\_rate}) + \text{fixed\_fee}$$
  $$\text{Slippage Cost} = \text{Base Price} \times \text{slippage\_pct} \times \text{Quantity}$$
* Costs are deducted from cash on execution and attributed to both the portfolio aggregate and the initiating strategy. Fees are never applied twice.

---

### 6. Correlation Methodology & Diversification Analytics

#### A. Return Alignment
* **Instrument Correlations**: Computes Pearson correlation $r$ across aligned bar close-to-close returns:
  $$R_{i, T} = \frac{P_{i, T} - P_{i, T-1}}{P_{i, T-1}}$$
* **Strategy Correlations**: Computes Pearson correlation across aligned periodic strategy return contributions:
  $$R_{s, T} = \frac{\Delta \text{Equity}_{s, T}}{\text{Allocated Capital}_s}$$
* Timestamp alignment uses strict intersection matching. No missing observations are fabricated or filled with synthetic numbers.

#### B. Safe Zero & Undefined Handling
* If common aligned observations $N < \text{min\_correlation\_observations}$ (default 10): returns `None` and records a diagnostic warning.
* If either return series has zero variance ($\sigma^2 < 10^{-12}$, constant prices or no trades): returns `None` safely without division by zero.
* Clamps calculated correlation strictly to $[-1.0, 1.0]$.

#### C. Capital Concentration (HHI)
* Computes normalized Herfindahl-Hirschman Index across strategy weights:
  $$\text{HHI} = \sum_{i=1}^K \left(\frac{w_i}{\sum w}\right)^2, \quad N_{\text{eff}} = \frac{1}{\text{HHI}}$$
* For 2 equal-weight strategies (0.5/0.5), $\text{HHI} = 0.50$ and $N_{\text{eff}} = 2.0$.

---

### 7. Orchestrator Integration

The MasterOrchestrator handler `run_portfolio_simulation` provides structured execution:
* **Command**: `run_portfolio_simulation`
* **Input Parameters**:
  - `allocations`: Optional list of explicit strategy allocation definitions.
  - `strategies` / `symbols`: Fallback shorthand list for generating equal-weight allocations.
  - `timeframe`: Normalized timeframe (e.g. `'1m'`, `'15m'`).
  - `start_time` / `end_time`: Historical window epoch boundaries.
  - `initial_cash`: Starting virtual capital.
  - Risk limits: `max_portfolio_exposure_pct`, `max_strategy_exposure_pct`, `max_instrument_concentration_pct`, `min_cash_reserve_pct`, `max_drawdown_limit_pct`.
  - Transaction costs: `fee_model`, `fee_rate`, `fixed_fee`, `slippage_bps`, `half_spread_bps`.
* **Output Format**:
  - `status`: `'success'` / `'failed'`
  - `simulation_status`: `'SUCCESS'` / `'FAILED'` / `'CANCELLED'`
  - `initial_equity`, `final_equity`, `net_pnl`, `net_return_pct`, `max_drawdown_pct`
  - `total_trades`, `win_rate`, `profit_factor`
  - `strategy_contributions`: Granular per-strategy attribution
  - `diversification`: Correlation matrices and concentration indices
  - `warnings_count`, `disclaimer`

---

### 8. Verification & Test Results

#### A. Focused Portfolio Simulation Test Suite
```powershell
python -m pytest tests/test_portfolio_simulation.py -v
```
**Results**: **21 passed in 0.47s** (0 failed, 0 skipped).
* `test_strategy_allocation_model_validation`: PASSED
* `test_portfolio_simulation_config_validation`: PASSED
* `test_multi_strategy_shared_virtual_portfolio`: PASSED
* `test_balance_integrity_throughout_simulation`: PASSED
* `test_simultaneous_signals_deterministic_priority`: PASSED
* `test_cash_reserve_limit_enforcement`: PASSED
* `test_max_portfolio_exposure_limit`: PASSED
* `test_instrument_concentration_limit`: PASSED
* `test_per_strategy_exposure_limit`: PASSED
* `test_drawdown_circuit_breaker`: PASSED
* `test_shared_transaction_cost_accounting`: PASSED
* `test_pearson_correlation_calculations`: PASSED
* `test_diversification_metrics_computation`: PASSED
* `test_next_bar_execution_timing`: PASSED
* `test_deterministic_reproducibility`: PASSED
* `test_zero_trades_edge_case`: PASSED
* `test_state_isolation_between_consecutive_runs`: PASSED
* `test_quarantined_bars_data_quality_rejection`: PASSED
* `test_cooperative_cancellation`: PASSED
* `test_orchestrator_integration_run_portfolio_simulation`: PASSED
* `test_trading_safety_invariants`: PASSED

#### B. Full Test Suite Regression Check
```powershell
python -m pytest
```
**Results**: **609 passed in 12.13s** (0 failed, 0 skipped) across all 21 test modules.

#### C. Clean Startup Verification
```powershell
python run.py
```
**Output**:
```
2026-10-03 10:06:36 | INFO | kaiser.bootstrap | Portfolio simulation engine: ready
2026-10-03 10:06:36 | INFO | kaiser.bootstrap | Backtest engine: ready (strategies=['sma_crossover'])
2026-10-03 10:06:36 | INFO | kaiser.bootstrap | Walk-forward validator: ready
2026-10-03 10:06:36 | INFO | kaiser.bootstrap | Robustness test engine: ready
2026-10-03 10:06:36 | INFO | kaiser.bootstrap | Execution engine: locked
KAISER
System initialized successfully.
Environment: development
Voice-first architecture: ready
Trading engine: not initialized
Execution engine: locked
```

---

### 9. Known Limitations & Research Boundaries

1. **Research-Only Virtual Accounting**: Simulations operate entirely within memory and historical SQLite records. Live broker endpoints, real money transfers, or automated trade executions are not supported.
2. **Long-Only Base Assumptions**: Sizing and risk gates currently enforce long-only spot equity conventions (`allow_short = False`). Shorting or margin debt models are intentionally disabled.
3. **Synchronous Historical Data**: The simulation aligns bars on exact timestamp matches across instruments. Asynchronous multi-exchange tick matching or microstructure order book queue priority is beyond the scope of bar-level backtesting.
4. **Descriptive Correlations**: Historical return correlations do not imply future diversification. Correlations can break down and converge to 1.0 during severe market stress.

---

### 10. Safety Invariants Confirmation

* `Settings.execution_locked = True`: Confirmed and verified across all tests and live startup.
* External JUJU repository: Untouched (0 files modified outside `c:\Users\user\Desktop\KAISER`).
* Broker connections: Completely bypassed and disconnected.
* All changes saved to local git repository.
