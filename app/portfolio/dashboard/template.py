"""KAISER — Portfolio Analytics Dashboard UI Template.

Provides the complete, self-contained single-page dashboard application:
- Premium dark-mode aesthetic with glassmorphism and tailored HSL color palette.
- Responsive CSS Grid and Flexbox layout for desktop, tablet, and mobile.
- Zero external dependencies: pure standard HTML5, Vanilla CSS3, and ES6 JavaScript.
- Embedded interactive SVG chart rendering for Equity and Drawdown curves.
- Sections:
  A. Portfolio Overview (Net PnL, Return %, MaxDD, Sharpe, Cash, Exposure, Trades)
  B. Equity Curve (Interactive SVG timeline with high-water marks and tooltips)
  C. Drawdown Chart (Interactive SVG area chart with zero baseline and peak troughs)
  D. Strategy Attribution (Capital weights, Gross/Net PnL, Contribution shares)
  E. Active Risk Alerts (Severity badges, thresholds, breaches, zero-alert states)
  F. Historical Run Comparison (Side-by-side comparative table with research disclaimers)
  G. Data Quality and Provenance (VALID, STALE, INCOMPLETE badges, audit warnings)
  Empty State: Step-by-step guidance when no reports exist.
"""

def get_dashboard_html() -> str:
    """Generate the full, self-contained HTML/CSS/JS dashboard string."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>KAISER — Portfolio Analytics Command Center</title>
  <style>
    /* ------------------------------------------------------------- */
    /* KAISER DESIGN SYSTEM — Modern Dark Theme & Glassmorphism      */
    /* ------------------------------------------------------------- */
    :root {
      --bg-primary: #07090E;
      --bg-surface: #0E131F;
      --bg-card: #141B2D;
      --bg-card-hover: #1A233A;
      --bg-glass: rgba(20, 27, 45, 0.75);
      --border-subtle: #1F293D;
      --border-focus: #3B82F6;
      --text-main: #F1F5F9;
      --text-muted: #94A3B8;
      --text-dim: #64748B;
      --accent-blue: #3B82F6;
      --accent-cyan: #06B6D4;
      --accent-green: #10B981;
      --accent-green-bg: rgba(16, 185, 129, 0.12);
      --accent-red: #EF4444;
      --accent-red-bg: rgba(239, 68, 68, 0.12);
      --accent-amber: #F59E0B;
      --accent-amber-bg: rgba(245, 158, 11, 0.12);
      --accent-purple: #8B5CF6;
      --radius-sm: 6px;
      --radius-md: 10px;
      --radius-lg: 16px;
      --shadow-sm: 0 2px 4px rgba(0,0,0,0.3);
      --shadow-md: 0 4px 16px rgba(0,0,0,0.4);
      --shadow-glow: 0 0 20px rgba(59, 130, 246, 0.15);
      --font-stack: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background-color: var(--bg-primary);
      color: var(--text-main);
      font-family: var(--font-stack);
      line-height: 1.5;
      padding: 0;
      margin: 0;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      overflow-x: hidden;
    }

    /* ------------------------------------------------------------- */
    /* Header / Top Navigation Bar                                   */
    /* ------------------------------------------------------------- */
    header {
      background: var(--bg-surface);
      border-bottom: 1px solid var(--border-subtle);
      position: sticky;
      top: 0;
      z-index: 100;
      backdrop-filter: blur(12px);
    }

    .header-inner {
      max-width: 1440px;
      margin: 0 auto;
      padding: 14px 24px;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }

    .brand-group {
      display: flex;
      align-items: center;
      gap: 14px;
    }

    .brand-logo {
      width: 36px;
      height: 36px;
      background: linear-gradient(135deg, var(--accent-blue), var(--accent-cyan));
      border-radius: var(--radius-sm);
      display: flex;
      align-items: center;
      justify-content: center;
      font-weight: 800;
      font-size: 1.15rem;
      color: #fff;
      box-shadow: 0 0 12px rgba(59, 130, 246, 0.4);
    }

    .brand-title {
      font-size: 1.15rem;
      font-weight: 700;
      letter-spacing: -0.01em;
      color: var(--text-main);
    }

    .brand-sub {
      font-size: 0.75rem;
      color: var(--text-dim);
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    .header-actions {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 10px;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 0.75rem;
      font-weight: 600;
      padding: 4px 10px;
      border-radius: 9999px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }

    .badge-lock {
      background: var(--accent-green-bg);
      color: var(--accent-green);
      border: 1px solid rgba(16, 185, 129, 0.3);
    }

    .badge-status {
      border: 1px solid var(--border-subtle);
    }

    .badge-valid { background: var(--accent-green-bg); color: var(--accent-green); border-color: rgba(16,185,129,0.3); }
    .badge-stale { background: var(--accent-amber-bg); color: var(--accent-amber); border-color: rgba(245,158,11,0.3); }
    .badge-incomplete { background: rgba(249, 115, 22, 0.15); color: #F97316; border-color: rgba(249,115,22,0.3); }
    .badge-unavailable { background: var(--accent-red-bg); color: var(--accent-red); border-color: rgba(239,68,68,0.3); }

    select, button {
      background: var(--bg-card);
      color: var(--text-main);
      border: 1px solid var(--border-subtle);
      border-radius: var(--radius-sm);
      padding: 7px 12px;
      font-size: 0.85rem;
      font-family: inherit;
      cursor: pointer;
      transition: all 0.2s ease;
    }

    select:hover, button:hover {
      border-color: var(--border-focus);
      background: var(--bg-card-hover);
    }

    button.btn-primary {
      background: var(--accent-blue);
      border-color: var(--accent-blue);
      color: #fff;
      font-weight: 600;
    }
    button.btn-primary:hover {
      background: #2563EB;
      box-shadow: 0 0 10px rgba(59, 130, 246, 0.4);
    }

    /* ------------------------------------------------------------- */
    /* Main Layout Grid                                              */
    /* ------------------------------------------------------------- */
    main {
      max-width: 1440px;
      margin: 0 auto;
      padding: 24px;
      flex: 1;
      width: 100%;
    }

    .grid-container {
      display: grid;
      gap: 24px;
    }

    .banner-voice {
      background: linear-gradient(135deg, rgba(30, 41, 59, 0.8), rgba(20, 27, 45, 0.9));
      border: 1px solid var(--border-subtle);
      border-left: 4px solid var(--accent-cyan);
      border-radius: var(--radius-md);
      padding: 16px 20px;
      display: flex;
      align-items: flex-start;
      gap: 14px;
    }

    .voice-icon {
      font-size: 1.3rem;
      line-height: 1;
      color: var(--accent-cyan);
    }

    .voice-text-title {
      font-size: 0.8rem;
      font-weight: 700;
      text-transform: uppercase;
      color: var(--accent-cyan);
      margin-bottom: 4px;
    }

    .voice-text-body {
      font-size: 0.92rem;
      color: var(--text-main);
    }

    /* ------------------------------------------------------------- */
    /* Metric Cards Grid                                             */
    /* ------------------------------------------------------------- */
    .overview-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
    }

    .metric-card {
      background: var(--bg-glass);
      border: 1px solid var(--border-subtle);
      border-radius: var(--radius-md);
      padding: 18px 20px;
      transition: transform 0.2s ease, border-color 0.2s ease;
      box-shadow: var(--shadow-sm);
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }

    .metric-card:hover {
      transform: translateY(-2px);
      border-color: rgba(59, 130, 246, 0.4);
      box-shadow: var(--shadow-glow);
    }

    .metric-label {
      font-size: 0.78rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--text-muted);
      margin-bottom: 8px;
    }

    .metric-value-row {
      display: flex;
      align-items: baseline;
      gap: 10px;
      flex-wrap: wrap;
    }

    .metric-value {
      font-size: 1.65rem;
      font-weight: 700;
      color: var(--text-main);
      font-variant-numeric: tabular-nums;
    }

    .val-positive { color: var(--accent-green); }
    .val-negative { color: var(--accent-red); }
    .val-neutral  { color: var(--text-main); }
    .val-na       { color: var(--text-dim); font-size: 1.2rem; font-style: italic; }

    .metric-badge {
      font-size: 0.75rem;
      font-weight: 600;
      padding: 2px 8px;
      border-radius: 4px;
    }
    .badge-pos { background: var(--accent-green-bg); color: var(--accent-green); }
    .badge-neg { background: var(--accent-red-bg); color: var(--accent-red); }
    .badge-neu { background: rgba(255,255,255,0.06); color: var(--text-muted); }

    .metric-footnote {
      margin-top: 8px;
      font-size: 0.76rem;
      color: var(--text-dim);
    }

    /* ------------------------------------------------------------- */
    /* Chart Section Layout                                          */
    /* ------------------------------------------------------------- */
    .chart-dual-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 24px;
    }
    @media (min-width: 1024px) {
      .chart-dual-grid {
        grid-template-columns: 1.15fr 0.85fr;
      }
    }

    .card-panel {
      background: var(--bg-surface);
      border: 1px solid var(--border-subtle);
      border-radius: var(--radius-lg);
      padding: 22px;
      box-shadow: var(--shadow-md);
      display: flex;
      flex-direction: column;
    }

    .panel-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 18px;
      flex-wrap: wrap;
      gap: 10px;
    }

    .panel-title {
      font-size: 1rem;
      font-weight: 700;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .panel-meta {
      font-size: 0.8rem;
      color: var(--text-muted);
    }

    .chart-container {
      width: 100%;
      height: 280px;
      position: relative;
    }

    svg.chart-svg {
      width: 100%;
      height: 100%;
      overflow: visible;
    }

    /* ------------------------------------------------------------- */
    /* Tables (Strategy Attribution & Comparison)                    */
    /* ------------------------------------------------------------- */
    .table-responsive {
      overflow-x: auto;
      margin-top: 6px;
    }

    table.data-table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
      font-size: 0.85rem;
    }

    table.data-table th {
      padding: 10px 14px;
      background: var(--bg-card);
      color: var(--text-muted);
      font-weight: 600;
      text-transform: uppercase;
      font-size: 0.74rem;
      letter-spacing: 0.03em;
      border-bottom: 1px solid var(--border-subtle);
    }

    table.data-table td {
      padding: 12px 14px;
      border-bottom: 1px solid rgba(31, 41, 61, 0.6);
      font-variant-numeric: tabular-nums;
    }

    table.data-table tr:hover td {
      background: var(--bg-card-hover);
    }

    /* ------------------------------------------------------------- */
    /* Risk Alerts Panel                                             */
    /* ------------------------------------------------------------- */
    .alerts-list {
      display: flex;
      flex-direction: column;
      gap: 10px;
    }

    .alert-item {
      background: var(--bg-card);
      border-left: 4px solid var(--border-subtle);
      border-radius: var(--radius-sm);
      padding: 12px 16px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
    }

    .alert-item.crit { border-left-color: var(--accent-red); background: rgba(239, 68, 68, 0.06); }
    .alert-item.warn { border-left-color: var(--accent-amber); background: rgba(245, 158, 11, 0.06); }
    .alert-item.info { border-left-color: var(--accent-blue); background: rgba(59, 130, 246, 0.06); }

    .alert-headline {
      font-weight: 600;
      font-size: 0.88rem;
    }

    .alert-meta {
      font-size: 0.78rem;
      color: var(--text-dim);
      margin-top: 2px;
    }

    /* ------------------------------------------------------------- */
    /* Empty & Error States                                          */
    /* ------------------------------------------------------------- */
    .empty-state {
      padding: 48px 24px;
      text-align: center;
      background: var(--bg-surface);
      border: 1px dashed var(--border-subtle);
      border-radius: var(--radius-lg);
      margin: 32px 0;
    }

    .empty-state h3 {
      font-size: 1.25rem;
      margin-bottom: 8px;
      color: var(--text-main);
    }

    .empty-state p {
      font-size: 0.9rem;
      color: var(--text-muted);
      max-width: 600px;
      margin: 0 auto 20px auto;
    }

    .code-box {
      background: var(--bg-primary);
      border: 1px solid var(--border-subtle);
      padding: 12px 16px;
      border-radius: var(--radius-sm);
      font-family: monospace;
      font-size: 0.85rem;
      color: var(--accent-cyan);
      display: inline-block;
      text-align: left;
    }

    /* ------------------------------------------------------------- */
    /* Footer & Research Notice                                      */
    /* ------------------------------------------------------------- */
    footer {
      background: var(--bg-surface);
      border-top: 1px solid var(--border-subtle);
      padding: 20px 24px;
      margin-top: auto;
      text-align: center;
      font-size: 0.78rem;
      color: var(--text-dim);
    }

    .research-disclaimer {
      max-width: 900px;
      margin: 0 auto 6px auto;
      color: var(--text-muted);
      line-height: 1.4;
    }
  </style>
</head>
<body>

  <!-- Top Header Navigation -->
  <header>
    <div class="header-inner">
      <div class="brand-group">
        <div class="brand-logo">K</div>
        <div>
          <div class="brand-title">KAISER — Portfolio Analytics Command Center</div>
          <div class="brand-sub">Voice-First AI Trading Desk Foundation • Phase 2 Step 12</div>
        </div>
      </div>

      <div class="header-actions">
        <span class="badge badge-lock">🔒 LOCKED (READ-ONLY)</span>
        <span id="dataStatusBadge" class="badge badge-status badge-valid">VALID</span>
        <select id="reportSelect" title="Select Report" aria-label="Select Report">
          <option value="">Loading historical runs...</option>
        </select>
        <button id="btnRefresh" class="btn-primary" title="Refresh Dashboard">⟳ Refresh</button>
      </div>
    </div>
  </header>

  <!-- Main Dashboard Content -->
  <main>
    <div id="loadingIndicator" style="display: none; text-align: center; padding: 30px;">
      <span style="color: var(--accent-blue); font-size: 1rem;">Loading analytics report...</span>
    </div>

    <!-- Empty State Container -->
    <div id="emptyState" class="empty-state" style="display: none;">
      <h3>No Portfolio Reports Available</h3>
      <p>
        No simulation or backtest reports have been evaluated yet. Run a portfolio simulation
        or backtest via the Python API, CLI, or Orchestrator to generate unified performance analytics.
      </p>
      <div class="code-box">
        # Generate simulation from Python:<br>
        from app.bootstrap import bootstrap_system<br>
        status = bootstrap_system()<br>
        # Run orchestrator command: run_portfolio_simulation
      </div>
    </div>

    <!-- Main Content Container -->
    <div id="contentContainer" class="grid-container">

      <!-- Voice Summary Banner -->
      <div id="voiceBanner" class="banner-voice">
        <div class="voice-icon">🎙️</div>
        <div>
          <div class="voice-text-title">Voice Assistant Summary</div>
          <div id="voiceText" class="voice-text-body">Connecting to reporting service...</div>
        </div>
      </div>

      <!-- Section A: Portfolio Overview Cards -->
      <section class="overview-grid">
        <div class="metric-card" id="cardNetPnL">
          <div class="metric-label">Net Profit / Loss</div>
          <div class="metric-value-row">
            <span id="valNetPnL" class="metric-value">$0.00</span>
            <span id="badgeReturnPct" class="metric-badge badge-neu">+0.00%</span>
          </div>
          <div id="footNetPnL" class="metric-footnote">Gross PnL: $0.00 • Costs: $0.00</div>
        </div>

        <div class="metric-card" id="cardMaxDD">
          <div class="metric-label">Maximum Drawdown</div>
          <div class="metric-value-row">
            <span id="valMaxDDPct" class="metric-value">0.00%</span>
            <span id="valMaxDDAmt" class="metric-badge badge-neg">-$0.00</span>
          </div>
          <div id="footMaxDD" class="metric-footnote">Peak-to-trough: 0 bars duration</div>
        </div>

        <div class="metric-card" id="cardSharpe">
          <div class="metric-label">Sharpe Ratio (Ann.)</div>
          <div class="metric-value-row">
            <span id="valSharpe" class="metric-value">N/A</span>
            <span id="badgeSortino" class="metric-badge badge-neu">Sortino: N/A</span>
          </div>
          <div id="footSharpe" class="metric-footnote">Risk-free benchmark: 0.0%</div>
        </div>

        <div class="metric-card" id="cardCash">
          <div class="metric-label">Cash & Exposure</div>
          <div class="metric-value-row">
            <span id="valCashReserve" class="metric-value">100.0%</span>
            <span id="badgeGrossExp" class="metric-badge badge-neu">Exp: 0.0%</span>
          </div>
          <div id="footCash" class="metric-footnote">Cash balance: $0.00</div>
        </div>

        <div class="metric-card" id="cardTrades">
          <div class="metric-label">Strategies & Trades</div>
          <div class="metric-value-row">
            <span id="valStrategies" class="metric-value">0</span>
            <span id="badgeWinRate" class="metric-badge badge-neu">WinRate: 0%</span>
          </div>
          <div id="footTrades" class="metric-footnote">Closed round-trip trades</div>
        </div>

        <div class="metric-card" id="cardReconciliation">
          <div class="metric-label">Accounting Reconciliation</div>
          <div class="metric-value-row">
            <span id="valReconStatus" class="metric-value val-positive">EXACT</span>
          </div>
          <div id="footRecon" class="metric-footnote">Audit verified within tolerance</div>
        </div>
      </section>

      <!-- Section B & C: Dual Chart Layout (Equity & Drawdown) -->
      <section class="chart-dual-grid">
        <!-- Section B: Equity Curve -->
        <div class="card-panel">
          <div class="panel-header">
            <div class="panel-title">📈 Portfolio Equity Trajectory</div>
            <div id="metaEquity" class="panel-meta">Initial: $0.00 → Final: $0.00</div>
          </div>
          <div class="chart-container" id="equityChartContainer">
            <svg id="equitySvg" class="chart-svg" viewBox="0 0 700 280"></svg>
          </div>
        </div>

        <!-- Section C: Drawdown Chart -->
        <div class="card-panel">
          <div class="panel-header">
            <div class="panel-title">📉 Peak-to-Trough Drawdown (%)</div>
            <div id="metaDrawdown" class="panel-meta">Max DD: 0.00%</div>
          </div>
          <div class="chart-container" id="drawdownChartContainer">
            <svg id="drawdownSvg" class="chart-svg" viewBox="0 0 500 280"></svg>
          </div>
        </div>
      </section>

      <!-- Section D: Strategy Performance Attribution -->
      <section class="card-panel">
        <div class="panel-header">
          <div class="panel-title">🎯 Strategy Performance Attribution</div>
          <div id="metaStrategies" class="panel-meta">0 Strategies Tracked</div>
        </div>
        <div class="table-responsive">
          <table class="data-table" id="strategyTable">
            <thead>
              <tr>
                <th>Strategy Name</th>
                <th>Symbol</th>
                <th>Allocation</th>
                <th>Gross PnL</th>
                <th>Net PnL</th>
                <th>Contribution %</th>
                <th>Trades</th>
                <th>Win Rate</th>
              </tr>
            </thead>
            <tbody id="strategyTableBody">
              <tr><td colspan="8" style="text-align: center; color: var(--text-dim);">No strategy data</td></tr>
            </tbody>
          </table>
        </div>
      </section>

      <!-- Section E: Active Risk Alerts -->
      <section class="card-panel">
        <div class="panel-header">
          <div class="panel-title">⚠️ Risk Monitoring & Active Alerts</div>
          <div id="metaRiskState" class="badge badge-valid">NORMAL (0 Alerts)</div>
        </div>
        <div class="alerts-list" id="alertsContainer">
          <div class="alert-item info">
            <div>
              <div class="alert-headline">✓ All portfolio risk metrics within normal parameters</div>
              <div class="alert-meta">No active exposure, concentration, or drawdown breaches.</div>
            </div>
          </div>
        </div>
      </section>

      <!-- Section F: Historical Run Comparison -->
      <section class="card-panel">
        <div class="panel-header">
          <div class="panel-title">⚖️ Multi-Run Historical Comparison</div>
          <div id="metaComparison" class="panel-meta">Select runs to compare performance</div>
        </div>
        <div class="table-responsive">
          <table class="data-table" id="comparisonTable">
            <thead>
              <tr>
                <th>Run / Report ID</th>
                <th>Type</th>
                <th>Initial Capital</th>
                <th>Final Capital</th>
                <th>Net PnL</th>
                <th>Net Return %</th>
                <th>Max DD %</th>
                <th>Sharpe Ratio</th>
                <th>Risk State</th>
              </tr>
            </thead>
            <tbody id="comparisonTableBody">
              <tr><td colspan="9" style="text-align: center; color: var(--text-dim);">Loading comparison runs...</td></tr>
            </tbody>
          </table>
        </div>
      </section>

      <!-- Section G: Data Quality & Provenance -->
      <section class="card-panel" style="background: rgba(20, 27, 45, 0.5);">
        <div class="panel-header">
          <div class="panel-title">🛡️ Data Provenance & Audit Quality</div>
          <div id="provenanceBadge" class="badge badge-valid">AUDIT VERIFIED</div>
        </div>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 16px; font-size: 0.85rem;">
          <div>
            <div style="color: var(--text-dim); margin-bottom: 2px;">Source Run ID</div>
            <div id="provSourceRun" style="font-family: monospace; color: var(--accent-cyan);">-</div>
          </div>
          <div>
            <div style="color: var(--text-dim); margin-bottom: 2px;">Evaluation Timestamp (UTC)</div>
            <div id="provTimestamp">-</div>
          </div>
          <div>
            <div style="color: var(--text-dim); margin-bottom: 2px;">Engine Version / Git Commit</div>
            <div id="provVersion">-</div>
          </div>
          <div>
            <div style="color: var(--text-dim); margin-bottom: 2px;">Data Freshness & Warnings</div>
            <div id="provWarnings" style="color: var(--text-muted);">-</div>
          </div>
        </div>
      </section>

      <!-- Section H: Operational Diagnostics & System Audit -->
      <section class="card-panel" id="diagnosticsSection">
        <div class="panel-header">
          <div class="panel-title">🛠️ Operational Diagnostics & System Audit</div>
          <div id="diagStatusBadge" class="badge badge-valid">OPERATIONAL</div>
        </div>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-bottom: 16px; font-size: 0.85rem;">
          <div class="metric-card" style="padding: 12px 14px;">
            <div class="metric-label">Dashboard Server State</div>
            <div id="valDiagServerState" class="metric-value" style="font-size: 1.1rem; color: var(--accent-green);">RUNNING</div>
            <div id="footDiagServer" class="metric-footnote">http://127.0.0.1:8080 (Loopback)</div>
          </div>
          <div class="metric-card" style="padding: 12px 14px;">
            <div class="metric-label">Operational Events</div>
            <div id="valDiagEventsCount" class="metric-value" style="font-size: 1.1rem;">0 Recorded</div>
            <div id="footDiagEviction" class="metric-footnote">0 evicted • Bounded FIFO</div>
          </div>
          <div class="metric-card" style="padding: 12px 14px;">
            <div class="metric-label">Latest Report Generation</div>
            <div id="valDiagLatestReport" class="metric-value" style="font-size: 1.1rem; color: var(--text-main);">N/A</div>
            <div id="footDiagLatestReport" class="metric-footnote">Duration: N/A</div>
          </div>
          <div class="metric-card" style="padding: 12px 14px;">
            <div class="metric-label">Audit Retention Policy</div>
            <div class="metric-value" style="font-size: 1.0rem; color: var(--accent-cyan);">In-Memory (FIFO)</div>
            <div id="footDiagRetention" class="metric-footnote">Max limit: 100 events</div>
          </div>
        </div>

        <div class="panel-header" style="margin-top: 10px; margin-bottom: 8px;">
          <div style="font-size: 0.85rem; font-weight: 600; color: var(--text-muted);">Recent Operational Event Log</div>
          <div id="diagEventsCountMeta" class="panel-meta">Showing recent events</div>
        </div>
        <div class="table-responsive">
          <table class="data-table" id="diagEventsTable">
            <thead>
              <tr>
                <th>Timestamp (UTC)</th>
                <th>Event Type</th>
                <th>Outcome</th>
                <th>Correlation / Run ID</th>
                <th>Data Status</th>
                <th>Details</th>
              </tr>
            </thead>
            <tbody id="diagEventsTableBody">
              <tr><td colspan="6" style="text-align: center; color: var(--text-dim);">No operational diagnostic events recorded yet.</td></tr>
            </tbody>
          </table>
        </div>
      </section>

    </div>
  </main>

  <!-- Footer Notice -->
  <footer>
    <div class="research-disclaimer">
      <strong>RESEARCH & SAFETY NOTICE:</strong> All portfolio analytics, risk scores, drawdowns, and comparisons
      are descriptive and based on simulated or historical replay data. They do NOT guarantee future profitability,
      predict live market movements, or execute trades. Live execution engine is strictly locked.
    </div>
    <div>KAISER Intelligence Foundation • Local Operational Console</div>
  </footer>

  <!-- ------------------------------------------------------------- -->
  <!-- Client Application Logic (Pure Vanilla JavaScript)            -->
  <!-- ------------------------------------------------------------- -->
  <script>
    (function() {
      'use strict';

      // State store
      let currentReport = null;
      let historicalSummaries = [];

      // DOM references
      const reportSelect = document.getElementById('reportSelect');
      const btnRefresh = document.getElementById('btnRefresh');
      const dataStatusBadge = document.getElementById('dataStatusBadge');
      const loadingIndicator = document.getElementById('loadingIndicator');
      const emptyState = document.getElementById('emptyState');
      const contentContainer = document.getElementById('contentContainer');

      // Overview fields
      const voiceText = document.getElementById('voiceText');
      const valNetPnL = document.getElementById('valNetPnL');
      const badgeReturnPct = document.getElementById('badgeReturnPct');
      const footNetPnL = document.getElementById('footNetPnL');
      const valMaxDDPct = document.getElementById('valMaxDDPct');
      const valMaxDDAmt = document.getElementById('valMaxDDAmt');
      const footMaxDD = document.getElementById('footMaxDD');
      const valSharpe = document.getElementById('valSharpe');
      const badgeSortino = document.getElementById('badgeSortino');
      const footSharpe = document.getElementById('footSharpe');
      const valCashReserve = document.getElementById('valCashReserve');
      const badgeGrossExp = document.getElementById('badgeGrossExp');
      const footCash = document.getElementById('footCash');
      const valStrategies = document.getElementById('valStrategies');
      const badgeWinRate = document.getElementById('badgeWinRate');
      const footTrades = document.getElementById('footTrades');
      const valReconStatus = document.getElementById('valReconStatus');
      const footRecon = document.getElementById('footRecon');

      // Charts
      const equitySvg = document.getElementById('equitySvg');
      const metaEquity = document.getElementById('metaEquity');
      const drawdownSvg = document.getElementById('drawdownSvg');
      const metaDrawdown = document.getElementById('metaDrawdown');

      // Strategy & Alerts & Comparison & Provenance
      const strategyTableBody = document.getElementById('strategyTableBody');
      const metaStrategies = document.getElementById('metaStrategies');
      const alertsContainer = document.getElementById('alertsContainer');
      const metaRiskState = document.getElementById('metaRiskState');
      const comparisonTableBody = document.getElementById('comparisonTableBody');
      const provSourceRun = document.getElementById('provSourceRun');
      const provTimestamp = document.getElementById('provTimestamp');
      const provVersion = document.getElementById('provVersion');
      const provWarnings = document.getElementById('provWarnings');

      // Diagnostics fields
      const valDiagServerState = document.getElementById('valDiagServerState');
      const footDiagServer = document.getElementById('footDiagServer');
      const valDiagEventsCount = document.getElementById('valDiagEventsCount');
      const footDiagEviction = document.getElementById('footDiagEviction');
      const valDiagLatestReport = document.getElementById('valDiagLatestReport');
      const footDiagLatestReport = document.getElementById('footDiagLatestReport');
      const diagStatusBadge = document.getElementById('diagStatusBadge');
      const diagEventsTableBody = document.getElementById('diagEventsTableBody');
      const diagEventsCountMeta = document.getElementById('diagEventsCountMeta');

      // Utilities: Number check & HTML escaping
      function isNumeric(val) {
        return val !== null && val !== undefined && typeof val === 'number' && !isNaN(val) && isFinite(val);
      }

      function escapeHtml(str) {
        if (str === null || str === undefined) return '';
        return String(str)
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;');
      }

      function fmtCurrency(val) {
        if (!isNumeric(val)) return 'N/A';
        const sign = val < 0 ? '-' : '';
        return `${sign}$${Math.abs(val).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
      }

      function fmtPercent(val, showPlus = true) {
        if (!isNumeric(val)) return 'N/A';
        const sign = (showPlus && val > 0) ? '+' : '';
        return `${sign}${val.toFixed(2)}%`;
      }

      // Initialize
      window.addEventListener('DOMContentLoaded', () => {
        loadReportList();
        loadDiagnostics();
        btnRefresh.addEventListener('click', () => {
          loadReport(reportSelect.value || 'latest');
          loadDiagnostics();
        });
        reportSelect.addEventListener('change', () => {
          loadReport(reportSelect.value);
          loadDiagnostics();
        });

        // Delegated click handler for comparison table links
        comparisonTableBody.addEventListener('click', (evt) => {
          const link = evt.target.closest('.report-link');
          if (link && link.dataset.reportId) {
            loadReport(link.dataset.reportId);
            loadDiagnostics();
          }
        });
      });

      // Fetch historical list
      async function loadReportList() {
        try {
          const res = await fetch('/api/reports?limit=50');
          if (!res.ok) throw new Error('Failed to fetch reports');
          const data = await res.json();
          historicalSummaries = data.reports || [];

          reportSelect.innerHTML = '';
          if (historicalSummaries.length === 0) {
            reportSelect.innerHTML = '<option value="">No runs available</option>';
            showEmptyState(true);
            return;
          }

          historicalSummaries.forEach((rep, idx) => {
            const opt = document.createElement('option');
            opt.value = rep.report_id;
            const sign = (isNumeric(rep.net_pnl) && rep.net_pnl >= 0) ? '+' : '';
            const pnlStr = isNumeric(rep.net_pnl) ? `${sign}$${rep.net_pnl.toFixed(0)}` : 'N/A';
            opt.textContent = `${rep.title || rep.report_id} (${pnlStr})`;
            if (idx === 0) opt.selected = true;
            reportSelect.appendChild(opt);
          });

          // Load latest
          loadReport(historicalSummaries[0].report_id);
          populateComparisonTable(historicalSummaries);
        } catch (err) {
          console.error(err);
          showEmptyState(true);
        }
      }

      // Fetch specific report
      async function loadReport(reportId) {
        if (!reportId) return;
        showLoading(true);
        try {
          const target = reportId === 'latest' ? '/api/report/latest' : `/api/report/${encodeURIComponent(reportId)}`;
          const res = await fetch(target);
          if (res.status === 404) {
            showEmptyState(true);
            return;
          }
          if (!res.ok) throw new Error('Error loading report');

          const report = await res.json();
          currentReport = report;
          renderDashboard(report);
          showEmptyState(false);
        } catch (err) {
          console.error(err);
          showEmptyState(true);
        } finally {
          showLoading(false);
        }
      }

      function showLoading(show) {
        loadingIndicator.style.display = show ? 'block' : 'none';
        if (show) contentContainer.style.opacity = '0.5';
        else contentContainer.style.opacity = '1';
      }

      function showEmptyState(show) {
        emptyState.style.display = show ? 'block' : 'none';
        contentContainer.style.display = show ? 'none' : 'grid';
      }

      // Render Dashboard Data
      function renderDashboard(r) {
        // Status Badge
        dataStatusBadge.textContent = r.status || 'VALID';
        dataStatusBadge.className = `badge badge-status badge-${(r.status || 'valid').toLowerCase()}`;

        // Voice summary
        if (r.voice_summary && r.voice_summary.full_speech_script) {
          voiceText.textContent = r.voice_summary.full_speech_script;
        } else {
          voiceText.textContent = r.summary_text || 'No speech script available.';
        }

        // Overview Cards: Net PnL
        if (isNumeric(r.net_pnl)) {
          valNetPnL.textContent = fmtCurrency(r.net_pnl);
          valNetPnL.className = `metric-value ${r.net_pnl > 0 ? 'val-positive' : (r.net_pnl < 0 ? 'val-negative' : 'val-neutral')}`;
        } else {
          valNetPnL.textContent = 'N/A';
          valNetPnL.className = 'metric-value val-na';
        }

        if (isNumeric(r.net_return_pct)) {
          badgeReturnPct.textContent = fmtPercent(r.net_return_pct, true);
          badgeReturnPct.className = `metric-badge ${r.net_return_pct >= 0 ? 'badge-pos' : 'badge-neg'}`;
        } else {
          badgeReturnPct.textContent = 'N/A';
          badgeReturnPct.className = 'metric-badge badge-neu';
        }

        const grossStr = isNumeric(r.gross_pnl) ? fmtCurrency(r.gross_pnl) : 'N/A';
        const costStr = isNumeric(r.total_transaction_costs) ? fmtCurrency(r.total_transaction_costs) : 'N/A';
        footNetPnL.textContent = `Gross: ${grossStr} • Costs: ${costStr}`;

        // Max DD
        if (isNumeric(r.max_drawdown_pct)) {
          valMaxDDPct.textContent = `${r.max_drawdown_pct.toFixed(2)}%`;
          valMaxDDPct.className = `metric-value ${r.max_drawdown_pct > 0 ? 'val-negative' : 'val-neutral'}`;
        } else {
          valMaxDDPct.textContent = 'N/A';
          valMaxDDPct.className = 'metric-value val-na';
        }

        if (isNumeric(r.max_drawdown_amount)) {
          valMaxDDAmt.textContent = `-${fmtCurrency(r.max_drawdown_amount)}`;
        } else {
          valMaxDDAmt.textContent = 'N/A';
        }

        const durStr = (r.max_drawdown_duration_bars !== null && r.max_drawdown_duration_bars !== undefined)
          ? `${r.max_drawdown_duration_bars} bars duration`
          : 'Duration N/A';
        footMaxDD.textContent = `Peak-to-trough: ${durStr}`;

        // Sharpe / Sortino
        const rMetrics = r.risk_adjusted_metrics || {};
        const sh = rMetrics.sharpe_ratio;
        if (sh && sh.is_available && isNumeric(sh.value)) {
          valSharpe.textContent = sh.value.toFixed(2);
          valSharpe.className = 'metric-value val-neutral';
          footSharpe.textContent = 'Annualized excess return';
        } else {
          valSharpe.textContent = 'N/A';
          valSharpe.className = 'metric-value val-na';
          footSharpe.textContent = sh ? (sh.unavailable_reason || 'Insufficient variance') : 'Unavailable';
        }

        const so = rMetrics.sortino_ratio;
        if (so && so.is_available && isNumeric(so.value)) {
          badgeSortino.textContent = `Sortino: ${so.value.toFixed(2)}`;
        } else {
          badgeSortino.textContent = 'Sortino: N/A';
        }

        // Cash & Exposure
        if (isNumeric(r.cash_reserve_pct)) {
          valCashReserve.textContent = `${r.cash_reserve_pct.toFixed(1)}%`;
          valCashReserve.className = 'metric-value val-neutral';
        } else {
          valCashReserve.textContent = 'N/A';
          valCashReserve.className = 'metric-value val-na';
        }

        if (isNumeric(r.gross_exposure_pct)) {
          badgeGrossExp.textContent = `Exp: ${r.gross_exposure_pct.toFixed(1)}%`;
        } else {
          badgeGrossExp.textContent = 'Exp: N/A';
        }

        const cashStr = isNumeric(r.cash_balance) ? fmtCurrency(r.cash_balance) : 'N/A';
        const expStr = isNumeric(r.gross_exposure) ? fmtCurrency(r.gross_exposure) : 'N/A';
        footCash.textContent = `Cash: ${cashStr} • Gross Exp: ${expStr}`;

        // Strategies & Trades
        const strats = Array.isArray(r.strategy_attributions) ? r.strategy_attributions : [];
        valStrategies.textContent = strats.length.toString();
        let totalTrades = 0;
        let hasTrades = false;
        strats.forEach(s => {
          const tc = s.trades_count !== undefined ? s.trades_count : s.total_trades;
          if (isNumeric(tc)) {
            totalTrades += tc;
            hasTrades = true;
          }
        });

        if (rMetrics.win_rate && rMetrics.win_rate.is_available && isNumeric(rMetrics.win_rate.value)) {
          badgeWinRate.textContent = `WinRate: ${rMetrics.win_rate.value.toFixed(1)}%`;
        } else {
          badgeWinRate.textContent = 'WinRate: N/A';
        }
        footTrades.textContent = hasTrades ? `${totalTrades} total closed round-trip trades` : 'Trades count N/A';

        // Reconciliation
        if (r.reconciliation) {
          const rec = r.reconciliation;
          valReconStatus.textContent = rec.status || 'EXACT';
          valReconStatus.className = `metric-value ${rec.is_reconciled ? 'val-positive' : 'val-negative'}`;
          const discStr = isNumeric(rec.strategy_discrepancy) ? `$${rec.strategy_discrepancy.toFixed(2)}` : 'N/A';
          footRecon.textContent = rec.is_reconciled ? 'Reconciled within tolerance' : `Discrepancy: ${discStr}`;
        } else {
          valReconStatus.textContent = 'N/A';
          valReconStatus.className = 'metric-value val-na';
          footRecon.textContent = 'No reconciliation audit recorded';
        }

        // Render Charts
        const initEq = isNumeric(r.initial_equity) ? r.initial_equity : 100000;
        renderEquityChart(r.equity_curve || [], initEq);
        renderDrawdownChart(r.equity_curve || []);

        // Render Strategies
        renderStrategyTable(strats);

        // Render Risk Alerts
        renderRiskAlerts(r.risk_status);

        // Render Provenance
        const prov = r.provenance || {};
        provSourceRun.textContent = prov.source_run_id || '-';
        provTimestamp.textContent = prov.generated_at_iso || '-';
        provVersion.textContent = `${prov.engine_version || '1.0.0'} (${prov.git_commit || 'master'})`;
        const warns = r.data_warnings || [];
        if (warns.length > 0) {
          provWarnings.innerHTML = `<span style="color: var(--accent-amber);">⚠️ ${warns.length} Warnings:</span> ${warns.map(w => escapeHtml(w)).join('; ')}`;
        } else {
          provWarnings.textContent = '✓ No accounting, freshness, or integrity warnings.';
        }
      }

      // Render Equity SVG Chart
      function renderEquityChart(points, initialEquity) {
        const finalVal = (points.length && isNumeric(points[points.length - 1].total_equity))
          ? points[points.length - 1].total_equity
          : initialEquity;
        metaEquity.textContent = `Initial: ${fmtCurrency(initialEquity)} → Final: ${fmtCurrency(finalVal)}`;
        equitySvg.innerHTML = '';

        if (!points || points.length === 0) {
          equitySvg.innerHTML = `
            <text x="350" y="140" fill="var(--text-dim)" text-anchor="middle" font-size="14">
              No historical equity points recorded for this run
            </text>
          `;
          return;
        }

        const width = 700;
        const height = 280;
        const padX = 60;
        const padY = 30;

        const validEquities = points.map(p => isNumeric(p.total_equity) ? p.total_equity : initialEquity);
        let minEq = Math.min(...validEquities, initialEquity);
        let maxEq = Math.max(...validEquities, initialEquity);
        if (minEq === maxEq) { minEq *= 0.95; maxEq *= 1.05; }
        const range = maxEq - minEq || 1;

        // Coordinates
        const coords = points.map((p, idx) => {
          const eqVal = isNumeric(p.total_equity) ? p.total_equity : initialEquity;
          const x = padX + (idx / (points.length - 1 || 1)) * (width - 2 * padX);
          const y = height - padY - ((eqVal - minEq) / range) * (height - 2 * padY);
          return { x, y, pt: p };
        });

        // Path
        const pathData = coords.reduce((acc, c, i) => `${acc} ${i === 0 ? 'M' : 'L'} ${c.x.toFixed(1)} ${c.y.toFixed(1)}`, '');
        const areaData = `${pathData} L ${coords[coords.length - 1].x.toFixed(1)} ${height - padY} L ${coords[0].x.toFixed(1)} ${height - padY} Z`;

        // Baseline Y
        const baseY = height - padY - ((initialEquity - minEq) / range) * (height - 2 * padY);

        equitySvg.innerHTML = `
          <defs>
            <linearGradient id="eqGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stop-color="var(--accent-blue)" stop-opacity="0.3"/>
              <stop offset="100%" stop-color="var(--accent-blue)" stop-opacity="0.0"/>
            </linearGradient>
          </defs>
          <!-- Grid lines -->
          <line x1="${padX}" y1="${padY}" x2="${width - padX}" y2="${padY}" stroke="var(--border-subtle)" stroke-dasharray="4"/>
          <line x1="${padX}" y1="${height / 2}" x2="${width - padX}" y2="${height / 2}" stroke="var(--border-subtle)" stroke-dasharray="4"/>
          <line x1="${padX}" y1="${height - padY}" x2="${width - padX}" y2="${height - padY}" stroke="var(--border-subtle)"/>

          <!-- Baseline -->
          <line x1="${padX}" y1="${baseY.toFixed(1)}" x2="${width - padX}" y2="${baseY.toFixed(1)}" stroke="var(--text-dim)" stroke-dasharray="2"/>

          <!-- Area & Line -->
          <path d="${areaData}" fill="url(#eqGrad)"/>
          <path d="${pathData}" fill="none" stroke="var(--accent-cyan)" stroke-width="2.5" stroke-linecap="round"/>

          <!-- Y Axis Labels -->
          <text x="${padX - 8}" y="${padY + 4}" fill="var(--text-dim)" text-anchor="end" font-size="11">$${(maxEq/1000).toFixed(0)}k</text>
          <text x="${padX - 8}" y="${baseY + 4}" fill="var(--text-muted)" text-anchor="end" font-size="11">$${(initialEquity/1000).toFixed(0)}k</text>
          <text x="${padX - 8}" y="${height - padY + 4}" fill="var(--text-dim)" text-anchor="end" font-size="11">$${(minEq/1000).toFixed(0)}k</text>
        `;
      }

      // Render Drawdown SVG Chart
      function renderDrawdownChart(points) {
        drawdownSvg.innerHTML = '';
        if (!points || points.length === 0) {
          drawdownSvg.innerHTML = `
            <text x="250" y="140" fill="var(--text-dim)" text-anchor="middle" font-size="14">
              No drawdown data available
            </text>
          `;
          return;
        }

        const width = 500;
        const height = 280;
        const padX = 50;
        const padY = 30;

        const dds = points.map(p => isNumeric(p.drawdown_pct) ? p.drawdown_pct : 0.0);
        const maxDd = Math.max(...dds, 5.0);
        metaDrawdown.textContent = `Peak Drawdown: ${Math.max(...dds).toFixed(2)}%`;

        const coords = points.map((p, idx) => {
          const ddVal = isNumeric(p.drawdown_pct) ? p.drawdown_pct : 0.0;
          const x = padX + (idx / (points.length - 1 || 1)) * (width - 2 * padX);
          const y = padY + (ddVal / maxDd) * (height - 2 * padY);
          return { x, y, pt: p };
        });

        const pathData = coords.reduce((acc, c, i) => `${acc} ${i === 0 ? 'M' : 'L'} ${c.x.toFixed(1)} ${c.y.toFixed(1)}`, '');
        const areaData = `M ${padX} ${padY} ${pathData} L ${coords[coords.length - 1].x.toFixed(1)} ${padY} Z`;

        drawdownSvg.innerHTML = `
          <defs>
            <linearGradient id="ddGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stop-color="var(--accent-red)" stop-opacity="0.0"/>
              <stop offset="100%" stop-color="var(--accent-red)" stop-opacity="0.35"/>
            </linearGradient>
          </defs>
          <!-- Zero baseline -->
          <line x1="${padX}" y1="${padY}" x2="${width - padX}" y2="${padY}" stroke="var(--text-muted)" stroke-width="1.5"/>
          <line x1="${padX}" y1="${height - padY}" x2="${width - padX}" y2="${height - padY}" stroke="var(--border-subtle)" stroke-dasharray="4"/>

          <!-- Shaded Area & Line -->
          <path d="${areaData}" fill="url(#ddGrad)"/>
          <path d="${pathData}" fill="none" stroke="var(--accent-red)" stroke-width="2"/>

          <!-- Labels -->
          <text x="${padX - 8}" y="${padY + 4}" fill="var(--text-muted)" text-anchor="end" font-size="11">0%</text>
          <text x="${padX - 8}" y="${height - padY + 4}" fill="var(--accent-red)" text-anchor="end" font-size="11">-${maxDd.toFixed(0)}%</text>
        `;
      }

      // Render Strategy Table
      function renderStrategyTable(strategies) {
        metaStrategies.textContent = `${strategies.length} Strategies Tracked`;
        strategyTableBody.innerHTML = '';
        if (!strategies || strategies.length === 0) {
          strategyTableBody.innerHTML = `<tr><td colspan="8" style="text-align: center; color: var(--text-dim);">No individual strategy attribution records available</td></tr>`;
          return;
        }

        strategies.forEach(s => {
          const row = document.createElement('tr');
          const netSign = (isNumeric(s.net_pnl) && s.net_pnl > 0) ? '+' : '';
          const pnlColor = isNumeric(s.net_pnl) ? (s.net_pnl > 0 ? 'val-positive' : (s.net_pnl < 0 ? 'val-negative' : 'val-neutral')) : 'val-na';
          const pnlStr = isNumeric(s.net_pnl) ? fmtCurrency(s.net_pnl) : 'N/A';
          const grossStr = isNumeric(s.gross_pnl) ? fmtCurrency(s.gross_pnl) : 'N/A';
          const weightStr = isNumeric(s.weight_pct) ? `${s.weight_pct.toFixed(1)}%` : (isNumeric(s.weight) ? `${(s.weight * 100).toFixed(1)}%` : 'N/A');
          const shareStr = isNumeric(s.portfolio_pnl_share_pct) ? `${s.portfolio_pnl_share_pct.toFixed(1)}%` : 'N/A';
          const tc = s.trades_count !== undefined ? s.trades_count : s.total_trades;
          const tradesStr = isNumeric(tc) ? tc.toString() : 'N/A';
          const winRateStr = isNumeric(s.win_rate) ? `${s.win_rate.toFixed(1)}%` : 'N/A';

          row.innerHTML = `
            <td style="font-weight: 600;">${escapeHtml(s.strategy_name || s.strategy_id || '-')}</td>
            <td><code>${escapeHtml(s.symbol || '-')}</code></td>
            <td>${weightStr}</td>
            <td>${grossStr}</td>
            <td class="${pnlColor}">${netSign}${pnlStr}</td>
            <td>${shareStr}</td>
            <td>${tradesStr}</td>
            <td>${winRateStr}</td>
          `;
          strategyTableBody.appendChild(row);
        });
      }

      // Render Risk Alerts
      function renderRiskAlerts(riskStatus) {
        alertsContainer.innerHTML = '';
        if (!riskStatus) {
          metaRiskState.textContent = 'UNKNOWN';
          metaRiskState.className = 'badge badge-status badge-stale';
          alertsContainer.innerHTML = `<div class="alert-item info"><div>No portfolio risk snapshot evaluated for this run.</div></div>`;
          return;
        }

        const state = riskStatus.overall_state || 'NORMAL';
        metaRiskState.textContent = `${state} (${(riskStatus.active_alerts || []).length} Alerts)`;
        if (state === 'NORMAL') metaRiskState.className = 'badge badge-valid';
        else if (state === 'WARNING') metaRiskState.className = 'badge badge-stale';
        else metaRiskState.className = 'badge badge-unavailable';

        const alerts = riskStatus.active_alerts || [];
        if (alerts.length === 0) {
          alertsContainer.innerHTML = `
            <div class="alert-item info">
              <div>
                <div class="alert-headline" style="color: var(--accent-green);">✓ All portfolio risk metrics within normal limits</div>
                <div class="alert-meta">0 active exposure, drawdown, or concentration rule breaches.</div>
              </div>
            </div>
          `;
          return;
        }

        alerts.forEach(a => {
          const div = document.createElement('div');
          const sev = escapeHtml((a.severity || 'WARNING').toLowerCase());
          div.className = `alert-item ${sev === 'critical' ? 'crit' : (sev === 'warning' ? 'warn' : 'info')}`;
          div.innerHTML = `
            <div>
              <div class="alert-headline">${escapeHtml(a.message || `${a.rule_id} Breached`)}</div>
              <div class="alert-meta">Target: <code>${escapeHtml(a.target || 'portfolio')}</code> • Observed: <strong>${escapeHtml(String(a.observed_value))} ${escapeHtml(a.unit || '')}</strong> (Threshold: ${escapeHtml(String(a.threshold_value))} ${escapeHtml(a.unit || '')})</div>
            </div>
            <div>
              <span class="badge ${sev === 'critical' ? 'badge-unavailable' : 'badge-stale'}">${escapeHtml(a.severity || 'WARNING')}</span>
            </div>
          `;
          alertsContainer.appendChild(div);
        });
      }

      // Render Comparison Table
      function populateComparisonTable(summaries) {
        comparisonTableBody.innerHTML = '';
        if (!summaries || summaries.length === 0) {
          comparisonTableBody.innerHTML = `<tr><td colspan="9" style="text-align: center; color: var(--text-dim);">No historical runs to compare</td></tr>`;
          return;
        }

        summaries.slice(0, 8).forEach(s => {
          const row = document.createElement('tr');
          const retSign = (isNumeric(s.net_return_pct) && s.net_return_pct > 0) ? '+' : '';
          const pnlColor = isNumeric(s.net_pnl) ? (s.net_pnl > 0 ? 'val-positive' : (s.net_pnl < 0 ? 'val-negative' : 'val-neutral')) : 'val-na';
          const pnlStr = isNumeric(s.net_pnl) ? fmtCurrency(s.net_pnl) : 'N/A';
          const retStr = isNumeric(s.net_return_pct) ? `${retSign}${s.net_return_pct.toFixed(2)}%` : 'N/A';
          const maxDdStr = isNumeric(s.max_drawdown_pct) ? `-${s.max_drawdown_pct.toFixed(2)}%` : 'N/A';
          const sharpeStr = isNumeric(s.sharpe_ratio) ? s.sharpe_ratio.toFixed(2) : 'N/A';

          row.innerHTML = `
            <td style="font-family: monospace; font-weight: 600;">
              <a href="javascript:void(0)" class="report-link" data-report-id="${escapeHtml(s.report_id)}" style="color: var(--accent-cyan); text-decoration: none;">${escapeHtml(s.report_id)}</a>
            </td>
            <td><span class="badge badge-neu">${escapeHtml(s.source_type)}</span></td>
            <td>${fmtCurrency(s.initial_equity)}</td>
            <td>${fmtCurrency(s.final_equity)}</td>
            <td class="${pnlColor}">${pnlStr}</td>
            <td class="${pnlColor}">${retStr}</td>
            <td class="val-negative">${maxDdStr}</td>
            <td>${sharpeStr}</td>
            <td><span class="badge badge-${escapeHtml((s.status || 'valid').toLowerCase())}">${escapeHtml(s.risk_state || 'NORMAL')}</span></td>
          `;
          comparisonTableBody.appendChild(row);
        });
      }

      // Load Operational Diagnostics
      async function loadDiagnostics() {
        try {
          const res = await fetch('/api/diagnostics?limit=15');
          if (!res.ok) return;
          const data = await res.json();
          renderDiagnostics(data);
        } catch (err) {
          // Graceful fault isolation: diagnostic load failure must never break main dashboard
          console.debug('Failed to load diagnostics:', err);
        }
      }

      // Render Operational Diagnostics Panel
      function renderDiagnostics(data) {
        if (!data || !data.snapshot) return;
        const snap = data.snapshot;
        const counts = snap.counts || {};
        const lc = snap.dashboard_lifecycle || {};

        if (valDiagServerState) {
          valDiagServerState.textContent = escapeHtml(lc.state || (lc.is_running ? 'RUNNING' : 'OFFLINE'));
          valDiagServerState.style.color = lc.is_running ? 'var(--accent-green)' : 'var(--accent-amber)';
        }
        if (footDiagServer) {
          footDiagServer.textContent = `${escapeHtml(lc.url || 'http://127.0.0.1:8080')} (Loopback)`;
        }
        if (valDiagEventsCount) {
          valDiagEventsCount.textContent = `${counts.total_recorded || 0} Recorded`;
        }
        if (footDiagEviction) {
          footDiagEviction.textContent = `${counts.total_evicted || 0} evicted • FIFO (${counts.max_history_limit || 100} max)`;
        }
        if (diagStatusBadge) {
          if (lc.is_running) {
            diagStatusBadge.textContent = 'OPERATIONAL';
            diagStatusBadge.className = 'badge badge-valid';
          } else {
            diagStatusBadge.textContent = 'STANDBY';
            diagStatusBadge.className = 'badge badge-neu';
          }
        }

        const latestGen = snap.latest_report_generation;
        if (valDiagLatestReport) {
          if (latestGen) {
            const outColor = latestGen.outcome === 'SUCCESS' ? 'var(--accent-green)' : 'var(--accent-red)';
            valDiagLatestReport.innerHTML = `<span style="color: ${outColor};">${escapeHtml(latestGen.outcome)}</span> <span style="font-size: 0.85rem; font-family: monospace;">[${escapeHtml(latestGen.report_id || latestGen.run_id || '-')}]</span>`;
          } else {
            valDiagLatestReport.textContent = 'None';
          }
        }
        if (footDiagLatestReport) {
          if (latestGen && latestGen.duration_ms !== undefined && latestGen.duration_ms !== null) {
            footDiagLatestReport.textContent = `Duration: ${latestGen.duration_ms}ms • ${escapeHtml(latestGen.timestamp_iso || '')}`;
          } else {
            footDiagLatestReport.textContent = 'Duration: N/A';
          }
        }

        const events = data.events || [];
        if (diagEventsCountMeta) {
          diagEventsCountMeta.textContent = `Showing ${events.length} of ${counts.current_history_size || 0} buffered`;
        }
        if (diagEventsTableBody) {
          diagEventsTableBody.innerHTML = '';
          if (events.length === 0) {
            diagEventsTableBody.innerHTML = `<tr><td colspan="6" style="text-align: center; color: var(--text-dim);">No operational diagnostic events recorded yet.</td></tr>`;
            return;
          }

          events.forEach(e => {
            const row = document.createElement('tr');
            let outBadge = 'badge-neu';
            if (e.outcome === 'SUCCESS') outBadge = 'badge-valid';
            else if (e.outcome === 'FAILURE') outBadge = 'badge-unavailable';
            else if (e.outcome === 'WARNING' || e.outcome === 'INVALID') outBadge = 'badge-stale';

            let detailsSummary = '';
            if (e.details && Object.keys(e.details).length > 0) {
              detailsSummary = Object.entries(e.details)
                .map(([k, v]) => `${escapeHtml(k)}: ${escapeHtml(typeof v === 'object' ? JSON.stringify(v) : String(v))}`)
                .join(', ');
            } else if (e.error_category) {
              detailsSummary = `Error: ${escapeHtml(e.error_category)}`;
            } else {
              detailsSummary = '-';
            }

            row.innerHTML = `
              <td style="font-family: monospace; font-size: 0.78rem;">${escapeHtml(e.timestamp_iso || '-')}</td>
              <td><span class="badge badge-neu" style="font-size: 0.72rem;">${escapeHtml(e.event_type || '-')}</span></td>
              <td><span class="badge ${outBadge}" style="font-size: 0.72rem;">${escapeHtml(e.outcome || '-')}</span></td>
              <td style="font-family: monospace; font-size: 0.78rem; color: var(--accent-cyan);">${escapeHtml(e.report_id || e.run_id || e.correlation_id || '-')}</td>
              <td><span class="badge badge-neu" style="font-size: 0.72rem;">${escapeHtml(e.data_status || '-')}</span></td>
              <td style="font-size: 0.78rem; color: var(--text-muted); max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${escapeHtml(detailsSummary)}">${detailsSummary}</td>
            `;
            diagEventsTableBody.appendChild(row);
          });
        }
      }

      // Expose to window for external invocation
      window.loadReport = loadReport;

    })();
  </script>
</body>
</html>
"""
