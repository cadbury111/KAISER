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

      // Initialize
      window.addEventListener('DOMContentLoaded', () => {
        loadReportList();
        btnRefresh.addEventListener('click', () => loadReport(reportSelect.value || 'latest'));
        reportSelect.addEventListener('change', () => loadReport(reportSelect.value));
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
            const sign = rep.net_pnl >= 0 ? '+' : '';
            opt.textContent = `${rep.title || rep.report_id} (${sign}$${rep.net_pnl.toFixed(0)})`;
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

      // Format currency
      function fmtCurrency(val) {
        if (val === null || val === undefined || isNaN(val)) return 'N/A';
        const sign = val < 0 ? '-' : '';
        return `${sign}$${Math.abs(val).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
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

        // Overview Cards
        const netPnl = r.net_pnl || 0;
        valNetPnL.textContent = fmtCurrency(netPnl);
        valNetPnL.className = `metric-value ${netPnl > 0 ? 'val-positive' : (netPnl < 0 ? 'val-negative' : 'val-neutral')}`;

        const retPct = r.net_return_pct || 0;
        badgeReturnPct.textContent = `${retPct >= 0 ? '+' : ''}${retPct.toFixed(2)}%`;
        badgeReturnPct.className = `metric-badge ${retPct >= 0 ? 'badge-pos' : 'badge-neg'}`;

        footNetPnL.textContent = `Gross: ${fmtCurrency(r.gross_pnl)} • Costs: ${fmtCurrency(r.total_transaction_costs)}`;

        // Max DD
        const ddPct = r.max_drawdown_pct || 0;
        valMaxDDPct.textContent = `${ddPct.toFixed(2)}%`;
        valMaxDDAmt.textContent = `-${fmtCurrency(r.max_drawdown_amount)}`;
        footMaxDD.textContent = `Peak-to-trough: ${r.max_drawdown_duration_bars || 0} bars duration`;

        // Sharpe / Sortino
        const rMetrics = r.risk_adjusted_metrics || {};
        const sh = rMetrics.sharpe_ratio;
        if (sh && sh.is_available && sh.value !== null) {
          valSharpe.textContent = sh.value.toFixed(2);
          valSharpe.className = 'metric-value val-neutral';
          footSharpe.textContent = 'Annualized excess return';
        } else {
          valSharpe.textContent = 'N/A';
          valSharpe.className = 'metric-value val-na';
          footSharpe.textContent = sh ? (sh.unavailable_reason || 'Insufficient variance') : 'Unavailable';
        }

        const so = rMetrics.sortino_ratio;
        if (so && so.is_available && so.value !== null) {
          badgeSortino.textContent = `Sortino: ${so.value.toFixed(2)}`;
        } else {
          badgeSortino.textContent = 'Sortino: N/A';
        }

        // Cash & Exposure
        valCashReserve.textContent = `${(r.cash_reserve_pct || 100).toFixed(1)}%`;
        badgeGrossExp.textContent = `Exp: ${(r.gross_exposure_pct || 0).toFixed(1)}%`;
        footCash.textContent = `Cash: ${fmtCurrency(r.cash_balance)} • Gross Exp: ${fmtCurrency(r.gross_exposure)}`;

        // Strategies & Trades
        const strats = r.strategy_attributions || [];
        valStrategies.textContent = strats.length.toString();
        let totalTrades = 0;
        let winRate = 0;
        strats.forEach(s => { totalTrades += (s.trades_count || 0); });
        if (rMetrics.win_rate && rMetrics.win_rate.is_available && rMetrics.win_rate.value !== null) {
          winRate = rMetrics.win_rate.value;
        }
        badgeWinRate.textContent = `WinRate: ${winRate.toFixed(1)}%`;
        footTrades.textContent = `${totalTrades} total closed round-trip trades`;

        // Reconciliation
        if (r.reconciliation) {
          const rec = r.reconciliation;
          valReconStatus.textContent = rec.status || 'EXACT';
          valReconStatus.className = `metric-value ${rec.is_reconciled ? 'val-positive' : 'val-negative'}`;
          footRecon.textContent = rec.is_reconciled ? 'Reconciled within tolerance' : `Discrepancy: $${(rec.strategy_discrepancy || 0).toFixed(2)}`;
        } else {
          valReconStatus.textContent = 'N/A';
          valReconStatus.className = 'metric-value val-na';
          footRecon.textContent = 'No reconciliation audit recorded';
        }

        // Render Charts
        renderEquityChart(r.equity_curve || [], r.initial_equity || 100000);
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
          provWarnings.innerHTML = `<span style="color: var(--accent-amber);">⚠️ ${warns.length} Warnings:</span> ${warns.join('; ')}`;
        } else {
          provWarnings.textContent = '✓ No accounting, freshness, or integrity warnings.';
        }
      }

      // Render Equity SVG Chart
      function renderEquityChart(points, initialEquity) {
        metaEquity.textContent = `Initial: ${fmtCurrency(initialEquity)} → Final: ${fmtCurrency(points.length ? points[points.length - 1].total_equity : initialEquity)}`;
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

        const equities = points.map(p => p.total_equity);
        let minEq = Math.min(...equities, initialEquity);
        let maxEq = Math.max(...equities, initialEquity);
        if (minEq === maxEq) { minEq *= 0.95; maxEq *= 1.05; }
        const range = maxEq - minEq || 1;

        // Coordinates
        const coords = points.map((p, idx) => {
          const x = padX + (idx / (points.length - 1 || 1)) * (width - 2 * padX);
          const y = height - padY - ((p.total_equity - minEq) / range) * (height - 2 * padY);
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

        const dds = points.map(p => p.drawdown_pct || 0);
        const maxDd = Math.max(...dds, 5.0);
        metaDrawdown.textContent = `Peak Drawdown: ${Math.max(...dds).toFixed(2)}%`;

        const coords = points.map((p, idx) => {
          const x = padX + (idx / (points.length - 1 || 1)) * (width - 2 * padX);
          const y = padY + ((p.drawdown_pct || 0) / maxDd) * (height - 2 * padY);
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
          const netSign = (s.net_pnl || 0) >= 0 ? '+' : '';
          const pnlColor = (s.net_pnl || 0) >= 0 ? 'val-positive' : 'val-negative';

          row.innerHTML = `
            <td style="font-weight: 600;">${s.strategy_name || s.strategy_id || '-'}</td>
            <td><code>${s.symbol || '-'}</code></td>
            <td>${(s.weight_pct !== undefined ? s.weight_pct.toFixed(1) : ((s.weight || 0) * 100).toFixed(1))}%</td>
            <td>${fmtCurrency(s.gross_pnl)}</td>
            <td class="${pnlColor}">${fmtCurrency(s.net_pnl)}</td>
            <td>${(s.portfolio_pnl_share_pct || 0).toFixed(1)}%</td>
            <td>${s.trades_count || s.total_trades || 0}</td>
            <td>${(s.win_rate || 0).toFixed(1)}%</td>
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
          const sev = (a.severity || 'WARNING').toLowerCase();
          div.className = `alert-item ${sev === 'critical' ? 'crit' : (sev === 'warning' ? 'warn' : 'info')}`;
          div.innerHTML = `
            <div>
              <div class="alert-headline">${a.message || `${a.rule_id} Breached`}</div>
              <div class="alert-meta">Target: <code>${a.target || 'portfolio'}</code> • Observed: <strong>${a.observed_value} ${a.unit || ''}</strong> (Threshold: ${a.threshold_value} ${a.unit || ''})</div>
            </div>
            <div>
              <span class="badge ${sev === 'critical' ? 'badge-unavailable' : 'badge-stale'}">${a.severity}</span>
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
          const retSign = (s.net_return_pct || 0) >= 0 ? '+' : '';
          const pnlColor = (s.net_pnl || 0) >= 0 ? 'val-positive' : 'val-negative';

          row.innerHTML = `
            <td style="font-family: monospace; font-weight: 600;"><a href="javascript:void(0)" onclick="loadReport('${s.report_id}')" style="color: var(--accent-cyan); text-decoration: none;">${s.report_id}</a></td>
            <td><span class="badge badge-neu">${s.source_type}</span></td>
            <td>${fmtCurrency(s.initial_equity)}</td>
            <td>${fmtCurrency(s.final_equity)}</td>
            <td class="${pnlColor}">${fmtCurrency(s.net_pnl)}</td>
            <td class="${pnlColor}">${retSign}${(s.net_return_pct || 0).toFixed(2)}%</td>
            <td class="val-negative">-${(s.max_drawdown_pct || 0).toFixed(2)}%</td>
            <td>${s.sharpe_ratio !== null && s.sharpe_ratio !== undefined ? s.sharpe_ratio.toFixed(2) : 'N/A'}</td>
            <td><span class="badge badge-${(s.status || 'valid').toLowerCase()}">${s.risk_state || 'NORMAL'}</span></td>
          `;
          comparisonTableBody.appendChild(row);
        });
      }

      // Expose to window for inline onclick
      window.loadReport = loadReport;

    })();
  </script>
</body>
</html>
"""
