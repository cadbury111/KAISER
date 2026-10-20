# KAISER — Voice-First AI Trading Desk

> **Phase 1 / Step 01: Project Foundation & Architecture**

KAISER is a voice-first AI trading desk and trading intelligence system designed for high-conviction decision support, specialist multi-agent market analysis, deterministic risk verification, and human-in-the-loop trade execution.

---

## Current Status: Phase 1 / Step 01 (Foundation)

This phase establishes the production-grade foundation layer. In accordance with safety invariants, **no live trading, market feeds, broker connections, voice models, or execution capabilities are active**.

### Core Foundation Elements
- **Configuration Management**: Strongly typed, environment-driven configuration with standard `.env` parsing and safe defaults.
- **Centralized Logging**: Structured logging format with timestamps, log levels, and module namespaces (`kaiser.*`) across console and file outputs.
- **Domain Exception Hierarchy**: Extensible base error hierarchy rooted at `KaiserError`.
- **Architectural Safety Invariants**: `execution_locked = True`, `trading_engine = "not initialized"`, and `paper_trading_only = True`.
- **System Bootstrap & Entry Point**: Clean bootstrap sequence (`app/bootstrap.py`) and CLI entry point (`run.py`).
- **Deterministic Test Suite**: Comprehensive tests covering configuration, logging, safety invariants, exceptions, and startup.

---

## Architectural Safety Rules

KAISER enforces strict architectural guardrails:

1. **No Direct Execution**: KAISER market analysis must never directly trigger a trade.
2. **Deterministic Risk Gate**: All order intents must pass through an independent, deterministic risk validation gate.
3. **Mandatory Human Approval**: Real-money trade execution strictly requires explicit human sign-off.
4. **Isolated Risk Logic**: Risk rules and position limits are deterministic and independently testable.
5. **Zero Secret Leakage**: API credentials and secrets are loaded from environment variables and must never be committed.
6. **Execution Locked by Default**: The order execution engine remains locked until explicitly unlocked by validated processes.
7. **No Mock Live Claims**: Systems never simulate or misrepresent real broker connections or live market states.

---

## Directory Structure

```text
kaiser/
│
├── app/
│   ├── __init__.py               # Top-level KAISER package metadata
│   ├── bootstrap.py              # Application bootstrap & status lifecycle
│   │
│   ├── core/                     # Core foundation infrastructure
│   │   ├── __init__.py
│   │   ├── config.py             # Environment settings & .env parser
│   │   ├── logging_config.py     # Centralized logging setup
│   │   └── exceptions.py         # Domain exception hierarchy
│   │
│   ├── orchestrator/             # Master orchestrator & agent routing
│   │   └── __init__.py
│   │
│   ├── agents/                   # Specialist trading & intelligence agents
│   │   └── __init__.py
│   │
│   ├── voice/                    # English + Tanglish voice interface & NLP
│   │   └── __init__.py
│   │
│   ├── market/                   # Market data ingestion & feed management
│   │   └── __init__.py
│   │
│   ├── analysis/                 # Technical, fundamental & quant analysis
│   │   └── __init__.py
│   │
│   ├── risk/                     # Deterministic pre-trade risk gate
│   │   └── __init__.py
│   │
│   ├── execution/                # Order routing & broker interface (locked)
│   │   └── __init__.py
│   │
│   ├── portfolio/                # Holdings, position & PnL monitoring
│   │   └── __init__.py
│   │
│   ├── ledger/                   # Immutable trade intent & audit trail
│   │   └── __init__.py
│   │
│   └── ui/                       # Trading Command Center UI adapters
│       └── __init__.py
│
├── tests/
│   ├── __init__.py
│   └── test_foundation.py        # Complete foundation layer verification suite
│
├── config/                       # Application configuration schemas
│   └── .gitkeep
│
├── data/                         # Local storage & data tiers
│   ├── raw/
│   │   └── .gitkeep
│   ├── processed/
│   │   └── .gitkeep
│   └── cache/
│       └── .gitkeep
│
├── logs/                         # Runtime logs (git-ignored)
│   └── .gitkeep
│
├── .env.example                  # Environment configuration template
├── .gitignore                    # Git ignore specifications
├── README.md                     # Architecture documentation
├── pyproject.toml                # Project packaging specification (PEP 517/518/621)
├── requirements.txt              # Core production runtime dependencies
├── requirements-dev.txt          # Development & test dependencies
├── requirements-stt.txt          # Optional offline Speech-to-Text dependencies
└── run.py                        # Application entry point
```

---

## Getting Started

### Prerequisites
- Python 3.11+
- Virtual environment (recommended)

### 1. Configure Environment
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```
Default configuration values use safe local/mock providers and keep all execution engines locked.

### 2. Install Dependencies

#### Core Production Runtime
Install direct runtime dependencies:
```bash
pip install -r requirements.txt
```

#### Development & Testing
To run the automated test suite, install development dependencies:
```bash
pip install -r requirements-dev.txt
```

#### Optional Offline Speech-to-Text (STT)
To enable offline local speech transcription using `faster-whisper`:
1. Install the STT requirements:
   ```bash
   pip install -r requirements-stt.txt
   ```
2. Download CTranslate2-compatible Whisper weights (e.g. `whisper-base`) manually and place them in:
   ```text
   data/models/whisper/whisper-base/
   ```
3. Enable in `.env`:
   ```bash
   STT_PROVIDER=local_whisper
   STT_MODEL_PATH=data/models/whisper/whisper-base
   STT_MODEL_ID=whisper-base
   ```
*(Note: Per KAISER safety invariants, automatic internet model weight downloads are strictly disabled).*

### 3. Bootstrap the System
Run the KAISER entry point:
```bash
python run.py
```

Expected startup output:
```text
2026-10-01 10:24:32 | INFO     | kaiser.bootstrap | Bootstrapping KAISER foundation layer...
2026-10-01 10:24:32 | INFO     | kaiser.bootstrap | System initialized successfully in 'development' mode.
2026-10-01 10:24:32 | INFO     | kaiser.bootstrap | Voice-first architecture: ready
2026-10-01 10:24:32 | INFO     | kaiser.bootstrap | Trading engine: not initialized
2026-10-01 10:24:32 | INFO     | kaiser.bootstrap | Execution engine: locked
KAISER
System initialized successfully.
Environment: development
Voice-first architecture: ready
Trading engine: not initialized
Execution engine: locked
```

---

## Operational Security & Dashboard Access Control (Phase 2, Steps 56, 57, & 58)

### Access Control Model
The Portfolio Analytics Command Center dashboard enforces strict read-only operation and defense-in-depth access controls:
- **Loopback-Only Interface**: The dashboard server strictly binds to local loopback addresses (`127.0.0.1`, `localhost`, `::1`). Any attempt to configure an external or non-loopback host (such as `0.0.0.0` or public IP interfaces) is rejected at startup with a `ValidationError` / `ConfigurationError`.
- **Host & Port Validation Rules**:
  - **Host Validation**: Only loopback identifiers (`127.0.0.1`, `localhost`, `::1`) are accepted.
  - **Port Validation**: Ports must be integers in the range `0`–`65535`. Ports outside this range (e.g., `-1`, `65536`) or non-integer representations raise `ConfigurationError`.
  - **Port 0 Ephemeral Binding Policy**: Port `0` enables the operating system to dynamically assign an unused ephemeral port. This capability is supported in `development` and `test` environments to prevent socket port collision across concurrent test fixtures. However, port `0` is strictly prohibited in `production` environments (`APP_ENV=production`), failing startup with `ConfigurationError` to guarantee deterministic, monitored port binding.
  - **Strict Boolean Parsing**: Environment boolean flags (`DASHBOARD_ENABLED`, `DASHBOARD_REQUIRE_AUTH`, etc.) accept standard explicit tokens (`true`, `false`, case-insensitive). Ambiguous strings (e.g. `yes`, `no`, `1`, `0`, `enable`, `disabled`) raise `ConfigurationError` rather than silently defaulting or misinterpreting configuration intent.
- **Token-Based Authorization**: When `DASHBOARD_AUTH_TOKEN` is configured or `DASHBOARD_REQUIRE_AUTH=true`:
  - Protected endpoints require an authorization token provided via HTTP header:
    - Standard header: `Authorization: Bearer <token>`
    - Alternative header: `X-API-Key: <token>`
  - When **both** `Authorization` and `X-API-Key` headers are supplied, **both must match** the server authentication token. Conflicting or mismatched headers are rejected with HTTP `401 Unauthorized` to prevent ambiguous header bypasses.
  - Token comparison is strictly performed in constant time using Python standard library `secrets.compare_digest` to prevent timing side-channel attacks.
  - Missing or invalid credentials result in HTTP `401 Unauthorized` with `WWW-Authenticate: Bearer realm="KAISER Dashboard"`.
  - Empty or whitespace-only tokens in headers (`Bearer ` or `X-API-Key: `) are rejected with HTTP `401 Unauthorized`.
  - Authentication credentials passed via URL query parameters (`?token=...`, `?api_key=...`, `?auth_token=...`) are strictly ignored and rejected with HTTP `401 Unauthorized`.
  - When `DASHBOARD_REQUIRE_AUTH=true` is set without configuring `DASHBOARD_AUTH_TOKEN`, the server fails closed, returning HTTP `403 Forbidden`.
  - Authentication failure never mutates order, ledger, portfolio, session, or execution state.
- **Unauthenticated Local Development Mode**: Permitted **only** when `DASHBOARD_REQUIRE_AUTH=false` and `DASHBOARD_AUTH_TOKEN` is empty/unset, restricted strictly to local loopback interfaces. In this mode, local development tools may query dashboard endpoints without credentials.
- **Zero Credential Reflection**: Authentication tokens, secrets, and environment keys are never echoed in error responses, server access logs, or exception payloads. Server request logs redact any query parameters containing secret identifiers. In settings serialization (`Settings.to_dict()`), tokens and API keys are masked (`***`).
- **Server Header Hardening**: Server response headers suppress detailed runtime and OS version strings (`Server: KAISER-Dashboard`).

### Endpoint Inventory and Authorization Classification
| Route Pattern | Method | Classification | Description & Sensitivity |
| :--- | :--- | :--- | :--- |
| `/` and `/index.html` | `GET`, `HEAD` | **Public** | Static SPA dashboard shell; contains no financial or portfolio data. |
| `/api/status` | `GET`, `HEAD` | **Public** | Minimal operational health check (`status`, `execution_locked: true`, `read_only: true`). |
| `/api/paper/summary`, `/api/paper/status` | `GET`, `HEAD` | **Protected** | Sensitive paper portfolio, positions, cash balance, orders, and execution state. |
| `/api/report/latest`, `/api/reports`, `/api/report/<id>` | `GET`, `HEAD` | **Protected** | Sensitive portfolio performance metrics, attribution, risk measures, and trade history. |
| `/api/series/<report_id>`, `/api/compare` | `GET`, `HEAD` | **Protected** | Sensitive equity curves, drawdown series, and multi-run comparative metrics. |
| `/api/diagnostics` | `GET`, `HEAD` | **Protected** | Sensitive operational diagnostics snapshot, system telemetry, and audit event logs. |
| `/api/data-quality`, `/api/data-provenance`, `/api/data-lineage` | `GET`, `HEAD` | **Protected** | Sensitive market data quality evaluations, anomaly logs, and data lineage audit graphs. |
| `/api/analysis-runs`, `/api/analysis-comparisons` | `GET`, `HEAD` | **Protected** | Sensitive reproducible analysis run records and experiment comparisons. |
| `/api/evidence-syntheses`, `/api/opportunity-*` | `GET`, `HEAD` | **Protected** | Sensitive opportunity assessments, prioritization records, and investigation queues. |
| `/api/investigation-*`, `/api/discrepancy-*`, `/api/historical-*` | `GET`, `HEAD` | **Protected** | Sensitive human investigation reviews, outcomes, audit reports, and consistency metrics. |
| `/api/unknown_route` (unrecognized API paths) | `GET`, `HEAD` | **Protected** | Requires auth when auth is enforced (401); returns 404 when authenticated or in local dev mode. |
| *All routes* | `POST`, `PUT`, `PATCH`, `DELETE`, `OPTIONS`, `TRACE` | **Prohibited** | Strictly rejected with HTTP `405 Method Not Allowed` (`Allow: GET, HEAD`). |

### Prohibited HTTP Methods and Read-Only Guarantees
- The dashboard is **strictly read-only**.
- Any attempt to send `POST`, `PUT`, `PATCH`, `DELETE`, `OPTIONS`, or `TRACE` requests is rejected with HTTP `405 Method Not Allowed` and `Allow: GET, HEAD`.
- There is **no HTTP mutation endpoint** for order submission, cancellation, execution, position modification, or ledger updates. All paper trading orders must be submitted programmatically through the Python `PaperTradingWorkflowCoordinator`.

### Paper Trading vs. Live Trading Boundary
- **Simulated Paper Trading Only**: Paper trading executes exclusively against simulated order models, internal cash ledgers, and in-memory/SQLite accounting.
- **Global Execution Lock Active**: `Settings.execution_locked` remains permanently `True`.
- **No Broker Connection**: `Settings.broker_provider` remains `"none"`. Live broker adapters are never initialized, contacted, or loaded.
- **Voice Safety Boundary**: Spoken trading commands (e.g. "buy", "sell", "order", "execute trade") are intercepted by the `VoiceCommandIntentResolver` as `PROHIBITED_TRADING_COMMAND` directives and rejected; voice intents have zero execution authority.

### Configuration Environment Variables
| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `DASHBOARD_ENABLED` | `false` | Enable local dashboard HTTP server |
| `DASHBOARD_HOST` | `127.0.0.1` | Bind address (must be loopback: `127.0.0.1`, `localhost`, `::1`) |
| `DASHBOARD_PORT` | `8080` | Bind port (0 for ephemeral dynamic port in dev/test) |
| `DASHBOARD_AUTH_TOKEN` | *(empty)* | Optional authorization secret token for protected dashboard endpoints |
| `DASHBOARD_REQUIRE_AUTH` | `false` | When true, strictly enforces authentication across data endpoints |

### Known Limitations & Security Notice
- **No TLS Termination**: The built-in dashboard server uses Python's standard library `http.server` without built-in TLS. It is strictly engineered for local loopback development and research. Remote exposure across untrusted networks is prohibited.
- **Not Enterprise Identity Management**: Token authorization provides a defense-in-depth barrier on the local host. It does not provide role-based access control (RBAC), OAuth2/OIDC federation, or multi-tenant user sessions.
- **Security Certification Notice**: A passing test suite demonstrates automated compliance with defined unit, integration, and invariant criteria, but does not constitute an external penetration test, formal security audit, or production security certification.

---

## Paper Trading Simulation Reliability & Deterministic Replay (Phase 2, Step 59)

### Deterministic Replay Validation
KAISER enforces reproducible, bit-level deterministic paper trading simulations across market data replays and portfolio accounting:
- **Strict Chronological Ordering**: `ReplayMarketFeed(strict_chronological=True)` validates that historical or recorded market data timestamps are strictly monotonically non-decreasing. Out-of-order records raise `ValidationError` and increment `_records_rejected`.
- **Deterministic Fingerprints**:
  - `compute_input_digest()`: Computes an SHA-256 digest of normalized market data inputs (`symbol`, `timestamp`, `bid`, `ask`, `last`, `volume`).
  - `compute_financial_digest()`: Computes an SHA-256 digest over portfolio cash, equity, realized/unrealized P&L, cumulative fees, sorted position holdings, and ledger entries.
  - Repeated simulation sessions running over identical input events and initial state produce bit-level identical financial outcomes and matching SHA-256 fingerprints.
- **Input Integrity & Non-Finite Value Rejection**:
  - Non-finite numeric values (`NaN`, `Inf`, `-Inf`) are rejected at the data feed, order submission, fill execution, and ledger entry boundaries.
  - Inverted quote spreads (`bid > ask`) or negative/zero prices are rejected safely.
  - Malformed or out-of-order replay events are rejected without halting feed iteration or corrupting state.
- **Accounting & Persistence Invariants**:
  - **Rejected Order Invariance**: Rejected orders (e.g. risk breaches, non-finite limit prices) leave portfolio cash, positions, and ledger entries completely unmodified.
  - **Duplicate Order Idempotency**: Resubmission of an already accepted/filled `order_id` is rejected as duplicate without double-counting financial effects.
  - **Long-Only Enforcement**: Short-selling is strictly rejected (`INSUFFICIENT_POSITION`).
  - **Cost Basis & Realized P&L**: Weighted average cost basis is maintained across incremental buys; full and partial sells compute exact realized P&L, clean up closed positions, and update cash atomically.
  - **Ledger Restart Recovery**: Full crash recovery from SQLite ledger accurately reconstructs cash, positions, fees, and realized P&L; subsequent recovery passes are idempotent.
  - **Transaction Atomicity**: Database failures during fill persistence roll back cleanly, preserving ledger balance consistency.

---

## Running Tests

Execute the full deterministic test suite:
```bash
python -m pytest -q
```

Execute the Step 57 Security Configuration Audit & Endpoint Authorization test suite:
```bash
python -m pytest tests/test_phase_2_step_57_security_audit.py -v
```

Execute the Step 58 Security Regression & Configuration Matrix test suite:
```bash
python -m pytest tests/test_phase_2_step_58_security_matrix.py -v
```

Execute the Step 59 Paper Trading Simulation Reliability & Deterministic Replay test suite:
```bash
python -m pytest tests/test_phase_2_step_59_simulation_replay.py -v
```

