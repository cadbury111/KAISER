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

## Operational Security & Dashboard Access Control (Phase 2, Step 56)

### Access Control Model
The Portfolio Analytics Command Center dashboard enforces strict read-only operation and defense-in-depth access controls:
- **Loopback-Only Interface**: The dashboard server strictly binds to local loopback addresses (`127.0.0.1`, `localhost`, `::1`). Any attempt to configure an external or non-loopback host (such as `0.0.0.0` or public IP interfaces) is rejected at startup with a `ValidationError` / `ConfigurationError`.
- **Token-Based Authorization**: When `DASHBOARD_AUTH_TOKEN` is configured or `DASHBOARD_REQUIRE_AUTH=true`:
  - Protected endpoints require an authorization token provided via HTTP header:
    - Standard header: `Authorization: Bearer <token>`
    - Alternative header: `X-API-Key: <token>`
  - Token comparison is strictly performed in constant time using Python standard library `secrets.compare_digest` to prevent timing side-channel attacks.
  - Missing or invalid credentials result in HTTP `401 Unauthorized` with `WWW-Authenticate: Bearer realm="KAISER Dashboard"`.
  - When `DASHBOARD_REQUIRE_AUTH=true` is set without configuring `DASHBOARD_AUTH_TOKEN`, the server fails closed, returning HTTP `403 Forbidden`.
  - Authentication failure never mutates order, ledger, portfolio, or session state.
- **Zero Credential Reflection**: Authentication tokens, secrets, and environment keys are never echoed in error responses, server access logs, or exception payloads. In settings serialization (`Settings.to_dict()`), tokens are masked (`***`).

### Endpoints and HTTP Method Safety
- **Read-Only Endpoints**:
  - `GET /` and `GET /index.html`: Single-page analytics interface shell.
  - `GET /api/status`: Public local health and invariant check (`execution_locked: true`, `read_only: true`).
  - `GET /api/paper/summary` & `GET /api/paper/status`: Paper trading session, execution status, order statistics, and position summary (protected by token authorization).
  - `GET /api/report/latest`, `/api/reports`, `/api/series/<report_id>`, `/api/diagnostics`, etc.: Read-only portfolio and diagnostic query endpoints.
- **Prohibited HTTP Methods**:
  - `POST`, `PUT`, `PATCH`, `DELETE`, `OPTIONS`, `TRACE` are rejected on all dashboard routes with HTTP `405 Method Not Allowed` and `Allow: GET, HEAD`.
  - There is **no dashboard mutation endpoint** for order submission, cancellation, execution, or ledger modification. All paper trading orders must be submitted programmatically through the Python `PaperTradingWorkflowCoordinator`.

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
| `DASHBOARD_PORT` | `8080` | Bind port (0 for ephemeral dynamic port) |
| `DASHBOARD_AUTH_TOKEN` | *(empty)* | Optional authorization secret token for protected dashboard endpoints |
| `DASHBOARD_REQUIRE_AUTH` | `false` | When true, strictly enforces authentication across data endpoints |

### Known Limitations & Security Notice
- **No TLS Termination**: The built-in dashboard server uses Python's standard library `http.server` without built-in TLS. It is strictly engineered for local loopback development and research. Remote exposure across untrusted networks is prohibited.
- **Not Enterprise Identity Management**: Token authorization provides a defense-in-depth barrier on the local host. It does not provide role-based access control (RBAC), OAuth2/OIDC federation, or multi-tenant user sessions.
- **Security Certification Notice**: A passing test suite demonstrates automated compliance with defined unit, integration, and invariant criteria, but does not constitute an external penetration test, formal security audit, or production security certification.

---

## Running Tests

Execute the deterministic test suite:
```bash
python -m pytest -q
```

