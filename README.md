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

## Running Tests

Execute the deterministic test suite:
```bash
python -m pytest -q
```

