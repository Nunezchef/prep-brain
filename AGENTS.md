# AGENTS.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

Prep-Brain is a Telegram-first kitchen operations agent that runs continuously in the background. It manages recipes, inventory, prep lists, and vendor ordering for professional kitchens. The system uses RAG for knowledge retrieval and routes explicit operational commands directly to SQLite DB writes.

## Development Commands

### Run the application
```bash
# Bot + Telegram (main entry point)
python -m prep_brain.app

# Dashboard (Streamlit)
streamlit run prep_brain/dashboard/app.py

# Backend API (FastAPI)
uvicorn services.dashboard_api:app --host 0.0.0.0 --port 8000

# Frontend (Vite/React)
cd frontend && npm run dev

# Full stack via Docker
./run_docker.sh
```

### Quality Gates
```bash
# Lint (auto-fix enabled)
ruff check . --fix

# Format
black .

# Tests (all)
pytest

# Single test file
pytest tests/test_commands.py

# Single test function
pytest tests/test_commands.py::test_function_name -v

# Type checking
mypy prep_brain services
```

### Pre-commit
Pre-commit hooks run ruff (lint+format) and black automatically. Install with:
```bash
pre-commit install
```

## Architecture

### Migration Structure
`prep_brain/` is the canonical target package structure, but most implementation lives in `services/` during an ongoing migration. The `prep_brain/*` modules re-export from `services/*`:

```
prep_brain/           # Canonical interface (re-exports during migration)
  app.py              # Entry point → runs services.bot.run_bot()
  config.py           # Central config loader (THE source of truth)
  telegram/           # → services.bot
  ops/                # → services.ops_router
  rag/                # → services.rag
  llm/                # → services.brain (Ollama wrapper)
  db/                 # → services.memory

services/             # Active implementation modules
  bot.py              # Telegram bot runner
  brain.py            # LLM/RAG query handling
  autonomy.py         # Background task processing
  commands_registry.py # Command definitions (source of truth for /help)
  commands.py         # Command handlers
  ops_router.py       # Intent detection + DB action routing
  recipes.py          # Recipe CRUD
  inventory.py        # Inventory management
  prep.py, prep_list.py # Prep board logic
  costing.py          # Recipe cost calculations
  rag.py              # RAG engine (ChromaDB)
  memory.py           # SQLite connection + schema
```

### Key Patterns

**Config Interface**: Always use `prep_brain.config.load_config()` for configuration. It merges `config.yaml` with `.env` overrides. Do not read config files directly.

**Command Registry**: All Telegram commands are defined in `services/commands_registry.py`. This is the single source of truth used by `/help` and `/commands`. When adding commands, add the `CommandSpec` there.

**Ops vs RAG Routing**: Explicit operational intents (price updates, inventory changes, etc.) bypass RAG and route directly to DB actions via `services.ops_router`. Knowledge queries go through RAG.

**Knowledge Tiers**:
- `restaurant_recipes` (Tier 1): Operational authority, can be promoted to recipe tables
- `general_knowledge` (Tier 3): RAG-only, never auto-promotes to operational data

**Response Style**: Default to concise output (3-6 lines). Telegram is treated as an ops console, not a chat interface. See `SYSTEM_PROMPT.md` for LLM behavior guidelines.

### Data Flow
1. Telegram message → `services.bot` → command parsing
2. If slash command → `services.commands` handler
3. If natural language:
   - Check for ops intent → `ops_router.detect_ops_intent()` → DB action
   - Otherwise → RAG query via `services.brain.ask_brain()`

### Database
SQLite database at `data/memory.db`. Schema defined in `prep_brain/db/schema.sql`. Access via `services.memory.get_conn()`.

### Document Ingestion
Documents uploaded to Telegram queue ingest jobs processed by the autonomy loop. Pipeline: extract text → chunk → embed → index in ChromaDB → optionally extract recipes to drafts.

## Infrastructure Components

### Observability
- **Correlation IDs**: `prep_brain.logging` provides `get_correlation_id()`, `correlation_context()` for request tracing
- **Health Checks**: `services.health` has `get_system_health()` checking SQLite, Ollama, ChromaDB, autonomy
- **Metrics**: `services.metrics` provides counters, gauges, histograms with `record_llm_call()`, `record_command()`, etc.

### Reliability
- **Retry Logic**: `services.retry` provides `@retry_with_backoff` decorator with exponential backoff
- **Soft Deletes**: `services.soft_delete` provides `soft_delete()`, `restore()`, `list_deleted()` for recipes, inventory, vendors

### Operations
- **Database Backup**: `scripts/backup_db.py` for daily SQLite backups with rotation
  ```bash
  python scripts/backup_db.py           # Create backup
  python scripts/backup_db.py --list    # List backups
  python scripts/backup_db.py --restore <file>  # Restore
  ```

## Testing

Tests are in `tests/` with naming pattern `test_*.py`. Most tests are unit tests that mock external dependencies (Ollama, Telegram). Use pytest fixtures for database setup.

Key test files:
- `test_commands.py` / `test_commands_registry.py` - Command routing
- `test_ops_router.py` / `test_ops_layer.py` - Ops intent detection
- `test_brain_citations.py` - RAG grounding
- `test_recipes.py` - Recipe CRUD
- `test_autonomy_*.py` - Background processing

## Environment Variables

Required in `.env`:
- `TELEGRAM_BOT_TOKEN` - Telegram bot token
- `TELEGRAM_ALLOWED_USER_IDS` - Comma-separated user IDs

Optional:
- `OLLAMA_URL` - Override Ollama endpoint (default: http://localhost:11434)
- `PREP_BRAIN_DB_PATH` - Override database path
- `PREP_BRAIN_CONFIG` - Override config file path
