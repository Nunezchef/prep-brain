# Prep-Brain

Prep-Brain is a Telegram-first, always-on kitchen operations agent.

- Runs continuously in the background
- Keeps Telegram quiet by default
- Routes explicit ops intents to DB writes (bypasses RAG)
- Keeps restaurant data separate from reference/general knowledge

## Architecture

```text
prep_brain/
  app.py
  config.py
  logging.py
  db/
  telegram/
  autonomy/
  rag/
  llm/
  ops/
  dashboard/
services/
  (compatibility + legacy modules during migration)
```

`prep_brain/*` is the canonical target structure. `services/*` still provides compatibility while migration continues.

## Core Rules

- Autonomy is always on when bot process is running.
- Telegram is an ops console: silent on success, concise on alerts/actions.
- House/restaurant documents may populate operational DB.
- General/reference/web knowledge stays RAG-only and never auto-promotes to operational recipe tables.
- House recipe responses must be complete and source-faithful (no truncation/summarization of ingredient sections).

## Configuration

Single config interface: `prep_brain.config`.

Sources:
- `config.yaml`
- `.env` overrides (loaded once)

Important env vars:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_ALLOWED_USER_IDS`
- `PREP_BRAIN_DB_PATH` (optional absolute DB override)
- `OLLAMA_URL`

## Command Surface

Canonical command definitions live in `services/commands_registry.py`.

- `/help` and `/commands` are generated from the registry.
- Unknown commands return: `Unknown command. /help`

Full grouped list: see `COMMANDS.md`.

## Dev Checklist

1. Create virtualenv and install deps.
2. Copy `.env.example` to `.env`.
3. Start bot.
4. Start dashboard.
5. Run lint/format/tests.

### Run

```bash
python -m prep_brain.app
streamlit run prep_brain/dashboard/app.py
```

### Quality Gates

```bash
ruff check .
black .
pytest
```

## Verification Quick Pass

1. Start bot and run `/status`.
2. Run `/autonomy` and confirm tick updates.
3. Upload a document and confirm ingest is queued + visible via `/ingests`.
4. Run: `update the price of braised ribs to 4.32 dollars a portion` and verify DB update confirmation.
5. Query a known house recipe and verify full multi-section output.

## Security

- Never commit real secrets.
- `.env` is ignored; use `.env.example` placeholders.
- Runtime logs and data are local-only (`logs/`, `data/`).
