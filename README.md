# Prep-Brain

> Local, voice-first kitchen assistant powered by Telegram + Ollama + RAG.

## What Is Working Right Now

These are the features currently implemented in this repo:

| Function | Status | Where |
|---|---|---|
| Telegram text chat -> Ollama reply | Working | `services/bot.py` + `services/brain.py` |
| Voice note transcription (`ffmpeg` + `whisper-cli`) -> Ollama reply | Working | `services/bot.py` + `services/transcriber.py` |
| Conversation memory (users/sessions/messages in SQLite) | Working | `services/memory.py` |
| RAG ingest/query with source activation controls | Working | `services/rag.py` |
| Telegram document upload -> knowledge ingestion | Working | `services/bot.py` |
| Streamlit dashboard controls and monitoring | Working | `dashboard/app.py` + `dashboard/pages/*` |

Dashboard functions currently available:

- Bot start/stop/restart
- Ollama start/status checks
- Log viewing
- Session history browsing/clearing
- Manual RAG source upload/manage (enable/disable/delete)
- Config and system prompt editing

## Stack

- Python
- `python-telegram-bot`
- Ollama (local LLM backend)
- ChromaDB + `sentence-transformers`
- Streamlit
- SQLite
- `ffmpeg` + `whisper-cli` for audio transcription

## Project Layout

```text
prep-brain/
├── services/
│   ├── bot.py           # Telegram handlers (text, voice, documents)
│   ├── brain.py         # Ollama chat client + optional RAG context injection
│   ├── memory.py        # SQLite memory/session storage
│   ├── rag.py           # Ingestion/query engine (ChromaDB)
│   └── transcriber.py   # whisper-cli wrapper
├── dashboard/
│   ├── app.py           # Main Streamlit control panel
│   └── pages/           # Sessions, Test Lab, Settings, Knowledge
├── scripts/
│   ├── verify_rag.py
│   └── reingest_flavor_bible.py
├── config.yaml
├── .env.example
└── requirements.txt
```

## Quick Start

### Prerequisites

- Python 3.10+
- `ffmpeg` installed and available in PATH
- `whisper-cli` installed and available in PATH
- Ollama installed locally

### 1) Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Configure environment

```bash
cp .env.example .env
```

Set `TELEGRAM_BOT_TOKEN` in `.env`.

### 3) Configure app

Update `config.yaml` as needed:

- `ollama.base_url`
- `ollama.model`
- `telegram.allowed_user_ids` (optional allow-list)
- `rag.enabled`

### 4) Start Ollama

```bash
ollama serve
```

Pull model if needed (example):

```bash
ollama pull llama3.1:8b
```

### 5) Run the bot

```bash
source .venv/bin/activate
python -m services.bot
```

### 6) Run the dashboard

```bash
./run_dashboard.sh
```

or:

```bash
source .venv/bin/activate
streamlit run dashboard/app.py
```

## Telegram Flows (Implemented)

- Send text -> bot stores message in session memory -> sends to Ollama -> replies
- Send voice note -> bot converts/transcribes -> stores transcript -> sends to Ollama -> replies
- Send PDF/TXT document -> bot ingests into RAG store -> confirms chunk count

## Dashboard Pages

- Main Control: process controls + logs
- Sessions: inspect and clear message history by session
- Test Lab: direct brain and transcription tests
- Settings: system prompt + raw YAML editor
- Knowledge: inspect, upload, enable/disable, and remove sources

## Helpful Scripts

```bash
python scripts/verify_rag.py
python scripts/reingest_flavor_bible.py
```

## Notes

- Data/runtime folders (`data/`, `logs/`, `run/`, `models/`) are intentionally ignored by git.
- `whisper-cli` expects a model file at `models/ggml-medium.bin` by default.
- RAG retrieval uses only sources currently marked as `active`.
