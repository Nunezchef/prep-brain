🧠 Prep-Brain

A calm, local kitchen brain built for real service.

Prep-Brain is a local, voice-first restaurant assistant that helps you think, remember, and act under real kitchen conditions — not demos, not hype, not “AI for vibes.”

You talk to it through Telegram.
It listens, remembers context, reasons with your documents, and answers clearly.

Think of it as:

a senior operations brain that knows your restaurants, your systems, and your constraints — and stays quiet when it should.

⸻

What Prep-Brain Is (and Is Not)

Prep-Brain is:
	•	Local-first (your data stays on your machine)
	•	Voice-first (built for kitchens, not keyboards)
	•	Context-aware (sessions + memory)
	•	Document-grounded (RAG, not hallucination)
	•	Inspectable and reversible

Prep-Brain is not:
	•	A SaaS
	•	A generic chatbot wrapper
	•	A stateless demo
	•	An “omniscient” system with hidden memory

⸻

What’s Working Right Now

These features are implemented and functional.

Capability	Status	Location
Telegram text → contextual AI reply	✅ Working	services/bot.py + services/brain.py
Voice notes → transcription → AI reply	✅ Working	services/bot.py + services/transcriber.py
Persistent memory (users / sessions / messages)	✅ Working	services/memory.py
RAG ingestion + retrieval with source controls	✅ Working	services/rag.py
Telegram document upload → knowledge ingestion	✅ Working	services/bot.py
Local dashboard for control & inspection	✅ Working	dashboard/app.py


⸻

What the Dashboard Can Do
	•	Start / stop / restart the bot
	•	Check Ollama status
	•	View live logs
	•	Inspect and clear session history
	•	Upload and manage knowledge sources
	•	Enable / disable / remove RAG sources
	•	Edit config and system prompt live (no restarts)

The dashboard exists for trust and control, not decoration.

⸻

Knowledge & RAG (How It Actually Works)

Prep-Brain uses a Retrieval-Augmented Generation (RAG) system to reason over your documents.

You can ingest:
	•	PDFs
	•	SOPs
	•	prep bibles
	•	recipes
	•	station notes
	•	menus
	•	vendor sheets
	•	post-service notes

Each source is:
	•	indexed
	•	embedded
	•	stored with metadata
	•	individually controllable (active / disabled / removed)

Important:
	•	The assistant does not blindly “learn” documents.
	•	It retrieves relevant sections and reasons over them at runtime.
	•	Sources are always inspectable and reversible.
	•	Web research (if enabled) is contextual and not auto-saved.

This allows per-restaurant / per-project knowledge separation, so answers stay grounded in the correct venue context.

⸻

Tech Stack (Chosen on Purpose)
	•	Python
	•	python-telegram-bot
	•	Ollama (local LLM backend)
	•	ChromaDB + sentence-transformers (RAG)
	•	Streamlit (dashboard)
	•	SQLite (memory)
	•	ffmpeg + whisper-cli (audio transcription)

Boring. Replaceable. Reliable.

⸻

Project Layout

prep-brain/
├── services/
│   ├── bot.py           # Telegram handlers (text, voice, documents)
│   ├── brain.py         # Ollama client + RAG context injection
│   ├── memory.py        # SQLite session & message memory
│   ├── rag.py           # Ingestion + retrieval engine
│   └── transcriber.py   # whisper-cli wrapper
├── dashboard/
│   ├── app.py           # Main Streamlit control panel
│   └── pages/           # Sessions, Test Lab, Settings, Knowledge
├── scripts/
│   └── verify_rag.py
├── config.yaml
├── .env.example
└── requirements.txt


⸻

Quick Start

Prerequisites
	•	Python 3.10+
	•	ffmpeg in PATH
	•	whisper-cli in PATH
	•	Ollama installed locally

⸻

1) Install dependencies

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt


⸻

2) Environment setup

cp .env.example .env

Set your Telegram bot token in .env.

⸻

3) Configure the app

Edit config.yaml:
	•	Ollama base URL & model
	•	Optional Telegram allow-list
	•	RAG enable/disable

All of this can also be edited live from the dashboard.

⸻

4) Start Ollama

ollama serve

Pull a model if needed:

ollama pull llama3.1:8b


⸻

5) Run the bot

source .venv/bin/activate
python -m services.bot


⸻

6) Run the dashboard

streamlit run dashboard/app.py


⸻

Message Flows (Implemented)
	•	Text
	•	Message → session memory → Ollama → reply
	•	Voice
	•	Voice note → ffmpeg → whisper → transcript
	•	Transcript → memory → Ollama → reply
	•	Documents
	•	Upload → ingestion → indexed knowledge source
	•	Source can be enabled/disabled at any time

⸻

Notes & Safety
	•	Runtime data (data/, logs/, models/) is intentionally git-ignored
	•	Mixed image/text PDFs require OCR before ingestion
	•	RAG retrieval only uses sources marked active
	•	Knowledge sources can always be removed

Nothing is hidden. Nothing is irreversible.

⸻

Status

Prep-Brain is an active, evolving system.

It’s built for:
	•	real kitchens
	•	real constraints
	•	real thinking under pressure

If you’re looking for a chatbot demo, this isn’t it.

If you’re building a thinking tool for operations, welcome.