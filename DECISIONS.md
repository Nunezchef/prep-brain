# Architectural Decisions

## 1) Autonomy Always-On
Autonomy starts with the bot process and updates DB heartbeat/status every cycle.

## 2) Ops Intents Bypass RAG
Explicit operational commands route to deterministic DB actions first; RAG is fallback for knowledge queries.

## 3) Station-First Prep-List
Prep board is grouped by station as the primary operational view.

## 4) Single Command Source of Truth
Telegram command surface is defined in the command registry and used for `/help` + `/commands`.

## 5) Central Config Loader
Config is loaded through one interface (`prep_brain.config`) to reduce drift and DB path inconsistencies.
