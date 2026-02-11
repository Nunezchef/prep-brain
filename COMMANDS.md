# Commands

Canonical Telegram slash-command surface (generated from `services/commands_registry.py`).

## Core

- `/help [topic]` — show command help
- `/commands` — full canonical command list
- `/status [--detail]` — bot status
- `/health` — system checks
- `/mode service|admin` — output detail level _(admin)_
- `/silence <duration>` — mute alerts _(admin)_
- `/unsilence` — resume alerts _(admin)_
- `/log [N|errors]` — tail logs _(admin)_
- `/yes` — confirm pending action
- `/no` — cancel pending action

## Autonomy

- `/autonomy` — autonomy heartbeat
- `/autonomy detail` — autonomy detail card
- `/pause <duration>` — temporarily pause autonomous actions _(admin)_
- `/jobs` — recent ingest jobs _(admin)_
- `/job <id>` — ingest job detail _(admin)_

## Knowledge

- `/knowledge` — knowledge status overview
- `/sources` — knowledge sources
- `/source on <id>` — enable source _(admin)_
- `/source off <id>` — disable source _(admin)_
- `/ingests [N]` — recent document ingests
- `/ingest <id>` — re-run ingest _(admin)_
- `/reingest <id>` — re-run ingest alias _(admin)_
- `/forget source <id>` — delete source from index _(admin)_

## Drafts

- `/drafts [N] [-r TAG]` — pending drafts
- `/draft <id>` — view draft
- `/approve <id>` — promote draft _(admin)_
- `/hold <id> [reason]` — pause draft _(admin)_
- `/reject <id> [reason]` — discard draft _(admin)_
- `/next` — next draft
- `/prev` — previous draft
- `/setname <id> "New Name"` — set recipe name _(admin)_
- `/setyield <id> <amount> <unit>` — set yield _(admin)_
- `/setstation <id> "Station"` — set station _(admin)_
- `/setmethod <id> "Method..."` — set method _(admin)_
- `/seting <id> "Ingredient" <qty> <unit>` — set ingredient _(admin)_
- `/adding <id> "Ingredient" <qty> <unit> [notes]` — add ingredient _(admin)_
- `/deling <id> "Ingredient"` — remove ingredient _(admin)_
- `/noteing <id> "Ingredient" "note"` — set ingredient note _(admin)_

## Recipes

- `/recipe find "query" [-r TAG]` — find recipe
- `/recipe <id|name>` — view recipe
- `/recipe new` — guided recipe creation
- `/recipe activate <id>` — activate recipe _(admin)_
- `/recipe deactivate <id>` — deactivate recipe _(admin)_
- `/recipes new [N]` — recently added recipes
- `/price set "name" <amt> [per portion]` — set recipe sales price _(admin)_
- `/cost refresh <id|name>` — recalculate recipe cost _(admin)_

## Inventory

- `/inv` — inventory command help
- `/inv find "query"` — find inventory item
- `/inv <id|name>` — view inventory item
- `/inv set <id|name> <qty> <unit>` — set on-hand quantity _(admin)_
- `/inv add <id|name> <qty> <unit>` — add to on-hand quantity _(admin)_
- `/inv cost <id|name> <amount>` — set inventory unit cost _(admin)_
- `/inv low` — low inventory
- `/par set <recipe|inventory> <id|name> <value>` — set par level _(admin)_

## Vendors

- `/vendor list` — list vendors
- `/vendor <id|name>` — view vendor
- `/vendor new <name>` — create vendor _(admin)_
- `/order add <qty> <unit> <item>` — add routed order item
- `/order list` — list pending order items
- `/order clear [vendor_id]` — clear pending order items _(admin)_
- `/email vendor <vendor_id> [--detail]` — build vendor email draft
- `/review vendor <vendor_id>` — review draft email
- `/send vendor <vendor_id>` — send vendor draft

## Prep

- `/prep` — today's prep board
- `/prep station <name>` — station prep view
- `/prep status` — station summary
- `/prep add <recipe_name> <qty> <unit>` — add prep item _(admin)_
- `/prep assign <item_id> <staff_name>` — assign prep item _(admin)_
- `/prep hold <item_id> [reason]` — hold prep item _(admin)_
- `/prep done <item_id>` — mark prep done _(admin)_
- `/prep clear done` — clear completed prep items _(admin)_

## Debug

- `/debug on` — enable debug output _(admin)_
- `/debug off` — disable debug output _(admin)_
- `/debug ingest <last|id>` — ingest diagnostics _(admin)_
- `/debug chunks <last|id>` — chunk diagnostics _(admin)_
- `/debug sample <last|id> [n]` — sample chunk previews _(admin)_
- `/debug db` — database path and counts _(admin)_
- `/debug sources` — doc source diagnostics _(admin)_
- `/debug recipe <name|id>` — recipe assembly diagnostics _(admin)_

