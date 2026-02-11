from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

CommandHandler = Callable[[Any, Any, List[str]], Awaitable[None]]


GROUP_ORDER: List[str] = [
    "Core",
    "Autonomy",
    "Knowledge",
    "Drafts",
    "Recipes",
    "Inventory",
    "Vendors",
    "Prep",
    "Debug",
]


@dataclass
class CommandSpec:
    name: str
    usage: str
    description: str
    group: str
    admin_only: bool
    handler: Optional[CommandHandler]
    default_help: bool = True
    enabled_by_config: Optional[str] = None
    command_id: str = ""

    # Compatibility fields used by existing command help/rendering code.
    @property
    def key(self) -> str:
        return self.command_id

    @property
    def root(self) -> str:
        return self.name

    @property
    def command(self) -> str:
        return self.usage


COMMAND_ALIASES: Dict[str, str] = {
    "help_chef": "help",
}


def _c(
    command_id: str,
    name: str,
    usage: str,
    description: str,
    group: str,
    *,
    admin_only: bool = False,
    default_help: bool = True,
    enabled_by_config: Optional[str] = None,
) -> CommandSpec:
    return CommandSpec(
        name=name,
        usage=usage,
        description=description,
        group=group,
        admin_only=admin_only,
        handler=None,
        default_help=default_help,
        enabled_by_config=enabled_by_config,
        command_id=command_id,
    )


COMMANDS: Dict[str, CommandSpec] = {
    # Core
    "help": _c("help", "help", "/help [topic]", "show command help", "Core"),
    "commands": _c("commands", "commands", "/commands", "full canonical command list", "Core"),
    "status": _c("status", "status", "/status [--detail]", "bot status", "Core"),
    "health": _c("health", "health", "/health", "system checks", "Core"),
    "mode": _c("mode", "mode", "/mode service|admin", "output detail level", "Core", admin_only=True),
    "silence": _c("silence", "silence", "/silence <duration>", "mute alerts", "Core", admin_only=True),
    "unsilence": _c("unsilence", "unsilence", "/unsilence", "resume alerts", "Core", admin_only=True),
    "log": _c("log", "log", "/log [N|errors]", "tail logs", "Core", admin_only=True),
    "yes": _c("yes", "yes", "/yes", "confirm pending action", "Core", default_help=False),
    "no": _c("no", "no", "/no", "cancel pending action", "Core", default_help=False),
    # Autonomy
    "autonomy": _c("autonomy", "autonomy", "/autonomy", "autonomy heartbeat", "Autonomy"),
    "autonomy_detail": _c("autonomy_detail", "autonomy", "/autonomy detail", "autonomy detail card", "Autonomy"),
    "pause": _c("pause", "pause", "/pause <duration>", "temporarily pause autonomous actions", "Autonomy", admin_only=True),
    "jobs": _c("jobs", "jobs", "/jobs", "recent ingest jobs", "Autonomy", admin_only=True, default_help=False),
    "job": _c("job", "job", "/job <id>", "ingest job detail", "Autonomy", admin_only=True, default_help=False),
    # Knowledge
    "knowledge": _c("knowledge", "knowledge", "/knowledge", "knowledge status overview", "Knowledge"),
    "sources": _c("sources", "sources", "/sources", "knowledge sources", "Knowledge"),
    "source_on": _c("source_on", "source", "/source on <id>", "enable source", "Knowledge", admin_only=True),
    "source_off": _c("source_off", "source", "/source off <id>", "disable source", "Knowledge", admin_only=True),
    "ingests": _c("ingests", "ingests", "/ingests [N]", "recent document ingests", "Knowledge"),
    "ingest": _c("ingest", "ingest", "/ingest <id>", "re-run ingest", "Knowledge", admin_only=True),
    "reingest": _c("reingest", "reingest", "/reingest <id>", "re-run ingest alias", "Knowledge", admin_only=True, default_help=False),
    "forget_source": _c("forget_source", "forget", "/forget source <id>", "delete source from index", "Knowledge", admin_only=True),
    # Drafts + edits
    "drafts": _c("drafts", "drafts", "/drafts [N] [-r TAG]", "pending drafts", "Drafts"),
    "draft": _c("draft", "draft", "/draft <id>", "view draft", "Drafts"),
    "approve": _c("approve", "approve", "/approve <id>", "promote draft", "Drafts", admin_only=True),
    "hold": _c("hold", "hold", "/hold <id> [reason]", "pause draft", "Drafts", admin_only=True),
    "reject": _c("reject", "reject", "/reject <id> [reason]", "discard draft", "Drafts", admin_only=True),
    "next": _c("next", "next", "/next", "next draft", "Drafts"),
    "prev": _c("prev", "prev", "/prev", "previous draft", "Drafts"),
    "setname": _c("setname", "setname", '/setname <id> "New Name"', "set recipe name", "Drafts", admin_only=True, default_help=False),
    "setyield": _c("setyield", "setyield", "/setyield <id> <amount> <unit>", "set yield", "Drafts", admin_only=True, default_help=False),
    "setstation": _c("setstation", "setstation", '/setstation <id> "Station"', "set station", "Drafts", admin_only=True, default_help=False),
    "setmethod": _c("setmethod", "setmethod", '/setmethod <id> "Method..."', "set method", "Drafts", admin_only=True, default_help=False),
    "seting": _c("seting", "seting", '/seting <id> "Ingredient" <qty> <unit>', "set ingredient", "Drafts", admin_only=True, default_help=False),
    "adding": _c("adding", "adding", '/adding <id> "Ingredient" <qty> <unit> [notes]', "add ingredient", "Drafts", admin_only=True, default_help=False),
    "deling": _c("deling", "deling", '/deling <id> "Ingredient"', "remove ingredient", "Drafts", admin_only=True, default_help=False),
    "noteing": _c("noteing", "noteing", '/noteing <id> "Ingredient" "note"', "set ingredient note", "Drafts", admin_only=True, default_help=False),
    # Recipes
    "recipe_find": _c("recipe_find", "recipe", '/recipe find "query" [-r TAG]', "find recipe", "Recipes"),
    "recipe_get": _c("recipe_get", "recipe", "/recipe <id|name>", "view recipe", "Recipes"),
    "recipe_new": _c("recipe_new", "recipe", "/recipe new", "guided recipe creation", "Recipes"),
    "recipe_activate": _c("recipe_activate", "recipe", "/recipe activate <id>", "activate recipe", "Recipes", admin_only=True),
    "recipe_deactivate": _c("recipe_deactivate", "recipe", "/recipe deactivate <id>", "deactivate recipe", "Recipes", admin_only=True),
    "recipes_new": _c("recipes_new", "recipes", "/recipes new [N]", "recently added recipes", "Recipes", default_help=False),
    "price_set": _c("price_set", "price", '/price set "name" <amt> [per portion]', "set recipe sales price", "Recipes", admin_only=True),
    "cost_refresh": _c("cost_refresh", "cost", "/cost refresh <id|name>", "recalculate recipe cost", "Recipes", admin_only=True),
    # Inventory
    "inv": _c("inv", "inv", "/inv", "inventory command help", "Inventory"),
    "inv_find": _c("inv_find", "inv", '/inv find "query"', "find inventory item", "Inventory"),
    "inv_get": _c("inv_get", "inv", "/inv <id|name>", "view inventory item", "Inventory"),
    "inv_set": _c("inv_set", "inv", "/inv set <id|name> <qty> <unit>", "set on-hand quantity", "Inventory", admin_only=True),
    "inv_add": _c("inv_add", "inv", "/inv add <id|name> <qty> <unit>", "add to on-hand quantity", "Inventory", admin_only=True),
    "inv_cost": _c("inv_cost", "inv", "/inv cost <id|name> <amount>", "set inventory unit cost", "Inventory", admin_only=True),
    "inv_low": _c("inv_low", "inv", "/inv low", "low inventory", "Inventory"),
    "par_set": _c("par_set", "par", "/par set <recipe|inventory> <id|name> <value>", "set par level", "Inventory", admin_only=True),
    # Vendors + ordering
    "vendor_list": _c("vendor_list", "vendor", "/vendor list", "list vendors", "Vendors"),
    "vendor_get": _c("vendor_get", "vendor", "/vendor <id|name>", "view vendor", "Vendors"),
    "vendor_new": _c("vendor_new", "vendor", "/vendor new <name>", "create vendor", "Vendors", admin_only=True),
    "order_add": _c("order_add", "order", "/order add <qty> <unit> <item>", "add routed order item", "Vendors"),
    "order_list": _c("order_list", "order", "/order list", "list pending order items", "Vendors"),
    "order_clear": _c("order_clear", "order", "/order clear [vendor_id]", "clear pending order items", "Vendors", admin_only=True),
    "email_vendor": _c("email_vendor", "email", "/email vendor <vendor_id> [--detail]", "build vendor email draft", "Vendors"),
    "review_vendor": _c("review_vendor", "review", "/review vendor <vendor_id>", "review draft email", "Vendors"),
    "send_vendor": _c("send_vendor", "send", "/send vendor <vendor_id>", "send vendor draft", "Vendors", enabled_by_config="smtp_enabled"),
    # Prep
    "prep": _c("prep", "prep", "/prep", "today's prep board", "Prep"),
    "prep_station": _c("prep_station", "prep", "/prep station <name>", "station prep view", "Prep"),
    "prep_status": _c("prep_status", "prep", "/prep status", "station summary", "Prep"),
    "prep_add": _c("prep_add", "prep", "/prep add <recipe_name> <qty> <unit>", "add prep item", "Prep", admin_only=True),
    "prep_assign": _c("prep_assign", "prep", "/prep assign <item_id> <staff_name>", "assign prep item", "Prep", admin_only=True),
    "prep_hold": _c("prep_hold", "prep", "/prep hold <item_id> [reason]", "hold prep item", "Prep", admin_only=True),
    "prep_done": _c("prep_done", "prep", "/prep done <item_id>", "mark prep done", "Prep", admin_only=True),
    "prep_clear_done": _c("prep_clear_done", "prep", "/prep clear done", "clear completed prep items", "Prep", admin_only=True),
    # Debug
    "debug_on": _c("debug_on", "debug", "/debug on", "enable debug output", "Debug", admin_only=True),
    "debug_off": _c("debug_off", "debug", "/debug off", "disable debug output", "Debug", admin_only=True),
    "debug_ingest": _c("debug_ingest", "debug", "/debug ingest <last|id>", "ingest diagnostics", "Debug", admin_only=True),
    "debug_chunks": _c("debug_chunks", "debug", "/debug chunks <last|id>", "chunk diagnostics", "Debug", admin_only=True),
    "debug_sample": _c("debug_sample", "debug", "/debug sample <last|id> [n]", "sample chunk previews", "Debug", admin_only=True),
    "debug_db": _c("debug_db", "debug", "/debug db", "database path and counts", "Debug", admin_only=True),
    "debug_sources": _c("debug_sources", "debug", "/debug sources", "doc source diagnostics", "Debug", admin_only=True),
    "debug_recipe": _c("debug_recipe", "debug", "/debug recipe <name|id>", "recipe assembly diagnostics", "Debug", admin_only=True, default_help=False),
}


def resolve_root(command_name: str) -> str:
    key = str(command_name or "").strip().lower()
    if not key:
        return ""
    return COMMAND_ALIASES.get(key, key)


def command_specs() -> List[CommandSpec]:
    return list(COMMANDS.values())


def get_group_order() -> List[str]:
    return list(GROUP_ORDER)


def grouped_commands(*, include_non_default: bool = False) -> Dict[str, List[CommandSpec]]:
    groups: Dict[str, List[CommandSpec]] = {name: [] for name in GROUP_ORDER}
    for spec in command_specs():
        if not include_non_default and not spec.default_help:
            continue
        groups.setdefault(spec.group, []).append(spec)
    return {k: v for k, v in groups.items() if v}


def known_roots() -> Set[str]:
    return {spec.name for spec in command_specs()}


def get_specs_for_root(root: str) -> List[CommandSpec]:
    resolved = resolve_root(root)
    return [spec for spec in command_specs() if spec.name == resolved]


def get_root_spec(root: str) -> Optional[CommandSpec]:
    specs = get_specs_for_root(root)
    return specs[0] if specs else None


def bind_default_handler(handler: CommandHandler) -> None:
    for spec in command_specs():
        if spec.handler is None:
            spec.handler = handler


def command_enabled_map(config: Optional[Dict[str, object]] = None) -> Dict[str, bool]:
    cfg = config or {}
    smtp = cfg.get("smtp", {}) if isinstance(cfg.get("smtp", {}), dict) else {}
    smtp_enabled = bool(smtp.get("enabled", False))

    enabled: Dict[str, bool] = {}
    for spec in command_specs():
        flag = True
        if spec.enabled_by_config == "smtp_enabled":
            flag = smtp_enabled
        enabled[spec.key] = flag
    return enabled


def validate_registry() -> List[str]:
    issues: List[str] = []
    seen_ids: Set[str] = set()
    seen_usage: Set[str] = set()

    for spec in command_specs():
        cid = str(spec.command_id or "").strip()
        if not cid:
            issues.append(f"Command has empty id: {spec.usage}")
        elif cid in seen_ids:
            issues.append(f"Duplicate command id: {cid}")
        seen_ids.add(cid)

        usage = str(spec.usage or "").strip().lower()
        if not usage.startswith("/"):
            issues.append(f"Command usage must start with '/': {spec.command_id}")
        if usage in seen_usage:
            # Duplicate usage is allowed for subcommands under same root, so ignore exact duplicates only if id differs.
            pass
        seen_usage.add(usage)

        if not spec.handler:
            issues.append(f"Missing handler: {spec.command_id}")

        if spec.group not in GROUP_ORDER:
            issues.append(f"Unknown command group '{spec.group}' on {spec.command_id}")

    if not known_roots():
        issues.append("No command roots registered")

    return issues
