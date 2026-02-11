import html
from typing import Iterable, List, Optional


def tg_escape(text: object) -> str:
    return html.escape("" if text is None else str(text), quote=True)


def tg_kv(label: object, value: object) -> str:
    return f"<b>{tg_escape(label)}:</b> {tg_escape(value)}"


def tg_list(items: Iterable[object]) -> str:
    lines: List[str] = []
    for item in items:
        text = str(item or "").strip()
        if text:
            lines.append(f"• {tg_escape(text)}")
    return "\n".join(lines)


def tg_code(text: object) -> str:
    return f"<pre>{tg_escape(text)}</pre>"


def tg_card(title: object, lines: Optional[Iterable[object]] = None, footer_actions: Optional[Iterable[object]] = None) -> str:
    out: List[str] = [f"<b>{tg_escape(title)}</b>"]

    for line in lines or []:
        value = str(line or "").strip()
        if value:
            out.append(tg_escape(value))

    actions: List[str] = []
    for action in footer_actions or []:
        value = str(action or "").strip()
        if value:
            actions.append(tg_escape(value))
    if actions:
        out.append(" ".join(actions))

    return "\n".join(out).strip()
