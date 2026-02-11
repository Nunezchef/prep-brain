import html
import re
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


def tg_render_answer(text: object) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""

    # Keep preformatted HTML answers untouched (house recipes/cards/etc).
    if re.search(r"<(?:b|i|code|pre|u|s|a)(?:\s|>)", raw, flags=re.I):
        return raw

    cleaned = raw.replace("\r\n", "\n")
    cleaned = re.sub(r"`{1,3}", "", cleaned)
    cleaned = cleaned.replace("**", "").replace("__", "")
    cleaned = re.sub(r"^#{1,6}\s*", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"\[(.*?)\]\((.*?)\)", r"\1", cleaned)

    out_lines: List[str] = []
    bullet_re = re.compile(r"^(?:[-*•]|\d+[\.)])\s+(.+)$")
    for line in cleaned.split("\n"):
        value = line.strip()
        if not value:
            if out_lines and out_lines[-1] != "":
                out_lines.append("")
            continue

        bullet_match = bullet_re.match(value)
        if bullet_match:
            out_lines.append(f"• {tg_escape(bullet_match.group(1).strip())}")
            continue
        out_lines.append(tg_escape(value))

    while out_lines and out_lines[0] == "":
        out_lines.pop(0)
    while out_lines and out_lines[-1] == "":
        out_lines.pop()

    return "\n".join(out_lines).strip()
