"""Automated groundedness check: every number in a narrative must come from a tool output.

A number in the narrative is grounded if some tool output contains a number
that matches it at the precision it was written with (so "56,834" matches
56833.67, and "3.8 bps" matches 3.830683...). Claims are checked more strictly:
the cited tool call's own output must contain the value.
"""

from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

NUMBER = re.compile(r"(?<![\w.])[-+]?\d[\d,]*(?:\.\d+)?(?![\w])")
# Numbers that are structural, not evidence: list markers, small counts like "two"
# written as digits below this are still checked; only ordinals/years are skipped.
YEAR = re.compile(r"^(19|20)\d{2}$")


def numbers_in_text(text: str) -> list[str]:
    out = []
    for m in NUMBER.finditer(text):
        s = m.group(0).replace(",", "")
        if YEAR.match(s):
            continue
        out.append(s)
    return out


def _walk(obj: Any) -> Iterable[str]:
    if isinstance(obj, dict):
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v)
    elif isinstance(obj, bool):
        return
    elif isinstance(obj, (int, float)):
        yield repr(obj)
    elif isinstance(obj, str):
        yield from numbers_in_text(obj)
        try:
            Decimal(obj)
            yield obj
        except InvalidOperation:
            pass


def numbers_in_output(output: Any) -> list[Decimal]:
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except ValueError:
            pass
    vals = []
    for s in _walk(output):
        try:
            vals.append(Decimal(s.replace(",", "")))
        except InvalidOperation:
            continue
    return vals


def matches(written: str, candidates: list[Decimal]) -> bool:
    """True if `written`, at its own precision, equals some candidate rounded the same way.
    Also accepts a candidate expressed as a percentage of a 0-1 ratio (0.99 -> 99%)."""
    try:
        w = Decimal(written)
    except InvalidOperation:
        return False
    exp = w.as_tuple().exponent
    q = Decimal(1).scaleb(exp) if isinstance(exp, int) else Decimal(1)
    for c in candidates:
        for v in (c, c * 100):
            try:
                if v.quantize(q) == w or abs(v - w) <= q / 2:
                    return True
            except InvalidOperation:
                continue
            # tolerate rounding to fewer significant figures (e.g. "56,834" for 56833.67)
            if w != 0 and abs(v - w) / abs(w) <= Decimal("0.0005"):
                return True
    return False


def check(narrative: str, claims: list[dict], tool_log: list[dict], alert: dict | None = None) -> dict:
    sources = [t["output"] for t in tool_log] + ([alert] if alert else [])
    pool = [n for s in sources for n in numbers_in_output(s)]
    by_id = {t["id"]: numbers_in_output(t["output"]) for t in tool_log}

    written = numbers_in_text(narrative)
    ungrounded = [w for w in written if not matches(w, pool)]
    bad_claims = []
    for c in claims:
        cands = by_id.get(c.get("tool_call_id", ""), [])
        if not matches(c.get("value", ""), cands):
            bad_claims.append(c)
    n = len(written)
    return {
        "numbers_in_narrative": n,
        "ungrounded_numbers": ungrounded,
        "groundedness": 1.0 if n == 0 else round((n - len(ungrounded)) / n, 4),
        "claims": len(claims),
        "claims_not_supported_by_cited_call": bad_claims,
        "claim_support": 1.0 if not claims else round((len(claims) - len(bad_claims)) / len(claims), 4),
    }
