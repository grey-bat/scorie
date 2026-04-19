from __future__ import annotations

from collections import Counter

STANDARD_REASON_OPTIONS = [
    "friend",
    "family office",
    "service provider",
    "crypto",
    "competitor",
    "investor only",
    "too broad/too big",
    "not qualified",
    "better channel than buyer",
    "broken link rewritten",
    "company mismatch",
    "good but customize",
    "channel not allocator",
    "missing company context",
]


def normalize_reason(reason: str) -> str:
    text = " ".join(str(reason or "").strip().split())
    if not text:
        return ""
    lower = text.lower()
    mapping = {
        "friends": "friend",
        "family office": "family office",
        "fo": "family office",
        "service provider to fintechs": "service provider",
        "service provider to fintech": "service provider",
        "crypto - competitor": "crypto",
        "crypto competitor": "crypto",
        "investor": "investor only",
        "too big": "too broad/too big",
        "not qualified": "not qualified",
        "better channel than allocator": "better channel than buyer",
        "broken link": "broken link rewritten",
        "company fit weak": "missing company context",
        "missing company": "missing company context",
    }
    return mapping.get(lower, text)


def reason_suggestions(reason: str | None = None) -> list[str]:
    reason = normalize_reason(reason or "")
    if reason and reason in STANDARD_REASON_OPTIONS:
        return [reason, *[item for item in STANDARD_REASON_OPTIONS if item != reason]]
    return STANDARD_REASON_OPTIONS[:]


def reason_counter(values: list[str]) -> Counter:
    c = Counter()
    for value in values:
        norm = normalize_reason(value)
        if norm:
            c[norm] += 1
    return c
