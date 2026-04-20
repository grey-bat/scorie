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
    "channel not allocator",
    "missing company context",
]


def normalize_reason(reason: str) -> str:
    text = " ".join(str(reason or "").strip().split())
    if not text:
        return ""
    lower = text.lower()
    exact_mapping = {
        "friends": "friend",
        "friend": "friend",
        "family office": "family office",
        "fo": "family office",
        "fam office": "family office",
        "service provider to fintechs": "service provider",
        "service provider to fintech": "service provider",
        "crypto - competitor": "crypto",
        "crypto competitor": "crypto",
        "investor": "investor only",
        "general investor": "investor only",
        "general investro": "investor only",
        "pe": "investor only",
        "too big": "too broad/too big",
        "not qualified": "not qualified",
        "better channel than allocator": "better channel than buyer",
        "broken link": "broken link rewritten",
        "company fit weak": "missing company context",
        "missing company": "missing company context",
    }
    if lower in exact_mapping:
        return exact_mapping[lower]

    contains_mapping = [
        ("friend", "friend"),
        ("family office", "family office"),
        ("fam office", "family office"),
        ("service provider", "service provider"),
        ("flip script", "service provider"),
        ("crypto", "crypto"),
        ("defi", "competitor"),
        ("competitor", "competitor"),
        ("paypal", "too broad/too big"),
        ("too big", "too broad/too big"),
        ("too old", "not qualified"),
        ("not fintech", "company mismatch"),
        ("credit infra", "better channel than buyer"),
        ("remittances", "better channel than buyer"),
        ("banking as a service", "better channel than buyer"),
        ("investor", "investor only"),
        ("real estate", "company mismatch"),
        ("lending", "company mismatch"),
        ("fireblocks", "better channel than buyer"),
        ("infra", "better channel than buyer"),
    ]
    for needle, normalized in contains_mapping:
        if needle in lower:
            return normalized
    return text


def categorize_reason(reason: str) -> str:
    normalized = normalize_reason(reason)
    category_map = {
        "friend": "relationship_override",
        "family office": "buyer_fit_positive",
        "service provider": "service_provider",
        "crypto": "company_mismatch",
        "competitor": "company_mismatch",
        "investor only": "allocator_mismatch",
        "too broad/too big": "buyer_fit_negative",
        "not qualified": "buyer_fit_negative",
        "better channel than buyer": "channel_vs_buyer",
        "broken link rewritten": "data_quality",
        "company mismatch": "company_mismatch",
        "channel not allocator": "channel_vs_buyer",
        "missing company context": "missing_context",
    }
    return category_map.get(normalized, "other")


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
