import json
import math
import os
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, Iterable
from urllib.parse import unquote

MODEL_CONTEXT_COLUMNS = [
    "Current Company",
    "Current Title",
    "Headline",
    "Industry",
    "Mutual Count",
    "Degree",
    "Summary",
    "Alumni Signal",
    "Position 1 Description",
    "Position 2 Description",
    "Position 3 Description",
    "Organization 1",
    "Organization 2",
    "Organization 3",
    "Organization 1 Title",
    "Organization 2 Title",
    "Organization 3 Title",
    "Organization 1 Description",
    "Organization 2 Description",
    "Organization 3 Description",
]

PREPARED_OUTPUT_COLUMNS = ["Match Key", "Raw ID", "Best Email", "Full Name", *MODEL_CONTEXT_COLUMNS]

RAW_SCORE_COLUMNS = {
    "fo_persona": "Persona Signal - Family Office",
    "ft_persona": "Persona Signal - Fintech",
    "allocator": "Allocator Score",
    "access": "Access Score",
}

DISPLAY_COLUMNS = [
    ("done", 6, "Cnt"),
    ("Full Name", 22, "Name"),
    ("Current Company", 22, "Company"),
    ("fo_persona", 4, "FO"),
    ("ft_persona", 4, "FT"),
    ("allocator", 5, "Alloc"),
    ("Degree", 6, "Degree"),
    ("access", 6, "Access"),
    ("Headline", 42, "Headline"),
    ("Summary", 60, "Summary"),
]

STAGE_RANK = {
    "new": 1,
    "on deck": 2,
    "qualified": 3,
    "outreached": 4,
    "responded": 5,
    "booked": 6,
    "met": 7,
    "followup": 8,
}

RICHNESS_FIELDS = [
    "Current Company", "Current Title", "Headline", "Summary", "Industry", "Stage",
    "Position 1 Description", "Position 2 Description", "Position 3 Description",
    "Organization 1", "Organization 2", "Organization 3",
    "Organization 1 Title", "Organization 2 Title", "Organization 3 Title",
    "Organization 1 Description", "Organization 2 Description", "Organization 3 Description",
    "Mutual Count", "Berkeley Signal", "Columbia Signal",
]

TIMESTAMP_FIELDS = ["Last Touch Date", "Last Sent At", "Last Received At", "Connected At", "Created"]


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    s = str(value)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def canonicalize_identifier(value: Any) -> str:
    s = normalize_text(value)
    if not s:
        return ""
    s = unquote(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^0-9A-Za-z]+", "", s)
    return s.lower()


def normalize_key(value: Any) -> str:
    return normalize_text(value)


def normalize_email(value: Any) -> str:
    return normalize_text(value).lower()


def truthy_field(value: Any) -> bool:
    return normalize_text(value) != ""


def recompute_alumni_signal(berkeley_signal: Any, columbia_signal: Any) -> str:
    has_berkeley = truthy_field(berkeley_signal)
    has_columbia = truthy_field(columbia_signal)
    if has_berkeley and has_columbia:
        return "Cal+CBS"
    if has_berkeley:
        return "Cal"
    if has_columbia:
        return "CBS"
    return ""


def normalize_mutual_count(value: Any) -> int:
    s = normalize_text(value)
    if not s:
        return 0
    s = s.replace(",", "")
    try:
        return int(float(s))
    except Exception:
        return 0


def map_distance_label(value: Any) -> int:
    s = normalize_text(value).upper()
    if s == "DISTANCE_1":
        return 1
    if s == "DISTANCE_2":
        return 2
    return 3


def normalize_stage(value: Any) -> str:
    s = normalize_text(value).lower()
    if not s:
        return ""
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    mapping = {
        "ondeck": "on deck",
        "on deck": "on deck",
        "replied": "responded",
        "responded": "responded",
        "follow up": "followup",
        "follow-up": "followup",
        "followup": "followup",
    }
    return mapping.get(s, s)


def stage_rank(value: Any) -> int:
    return STAGE_RANK.get(normalize_stage(value), 0)


def parse_ts(value: Any):
    s = normalize_text(value)
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def best_timestamp(row: Dict[str, Any]):
    vals = [parse_ts(row.get(c, "")) for c in TIMESTAMP_FIELDS]
    vals = [v for v in vals if v is not None]
    return max(vals) if vals else datetime(1970, 1, 1, tzinfo=timezone.utc)


def richness_score(row: Dict[str, Any]) -> int:
    score = 0
    for c in RICHNESS_FIELDS:
        if truthy_field(row.get(c, "")):
            score += 1
    return score


def canonical_match_key(raw_id: Any, best_email: Any) -> str:
    rid = normalize_key(raw_id)
    if rid:
        return f"raw:{rid}"
    email = normalize_email(best_email)
    if email:
        return f"email:{email}"
    return ""


def choose_best_duplicate(group):
    rows = list(group)
    def sort_key(row):
        stage_present = 1 if truthy_field(row.get("Stage", "")) else 0
        return (
            stage_present,
            stage_rank(row.get("Stage", "")),
            best_timestamp(row),
            richness_score(row),
        )
    rows.sort(key=sort_key, reverse=True)
    return rows[0]


def truncate(value: Any, width: int) -> str:
    s = normalize_text(value)
    if len(s) <= width:
        return s.ljust(width)
    if width <= 2:
        return s[:width]
    return s[: width - 2] + ".."


def make_row_line(row: Dict[str, Any]) -> str:
    parts = []
    for key, width, _label in DISPLAY_COLUMNS:
        parts.append(truncate(row.get(key, ""), width))
    return "| " + " | ".join(parts) + " |"


def make_header_lines() -> str:
    labels = []
    for _key, width, label in DISPLAY_COLUMNS:
        labels.append(label.ljust(width))
    header = "| " + " | ".join(labels) + " |"
    rule = "-" * len(header)
    return f"{rule}\n{header}\n{rule}"


def parse_json_from_content(content: str):
    content = content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```(?:json)?", "", content).strip()
        content = re.sub(r"```$", "", content).strip()
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        match = re.search(r"(\[.*\]|\{.*\})", content, re.S)
        if not match:
            raise
        return json.loads(match.group(1))


def to_int_score(value: Any) -> int:
    if value is None or value == "":
        raise ValueError("empty score")
    iv = int(round(float(value)))
    if iv < 0 or iv > 5:
        raise ValueError(f"score out of range: {value}")
    return iv


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def utc_stamp() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _extract_formula(prop: Dict[str, Any]) -> str:
    f = prop.get("formula", {})
    ftype = f.get("type")
    if ftype == "string":
        return f.get("string") or ""
    if ftype == "number":
        n = f.get("number")
        return "" if n is None else str(n)
    if ftype == "boolean":
        return "true" if f.get("boolean") else "false"
    if ftype == "date":
        d = f.get("date")
        return "" if not d else (d.get("start") or "")
    return ""


def _extract_unique_id(prop: Dict[str, Any]) -> str:
    uid = prop.get("unique_id")
    return "" if not uid else f"{uid.get('prefix', '')}{uid.get('number', '')}"


_NOTION_PLAIN_TEXT_EXTRACTORS: Dict[str, Any] = {
    "title":           lambda p: "".join(t.get("plain_text", "") for t in p.get("title", [])),
    "rich_text":       lambda p: "".join(t.get("plain_text", "") for t in p.get("rich_text", [])),
    "number":          lambda p: "" if p.get("number") is None else str(p["number"]),
    "select":          lambda p: "" if not p.get("select") else p["select"].get("name", ""),
    "multi_select":    lambda p: ", ".join(x.get("name", "") for x in p.get("multi_select", [])),
    "status":          lambda p: "" if not p.get("status") else p["status"].get("name", ""),
    "url":             lambda p: p.get("url") or "",
    "email":           lambda p: p.get("email") or "",
    "phone_number":    lambda p: p.get("phone_number") or "",
    "date":            lambda p: "" if not p.get("date") else (p["date"].get("start") or ""),
    "checkbox":        lambda p: "true" if p.get("checkbox") else "false",
    "created_time":    lambda p: p.get("created_time") or "",
    "last_edited_time": lambda p: p.get("last_edited_time") or "",
    "people":          lambda p: ", ".join(x.get("name", "") or x.get("id", "") for x in p.get("people", [])),
    "formula":         _extract_formula,
    "unique_id":       _extract_unique_id,
}


def notion_plain_text(prop: Dict[str, Any]) -> str:
    if not isinstance(prop, dict):
        return ""
    extractor = _NOTION_PLAIN_TEXT_EXTRACTORS.get(prop.get("type"))
    return extractor(prop) if extractor else ""


def notion_set_payload(prop_type: str, value: Any) -> Dict[str, Any]:
    if prop_type == "number":
        if normalize_text(value) == "":
            return {"number": None}
        return {"number": int(value)}
    if prop_type == "rich_text":
        text = normalize_text(value)
        if not text:
            return {"rich_text": []}
        return {"rich_text": [{"type": "text", "text": {"content": text}}]}
    if prop_type == "select":
        name = normalize_text(value)
        if not name:
            return {"select": None}
        if name == "Cal+CBS":
            name = "Cal + CBS"
        return {"select": {"name": name}}
    if prop_type == "status":
        name = normalize_text(value)
        if not name:
            return {"status": None}
        return {"status": {"name": name}}
    raise ValueError(f"Unsupported target property type for writeback: {prop_type}")
