from __future__ import annotations

import csv
import json
import re
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: List[Dict[str, str]] = []
        for row in reader:
            cleaned = {k: (v or "").strip() for k, v in row.items() if k is not None}
            rows.append(cleaned)
        return rows


def read_csv_header(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, [])
    return [h.strip() for h in header]


def write_csv(path: Path, fieldnames: List[str], rows: Iterable[Dict[str, object]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def read_jsonl(path: Path) -> List[Dict[str, object]]:
    if not path.exists():
        return []
    records: List[Dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def write_jsonl(path: Path, rows: Iterable[Dict[str, object]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_jsonl(path: Path, rows: Iterable[Dict[str, object]]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def safe_slug(value: str) -> str:
    value = re.sub(r"\s+", "-", value.strip())
    value = re.sub(r"[^\w\-\u4e00-\u9fff]", "", value)
    return value[:80] or "page"


def today_str() -> str:
    return date.today().isoformat()


def parse_json_obj(text: str) -> Dict[str, object]:
    text = text.strip()
    if not text:
        return {}
    try:
        value = json.loads(text)
        return value if isinstance(value, dict) else {}
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{[\s\S]*\}", text)
    if not match:
        return {}
    try:
        value = json.loads(match.group(0))
        return value if isinstance(value, dict) else {}
    except json.JSONDecodeError:
        return {}


def parse_json_list(text: str) -> List[str]:
    text = text.strip()
    if not text:
        return []
    try:
        value = json.loads(text)
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
    except json.JSONDecodeError:
        pass

    match = re.search(r"\[[\s\S]*\]", text)
    if not match:
        return []
    try:
        value = json.loads(match.group(0))
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
    except json.JSONDecodeError:
        return []
    return []


def validate_seed_rows(rows: List[Dict[str, str]]) -> Tuple[List[Dict[str, str]], List[Dict[str, object]]]:
    valid_rows: List[Dict[str, str]] = []
    issues: List[Dict[str, object]] = []
    required_fields = ["department_name_zh", "school_name_zh", "faculty_list_url"]

    for idx, row in enumerate(rows):
        missing = [field for field in required_fields if not row.get(field, "").strip()]
        if missing:
            issues.append(
                {
                    "seed_row_index": idx,
                    "issue": "missing_required_fields",
                    "missing_fields": missing,
                    "row": row,
                }
            )
            continue

        url = row["faculty_list_url"].strip()
        if not (url.startswith("http://") or url.startswith("https://")):
            issues.append(
                {
                    "seed_row_index": idx,
                    "issue": "invalid_url",
                    "row": row,
                }
            )
            continue

        valid_rows.append(row)

    return valid_rows, issues
