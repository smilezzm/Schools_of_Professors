from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from .config import ENRICHED_JSONL, NORMALIZATION_REVIEW_JSONL, NORMALIZED_JSONL
from .deepseek_client import DeepSeekClient
from .utils import parse_json_obj, read_jsonl, today_str, write_jsonl

ALIAS_TO_STD: Dict[str, str] = {
    "北大": "PKU",
    "北京大学": "PKU",
    "北京大学物理学院": "PKU",
    "北京大学物理系": "PKU",
    "中国科学院": "CAS",
    "中科院": "CAS",
    "中科院物理所": "CAS",
    "中国科学院物理研究所": "CAS",
    "清华大学": "THU",
    "复旦大学": "FDU",
    "上海交通大学": "SJTU",
    "浙江大学": "ZJU",
    "南京大学": "NJU",
    "中国科学技术大学": "USTC",
    "武汉大学": "WHU",
    "吉林大学": "JLU",
}


def _map_deterministic(name: str) -> Optional[str]:
    text = (name or "").strip()
    if not text:
        return ""
    if text in ALIAS_TO_STD:
        return ALIAS_TO_STD[text]

    for alias, standard in ALIAS_TO_STD.items():
        if alias and alias in text:
            return standard
    return None


def _map_with_deepseek(name: str, client: DeepSeekClient) -> Tuple[str, float, str]:
    if not name.strip():
        return "", 1.0, "empty"
    if not client.enabled:
        return "", 0.0, "client_disabled"

    prompt = (
        "你负责把学校/科研机构名称标准化成简写（如 PKU, CAS, THU）。"
        "返回JSON对象：{\"abbr\":\"\",\"confidence\":0~1,\"reason\":\"\"}。"
        "若不确定，abbr返回空字符串。"
        f"\n待标准化名称: {name}"
    )
    try:
        text = client.chat_json(prompt, temperature=0.0)
        payload = parse_json_obj(text)
        abbr = str(payload.get("abbr", "")).strip().upper()
        confidence_raw = payload.get("confidence", 0)
        try:
            confidence = float(confidence_raw)
        except Exception:
            confidence = 0.0
        reason = str(payload.get("reason", "")).strip()
        return abbr, confidence, reason
    except Exception as exc:
        return "", 0.0, f"deepseek_error: {exc}"


def _normalize_field(
    source_value: str,
    client: DeepSeekClient,
    review_rows: List[Dict[str, object]],
    row_key: str,
    field_name: str,
) -> str:
    source_value = (source_value or "").strip()
    if not source_value:
        return ""

    mapped = _map_deterministic(source_value)
    if mapped is not None:
        return mapped

    abbr, confidence, reason = _map_with_deepseek(source_value, client)
    if abbr and confidence >= 0.8:
        return abbr

    review_rows.append(
        {
            "row_key": row_key,
            "field": field_name,
            "original_value": source_value,
            "model_abbr": abbr,
            "confidence": confidence,
            "reason": reason,
            "created_at": today_str(),
        }
    )
    return source_value


def run(limit: Optional[int] = None, resume: bool = True) -> None:
    rows = read_jsonl(ENRICHED_JSONL)
    if limit is not None and limit > 0:
        rows = rows[:limit]

    existing_normalized_rows = read_jsonl(NORMALIZED_JSONL) if resume else []
    existing_review_rows = read_jsonl(NORMALIZATION_REVIEW_JSONL) if resume else []

    def row_key(row: Dict[str, object]) -> str:
        return "|".join(
            [
                str(row.get("department_name_zh", "")),
                str(row.get("school_name_zh", "")),
                str(row.get("name_zh", "")),
                str(row.get("name_en", "")),
            ]
        )

    existing_keys = {row_key(row) for row in existing_normalized_rows}
    pending_rows = [row for row in rows if row_key(row) not in existing_keys] if resume else rows

    client = DeepSeekClient()
    review_rows: List[Dict[str, object]] = []
    normalized_rows_new: List[Dict[str, object]] = []

    for row in pending_rows:
        normalized = dict(row)
        row_key = "|".join(
            [
                str(row.get("department_name_zh", "")),
                str(row.get("school_name_zh", "")),
                str(row.get("name_zh", "")),
                str(row.get("name_en", "")),
            ]
        )

        for field in ["bs_school", "ms_school", "phd_school"]:
            normalized[field] = _normalize_field(
                str(row.get(field, "")),
                client=client,
                review_rows=review_rows,
                row_key=row_key,
                field_name=field,
            )
        normalized_rows_new.append(normalized)

    merged_normalized_map = {row_key(row): row for row in existing_normalized_rows}
    for row in normalized_rows_new:
        merged_normalized_map[row_key(row)] = row
    merged_normalized_rows = list(merged_normalized_map.values())

    merged_review_map = {
        "|".join(
            [
                str(row.get("row_key", "")),
                str(row.get("field", "")),
                str(row.get("original_value", "")),
            ]
        ): row
        for row in existing_review_rows + review_rows
    }

    write_jsonl(NORMALIZATION_REVIEW_JSONL, list(merged_review_map.values()))
    write_jsonl(NORMALIZED_JSONL, merged_normalized_rows)
    print(
        "Phase3 finished: "
        f"new_normalized={len(normalized_rows_new)}, total_normalized={len(merged_normalized_rows)}, "
        f"new_manual_review={len(review_rows)}"
    )


if __name__ == "__main__":
    run()
