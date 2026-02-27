from __future__ import annotations

from typing import Dict, List, Optional

from .config import ENRICHED_JSONL, PROFESSOR_NAMES_JSONL
from .deepseek_client import DeepSeekClient
from .utils import parse_json_obj, read_jsonl, today_str, write_jsonl


def _default_enriched(row: Dict[str, object]) -> Dict[str, object]:
    return {
        "department_name_zh": row.get("department_name_zh", ""),
        "school_name_zh": row.get("school_name_zh", ""),
        "name_zh": row.get("name_zh", ""),
        "name_en": row.get("name_en", ""),
        "title": "",
        "profile_url": "",
        "bs_school": "",
        "ms_school": "",
        "phd_school": "",
        "join_pku_year": "",
        "status": "incomplete",
        "notes": "",
        "crawl_date": today_str(),
    }


def _enrich_one(row: Dict[str, object], client: DeepSeekClient) -> Dict[str, object]:
    result = _default_enriched(row)
    if not client.enabled:
        return result

    identity = result["name_zh"] or result["name_en"]
    prompt = (
        "请基于公开网页检索信息，抽取北京大学教师信息。"
        "输出JSON对象，字段必须完整："
        "name_en,title,profile_url,bs_school,ms_school,phd_school,join_pku_year,status,notes。"
        "如果不确定请留空字符串，不要编造。"
        "profile_url 需要通过检索给出最可信的教师个人主页或官方简介页面链接。"
        f"\n姓名: {identity}"
        f"\n中文姓名: {result['name_zh']}"
        f"\n英文姓名: {result['name_en']}"
        f"\n院系: {result['department_name_zh']}"
        f"\n单位: {result['school_name_zh']}"
    )
    try:
        text = client.chat_json(prompt, temperature=0.05)
        payload = parse_json_obj(text)
    except Exception as exc:
        result["notes"] = f"deepseek_error: {exc}"
        return result

    for key in [
        "name_en",
        "title",
        "profile_url",
        "bs_school",
        "ms_school",
        "phd_school",
        "join_pku_year",
        "status",
        "notes",
    ]:
        value = payload.get(key, "")
        result[key] = str(value).strip() if value is not None else ""

    if not result["status"]:
        result["status"] = "incomplete"
    return result


def _row_key(row: Dict[str, object]) -> str:
    return "|".join(
        [
            str(row.get("department_name_zh", "")),
            str(row.get("school_name_zh", "")),
            str(row.get("name_zh", "")),
            str(row.get("name_en", "")),
        ]
    )


def run(limit: Optional[int] = None, resume: bool = True) -> None:
    names = read_jsonl(PROFESSOR_NAMES_JSONL)

    existing_rows = read_jsonl(ENRICHED_JSONL) if resume else []
    existing_keys = {_row_key(row) for row in existing_rows}

    pending = [row for row in names if _row_key(row) not in existing_keys]
    if limit is not None and limit > 0:
        pending = pending[:limit]

    client = DeepSeekClient()
    new_rows: List[Dict[str, object]] = []
    for row in pending:
        new_rows.append(_enrich_one(row, client))

    merged_map = {_row_key(row): row for row in existing_rows}
    for row in new_rows:
        merged_map[_row_key(row)] = row
    enriched_rows = list(merged_map.values())

    write_jsonl(ENRICHED_JSONL, enriched_rows)
    print(f"Phase2 finished: new={len(new_rows)}, total={len(enriched_rows)}")


if __name__ == "__main__":
    run()
