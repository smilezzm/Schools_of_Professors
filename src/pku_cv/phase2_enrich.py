from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from .config import ENRICHED_JSONL, PROFESSOR_NAMES_JSONL
from .deepseek_client import DeepSeekClient
from .utils import parse_json_obj, read_jsonl, today_str, write_jsonl


def _completion_status(row: Dict[str, object]) -> str:
    bs_school = str(row.get("bs_school", "")).strip()
    phd_school = str(row.get("phd_school", "")).strip()
    join_pku_year = str(row.get("join_pku_year", "")).strip()
    if bs_school and phd_school and join_pku_year:
        return "complete"
    return "incomplete"


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
        "基于北京大学教师主页检索教师信息。"
        "只输出纯文本JSON对象，字段完整："
        "name_en,title,profile_url,bs_school,ms_school,phd_school,join_pku_year,notes。"
        "如果不确定请留空字符串，不要编造。"
        "profile_url 需要通过检索给出最可信的教师个人主页链接。"
        "join_pku_year 只给出年份数字，不要其他文字。"
        "bs_school/ms_school/phd_school 只给出学校名称，不要其他文字（如本科、学士等）。"
        f"\n姓名: {identity}"
        f"\n学部: {result['department_name_zh']}"
        f"\n学院: {result['school_name_zh']}"
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
        "notes",
    ]:
        value = payload.get(key, "")
        result[key] = str(value).strip() if value is not None else ""

    result["status"] = _completion_status(result)
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


def _enrich_task(row: Dict[str, object]) -> Dict[str, object]:
    client = DeepSeekClient()
    return _enrich_one(row, client)


def run(limit: Optional[int] = None, resume: bool = True, workers: int = 3) -> None:
    names = read_jsonl(PROFESSOR_NAMES_JSONL)

    existing_rows = read_jsonl(ENRICHED_JSONL) if resume else []
    existing_keys = {_row_key(row) for row in existing_rows}

    pending = [row for row in names if _row_key(row) not in existing_keys]
    if limit is not None and limit > 0:
        pending = pending[:limit]

    workers = max(1, workers)
    new_rows: List[Dict[str, object]] = []
    if workers == 1:
        client = DeepSeekClient()
        for row in pending:
            new_rows.append(_enrich_one(row, client))
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {executor.submit(_enrich_task, row): row for row in pending}
            for future in as_completed(future_map):
                seed_row = future_map[future]
                try:
                    new_rows.append(future.result())
                except Exception as exc:
                    fallback = _default_enriched(seed_row)
                    fallback["notes"] = f"concurrent_enrich_error: {exc}"
                    fallback["status"] = _completion_status(fallback)
                    new_rows.append(fallback)

    merged_map = {_row_key(row): row for row in existing_rows}
    for row in new_rows:
        merged_map[_row_key(row)] = row
    enriched_rows = list(merged_map.values())
    for row in enriched_rows:
        row["status"] = _completion_status(row)

    write_jsonl(ENRICHED_JSONL, enriched_rows)
    print(f"Phase2 finished: new={len(new_rows)}, total={len(enriched_rows)}")


if __name__ == "__main__":
    run()
