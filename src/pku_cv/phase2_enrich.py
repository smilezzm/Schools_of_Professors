from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .config import ENRICHED_JSONL, PROFESSOR_NAMES_JSONL
from .deepseek_client import DeepSeekClient
from .utils import parse_json_obj, read_jsonl, today_str, write_jsonl


PROFILE_LINK_HINTS = [
    "个人主页",
    "个人简历",
    "个人信息",
    "个人简介",
    "详细信息",
    "详情",
    "简历",
    "主页",
    "cv",
    "resume",
    "profile",
]


def _same_domain_or_subdomain(base_url: str, target_url: str) -> bool:
    try:
        base_host = (urlparse(base_url).hostname or "").lower()
        target_host = (urlparse(target_url).hostname or "").lower()
    except Exception:
        return False
    if not base_host or not target_host:
        return False
    return target_host == base_host or target_host.endswith("." + base_host)


def _looks_like_profile_hint(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    return any(hint in lowered for hint in PROFILE_LINK_HINTS)


def _score_profile_link(text: str, href: str) -> int:
    lowered = (str(text or "") + " " + str(href or "")).lower()
    score = 0
    if "个人主页" in lowered or "homepage" in lowered:
        score += 5
    if "简历" in lowered or "cv" in lowered or "resume" in lowered:
        score += 4
    if "详细" in lowered or "详情" in lowered or "profile" in lowered:
        score += 2
    return score


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
        "profile_url": row.get("profile_url", ""),
        "bs_school": "",
        "ms_school": "",
        "phd_school": "",
        "join_pku_year": "",
        "status": "incomplete",
        "notes": "",
        "crawl_date": today_str(),
    }


def _normalize_year(value: str) -> str:
    text = str(value or "").strip()
    match = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    return match.group(1) if match else ""


def _fetch_profile_text(profile_url: str, timeout: int = 20) -> str:
    url = str(profile_url or "").strip()
    if not url:
        return ""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
    }
    try:
        response = requests.get(url, timeout=timeout, headers=headers)
        response.raise_for_status()
        response.encoding = response.apparent_encoding or response.encoding
    except Exception:
        return ""

    soup = BeautifulSoup(response.text, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "footer", "nav"]):
        tag.extract()
    text = soup.get_text("\n", strip=True)
    text = re.sub(r"\n{2,}", "\n", text)
    return text[:12000]


def _fetch_profile_page(profile_url: str, timeout: int = 20) -> Tuple[str, BeautifulSoup, str]:
    url = str(profile_url or "").strip()
    empty_soup = BeautifulSoup("", "html.parser")
    if not url:
        return "", empty_soup, ""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
    }
    try:
        response = requests.get(url, timeout=timeout, headers=headers)
        response.raise_for_status()
        response.encoding = response.apparent_encoding or response.encoding
    except Exception:
        return "", empty_soup, url

    soup = BeautifulSoup(response.text, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "footer", "nav"]):
        tag.extract()
    text = soup.get_text("\n", strip=True)
    text = re.sub(r"\n{2,}", "\n", text)
    return text[:12000], soup, str(response.url or url)


def _discover_secondary_profile_links(soup: BeautifulSoup, base_url: str, max_links: int = 2) -> List[str]:
    scored: List[Tuple[int, str]] = []
    for anchor in soup.find_all("a"):
        link_text = str(anchor.get_text(" ", strip=True) or "")
        href = str(anchor.get("href") or "").strip()
        if not href:
            continue
        absolute = urljoin(base_url, href)
        if not absolute.startswith("http"):
            continue
        if not _same_domain_or_subdomain(base_url, absolute):
            continue
        if not _looks_like_profile_hint(link_text) and not _looks_like_profile_hint(href):
            continue
        score = _score_profile_link(link_text, href)
        scored.append((score, absolute))

    scored.sort(key=lambda item: item[0], reverse=True)
    selected: List[str] = []
    seen: Set[str] = set()
    for _, url in scored:
        if url in seen:
            continue
        seen.add(url)
        selected.append(url)
        if len(selected) >= max_links:
            break
    return selected


def _fetch_profile_text_with_secondary_links(profile_url: str) -> str:
    primary_text, soup, resolved_url = _fetch_profile_page(profile_url)
    combined_parts: List[str] = []
    if primary_text:
        combined_parts.append(primary_text)
    if not resolved_url:
        return "\n".join(combined_parts)[:16000]

    secondary_links = _discover_secondary_profile_links(soup, resolved_url, max_links=2)
    for link in secondary_links:
        secondary_text, _, _ = _fetch_profile_page(link)
        if secondary_text:
            combined_parts.append(secondary_text)

    combined = "\n\n".join(part for part in combined_parts if part)
    return combined[:16000]


def _apply_payload(result: Dict[str, object], payload: Dict[str, object], fill_only_missing: bool) -> None:
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
        value = str(payload.get(key, "") or "").strip()
        if fill_only_missing:
            if not str(result.get(key, "") or "").strip() and value:
                result[key] = value
        else:
            result[key] = value
    result["join_pku_year"] = _normalize_year(str(result.get("join_pku_year", "")))


def _needs_search_fallback(result: Dict[str, object]) -> bool:
    return not (
        str(result.get("bs_school", "")).strip()
        and str(result.get("phd_school", "")).strip()
        and str(result.get("join_pku_year", "")).strip()
    )


def _enrich_one(
    row: Dict[str, object],
    client: DeepSeekClient,
    enable_web_search_fallback: bool = True,
) -> Dict[str, object]:
    result = _default_enriched(row)
    if not client.enabled:
        return result

    identity = result["name_zh"] or result["name_en"]
    profile_text = _fetch_profile_text_with_secondary_links(str(result.get("profile_url", "")))
    if profile_text:
        profile_prompt = (
            "你会收到某位教师个人主页的正文文本，请仅基于这些文本抽取字段。"
            "不能使用外部搜索，不能猜测。"
            "\n只输出纯文本JSON对象，字段完整："
            "name_en,title,profile_url,bs_school,ms_school,phd_school,join_pku_year,notes。"
            "\n字段为空时用空字符串。"
            "\nbs_school/ms_school/phd_school 仅输出学校或科研机构名称，"
            "不要包含院系、专业、实验室或项目描述（例如不要输出‘物理系’）。"
            f"\n姓名: {identity}"
            f"\n学部: {result['department_name_zh']}"
            f"\n学院: {result['school_name_zh']}"
            f"\n主页URL: {result.get('profile_url', '')}"
            f"\n主页正文:\n{profile_text}"
        )
        try:
            profile_text_resp = client.chat_json(profile_prompt, temperature=0.0)
            profile_payload = parse_json_obj(profile_text_resp)
            _apply_payload(result, profile_payload, fill_only_missing=False)
        except Exception as exc:
            result["notes"] = f"profile_extract_error: {exc}"

    if enable_web_search_fallback and _needs_search_fallback(result):
        prompt = (
            "网络搜索北京大学相关学院教师主页或个人简历，检索这位北京大学教师信息。"
            "优先补全缺失字段，不要覆盖已有且看似合理的信息。"
            f"\n姓名: {identity}"
            f"\n学部: {result['department_name_zh']}"
            f"\n学院: {result['school_name_zh']}"
            "\n只输出纯文本JSON对象，字段完整："
            "name_en,title,profile_url,bs_school,ms_school,phd_school,join_pku_year,notes。"
            "\n如果不确定请留空字符串，不要有任何编造！"
            "\nbs_school/ms_school/phd_school仅写学校或机构名称，"
            "不要包含院系、专业、实验室或项目描述（例如不要输出‘物理系’）。"
            "\njoin_pku_year只保留4位年份。"
            "\n当前已提取信息："
            f"\nname_en={result.get('name_en','')}"
            f"\ntitle={result.get('title','')}"
            f"\nprofile_url={result.get('profile_url','')}"
            f"\nbs_school={result.get('bs_school','')}"
            f"\nms_school={result.get('ms_school','')}"
            f"\nphd_school={result.get('phd_school','')}"
            f"\njoin_pku_year={result.get('join_pku_year','')}"
        )
        try:
            text = client.chat_json(prompt, temperature=0.05)
            payload = parse_json_obj(text)
            _apply_payload(result, payload, fill_only_missing=True)
        except Exception as exc:
            existing_notes = str(result.get("notes", "")).strip()
            suffix = f"deepseek_error: {exc}"
            result["notes"] = f"{existing_notes}; {suffix}" if existing_notes else suffix
    elif (not enable_web_search_fallback) and _needs_search_fallback(result):
        existing_notes = str(result.get("notes", "")).strip()
        suffix = "web_search_disabled"
        result["notes"] = f"{existing_notes}; {suffix}" if existing_notes else suffix

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


def _enrich_task(row: Dict[str, object], enable_web_search_fallback: bool) -> Dict[str, object]:
    client = DeepSeekClient()
    return _enrich_one(row, client, enable_web_search_fallback=enable_web_search_fallback)


def run(
    limit: Optional[int] = None,
    resume: bool = True,
    workers: int = 3,
    enable_web_search_fallback: bool = True,
) -> None:
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
            new_rows.append(
                _enrich_one(row, client, enable_web_search_fallback=enable_web_search_fallback)
            )
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {
                executor.submit(_enrich_task, row, enable_web_search_fallback): row
                for row in pending
            }
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
