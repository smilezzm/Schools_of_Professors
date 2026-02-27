from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from .config import (
    DEFAULT_MAX_PAGES,
    DEFAULT_TIMEOUT_SECONDS,
    LISTING_PAGES_JSONL,
    NAME_CANDIDATES_JSONL,
    PAGES_DIR,
    PROFESSOR_NAMES_JSONL,
    SCHOOLS_SEED_CSV,
    SEED_ISSUES_JSONL,
)
from .deepseek_client import DeepSeekClient
from .utils import (
    ensure_dir,
    parse_json_list,
    read_csv_rows,
    read_jsonl,
    safe_slug,
    today_str,
    validate_seed_rows,
    write_jsonl,
)

try:
    from playwright.sync_api import sync_playwright  # type: ignore

    PLAYWRIGHT_AVAILABLE = True
except Exception:
    sync_playwright = None  # type: ignore
    PLAYWRIGHT_AVAILABLE = False


ZH_NAME_RE = re.compile(r"^[\u4e00-\u9fff]{2,4}$")
EN_WORD_RE = re.compile(r"^[A-Za-z][A-Za-z\-'.]*$")
STOPWORDS = {
    "导航",
    "门户",
    "概况",
    "简介",
    "历史",
    "学院",
    "新闻",
    "公告",
    "招生",
    "校友",
    "联系",
    "首页",
    "教研人员",
    "教师队伍",
    "快速",
    "主页",
    "北大",
    "网络",
    "课题组",
    "组长",
    "在职",
    "教师",
}
EN_STOPWORDS = {
    "home",
    "portal",
    "about",
    "overview",
    "news",
    "notice",
    "admission",
    "alumni",
    "contact",
    "faculty",
    "teacher",
    "staff",
    "research",
    "group",
    "navigation",
}
NEXT_SELECTORS = [
    "#pageBarNextPageIdu12",
    "a:has-text('下一页')",
    "a:has-text('下页')",
    "span:has-text('下一页')",
    "span:has-text('下页')",
    "li:has-text('下一页')",
]


def _normalize_token(token: str) -> str:
    token = re.sub(r"\s+", " ", token.strip())
    return token


def _is_zh_name(token: str) -> bool:
    token = token.strip()
    if not ZH_NAME_RE.match(token):
        return False
    if token in STOPWORDS:
        return False
    if any(word in token for word in STOPWORDS):
        return False
    return True


def _is_en_name(token: str) -> bool:
    token = _normalize_token(token)
    if not token:
        return False
    if any(ch.isdigit() for ch in token):
        return False
    words = token.split(" ")
    if len(words) < 2 or len(words) > 3:
        return False

    if not all(EN_WORD_RE.match(word) for word in words):
        return False

    if all(len(word) == 1 for word in words):
        return False

    def _is_word_valid(word: str) -> bool:
        return word.isupper() or (word[0].isupper() and word[1:].islower())

    if not all(_is_word_valid(word) for word in words):
        return False

    lower = token.lower()
    if lower in EN_STOPWORDS:
        return False
    lower_words = set(lower.split(" "))
    if any(stop in lower_words for stop in EN_STOPWORDS):
        return False
    return True


def _looks_like_name(token: str) -> bool:
    token = _normalize_token(token)
    return _is_zh_name(token) or _is_en_name(token)


def _name_type(token: str) -> str:
    token = token.strip()
    if _is_zh_name(token):
        return "zh"
    if _is_en_name(token):
        return "en"
    return ""


def _collect_candidate_pairs(html: str, page_url: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    pairs: List[Tuple[str, str]] = []

    for anchor in soup.find_all("a"):
        text = _normalize_token(anchor.get_text() or "")
        href = (anchor.get("href") or "").strip()
        if not _looks_like_name(text):
            continue
        profile_url = urljoin(page_url, href) if href else ""
        pairs.append((text, profile_url))

    raw_text = soup.get_text(" ", strip=True)
    for token in re.split(r"[\s,，。；;：:、|/()（）\[\]<>《》‘’“”\-]+", raw_text):
        token = _normalize_token(token)
        if _looks_like_name(token):
            pairs.append((token, ""))

    for match in re.finditer(r"\b[A-Z][A-Za-z\-'.]*(?:\s+[A-Z][A-Za-z\-'.]*){0,2}\b", raw_text):
        token = _normalize_token(match.group(0))
        if _looks_like_name(token):
            pairs.append((token, ""))

    dedup_map: Dict[str, str] = {}
    for name, profile_url in pairs:
        if name not in dedup_map or (not dedup_map[name] and profile_url):
            dedup_map[name] = profile_url
    return sorted(dedup_map.items(), key=lambda item: item[0])


def _extract_signature(html: str) -> str:
    names = [name for name, _ in _collect_candidate_pairs(html, "")]
    return "|".join(names[:20])


def _click_next(page: Any) -> bool:
    for selector in NEXT_SELECTORS:
        locator = page.locator(selector)
        if locator.count() == 0:
            continue
        target = locator.first
        if not target.is_visible():
            continue
        try:
            target.click(timeout=2500)
            page.wait_for_timeout(1300)
            return True
        except Exception:
            continue
    return False


def _discover_with_playwright(
    start_url: str,
    department_name_zh: str,
    school_name_zh: str,
    seed_row_index: int,
    max_pages_per_seed: int,
    timeout: int,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []

    with sync_playwright() as p:  # type: ignore[misc]
        browser = None
        try:
            browser = p.chromium.launch(channel="chrome", headless=True)
        except Exception:
            browser = p.chromium.launch(headless=True)

        page = browser.new_page()
        page.goto(start_url, wait_until="networkidle", timeout=timeout * 1000)
        page.wait_for_timeout(1600)

        last_signature = ""
        repeat_count = 0

        for page_index in range(1, max_pages_per_seed + 1):
            html = page.content()
            slug = safe_slug(f"{school_name_zh}-{seed_row_index}-{page_index}")
            html_path = PAGES_DIR / f"{slug}.html"
            html_path.write_text(html, encoding="utf-8")

            rows.append(
                {
                    "department_name_zh": department_name_zh,
                    "school_name_zh": school_name_zh,
                    "seed_faculty_list_url": start_url,
                    "listing_page_url": page.url,
                    "page_index": page_index,
                    "seed_row_index": seed_row_index,
                    "html_path": str(html_path),
                    "crawl_date": today_str(),
                }
            )

            signature = _extract_signature(html)
            if signature and signature == last_signature:
                repeat_count += 1
            else:
                repeat_count = 0
            last_signature = signature

            if repeat_count >= 2:
                break

            if not _click_next(page):
                break

        browser.close()

    return rows


def _find_next_url(current_url: str, html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.find_all("a"):
        text = (anchor.get_text() or "").strip().lower()
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        if "下一页" in text or "下页" in text or text == "next":
            return urljoin(current_url, href)
    return None


def _discover_with_requests(
    start_url: str,
    department_name_zh: str,
    school_name_zh: str,
    seed_row_index: int,
    max_pages_per_seed: int,
    timeout: int,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    session = requests.Session()
    visited: Set[str] = set()
    current_url = start_url

    for page_index in range(1, max_pages_per_seed + 1):
        if current_url in visited:
            break
        visited.add(current_url)

        response = session.get(current_url, timeout=timeout)
        response.raise_for_status()
        response.encoding = response.apparent_encoding or response.encoding
        html = response.text

        slug = safe_slug(f"{school_name_zh}-{seed_row_index}-{page_index}")
        html_path = PAGES_DIR / f"{slug}.html"
        html_path.write_text(html, encoding="utf-8")

        rows.append(
            {
                "department_name_zh": department_name_zh,
                "school_name_zh": school_name_zh,
                "seed_faculty_list_url": start_url,
                "listing_page_url": current_url,
                "page_index": page_index,
                "seed_row_index": seed_row_index,
                "html_path": str(html_path),
                "crawl_date": today_str(),
            }
        )

        next_url = _find_next_url(current_url, html)
        if not next_url:
            break
        current_url = next_url

    return rows


def _filter_names_with_deepseek(
    school_name_zh: str,
    department_name_zh: str,
    candidates: List[str],
    client: DeepSeekClient,
) -> List[str]:
    if not candidates:
        return []

    deterministic = [name for name in candidates if _looks_like_name(name)]
    if not client.enabled:
        return sorted(set(deterministic))

    selected: Set[str] = set()
    batch_size = 60
    for start in range(0, len(deterministic), batch_size):
        batch = deterministic[start : start + batch_size]
        prompt = (
            "请从候选词中筛选‘真实教师姓名’，返回JSON数组。"
            "只保留人名，剔除栏目词、职位词、页面导航词、学科词、机构词。"
            "中文姓名一般2-4个汉字；英文姓名2-3词，且每个词应为首字母大写或全大写。"
            f"\n院系: {department_name_zh}"
            f"\n单位: {school_name_zh}"
            "\n候选词列表: "
            f"{batch}"
            "\n仅输出JSON数组，例如: [\"张三\", \"Li Ming\"]"
        )

        try:
            text = client.chat_json(prompt, temperature=0.0)
            names = parse_json_list(text)
            clean = [name for name in names if _looks_like_name(name)]
            selected.update(clean)
        except Exception:
            selected.update(batch)

    return sorted(selected)


def run(
    max_pages_per_seed: int = DEFAULT_MAX_PAGES,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    seed_start: int = 0,
    seed_limit: Optional[int] = None,
    resume: bool = True,
    require_deepseek: bool = False,
) -> None:
    ensure_dir(PAGES_DIR)

    raw_seed_rows = read_csv_rows(SCHOOLS_SEED_CSV)
    valid_rows, issues = validate_seed_rows(raw_seed_rows)
    write_jsonl(SEED_ISSUES_JSONL, issues)

    if seed_start > 0:
        valid_rows = valid_rows[seed_start:]
    if seed_limit is not None and seed_limit > 0:
        valid_rows = valid_rows[:seed_limit]

    existing_listing_rows = read_jsonl(LISTING_PAGES_JSONL) if resume else []
    processed_seed_indices: Set[int] = set()
    for row in existing_listing_rows:
        idx = row.get("seed_row_index")
        if isinstance(idx, int):
            processed_seed_indices.add(idx)
        else:
            try:
                processed_seed_indices.add(int(str(idx)))
            except Exception:
                pass

    listing_rows_new: List[Dict[str, object]] = []
    for local_index, seed in enumerate(valid_rows):
        seed_row_index = seed_start + local_index
        if resume and seed_row_index in processed_seed_indices:
            continue

        department_name_zh = seed.get("department_name_zh", "").strip()
        school_name_zh = seed.get("school_name_zh", "").strip()
        start_url = seed.get("faculty_list_url", "").strip()

        if not start_url:
            continue

        if PLAYWRIGHT_AVAILABLE:
            try:
                rows = _discover_with_playwright(
                    start_url=start_url,
                    department_name_zh=department_name_zh,
                    school_name_zh=school_name_zh,
                    seed_row_index=seed_row_index,
                    max_pages_per_seed=max_pages_per_seed,
                    timeout=timeout,
                )
                listing_rows_new.extend(rows)
                continue
            except Exception:
                pass

        rows = _discover_with_requests(
            start_url=start_url,
            department_name_zh=department_name_zh,
            school_name_zh=school_name_zh,
            seed_row_index=seed_row_index,
            max_pages_per_seed=max_pages_per_seed,
            timeout=timeout,
        )
        listing_rows_new.extend(rows)

    merged_listing_map: Dict[str, Dict[str, object]] = {}
    for row in existing_listing_rows + listing_rows_new:
        key = str(row.get("html_path") or "").strip()
        if not key:
            key = "|".join(
                [
                    str(row.get("seed_row_index") or ""),
                    str(row.get("listing_page_url") or ""),
                    str(row.get("page_index") or ""),
                ]
            )
        merged_listing_map[key] = row
    listing_rows = list(merged_listing_map.values())

    write_jsonl(LISTING_PAGES_JSONL, listing_rows)

    existing_candidate_rows = read_jsonl(NAME_CANDIDATES_JSONL) if resume else []
    existing_candidate_html_paths = {
        str(row.get("html_path") or "").strip() for row in existing_candidate_rows if row.get("html_path")
    }

    candidate_rows_new: List[Dict[str, object]] = []

    for listing in listing_rows:
        html_path = str(listing.get("html_path") or "").strip()
        listing_url = str(listing.get("listing_page_url") or "").strip()
        if not html_path:
            continue
        if resume and html_path in existing_candidate_html_paths:
            continue

        try:
            content = Path(html_path).read_text(encoding="utf-8")
        except Exception:
            continue

        pairs = _collect_candidate_pairs(content, listing_url)
        department_name_zh = str(listing.get("department_name_zh") or "")
        school_name_zh = str(listing.get("school_name_zh") or "")
        key = (department_name_zh, school_name_zh)

        for name, profile_url in pairs:
            candidate_rows_new.append(
                {
                    "department_name_zh": department_name_zh,
                    "school_name_zh": school_name_zh,
                    "name_candidate": name,
                    "profile_url": profile_url,
                    "html_path": html_path,
                    "listing_page_url": listing_url,
                    "seed_row_index": listing.get("seed_row_index"),
                    "crawl_date": listing.get("crawl_date"),
                }
            )

    candidate_merged_map: Dict[str, Dict[str, object]] = {}
    for row in existing_candidate_rows + candidate_rows_new:
        key = "|".join(
            [
                str(row.get("department_name_zh") or ""),
                str(row.get("school_name_zh") or ""),
                str(row.get("name_candidate") or ""),
                str(row.get("profile_url") or ""),
                str(row.get("listing_page_url") or ""),
            ]
        )
        candidate_merged_map[key] = row
    candidate_rows = list(candidate_merged_map.values())
    write_jsonl(NAME_CANDIDATES_JSONL, candidate_rows)

    school_candidates: Dict[Tuple[str, str], Dict[str, str]] = defaultdict(dict)
    for row in candidate_rows:
        key = (
            str(row.get("department_name_zh") or ""),
            str(row.get("school_name_zh") or ""),
        )
        name = str(row.get("name_candidate") or "").strip()
        profile_url = str(row.get("profile_url") or "").strip()
        if not name:
            continue
        school_candidates[key].setdefault(name, profile_url)

    client = DeepSeekClient()
    if require_deepseek and not client.enabled:
        raise RuntimeError(
            "Phase1 DeepSeek filtering is required but DEEPSEEK_API_KEY is missing. "
            "Set it in .env or environment variables."
        )
    existing_professor_rows = read_jsonl(PROFESSOR_NAMES_JSONL) if resume else []
    existing_by_school: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    for row in existing_professor_rows:
        key = (
            str(row.get("department_name_zh") or ""),
            str(row.get("school_name_zh") or ""),
        )
        zh_name = str(row.get("name_zh") or "").strip()
        en_name = str(row.get("name_en") or "").strip()
        if zh_name:
            existing_by_school[key].add(zh_name)
        if en_name:
            existing_by_school[key].add(en_name)

    professor_rows_new: List[Dict[str, object]] = []
    for (department_name_zh, school_name_zh), candidate_map in school_candidates.items():
        all_candidates = sorted(candidate_map.keys())
        existing_names = existing_by_school[(department_name_zh, school_name_zh)] if resume else set()
        candidates = [name for name in all_candidates if name not in existing_names]
        if not candidates:
            continue

        filtered_names = _filter_names_with_deepseek(
            school_name_zh=school_name_zh,
            department_name_zh=department_name_zh,
            candidates=candidates,
            client=client,
        )

        for name in filtered_names:
            name_kind = _name_type(name)
            professor_rows_new.append(
                {
                    "department_name_zh": department_name_zh,
                    "school_name_zh": school_name_zh,
                    "name_zh": name if name_kind == "zh" else "",
                    "name_en": name if name_kind == "en" else "",
                    "profile_url": "",
                    "source": "phase1_discovery",
                    "crawl_date": today_str(),
                }
            )

    dedup: Dict[str, Dict[str, object]] = {}
    for row in existing_professor_rows + professor_rows_new:
        key = "|".join(
            [
                str(row.get("department_name_zh") or ""),
                str(row.get("school_name_zh") or ""),
                str(row.get("name_zh") or ""),
                str(row.get("name_en") or ""),
            ]
        )
        if key not in dedup:
            dedup[key] = row

    write_jsonl(PROFESSOR_NAMES_JSONL, list(dedup.values()))
    print(
        "Phase1 finished: "
        f"new_pages={len(listing_rows_new)}, total_pages={len(listing_rows)}, "
        f"new_candidates={len(candidate_rows_new)}, total_candidates={len(candidate_rows)}, "
        f"new_names={len(professor_rows_new)}, total_names={len(dedup)}"
    )


if __name__ == "__main__":
    run()
