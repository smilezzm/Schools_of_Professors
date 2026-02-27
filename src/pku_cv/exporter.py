from __future__ import annotations

from typing import Dict, List

from .config import FINAL_OUTPUT_CSV, NORMALIZED_JSONL, PROFESSORS_TEMPLATE_CSV
from .utils import read_csv_header, read_csv_rows, read_jsonl, today_str, write_csv


def _row_key(row: Dict[str, object]) -> str:
    return "|".join(
        [
            str(row.get("department_name_zh", "")).strip(),
            str(row.get("school_name_zh", "")).strip(),
            str(row.get("name_zh", "")).strip(),
            str(row.get("name_en", "")).strip(),
        ]
    )


def run() -> None:
    headers = read_csv_header(PROFESSORS_TEMPLATE_CSV)
    records = read_jsonl(NORMALIZED_JSONL)

    merged_map: Dict[str, Dict[str, object]] = {}

    if FINAL_OUTPUT_CSV.exists():
        for row in read_csv_rows(FINAL_OUTPUT_CSV):
            mapped_existing = {field: row.get(field, "") for field in headers}
            merged_map[_row_key(mapped_existing)] = mapped_existing

    for row in records:
        mapped = {field: row.get(field, "") for field in headers}
        if not mapped.get("crawl_date"):
            mapped["crawl_date"] = today_str()
        merged_map[_row_key(mapped)] = mapped

    output_rows: List[Dict[str, object]] = list(merged_map.values())

    write_csv(FINAL_OUTPUT_CSV, headers, output_rows)
    print(f"Export finished: rows={len(output_rows)} -> {FINAL_OUTPUT_CSV}")


if __name__ == "__main__":
    run()
