from __future__ import annotations

from typing import Dict, List

from .config import FINAL_OUTPUT_CSV, NORMALIZED_JSONL, PROFESSORS_TEMPLATE_CSV
from .utils import read_csv_header, read_jsonl, today_str, write_csv


def run() -> None:
    headers = read_csv_header(PROFESSORS_TEMPLATE_CSV)
    records = read_jsonl(NORMALIZED_JSONL)

    output_rows: List[Dict[str, object]] = []
    for row in records:
        mapped = {field: row.get(field, "") for field in headers}
        if not mapped.get("crawl_date"):
            mapped["crawl_date"] = today_str()
        output_rows.append(mapped)

    write_csv(FINAL_OUTPUT_CSV, headers, output_rows)
    print(f"Export finished: rows={len(output_rows)} -> {FINAL_OUTPUT_CSV}")


if __name__ == "__main__":
    run()
