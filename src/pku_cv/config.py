from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

SCHOOLS_SEED_CSV = PROJECT_ROOT / "schools_seed.csv"
PROFESSORS_TEMPLATE_CSV = PROJECT_ROOT / "professors_template.csv"

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PAGES_DIR = RAW_DIR / "pages"
INTERIM_DIR = DATA_DIR / "interim"
OUTPUT_DIR = DATA_DIR / "output"
MANUAL_DIR = DATA_DIR / "manual"

SEED_ISSUES_JSONL = INTERIM_DIR / "seed_issues.jsonl"
LISTING_PAGES_JSONL = INTERIM_DIR / "phase1_listing_pages.jsonl"
NAME_CANDIDATES_JSONL = INTERIM_DIR / "phase1_name_candidates.jsonl"
PROFESSOR_NAMES_JSONL = INTERIM_DIR / "phase1_professor_names.jsonl"
ENRICHED_JSONL = INTERIM_DIR / "phase2_professors_enriched.jsonl"
NORMALIZED_JSONL = INTERIM_DIR / "phase3_professors_normalized.jsonl"
NORMALIZATION_REVIEW_JSONL = MANUAL_DIR / "normalization_review.jsonl"
FINAL_OUTPUT_CSV = OUTPUT_DIR / "professors_output.csv"

DEFAULT_TIMEOUT_SECONDS = int(os.getenv("PKU_CRAWL_TIMEOUT", "25"))
DEFAULT_MAX_PAGES = int(os.getenv("PKU_MAX_PAGES_PER_SEED", "30"))
DEFAULT_DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
DEFAULT_DEEPSEEK_ENDPOINT = os.getenv(
    "DEEPSEEK_ENDPOINT", "https://api.deepseek.com/chat/completions"
)
