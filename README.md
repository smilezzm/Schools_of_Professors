# School_of_Professor

Phase-based pipeline to collect PKU professor information from school seed URLs, enrich profile data, normalize institution names, and export a final CSV aligned to `professors_template.csv`.

---

## 1) What this repo does

Input:
- `schools_seed.csv`: school/faculty list seeds (one row per school unit)
- `professors_template.csv`: required output column schema (This is already given and not reconfigureable)

Pipeline:
1. **Phase1** (`phase1_discovery.py`)
   - Crawl faculty list pages from seeds
   - Handle static pagination and JS pages
   - Extract name candidates and profile links
   - Filter to professor names (deterministic + optional DeepSeek)
2. **Phase2** (`phase2_enrich.py`)
   - Enrich per professor record (title, schools, join year, etc.)
   - Profile-page-first extraction with DeepSeek
   - Optional fallback to web search (`--no-web-search` disables fallback). It is strongly recommended to turn off web-search (i.e. add --no-web-search when running in terminal) because AI web-search is not accurate via API. This problem is goint to be resolved with efforts. 
3. **Phase3** (`phase3_normalize.py`)
   - Normalize `bs_school/ms_school/phd_school` to abbreviations
   - Deterministic dictionary first (`university_alias_dictionary.csv`)
   - AI fallback for uncertain values
   - Write uncertain cases to manual review file
   - **Apply manual overrides from `data/manual/normalization_review.jsonl`**
4. **Export** (`exporter.py`)
   - Export merged final CSV to `data/output/professors_output.csv`

---

## 2) Project structure

```text
School_of_Professor/
├─ run_pipeline.py
├─ schools_seed.csv
├─ professors_template.csv
├─ university_alias_dictionary.csv
├─ src/pku_cv/
│  ├─ cli.py
│  ├─ config.py
│  ├─ utils.py
│  ├─ deepseek_client.py
│  ├─ phase1_discovery.py
│  ├─ phase2_enrich.py
│  ├─ phase3_normalize.py
│  └─ exporter.py
└─ data/
   ├─ raw/pages/
   ├─ interim/
   │  ├─ seed_issues.jsonl
   │  ├─ phase1_listing_pages.jsonl
   │  ├─ phase1_name_candidates.jsonl
   │  ├─ phase1_professor_names.jsonl
   │  ├─ phase2_professors_enriched.jsonl
   │  └─ phase3_professors_normalized.jsonl
   ├─ manual/
   │  └─ normalization_review.jsonl
   └─ output/
      └─ professors_output.csv
```

---

## 3) Setup

## 3.1 Python env (recommended)

If `.venv` does not exist yet, create it first:

```powershell
python -m venv .venv
```

Then verify the virtualenv interpreter:

```powershell
.venv\Scripts\python.exe --version
```

If needed, install dependencies in this env:

```powershell
.venv\Scripts\python.exe -m pip install -U pip
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe -m playwright install chromium 
```
where 
```
.venv\Scripts\python.exe -m playwright install chromium 
```
may take some time. If there is chrome on your system, you can skip this installation and install it only when failures occur:
```
"Playwright browser launch failed. "
"Tried system Chrome first, then bundled Chromium. "
"Install Chrome or run 'python -m playwright install chromium'. "
```

Optional (activate venv in terminal first, then use plain `python`):

```powershell
.venv\Scripts\activate
python --version
python -m pip install -r requirements.txt
python -m playwright install chromium
```

If you do not want to use a venv, you can use system Python directly:

```powershell
python --version
python -m pip install -r requirements.txt
python -m playwright install chromium
```

But venv is recommended to avoid dependency/version conflicts.

## 3.2 DeepSeek key (optional but recommended)

Put API key in `.env` (repo root), if there is no file named `.env`, create one:

```env
DEEPSEEK_API_KEY=your_key
```

Optional envs (see `config.py`):
- `DEEPSEEK_MODEL` (default: `deepseek-chat`)
- `DEEPSEEK_ENDPOINT` (default: `https://api.deepseek.com/chat/completions`)
- `PKU_CRAWL_TIMEOUT`
- `PKU_MAX_PAGES_PER_SEED`

---

## 4) CLI usage (`cli.py`)

Entry point:

```powershell
.venv\Scripts\python.exe run_pipeline.py --help
```

`cli.py` defines five commands:

## 4.1 `phase1`

```powershell
.venv\Scripts\python.exe run_pipeline.py phase1 --help
```

Arguments:
- `--seed-start INT` start index in `schools_seed.csv` (default `0`)
- `--seed-limit INT` number of seeds to process in `schools_seed.csv`(default `0` = no limit)
- `--max-pages-per-seed INT` pagination cap per seed (default `30`)
- `--timeout INT` request/browser timeout seconds (default `25`)
- `--no-resume` ignore existing phase1 artifacts and rebuild phase1 outputs
- `--require-deepseek` fail fast if DeepSeek is unavailable for name filtering

## 4.2 `phase2`

```powershell
.venv\Scripts\python.exe run_pipeline.py phase2 --help
```

Arguments:
- `--limit INT` max number of phase1 professor rows to process
- `--no-resume` rebuild phase2 output from scratch for selected set
- `--workers INT` concurrent enrichment workers (default `3`)
- `--no-web-search` disable search fallback (profile-page-only extraction)

## 4.3 `phase3`

```powershell
.venv\Scripts\python.exe run_pipeline.py phase3 --help
```

Arguments:
- `--limit INT` limit input rows from phase2
- `--no-resume` rebuild normalized output from scratch (recommended after manual edits)

## 4.4 `export`

```powershell
.venv\Scripts\python.exe run_pipeline.py export
```

- Exports `data/interim/phase3_professors_normalized.jsonl` into
  `data/output/professors_output.csv` using `professors_template.csv` headers.
- Export is merge/additive by row identity key.

## 4.5 `all`

```powershell
.venv\Scripts\python.exe run_pipeline.py all --help
```

Runs phase1 → phase2 → phase3 → export in sequence with shared arguments:
- `--seed-start --seed-limit --max-pages-per-seed --timeout`
- `--limit --no-resume --require-deepseek --workers --no-web-search`

---

## 5) Typical workflows

## 5.1 Full run

```powershell
.venv\Scripts\python.exe run_pipeline.py all --seed-start 0 --seed-limit 23 --max-pages-per-seed 25 --timeout 50 --workers 4
```

## 5.2 Incremental run (resume)

```powershell
.venv\Scripts\python.exe run_pipeline.py phase1
.venv\Scripts\python.exe run_pipeline.py phase2
.venv\Scripts\python.exe run_pipeline.py phase3
.venv\Scripts\python.exe run_pipeline.py export
```

## 5.3 Rebuild only normalization/export after manual edits

```powershell
.venv\Scripts\python.exe run_pipeline.py phase3 --no-resume
.venv\Scripts\python.exe run_pipeline.py export
```

---

## 5.4 Resume/additive behavior (without `--no-resume`)

When you do **not** pass `--no-resume`, each phase loads existing output files, skips already-processed items by key, and merges new rows into existing files.

### Phase1 (`run_pipeline.py phase1`)

Reads existing (if present):
- `data/interim/phase1_listing_pages.jsonl`
- `data/interim/phase1_name_candidates.jsonl`
- `data/interim/phase1_professor_names.jsonl`

Skip conditions:
- **Seed-level skip:** if a seed row index already exists in `phase1_listing_pages.jsonl`, that seed is skipped.
- **HTML-level skip for candidates:** if `html_path` already exists in `phase1_name_candidates.jsonl`, candidate extraction for that page is skipped.
- **Name-level skip for final phase1 names:** within each `(department_name_zh, school_name_zh)`, names already present in `phase1_professor_names.jsonl` are skipped before DeepSeek filtering.

Merge behavior:
- Listing pages merged by `html_path` (fallback key: `seed_row_index|listing_page_url|page_index`).
- Name candidates merged by `department|school|name_candidate|profile_url|listing_page_url`.
- Professor names merged by `department|school|name_zh|name_en`.

### Phase2 (`run_pipeline.py phase2`)

Reads existing (if present):
- `data/interim/phase2_professors_enriched.jsonl`

Skip conditions:
- Rows from `phase1_professor_names.jsonl` are skipped if their key already exists in `phase2_professors_enriched.jsonl`.
- Row key is: `department|school|name_zh|name_en`.

Merge behavior:
- Existing enriched rows are kept.
- New enriched rows overwrite same-key rows (latest run wins for that key).

### Phase3 (`run_pipeline.py phase3`)

Reads existing (if present):
- `data/interim/phase3_professors_normalized.jsonl`
- `data/manual/normalization_review.jsonl`

Skip conditions:
- Rows from `phase2_professors_enriched.jsonl` are skipped if key already exists in `phase3_professors_normalized.jsonl`.
- Row key is: `department|school|name_zh|name_en`.

Merge behavior:
- Normalized rows merged by the same row key (`latest run wins`).
- Review rows merged by `row_key|field|original_value`.
- Manual override values in `normalization_review.jsonl` (for example `manual_abbr`) are loaded and applied when phase3 runs.

### Export (`run_pipeline.py export`)

Reads existing (if present):
- `data/output/professors_output.csv`

Merge behavior:
- Existing CSV rows are loaded, then rows from `phase3_professors_normalized.jsonl` are applied on top by key.
- Export key is `department|school|name_zh|name_en`.
- Result is additive/merge-style, not full replace of unrelated existing rows.

### Important note for manual normalization edits

If you manually edited `data/manual/normalization_review.jsonl` and want those edits applied to **all** existing rows, run:

```powershell
.venv\Scripts\python.exe run_pipeline.py phase3 --no-resume
.venv\Scripts\python.exe run_pipeline.py export
```

Reason: with resume enabled, already-normalized rows are skipped by key.

---

## 6) Manual editing `normalization_review.jsonl`

File:
- `data/manual/normalization_review.jsonl`

Important:
- **JSONL format** = one valid JSON object per line
- Do not wrap whole file in `[]`
- Do not leave trailing commas
- Keep each line valid JSON

Rows are generated for uncertain school-name normalization decisions. To manually override abbreviation, add:
- `manual_abbr` (preferred)

`phase3` also accepts fallback keys:
- `resolved_abbr`
- `canonical_abbr`

Matching rule:
- override is applied by `(field, original_value)`
- `field` is one of `bs_school`, `ms_school`, `phd_school`

Example line:

```json
{"row_key":"...","field":"phd_school","original_value":"法国南巴黎大学","model_abbr":"","confidence":0.3,"reason":"...","created_at":"2026-02-28","manual_abbr":"UPS"}
```

Apply manual edits:

```powershell
.venv\Scripts\python.exe run_pipeline.py phase3 --no-resume
.venv\Scripts\python.exe run_pipeline.py export
```

---

## 7) Notes and troubleshooting

- Prefer `.venv\Scripts\python.exe` over plain `python` to avoid interpreter mismatch.
- If Playwright is missing, JS-heavy school pages may degrade to static crawling quality.
- If phase3 fails with JSON decode errors, validate `data/manual/normalization_review.jsonl` line-by-line.
- `phase3 --no-resume` is the safest way to ensure all manual abbreviation changes are reapplied. It takes some time because it reprocess all the results from phase2 up from scratch.

---

## 8) One-command quick references

```powershell
# Phase1 subset
.venv\Scripts\python.exe run_pipeline.py phase1 --seed-start 1 --seed-limit 1 --max-pages-per-seed 25 --timeout 50 --no-resume

# Phase2 profile-only mode
.venv\Scripts\python.exe run_pipeline.py phase2 --no-web-search --workers 3 --no-resume

# Reapply manual normalization
.venv\Scripts\python.exe run_pipeline.py phase3 --no-resume
.venv\Scripts\python.exe run_pipeline.py export
```

---

## 9) Visualization (brief)

Visualization scripts are in `visualization/`, and figures are saved to `figures/`.

- Graduation top-10 per seed + overall (`BS/MS/PhD`, with coverage text):

```powershell
.venv\Scripts\python.exe visualization/plot_graduation_schools.py
```

- Sankey (`BS -> PhD`) for all professors with both schools collected (with included/total ratio text):

```powershell
.venv\Scripts\python.exe visualization/plot_bs_to_phd_sankey.py
```

- Top-5 seeds by PKU ratio for `BS/MS/PhD` (denominator = collected cases only):

```powershell
.venv\Scripts\python.exe visualization/plot_top5_pku_ratio.py
```

Key outputs include:
- `figures/per_seed/*.png`
- `figures/overall_graduation_top10.png`
- `figures/bs_to_phd_sankey.html`
- `figures/bs_to_phd_sankey.png`
- `figures/top5_pku_ratio_bs_school.png`
- `figures/top5_pku_ratio_ms_school.png`
- `figures/top5_pku_ratio_phd_school.png`
