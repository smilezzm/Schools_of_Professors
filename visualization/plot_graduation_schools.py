from __future__ import annotations

import csv
import re
from collections import Counter
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import matplotlib.pyplot as plt


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SEEDS_CSV = PROJECT_ROOT / "schools_seed.csv"
PROFESSORS_CSV = PROJECT_ROOT / "data" / "output" / "professors_output.csv"
FIGURES_DIR = PROJECT_ROOT / "figures"
PER_SEED_DIR = FIGURES_DIR / "per_seed"

DEGREE_FIELDS: Sequence[Tuple[str, str]] = (
    ("bs_school", "BS"),
    ("ms_school", "MS"),
    ("phd_school", "PhD"),
)


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


def _safe_filename(text: str) -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|]", "_", text)
    cleaned = re.sub(r"\s+", "_", cleaned).strip("_")
    return cleaned or "seed"


def _coverage_ratio(rows: List[Dict[str, str]], field: str) -> Tuple[int, int, float]:
    total = len(rows)
    filled = sum(1 for row in rows if str(row.get(field, "")).strip())
    ratio = (filled / total) if total else 0.0
    return filled, total, ratio


def _top_counter(rows: List[Dict[str, str]], field: str, top_n: int = 10) -> List[Tuple[str, int]]:
    counter: Counter[str] = Counter()
    for row in rows:
        value = str(row.get(field, "")).strip()
        if value:
            counter[value] += 1
    return counter.most_common(top_n)


def _setup_matplotlib() -> None:
    plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial Unicode MS", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False


def _draw_one_axis(ax: plt.Axes, rows: List[Dict[str, str]], field: str, degree_name: str) -> None:
    top = _top_counter(rows, field, top_n=10)
    filled, total, ratio = _coverage_ratio(rows, field)

    if top:
        labels = [name for name, _ in top]
        values = [count for _, count in top]
        ax.barh(labels, values, color="#4C78A8")
        ax.invert_yaxis()
        ax.set_xlabel("Count")
    else:
        ax.barh(["(No data)"], [0], color="#BBBBBB")
        ax.set_xlabel("Count")

    ax.set_title(f"{degree_name}: Top 10 Graduation Schools")
    ax.text(
        0.98,
        0.95,
        f"coverage: {filled}/{total} ({ratio:.1%})",
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=10,
        bbox={"boxstyle": "round,pad=0.25", "facecolor": "white", "alpha": 0.85, "edgecolor": "#CCCCCC"},
    )


def _plot_seed_figure(seed_name: str, rows: List[Dict[str, str]], output_path: Path) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(20, 6))
    for ax, (field, degree_name) in zip(axes, DEGREE_FIELDS):
        _draw_one_axis(ax, rows, field, degree_name)
    fig.suptitle(f"{seed_name} - Graduation Schools Distribution", fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def main() -> None:
    _setup_matplotlib()
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    PER_SEED_DIR.mkdir(parents=True, exist_ok=True)

    seeds = _read_csv_rows(SEEDS_CSV)
    professors = _read_csv_rows(PROFESSORS_CSV)

    for idx, seed in enumerate(seeds, start=1):
        department = str(seed.get("department_name_zh", "")).strip()
        school = str(seed.get("school_name_zh", "")).strip()
        if not school:
            continue

        seed_rows = [
            row
            for row in professors
            if str(row.get("department_name_zh", "")).strip() == department
            and str(row.get("school_name_zh", "")).strip() == school
        ]

        seed_title = f"{department}-{school}" if department else school
        file_name = f"{idx:02d}_{_safe_filename(seed_title)}.png"
        _plot_seed_figure(seed_title, seed_rows, PER_SEED_DIR / file_name)

    _plot_seed_figure("All Seeds (Overall)", professors, FIGURES_DIR / "overall_graduation_top10.png")
    print(f"Figures generated in: {FIGURES_DIR}")


if __name__ == "__main__":
    main()
