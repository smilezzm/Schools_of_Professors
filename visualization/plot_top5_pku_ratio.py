from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import matplotlib.pyplot as plt


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SEEDS_CSV = PROJECT_ROOT / "schools_seed.csv"
PROFESSORS_CSV = PROJECT_ROOT / "data" / "output" / "professors_output.csv"
FIGURES_DIR = PROJECT_ROOT / "figures"

DEGREE_FIELDS: Sequence[Tuple[str, str]] = (
    ("bs_school", "BS"),
    ("ms_school", "MS"),
    ("phd_school", "PhD"),
)


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [dict(row) for row in csv.DictReader(f)]


def _setup_matplotlib() -> None:
    plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial Unicode MS", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False


def _is_pku(value: str) -> bool:
    text = str(value or "").strip().upper()
    return text == "PKU"


def _seed_title(seed: Dict[str, str]) -> str:
    department = str(seed.get("department_name_zh", "")).strip()
    school = str(seed.get("school_name_zh", "")).strip()
    return f"{department}-{school}" if department else school


def _top5_for_degree(
    seeds: List[Dict[str, str]],
    professors: List[Dict[str, str]],
    field: str,
) -> List[Tuple[str, float, int, int]]:
    results: List[Tuple[str, float, int, int]] = []

    for seed in seeds:
        department = str(seed.get("department_name_zh", "")).strip()
        school = str(seed.get("school_name_zh", "")).strip()
        rows = [
            row
            for row in professors
            if str(row.get("department_name_zh", "")).strip() == department
            and str(row.get("school_name_zh", "")).strip() == school
        ]

        collected = [row for row in rows if str(row.get(field, "")).strip()]
        denominator = len(collected)
        if denominator == 0:
            continue
        numerator = sum(1 for row in collected if _is_pku(str(row.get(field, ""))))
        ratio = numerator / denominator
        results.append((_seed_title(seed), ratio, numerator, denominator))

    results.sort(key=lambda x: (x[1], x[2], x[3]), reverse=True)
    return results[:5]


def _plot_degree_top5(
    degree_name: str,
    top5: List[Tuple[str, float, int, int]],
    output_path: Path,
) -> None:
    if not top5:
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.text(0.5, 0.5, f"No collected {degree_name} data", ha="center", va="center")
        ax.axis("off")
        fig.savefig(output_path, dpi=180)
        plt.close(fig)
        return

    labels = [item[0] for item in top5]
    ratios = [item[1] for item in top5]
    numerators = [item[2] for item in top5]
    denominators = [item[3] for item in top5]

    fig, ax = plt.subplots(figsize=(14, 7))
    bars = ax.bar(labels, ratios, color="#4C78A8")
    ax.set_ylim(0, 1)
    ax.set_ylabel("PKU Ratio")
    ax.set_title(f"Top-5 Seeds by PKU Ratio ({degree_name})")
    ax.tick_params(axis="x", rotation=25)

    for bar, ratio, num, den in zip(bars, ratios, numerators, denominators):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            min(0.98, ratio + 0.03),
            f"{ratio:.1%}\n({num}/{den})",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def main() -> None:
    _setup_matplotlib()
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    seeds = _read_csv_rows(SEEDS_CSV)
    professors = _read_csv_rows(PROFESSORS_CSV)

    for field, degree_name in DEGREE_FIELDS:
        top5 = _top5_for_degree(seeds, professors, field)
        output_path = FIGURES_DIR / f"top5_pku_ratio_{field}.png"
        _plot_degree_top5(degree_name, top5, output_path)
        print(f"Saved: {output_path}")


if __name__ == "__main__":
    main()
