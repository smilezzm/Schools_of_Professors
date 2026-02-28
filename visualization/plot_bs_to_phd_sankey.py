from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple

import plotly.graph_objects as go


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROFESSORS_CSV = PROJECT_ROOT / "data" / "output" / "professors_output.csv"
FIGURES_DIR = PROJECT_ROOT / "figures"


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


def _build_sankey_data(rows: List[Dict[str, str]]) -> Tuple[List[str], List[int], List[int], List[int], int, int]:
    total_cases = len(rows)
    valid_pairs: List[Tuple[str, str]] = []
    for row in rows:
        bs = str(row.get("bs_school", "")).strip()
        phd = str(row.get("phd_school", "")).strip()
        if bs and phd:
            valid_pairs.append((bs, phd))

    included_cases = len(valid_pairs)
    pair_counter = Counter(valid_pairs)

    bs_nodes = sorted({bs for bs, _ in pair_counter.keys()})
    phd_nodes = sorted({phd for _, phd in pair_counter.keys()})
    labels = [f"BS: {name}" for name in bs_nodes] + [f"PhD: {name}" for name in phd_nodes]

    bs_index = {name: idx for idx, name in enumerate(bs_nodes)}
    phd_offset = len(bs_nodes)
    phd_index = {name: phd_offset + idx for idx, name in enumerate(phd_nodes)}

    source: List[int] = []
    target: List[int] = []
    value: List[int] = []
    for (bs, phd), count in pair_counter.items():
        source.append(bs_index[bs])
        target.append(phd_index[phd])
        value.append(count)

    return labels, source, target, value, included_cases, total_cases


def _node_totals(node_count: int, source: List[int], target: List[int], value: List[int]) -> List[int]:
    totals = [0] * node_count
    for s, t, v in zip(source, target, value):
        totals[s] += v
        totals[t] += v
    return totals


def main() -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    rows = _read_csv_rows(PROFESSORS_CSV)

    labels, source, target, value, included_cases, total_cases = _build_sankey_data(rows)
    ratio = (included_cases / total_cases) if total_cases else 0.0

    if not labels or not value:
        raise RuntimeError("No valid BS->PhD pairs found in professors_output.csv")

    totals = _node_totals(len(labels), source, target, value)
    display_labels = [label if totals[idx] > 1 else "" for idx, label in enumerate(labels)]

    fig = go.Figure(
        data=[
            go.Sankey(
                arrangement="snap",
                node=dict(
                    pad=14,
                    thickness=14,
                    line=dict(color="rgba(70,70,70,0.35)", width=0.6),
                    label=display_labels,
                    color=["#4C78A8"] * (len(labels) // 2) + ["#F58518"] * (len(labels) - len(labels) // 2),
                ),
                link=dict(
                    source=source,
                    target=target,
                    value=value,
                    color="rgba(130,130,130,0.32)",
                ),
            )
        ]
    )

    fig.update_layout(
        title="BS → PhD Graduation School Sankey (All Professors)",
        font=dict(size=9),
        width=1900,
        height=1200,
        margin=dict(l=15, r=15, t=70, b=20),
        annotations=[
            dict(
                x=0.995,
                y=1.08,
                xref="paper",
                yref="paper",
                xanchor="right",
                yanchor="top",
                text=f"included cases: {included_cases}/{total_cases} ({ratio:.1%})",
                showarrow=False,
                font=dict(size=12),
            )
        ],
    )

    html_path = FIGURES_DIR / "bs_to_phd_sankey.html"
    fig.write_html(str(html_path), include_plotlyjs="cdn")

    png_path = FIGURES_DIR / "bs_to_phd_sankey.png"
    try:
        fig.write_image(str(png_path), scale=2)
        png_status = "saved"
    except Exception:
        png_status = "skipped (install kaleido for static export)"

    print(f"Sankey saved: {html_path}")
    print(f"PNG status: {png_status}")


if __name__ == "__main__":
    main()
