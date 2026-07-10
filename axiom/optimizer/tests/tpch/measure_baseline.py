#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Measure and present v1-vs-v2 TPC-H plan quality and optimizer speed.

Two separate phases, so presentation can be iterated without re-measuring:

  measure  — slow. Runs the Axiom CLI many times and writes raw per-run samples
             to a JSON file. Implements the procedure in
             ``axiom/optimizer/v2/docs/TpchV1V2PlanComparison.md``.
  present  — fast. Reads the JSON and writes the Markdown baseline (medians,
             %-diff, noise bands). Re-run freely to tweak layout.

Metrics (per query x scale x engine):
  - plan quality  = total CPU = sum of per-operator ``Cpu time`` (no total line
                    is printed, so we sum).
  - optimizer speed = the ``Optimizing:`` wall time from ``--print_timing``.

Both engines run at the same worker / driver count (``--num-workers`` /
``--num-drivers``, default 1/1 for the single-node baseline), ``@mode/opt``, v1
with the greedy join-order fallback disabled so its reference is the
branch-and-bound plan. At more than one worker both engines emit distributed
multi-fragment plans, so the comparison reflects distributed plan quality
(shuffles included). Run on a quiet machine.

Usage:
  buck2 build @mode/opt //axiom/cli:cli
  ./measure_baseline.py measure --cli <cli> --data-root /home/$USER/tpch \
      --scales sf1,sf10 --runs 7 --raw baseline_raw.json
  # Multi-node: both engines at 4 workers / 2 drivers.
  ./measure_baseline.py measure --cli <cli> --data-root /home/$USER/tpch \
      --num-workers 4 --num-drivers 2 --raw baseline_4w_raw.json
  ./measure_baseline.py present --raw baseline_raw.json --out baseline.md
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import statistics
import subprocess
import sys

V1_COLOR = "#2EC4B6"  # teal
V2_COLOR = "#FF7A59"  # orange
V2_REGRESSION_COLOR = "#800020"  # burgundy — v2 bars that regress vs v1
SVG_BG = "#F7F7FF"  # near-white background
SVG_INK = "#2D3047"  # dark navy for title / labels / axis

# Queries with no shape matcher because a filter is not estimable (no usable
# selectivity); their numbers are informational, not a pass/fail signal.
NON_ESTIMABLE = {"q9"}

# v1 reference is branch-and-bound, not the greedy fallback that kicks in at >=5
# tables; disable greedy so the comparison is against v1's best plan.
V1_SETUP = "SET SESSION optimizer.greedy_join_threshold = 2147483647;\n"


# --------------------------------------------------------------------------
# measure phase
# --------------------------------------------------------------------------
def _to_ms(value: str, unit: str) -> float:
    return float(value) * {"ns": 1e-6, "us": 1e-3, "ms": 1.0, "s": 1e3}[unit]


def _sum_cpu_ms(output: str) -> float:
    """Total CPU = sum of every operator's ``Cpu time:`` in an EXPLAIN ANALYZE."""
    total = 0.0
    for match in re.finditer(r"Cpu time:\s*([\d.]+)(ns|us|ms|s)\b", output):
        total += _to_ms(match.group(1), match.group(2))
    return total


def _optimize_ms(output: str) -> float | None:
    """Optimize-phase wall time from ``--print_timing``.

    v1 runs a ``SET SESSION`` before the EXPLAIN, so there are two timing lines;
    the EXPLAIN's (the one we want) is the last.
    """
    matches = re.findall(r"Optimizing:\s*([\d.]+)(ns|us|ms|s)\b", output)
    if not matches:
        return None
    value, unit = matches[-1]
    return _to_ms(value, unit)


def build_cli() -> str:
    out = (
        subprocess.run(
            ["buck2", "build", "@mode/opt", "//axiom/cli:cli", "--show-simple-output"],
            capture_output=True,
            text=True,
            check=True,
        )
        .stdout.strip()
        .splitlines()[-1]
    )
    root = subprocess.run(
        ["buck2", "root"], capture_output=True, text=True, check=True
    ).stdout.strip()
    return os.path.join(root, out)


def run_once(
    cli: str,
    query_sql: str,
    data_path: str,
    v2: bool,
    num_workers: int,
    num_drivers: int,
) -> str:
    explain = f"EXPLAIN ANALYZE {query_sql}"
    sql = explain if v2 else V1_SETUP + explain
    args = [
        cli,
        "--data_path",
        data_path,
        "--num_workers",
        str(num_workers),
        "--num_drivers",
        str(num_drivers),
        "--print_timing",
        "--query",
        sql,
    ]
    if v2:
        args.append("--v2")
    return subprocess.run(args, capture_output=True, text=True).stdout


def measure_one(
    cli: str,
    query_sql: str,
    data_path: str,
    v2: bool,
    runs: int,
    num_workers: int,
    num_drivers: int,
) -> tuple[list[float], list[float]]:
    """Returns (cpu_samples, optimize_samples); the cold first run is dropped."""
    cpu: list[float] = []
    optimize: list[float] = []
    for i in range(runs + 1):
        out = run_once(cli, query_sql, data_path, v2, num_workers, num_drivers)
        if i == 0:
            continue
        cpu.append(_sum_cpu_ms(out))
        opt = _optimize_ms(out)
        if opt is not None:
            optimize.append(opt)
    return cpu, optimize


def cmd_measure(args: argparse.Namespace) -> int:
    cli = args.cli or build_cli()
    queries_dir = os.path.dirname(os.path.abspath(__file__)) + "/queries"
    scales = args.scales.split(",")
    queries = args.queries.split(",")

    # --merge re-measures a subset into an existing file: the listed
    # (query, scale) cells are overwritten, every other cell and the original
    # meta (full query/scale list) are preserved. Use it to firm up a noisy cell
    # without re-running the whole suite.
    if args.merge and os.path.exists(args.raw):
        with open(args.raw) as raw_file:
            raw = json.load(raw_file)
        data = raw["data"]
        meta = raw["meta"]
    else:
        data = {}
        meta = {
            "runs": args.runs,
            "scales": scales,
            "queries": queries,
            "config": (
                f"{args.num_workers} worker(s) / {args.num_drivers} driver(s), "
                "@mode/opt, v1 greedy disabled"
            ),
        }

    for scale in scales:
        data_path = os.path.join(args.data_root, scale)
        for query in queries:
            with open(f"{queries_dir}/{query}.sql") as query_file:
                sql = query_file.read().strip()
            v2_cpu, v2_opt = measure_one(
                cli, sql, data_path, True, args.runs, args.num_workers, args.num_drivers
            )
            cell = data.setdefault(query, {}).setdefault(scale, {})
            cell["v2_cpu"] = v2_cpu
            cell["v2_opt"] = v2_opt
            if not args.v2_only:
                v1_cpu, v1_opt = measure_one(
                    cli,
                    sql,
                    data_path,
                    False,
                    args.runs,
                    args.num_workers,
                    args.num_drivers,
                )
                cell["v1_cpu"] = v1_cpu
                cell["v1_opt"] = v1_opt
            print(f"{scale} {query}: done", file=sys.stderr)

    with open(args.raw, "w") as out:
        json.dump({"meta": meta, "data": data}, out, indent=2)
        out.write("\n")
    print(f"wrote {args.raw}")
    return 0


# --------------------------------------------------------------------------
# present phase
# --------------------------------------------------------------------------
class Stat:
    """Median over warm runs, with a trimmed noise band.

    The band drops one lowest and one highest sample to suppress an occasional
    outlier (e.g. machine contention) so the reported spread reflects steady
    state. The median is robust to it; the raw min..max is not, so the band is
    trimmed.
    """

    def __init__(self, samples: list[float]):
        self.samples = samples

    def median(self) -> float | None:
        return statistics.median(self.samples) if self.samples else None

    def band_pct(self) -> float | None:
        median = self.median()
        if median is None or median == 0:
            return None
        trimmed = sorted(self.samples)[1:-1] if len(self.samples) >= 4 else self.samples
        return (max(trimmed) - min(trimmed)) / median * 100


def fmt_median(stat: Stat, decimals: int = 0) -> str:
    median = stat.median()
    if median is None:
        return "ERR"
    # A zero total CPU means the query produced no operator stats, i.e. it
    # failed (e.g. a distributed-only planning error) rather than ran for free.
    if median == 0:
        return "FAIL"
    return f"{median:.{decimals}f}"


def fmt_delta(reference: Stat, candidate: Stat) -> str:
    """v2-vs-v1 percent difference of medians; + means v2 is slower/larger."""
    ref, cand = reference.median(), candidate.median()
    if ref is None or cand is None or ref == 0 or cand == 0:
        return "-"
    return f"{(cand - ref) / ref * 100:+.0f}%"


def fmt_band(*stats: Stat) -> str:
    """Largest trimmed band across the given metrics, as a percent of median."""
    bands = [stat.band_pct() for stat in stats if stat.band_pct() is not None]
    return f"{max(bands):.0f}%" if bands else "-"


# A v2 result counts as a regression when it is slower than v1 by at least this
# many percent AND that gap exceeds the trimmed noise band. A band this wide is
# flagged as unreliable regardless of any delta.
REGRESSION_PCT = 15.0
WIDE_BAND_PCT = 15.0


def worst_band(reference: Stat, candidate: Stat) -> float:
    """Largest trimmed band across v1 and v2, as a percent of median (0 if none)."""
    bands = [b for b in (reference.band_pct(), candidate.band_pct()) if b is not None]
    return max(bands) if bands else 0.0


def is_regression(reference: Stat, candidate: Stat) -> bool:
    """True if candidate (v2) is slower than reference (v1) beyond the band."""
    ref, cand = reference.median(), candidate.median()
    if not ref or not cand:
        return False
    delta = (cand - ref) / ref * 100
    return delta >= REGRESSION_PCT and delta > worst_band(reference, candidate)


def tag(query: str) -> str:
    """Marks a non-estimable query, whose numbers are informational only."""
    return f"{query}*" if query in NON_ESTIMABLE else query


def callout_section(data: dict, queries: list[str], scales: list[str]) -> list[str]:
    """Markdown callouts for the rows a reader must not miss — v2 failures, real
    regressions (v2 slower beyond the band), and unreliable wide bands. Each
    non-empty category is a bolded header with one sub-bullet per entry; returns
    an empty list when nothing fires."""
    failures: list[str] = []
    regressions: list[str] = []
    wide_bands: list[str] = []
    for query in queries:
        for scale in scales:
            v1 = Stat(data[query][scale]["v1_cpu"])
            v2 = Stat(data[query][scale]["v2_cpu"])
            if v2.median() is None:
                continue
            if v2.median() == 0:
                failures.append(f"{tag(query)} {scale}")
                continue
            band = worst_band(v1, v2)
            if band >= WIDE_BAND_PCT:
                wide_bands.append(f"{tag(query)} {scale} ({band:.0f}%)")
            if is_regression(v1, v2):
                delta = (v2.median() - v1.median()) / v1.median() * 100
                regressions.append(f"{tag(query)} {scale} (+{delta:.0f}%)")

    def block(label: str, items: list[str]) -> list[str]:
        return [f"- **{label}:**"] + [f"  - {item}" for item in items]

    body: list[str] = []
    if failures:
        body += block("Failures (v2 produced no plan)", failures)
    if regressions:
        body += block(
            f"Regressions (v2 slower by ≥{REGRESSION_PCT:.0f}% beyond band)",
            regressions,
        )
    if wide_bands:
        body += block(f"Wide bands (≥{WIDE_BAND_PCT:.0f}%, unreliable)", wide_bands)
    return ["", "## Callouts", "", *body] if body else []


def cmd_present(args: argparse.Namespace) -> int:
    with open(args.raw) as raw_file:
        raw = json.load(raw_file)
    meta, data = raw["meta"], raw["data"]
    scales = meta["scales"]
    queries = meta["queries"]

    def stats(query: str, scale: str, key: str) -> Stat:
        return Stat(data[query][scale][key])

    lines = [
        "# TPC-H v1 vs v2 baseline",
        "",
        f"Generated by `measure_baseline.py present` ({meta['config']}, "
        f"{meta['runs']} warm runs). Values are the **median** ms; `Δ%` is v2 vs v1 "
        "(+ = v2 slower). **`band%` is the trimmed noise band as a percent of the "
        "median — always check it: a large band (>~15%) means the measurement is "
        "unreliable (machine contention, or the dynamic-filter stats race), not a "
        "real difference.** `*` = non-estimable filter (informational only).",
        "",
        "## Query execution — total CPU (median ms)",
        "",
    ]

    header = "| query |"
    separator = "|---|"
    for scale in scales:
        header += f" {scale} v1 | {scale} v2 | {scale} Δ% | {scale} band% |"
        separator += "---|---|---|---|"
    lines += [header, separator]
    for query in queries:
        row = f"| {tag(query)} |"
        for scale in scales:
            v1, v2 = stats(query, scale, "v1_cpu"), stats(query, scale, "v2_cpu")
            row += (
                f" {fmt_median(v1)} | {fmt_median(v2)} | {fmt_delta(v1, v2)} "
                f"| {fmt_band(v1, v2)} |"
            )
        lines.append(row)

    lines += callout_section(data, queries, scales)

    # Optimizer time is scale-independent (it plans the same regardless of data
    # size), so report it once, from the first scale.
    opt_scale = scales[0]
    lines += [
        "",
        f"## Optimizer — `Optimizing` time (median ms, scale-independent; {opt_scale})",
        "",
        "| query | v1 | v2 | Δ% |",
        "|---|---|---|---|",
    ]
    for query in queries:
        v1, v2 = stats(query, opt_scale, "v1_opt"), stats(query, opt_scale, "v2_opt")
        lines.append(
            f"| {tag(query)} | {fmt_median(v1, 2)} | {fmt_median(v2, 2)} "
            f"| {fmt_delta(v1, v2)} |"
        )

    with open(args.out, "w") as out:
        out.write("\n".join(lines) + "\n")
    print(f"wrote {args.out}")
    return 0


# --------------------------------------------------------------------------
# chart phase (SVG, stdlib only)
# --------------------------------------------------------------------------
def _nice_max(value: float) -> float:
    """Round up to a 1/2/2.5/5 x 10^k axis maximum."""
    if value <= 0:
        return 1.0
    exponent = math.floor(math.log10(value))
    base = value / 10**exponent
    for step in (1, 2, 2.5, 5, 10):
        if base <= step:
            return step * 10**exponent
    return 10 ** (exponent + 1)


def svg_grouped_bars(
    title: str,
    queries: list[str],
    v1: list[float],
    v2: list[float],
    v2_regression: list[bool],
) -> str:
    """A vertical grouped bar chart (two bars per query) as standalone SVG.

    A v2 bar is drawn in burgundy when `v2_regression[i]` is set, so regressions
    stand out from the orange wins/ties.
    """
    # Fixed-pixel layout so bar width and gaps are constant regardless of how
    # many queries are charted; the canvas widens to fit.
    bar_w, pair_gap, group_gap = 12, 2, 14
    left, right, top, bottom = 60, 20, 40, 50
    height = 380
    plot_h = height - top - bottom
    group_w = bar_w * 2 + pair_gap
    slot = group_w + group_gap
    plot_w = slot * len(queries)
    width = left + plot_w + right
    ymax = _nice_max(max([*v1, *v2, 1]))

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'font-family="sans-serif" font-size="11">',
        f'<rect width="{width}" height="{height}" fill="{SVG_BG}"/>',
        f'<text x="{width / 2}" y="20" text-anchor="middle" font-size="14" '
        f'font-weight="bold" fill="{SVG_INK}">{title}</text>',
    ]
    for i in range(6):  # horizontal gridlines + y labels
        y = top + plot_h - plot_h * i / 5
        parts.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}" stroke="#D9DCE6"/>'
        )
        parts.append(
            f'<text x="{left - 6}" y="{y + 3:.1f}" text-anchor="end" fill="{SVG_INK}">'
            f"{ymax * i / 5:.0f}</text>"
        )
    for i, query in enumerate(queries):
        group_x = left + group_gap / 2 + slot * i
        center = group_x + group_w / 2
        v2_color = V2_REGRESSION_COLOR if v2_regression[i] else V2_COLOR
        for value, color, offset in (
            (v1[i], V1_COLOR, 0),
            (v2[i], v2_color, bar_w + pair_gap),
        ):
            bar_h = plot_h * value / ymax
            parts.append(
                f'<rect x="{group_x + offset:.1f}" y="{top + plot_h - bar_h:.1f}" '
                f'width="{bar_w}" height="{bar_h:.1f}" fill="{color}"/>'
            )
        parts.append(
            f'<text x="{center:.1f}" y="{top + plot_h + 14:.1f}" text-anchor="middle" '
            f'fill="{SVG_INK}">{query}</text>'
        )
    parts.append(
        f'<line x1="{left}" y1="{top + plot_h}" x2="{width - right}" '
        f'y2="{top + plot_h}" stroke="{SVG_INK}"/>'
    )
    legend_x = left + 8
    parts.append(
        f'<rect x="{legend_x}" y="{top + 2}" width="11" height="11" fill="{V1_COLOR}"/>'
        f'<text x="{legend_x + 15}" y="{top + 12}" fill="{SVG_INK}">v1</text>'
        f'<rect x="{legend_x + 45}" y="{top + 2}" width="11" height="11" fill="{V2_COLOR}"/>'
        f'<text x="{legend_x + 60}" y="{top + 12}" fill="{SVG_INK}">v2</text>'
        f'<rect x="{legend_x + 90}" y="{top + 2}" width="11" height="11" '
        f'fill="{V2_REGRESSION_COLOR}"/>'
        f'<text x="{legend_x + 105}" y="{top + 12}" fill="{SVG_INK}">v2 regression</text>'
    )
    parts.append("</svg>")
    return "\n".join(parts)


def cmd_chart(args: argparse.Namespace) -> int:
    with open(args.raw) as raw_file:
        raw = json.load(raw_file)
    data = raw["data"]
    queries = raw["meta"]["queries"]
    for scale in raw["meta"]["scales"]:

        def median(query: str, key: str, scale: str = scale) -> float:
            samples = data[query][scale][key]
            return statistics.median(samples) if samples else 0.0

        def regressed(query: str, scale: str = scale) -> bool:
            return is_regression(
                Stat(data[query][scale]["v1_cpu"]),
                Stat(data[query][scale]["v2_cpu"]),
            )

        svg = svg_grouped_bars(
            f"TPC-H total CPU, {scale} (median ms): v1 vs v2",
            queries,
            [median(q, "v1_cpu") for q in queries],
            [median(q, "v2_cpu") for q in queries],
            [regressed(q) for q in queries],
        )
        path = f"{args.out_prefix}_{scale}.svg"
        with open(path, "w") as out:
            out.write(svg + "\n")
        print(f"wrote {path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    measure = sub.add_parser("measure", help="Run the CLI and write raw samples.")
    measure.add_argument("--cli", help="Path to @mode/opt cli; built if omitted.")
    measure.add_argument("--data-root", required=True, help="Dir holding sf1/, sf10/.")
    measure.add_argument("--scales", default="sf1,sf10")
    measure.add_argument(
        "--queries", default=",".join(f"q{n}" for n in range(1, 23)) + ",q9_alt"
    )
    measure.add_argument(
        "--runs", type=int, default=7, help="Warm runs after discarding the first."
    )
    measure.add_argument(
        "--num-workers",
        type=int,
        default=1,
        help="Workers for both engines (>1 emits distributed plans).",
    )
    measure.add_argument(
        "--num-drivers",
        type=int,
        default=1,
        help="Drivers per worker for both engines.",
    )
    measure.add_argument("--raw", default="baseline_raw.json")
    measure.add_argument(
        "--merge",
        action="store_true",
        help="Update only the measured cells in an existing --raw file.",
    )
    measure.add_argument(
        "--v2-only",
        action="store_true",
        help="Re-measure only v2, preserving existing v1 samples (use with --merge "
        "when v1 has not changed).",
    )
    measure.set_defaults(func=cmd_measure)

    present = sub.add_parser("present", help="Format raw samples into Markdown.")
    present.add_argument("--raw", default="baseline_raw.json")
    present.add_argument("--out", default="baseline.md")
    present.set_defaults(func=cmd_present)

    chart = sub.add_parser("chart", help="Render grouped-bar SVGs from raw samples.")
    chart.add_argument("--raw", default="baseline_raw.json")
    chart.add_argument("--out-prefix", default="baseline_chart")
    chart.set_defaults(func=cmd_chart)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
