#!/usr/bin/env python3
"""
plot_timeseries.py — Plot TPS and latency timelines from WOC eval runs.

USAGE
-----
# Plot every eval case in a run directory (one PNG per case + auto comparison):
    python3 plot_timeseries.py results/hetero_plainmsg/20260520_202831

# Plot a single case directory:
    python3 plot_timeseries.py results/hetero_plainmsg/20260520_202831/eval_crash_leader

# Overlay multiple cases for comparison:
    python3 plot_timeseries.py results/hetero_plainmsg/20260520_202831 --compare \
        eval_crash_no_failure eval_crash_leader eval_crash_follower1

# D1 delay sweep bar chart:
    python3 plot_timeseries.py results/hetero_plainmsg/20260520_202831 --d1-sweep

HOW MULTIPLE CLIENT FILES ARE HANDLED
--------------------------------------
Each eval case folder contains one tps_timeline_*.csv per client (e.g. two files
for client5 and client6). This script merges them by:
  - Snapping all rows to a 500ms time grid
  - SUMMING TPS across clients  → total cluster throughput
  - AVERAGING latency across clients
  - Unioning event labels (first non-empty label per time bucket wins)

CLIENT-SIDE TIMESERIES IS CORRECT
-----------------------------------
Recording on the client side is intentional: the crashed server stops producing
data, but what matters for Cabinet-style evaluation is how the *client* observes
the crash — the TPS dip and recovery curve. That's exactly what these files show.
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import sys
from typing import Optional

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np
except ImportError:
    print("ERROR: matplotlib and numpy required.  pip install matplotlib numpy")
    sys.exit(1)


# ── Style ──────────────────────────────────────────────────────────────────────
COLORS      = ["#2196F3", "#FF5722", "#4CAF50", "#9C27B0", "#FF9800", "#00BCD4"]
EVENT_COLOR = "#E53935"
GRID_ALPHA  = 0.25
FIG_DPI     = 150


# ── CSV helpers ────────────────────────────────────────────────────────────────

def load_timeline_csv(path: str):
    """
    Returns (elapsed_s, tps, lat_ms, events{elapsed_s: label}).
    elapsed_ms is converted to seconds.
    """
    elapsed, tps, lat, events = [], [], [], {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                e = float(row["elapsed_ms"]) / 1000.0
                t = float(row["tps"])
                l = float(row["lat_avg_ms"])
                elapsed.append(e)
                tps.append(t)
                lat.append(l)
                ev = row.get("event", "").strip()
                if ev:
                    events[e] = ev
            except (ValueError, KeyError):
                continue
    return elapsed, tps, lat, events


def merge_client_timelines(csv_paths: list[str]):
    """
    Merge all per-client tps_timeline CSVs in a case folder.
    Multiple files exist because each client writes its own independently.
    Returns the same tuple as load_timeline_csv but with combined data.
    """
    if not csv_paths:
        return [], [], [], {}
    if len(csv_paths) == 1:
        return load_timeline_csv(csv_paths[0])

    all_data = [load_timeline_csv(p) for p in csv_paths]

    # Snap every elapsed value to nearest 0.5 s grid
    def snap(v: float) -> float:
        return round(v * 2) / 2

    all_keys: set[float] = set()
    for e_list, *_ in all_data:
        for t in e_list:
            all_keys.add(snap(t))
    grid = sorted(all_keys)

    merged_tps:    dict[float, list[float]] = {g: [] for g in grid}
    merged_lat:    dict[float, list[float]] = {g: [] for g in grid}
    merged_events: dict[float, str]         = {}

    for e_list, t_list, l_list, ev_dict in all_data:
        for e, t, l in zip(e_list, t_list, l_list):
            k = snap(e)
            if k in merged_tps:
                merged_tps[k].append(t)
                merged_lat[k].append(l)
        for e, label in ev_dict.items():
            k = snap(e)
            if k not in merged_events and label:
                merged_events[k] = label

    out_elapsed, out_tps, out_lat = [], [], []
    for g in grid:
        ts = merged_tps[g]
        ls = merged_lat[g]
        if ts:
            out_elapsed.append(g)
            out_tps.append(sum(ts))            # SUM  → total cluster TPS
            out_lat.append(sum(ls) / len(ls))  # AVG  → cluster avg latency

    return out_elapsed, out_tps, out_lat, merged_events


def find_timeline_csvs(case_dir: str) -> list[str]:
    return sorted(glob.glob(os.path.join(case_dir, "tps_timeline_*.csv")))


def find_merged_csv(case_dir: str) -> Optional[str]:
    files = sorted(glob.glob(os.path.join(case_dir, "merged_woc_clients_*.csv")))
    return files[-1] if files else None


def read_merged_throughput(merged_csv: str) -> Optional[float]:
    try:
        with open(merged_csv, newline="", encoding="utf-8") as f:
            for row in csv.reader(f):
                if row and row[0].strip() == "TOTAL_THROUGHPUT":
                    m = re.search(r"[\d.]+", row[1])
                    if m:
                        return float(m.group(0))
    except Exception:
        pass
    return None


# ── Single-case plot ───────────────────────────────────────────────────────────

def plot_case(case_dir: str, out_dir: str) -> Optional[str]:
    """
    Two-panel plot: TPS on top, latency on bottom, with event markers.
    Handles any number of client timeline files in the folder.
    """
    csv_paths = find_timeline_csvs(case_dir)
    if not csv_paths:
        print(f"  SKIP (no tps_timeline_*.csv): {case_dir}")
        return None

    case_name = os.path.basename(case_dir.rstrip("/"))
    print(f"  Plotting: {case_name}  ({len(csv_paths)} client file(s))")

    elapsed, tps, lat, events = merge_client_timelines(csv_paths)
    if not elapsed:
        print(f"  SKIP (empty after merge): {case_name}")
        return None

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True,
                                    gridspec_kw={"hspace": 0.08})
    fig.patch.set_facecolor("#FAFAFA")
    for ax in (ax1, ax2):
        ax.set_facecolor("#FAFAFA")
        ax.grid(True, alpha=GRID_ALPHA, linestyle="--")
        ax.spines[["top", "right"]].set_visible(False)

    n = len(csv_paths)
    tps_label = f"Total TPS ({n} client{'s' if n > 1 else ''})"
    ax1.plot(elapsed, tps, color=COLORS[0], linewidth=1.6, label=tps_label)
    ax1.fill_between(elapsed, tps, alpha=0.12, color=COLORS[0])
    ax1.set_ylabel("Throughput (Tx/s)", fontsize=11)
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:,.0f}"))

    ax2.plot(elapsed, lat, color=COLORS[1], linewidth=1.6, label="Avg latency")
    ax2.fill_between(elapsed, lat, alpha=0.12, color=COLORS[1])
    ax2.set_ylabel("Avg latency (ms)", fontsize=11)
    ax2.set_xlabel("Time (s)", fontsize=11)

    # Event markers
    event_handles: list = []
    seen_events: set[str] = set()
    ymax = max(tps) if tps else 1
    for e_s, label in sorted(events.items()):
        if not (min(elapsed) <= e_s <= max(elapsed)):
            continue
        for ax in (ax1, ax2):
            ax.axvline(e_s, color=EVENT_COLOR, linestyle="--", linewidth=1.4, alpha=0.85)
        ax1.text(e_s + 0.15, ymax * 0.96, label,
                 fontsize=8, color=EVENT_COLOR, rotation=90,
                 va="top", ha="left", alpha=0.9)
        if label not in seen_events:
            seen_events.add(label)
            event_handles.append(
                mpatches.Patch(color=EVENT_COLOR, label=f"event: {label}", alpha=0.75)
            )

    fig.suptitle(_case_title(case_name), fontsize=13, fontweight="bold", y=0.998)
    ax1.legend(loc="upper left", fontsize=9, framealpha=0.6)
    ax2.legend(loc="upper left", fontsize=9, framealpha=0.6)
    if event_handles:
        fig.legend(handles=event_handles, loc="lower center",
                   ncol=min(4, len(event_handles)), fontsize=8,
                   framealpha=0.6, bbox_to_anchor=(0.5, 0.0))
        fig.subplots_adjust(bottom=0.13)

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{case_name}.png")
    fig.savefig(out_path, dpi=FIG_DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"    saved → {out_path}")
    return out_path


# ── Comparison plot ────────────────────────────────────────────────────────────

def plot_comparison(run_dir: str, case_names: list[str], out_dir: str,
                    title: str = "WOC Crash Fault Comparison — Total Cluster TPS") -> Optional[str]:
    """Overlay TPS timelines from multiple cases on a single axes."""
    fig, ax = plt.subplots(figsize=(11, 5))
    fig.patch.set_facecolor("#FAFAFA")
    ax.set_facecolor("#FAFAFA")
    ax.grid(True, alpha=GRID_ALPHA, linestyle="--")
    ax.spines[["top", "right"]].set_visible(False)

    any_data = False
    for idx, name in enumerate(case_names):
        case_dir = os.path.join(run_dir, name)
        csv_paths = find_timeline_csvs(case_dir)
        if not csv_paths:
            print(f"  SKIP (no data): {name}")
            continue
        elapsed, tps, _lat, events = merge_client_timelines(csv_paths)
        if not elapsed:
            continue
        any_data = True
        color = COLORS[idx % len(COLORS)]
        ax.plot(elapsed, tps, color=color, linewidth=1.8, label=_case_title(name))
        for e_s, _label in sorted(events.items()):
            if min(elapsed) <= e_s <= max(elapsed):
                ax.axvline(e_s, color=color, linestyle=":", linewidth=1.1, alpha=0.6)

    if not any_data:
        plt.close(fig)
        return None

    ax.set_xlabel("Time (s)", fontsize=11)
    ax.set_ylabel("Total Throughput (Tx/s)", fontsize=11)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.legend(loc="upper left", fontsize=9, framealpha=0.7)

    os.makedirs(out_dir, exist_ok=True)
    safe = re.sub(r"[^\w]", "_", title)[:50]
    out_path = os.path.join(out_dir, f"{safe}.png")
    fig.savefig(out_path, dpi=FIG_DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"  Comparison → {out_path}")
    return out_path


# ── D1 delay sweep bar chart ───────────────────────────────────────────────────

def plot_d1_sweep(run_dir: str, out_dir: str) -> Optional[str]:
    """Bar chart: x = delay_ms, y = steady-state TPS from merged CSV or timeline mean."""
    cases = sorted(
        glob.glob(os.path.join(run_dir, "eval4_D1_*ms")),
        key=lambda p: _extract_delay_ms(os.path.basename(p))
    )
    if not cases:
        print("  No eval4_D1_*ms directories found.")
        return None

    delays, throughputs = [], []
    for case_dir in cases:
        delay_ms = _extract_delay_ms(os.path.basename(case_dir))
        merged   = find_merged_csv(case_dir)
        tpt      = read_merged_throughput(merged) if merged else None
        if tpt is None:
            paths = find_timeline_csvs(case_dir)
            if paths:
                _, tps, _, _ = merge_client_timelines(paths)
                tpt = float(np.mean(tps)) if tps else None
        if tpt is not None:
            delays.append(delay_ms)
            throughputs.append(tpt)

    if not delays:
        print("  D1 sweep: no data.")
        return None

    fig, ax = plt.subplots(figsize=(9, 5))
    fig.patch.set_facecolor("#FAFAFA")
    ax.set_facecolor("#FAFAFA")
    ax.grid(True, alpha=GRID_ALPHA, linestyle="--", axis="y")
    ax.spines[["top", "right"]].set_visible(False)

    bars = ax.bar([str(d) for d in delays], throughputs,
                  color=COLORS[0], width=0.6, alpha=0.85, edgecolor="white")
    for bar, val in zip(bars, throughputs):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 20,
                f"{val:,.0f}", ha="center", va="bottom", fontsize=8.5, fontweight="bold")

    ax.set_xlabel("Uniform Network Delay (ms)", fontsize=11)
    ax.set_ylabel("Total Throughput (Tx/s)", fontsize=11)
    ax.set_title("D1: Throughput vs. Uniform Network Delay — 5-node WOC", fontsize=12, fontweight="bold")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:,.0f}"))

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "d1_tps_vs_delay.png")
    fig.savefig(out_path, dpi=FIG_DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"  D1 bar chart → {out_path}")
    return out_path


# ── Helpers ────────────────────────────────────────────────────────────────────

def _case_title(name: str) -> str:
    replacements = [
        ("eval_crash_no_failure", "No failure (baseline)"),
        ("eval_crash_leader",     "Leader crash"),
        ("eval_crash_follower",   "Follower crash "),
        ("eval_crash_f_of_n",     "Random f-crash "),
        ("eval4_D4_burst",        "D4 burst (1s spikes)"),
        ("eval4_D1_",             "D1 delay="),
        ("eval1_ratio_",          "Ratio indep="),
        ("eval2_max_inflight_",   "Max-inflight="),
        ("eval3_",                ""),
        ("eval_",                 ""),
    ]
    for old, new in replacements:
        name = name.replace(old, new)
    return name.replace("_", " ").strip()


def _extract_delay_ms(dirname: str) -> int:
    m = re.search(r"(\d+)ms", dirname)
    return int(m.group(1)) if m else 0


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="Plot WOC eval timeseries results")
    ap.add_argument("run_dir",
                    help="Run directory (with eval_* sub-dirs) or a single case dir")
    ap.add_argument("--compare", nargs="+", metavar="CASE",
                    help="Sub-case names to overlay on one comparison plot")
    ap.add_argument("--d1-sweep", action="store_true",
                    help="Generate D1 delay sweep bar chart from eval4_D1_* dirs")
    ap.add_argument("--out", default=None,
                    help="Output directory (default: <run_dir>/plots/)")
    args = ap.parse_args()

    run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(run_dir):
        print(f"ERROR: not a directory: {run_dir}")
        return 1

    # If the directory itself has timeline CSVs, treat it as a single case
    if find_timeline_csvs(run_dir):
        out_dir = args.out or os.path.join(os.path.dirname(run_dir), "plots")
        plot_case(run_dir, out_dir)
        return 0

    out_dir = args.out or os.path.join(run_dir, "plots")

    if args.d1_sweep:
        plot_d1_sweep(run_dir, out_dir)
        return 0

    if args.compare:
        plot_comparison(run_dir, args.compare, out_dir)
        return 0

    # Default: scan all sub-directories
    print(f"Scanning: {run_dir}")
    sub_dirs = sorted([
        d for d in glob.glob(os.path.join(run_dir, "*/"))
        if os.path.isdir(d) and find_timeline_csvs(d)
    ])
    if not sub_dirs:
        print("  No sub-directories with tps_timeline_*.csv found.")
        return 1

    plotted = []
    for case_dir in sub_dirs:
        p = plot_case(case_dir, out_dir)
        if p:
            plotted.append(p)

    # Auto-generate crash comparison
    crash_cases = [
        os.path.basename(d.rstrip("/"))
        for d in sub_dirs
        if "crash" in os.path.basename(d.rstrip("/"))
    ]
    if len(crash_cases) >= 2:
        print("\nAuto-generating crash comparison plot...")
        plot_comparison(run_dir, crash_cases, out_dir)

    print(f"\nDone.  {len(plotted)} plot(s) written to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
