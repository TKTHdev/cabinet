#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import sys
from datetime import datetime
from typing import Optional


def _cell(row: list[str], idx: int) -> str:
    return row[idx].strip() if idx < len(row) else ""


def _parse_float(text: str) -> Optional[float]:
    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    return float(match.group(0))


def _parse_int(text: str) -> Optional[int]:
    match = re.search(r"-?\d+", text.replace(",", ""))
    if not match:
        return None
    return int(match.group(0))


def _extract_run_timestamp(path: str) -> Optional[str]:
    # Expected client filename pattern:
    # client<ID>_eval_YYYYMMDD_HHMMSS.csv
    match = re.search(r"_(\d{8}_\d{6})\.csv$", os.path.basename(path))
    if not match:
        return None
    return match.group(1)


def _pick_latest_csv(csvs: list[str]) -> str:
    # Prefer filename timestamp to avoid copy-order mtime skew.
    with_ts = [(path, _extract_run_timestamp(path)) for path in csvs]
    with_ts = [(path, ts) for path, ts in with_ts if ts is not None]
    if with_ts:
        return max(with_ts, key=lambda item: item[1])[0]
    return max(csvs, key=os.path.getmtime)


def _default_output(eval_dir: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(eval_dir, "merged", f"merged_cabinet_clients_{timestamp}.csv")


def _resolve_output(eval_dir: str, output: Optional[str]) -> str:
    if not output:
        return _default_output(eval_dir)

    if os.path.isdir(output) or output.endswith(os.sep) or output.endswith("/"):
        return os.path.join(output, os.path.basename(_default_output(eval_dir)))

    return output


def _parse_id_filter(ids_arg: Optional[str]) -> Optional[set[int]]:
    if not ids_arg:
        return None

    ids: set[int] = set()
    for token in ids_arg.split(","):
        token = token.strip()
        if not token:
            continue

        if "-" in token:
            start_str, end_str = token.split("-", 1)
            start = int(start_str)
            end = int(end_str)
            if start > end:
                start, end = end, start
            ids.update(range(start, end + 1))
        else:
            ids.add(int(token))

    if not ids:
        raise ValueError("--ids produced an empty set")
    return ids


def _extract_dir_id(path: str) -> Optional[int]:
    name = os.path.basename(os.path.normpath(path))
    match = re.fullmatch(r"client(\d+)", name)
    if not match:
        return None
    return int(match.group(1))


def _filter_client_dirs(client_dirs: list[str], allowed_ids: Optional[set[int]]) -> list[str]:
    if allowed_ids is None:
        return client_dirs

    filtered: list[str] = []
    for d in client_dirs:
        client_id = _extract_dir_id(d)
        if client_id is not None and client_id in allowed_ids:
            filtered.append(d)
    return filtered


def merge_client_csvs(
    eval_dir: str = "./eval",
    output: Optional[str] = None,
    allowed_client_ids: Optional[set[int]] = None,
) -> str:
    """Merge latest per-client Cabinet CSV files into one summary CSV.

    Cabinet client CSV format (from eval/meters.go):
    pclock,latency (ms) per batch,throughput (Tx/sec),slow path ops,conflict ops

    Summary rows consumed:
    - THROUGHPUT: column 2
    - GLOBAL_TOTALS: column 3 (slow ops), column 4 (conflict ops)
    - TOTAL_SLOW_COMMITS: column 1
    - TOTAL_CONFLICT_COMMITS: column 1
    """
    all_latencies: list[float] = []
    all_throughputs: list[float] = []
    total_slow = 0
    total_conflict = 0

    client_dirs_all = [
        d
        for d in glob.glob(os.path.join(eval_dir, "client*/"))
        if os.path.isdir(d)
    ]
    client_dirs = _filter_client_dirs(client_dirs_all, allowed_client_ids)

    if not client_dirs:
        if allowed_client_ids is None:
            raise RuntimeError(f"No client directories found in {eval_dir}")
        raise RuntimeError(
            f"No matching client directories found in {eval_dir} for ids: {sorted(allowed_client_ids)}"
        )

    if allowed_client_ids is None:
        print(f"Found {len(client_dirs)} client directories")
    else:
        print(f"Found {len(client_dirs)} client directories matching ids {sorted(allowed_client_ids)}")

    merged_clients = 0
    for client_dir in sorted(client_dirs):
        csvs = glob.glob(os.path.join(client_dir, "*.csv"))
        if not csvs:
            print(f"  WARNING: No CSV in {client_dir}")
            continue

        latest = _pick_latest_csv(csvs)
        merged_clients += 1
        print(f"  Reading: {latest}")

        # Prefer GLOBAL_TOTALS if present; fallback to TOTAL_* rows otherwise.
        seen_global_totals = False
        file_slow_from_totals = 0
        file_conflict_from_totals = 0

        with open(latest, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue

                label = _cell(row, 0)
                if label in ("pclock", "NO_DATA", ""):
                    continue

                # Per-batch rows have numeric pclock in column 0.
                try:
                    int(label)
                    lat = _parse_float(_cell(row, 1))
                    if lat is not None and lat > 0:
                        all_latencies.append(lat)
                    continue
                except ValueError:
                    pass

                if label == "THROUGHPUT":
                    tpt = _parse_float(_cell(row, 2))
                    if tpt is not None:
                        all_throughputs.append(tpt)
                elif label == "GLOBAL_TOTALS":
                    slow_val = _parse_int(_cell(row, 3))
                    conflict_val = _parse_int(_cell(row, 4))
                    if slow_val is not None:
                        total_slow += slow_val
                    if conflict_val is not None:
                        total_conflict += conflict_val
                    seen_global_totals = True
                elif label == "TOTAL_SLOW_COMMITS":
                    val = _parse_int(_cell(row, 1))
                    if val is not None:
                        file_slow_from_totals = val
                elif label == "TOTAL_CONFLICT_COMMITS":
                    val = _parse_int(_cell(row, 1))
                    if val is not None:
                        file_conflict_from_totals = val

        if not seen_global_totals:
            total_slow += file_slow_from_totals
            total_conflict += file_conflict_from_totals

    if not all_latencies:
        raise RuntimeError("No latency data found in client CSV files")

    output_path = _resolve_output(eval_dir, output)
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    all_latencies.sort()
    n = len(all_latencies)
    p50 = all_latencies[n * 50 // 100]
    p95 = all_latencies[n * 95 // 100]
    p99 = all_latencies[n * 99 // 100]
    avg_lat = sum(all_latencies) / n

    # Note: Summing per-client throughputs is only valid if all clients ran for exactly the same duration.
    # If clients finish at different times, the sum will be inflated. For accurate total throughput,
    # use: total_ops / (last_op_timestamp - first_op_timestamp).
    total_tpt = sum(all_throughputs)

    total_ops = total_slow + total_conflict
    slow_ratio = (total_slow / total_ops) if total_ops > 0 else 0.0

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value", "notes"])
        writer.writerow(["NUM_CLIENT_DIRS", len(client_dirs), "client* folders under eval"])
        writer.writerow(["NUM_CLIENTS_MERGED", merged_clients, "clients with CSV data"])
        writer.writerow(["TOTAL_THROUGHPUT", f"{total_tpt:.1f} Tx/sec", "sum of all clients"])
        writer.writerow(["AVG_LATENCY", f"{avg_lat:.3f} ms", "across all batches all clients"])
        writer.writerow(["P50_LATENCY", f"{p50:.1f} ms", ""])
        writer.writerow(["P95_LATENCY", f"{p95:.1f} ms", ""])
        writer.writerow(["P99_LATENCY", f"{p99:.1f} ms", ""])
        writer.writerow(["TOTAL_SLOW_COMMITS", total_slow, f"{slow_ratio * 100:.1f}%"])
        writer.writerow(["TOTAL_CONFLICT_COMMITS", total_conflict, ""])

    print(f"\nMerged client CSV written to: {output_path}")
    print(f"  Clients merged: {merged_clients}/{len(client_dirs)}")
    print(f"  Total throughput: {total_tpt:.1f} Tx/sec")
    print(f"  Avg latency: {avg_lat:.3f} ms")
    print(f"  P99 latency: {p99:.1f} ms")
    print(f"  Slow/Conflict ratio: {slow_ratio * 100:.1f}% / {(100.0 - slow_ratio * 100.0):.1f}%")

    return output_path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Merge latest per-client Cabinet evaluation CSV files into a summary CSV"
    )
    parser.add_argument(
        "eval_dir",
        nargs="?",
        default="./eval",
        help="Path to eval directory containing client*/ folders (default: ./eval)",
    )
    parser.add_argument(
        "output",
        nargs="?",
        default=None,
        help=(
            "Output CSV file path or output directory. "
            "Default: ./eval/merged/merged_cabinet_clients_<timestamp>.csv"
        ),
    )
    parser.add_argument(
        "--ids",
        default=None,
        help=(
            "Optional comma/range filter for client IDs to merge. "
            "Examples: '5,6' or '5-10'. Targets client<ID>."
        ),
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    try:
        allowed_ids = _parse_id_filter(args.ids)
        merge_client_csvs(
            eval_dir=args.eval_dir,
            output=args.output,
            allowed_client_ids=allowed_ids,
        )
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
