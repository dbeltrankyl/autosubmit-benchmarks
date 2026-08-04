#!/usr/bin/env python3
"""Compare two pytest-benchmark runs and produce a performance report.

Loads pytest-benchmark JSON runs (as saved by ``--benchmark-save``) and
produces:

- ``summary_{version}.md``: a comparison table between the current run and a
  baseline, flagging regressions that exceed the configured thresholds.
- ``summary_{version}.png``: a delta heatmap of every scenario x metric,
  current vs baseline (regressions in red).

Baselines are aggregated with the median across all provided run files, so the
``--previous`` argument can point to a directory holding several historical
runs (e.g. the last N master runs committed to the ``benchmark-reference``
branch). Baselines may be stored per CPU: ``.benchmarks/reference/<cpu-slug>/``,
in which case the baseline matching the current run's CPU is selected
automatically (a run on a CPU without a baseline yet is shown without
comparison and seeds it).

Usage::

    python .benchmarks/compare_results.py \\
        --current .benchmarks/data/current.json \\
        --previous .benchmarks/reference \\
        --thresholds .benchmarks/thresholds.yml \\
        --version 4.2.0 \\
        --output-dir .benchmarks/artifacts

When no ``--previous`` is provided the report is rendered current-only (used
for the first run after the baseline branch is created).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import pandas as pd

# Deterministic metrics that must not change between runs.
EXACT_METRICS = ["Total Jobs", "Total Dependencies"]

# Metric names as stored in ``benchmark.extra_info``, plus the wall-clock time
# that pytest-benchmark records in ``stats.median``.
METRIC_COLUMNS = [
    "Time Taken(Seconds)",
    "Memory consumption(MiB)",
    "Historical DB Disk Usage(MiB)",
    "Job list DB Usage",
    "FD GROW",
    "MEM GROW(MIB)",
    "OBJ GROW",
] + EXACT_METRICS

# Profiler growth metrics are only meaningful for the `run` scenarios;
# elsewhere they are absent or dominated by noise, so they are excluded.
_GROW_METRICS = {"FD GROW", "MEM GROW(MIB)", "OBJ GROW"}
_NO_GROW_TEST_TYPES = {"create", "recovery", "setstatus"}

# Metrics shown in the heatmap (curated; the rest stay in the markdown table).
_PLOT_METRICS = ["Time Taken(Seconds)", "Memory consumption(MiB)", "MEM GROW(MIB)", "OBJ GROW"]
_SHORT_METRICS = {
    "Time Taken(Seconds)": "Time (s)",
    "Memory consumption(MiB)": "Memory (MiB)",
    "MEM GROW(MIB)": "MEM grow (MiB)",
    "OBJ GROW": "Obj grow",
}


def _allowed_metrics(test_type: str) -> set[str]:
    """Return the metric names to report for the given test type."""
    if test_type in _NO_GROW_TEST_TYPES:
        return set(METRIC_COLUMNS) - _GROW_METRICS
    return set(METRIC_COLUMNS)


_TABLE_COLUMNS = ["test type", "ID", "metric", "baseline", "current", "delta %", "verdict"]


def _load_thresholds(path: Path) -> dict:
    """Load the thresholds YAML file using the project's YAML parser."""
    from ruamel.yaml import YAML

    yaml = YAML(typ="safe")
    with open(path, encoding="UTF-8") as file:
        data = yaml.load(file) or {}
    metrics = data.get("metrics", {})
    exact = data.get("exact_metrics", [])
    return {"metrics": metrics, "exact_metrics": exact}


def _iter_run_files(path: str | None, latest_only: bool = False) -> list[Path]:
    """Return the pytest-benchmark JSON files under ``path`` (file or dir).

    When ``latest_only`` is set and ``path`` is a directory, only the most
    recently modified run file is returned. This is used for the ``--current``
    argument so locally accumulated runs are not averaged together.
    """
    if not path:
        return []
    p = Path(path)
    if p.is_dir():
        files = sorted(p.rglob("*.json"), key=lambda f: f.stat().st_mtime)
        if latest_only and files:
            return [files[-1]]
        return files
    if p.is_file():
        return [p]
    return []


def _load_runs(files: list[Path]) -> list[dict]:
    """Load the raw JSON content of the given pytest-benchmark run files."""
    runs = []
    for file in files:
        try:
            with open(file, encoding="UTF-8") as fh:
                runs.append(json.load(fh))
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[WARNING] Skipping unreadable benchmark file {file}: {exc}")
    return runs


def _current_cpu(runs: list[dict]) -> str:
    """Return the CPU ``brand_raw`` of the first run, or ``''`` when unknown."""
    if not runs:
        return ""
    return (runs[0].get("machine_info", {}).get("cpu") or {}).get("brand_raw") or ""


def _cpu_slug(brand_raw: str) -> str:
    """Turn a CPU brand string into a directory-safe slug."""
    slug = re.sub(r"[^a-z0-9]+", "-", brand_raw.lower()).strip("-")
    return slug or "unknown-cpu"


def _is_machine_dir(name: str) -> bool:
    """pytest-benchmark stores runs under machine dirs like ``Linux-CPython-3.11-64bit``."""
    return bool(re.match(r"^(linux|darwin|windows)-", name, re.IGNORECASE))


def _select_previous(previous: str | None, current_cpu: str) -> tuple[list[Path], str | None]:
    """Return the baseline run files for the current CPU plus its slug.

    ``--previous`` may be a single file, a flat directory of JSON runs (legacy),
    or a directory of per-CPU subdirectories (``.benchmarks/reference/<slug>/``).
    When per-CPU subdirectories exist, the one matching ``current_cpu`` is used;
    if none matches, no baseline is available for this CPU.
    """
    if not previous:
        return [], None
    p = Path(previous)
    if p.is_file():
        return [p], None
    if p.is_dir():
        subdirs = [d for d in p.iterdir() if d.is_dir()]
        if any(not _is_machine_dir(d.name) for d in subdirs):
            slug = _cpu_slug(current_cpu) if current_cpu else ""
            target = p / slug if slug else None
            if target is not None and target.is_dir():
                return sorted(target.rglob("*.json")), slug
            return [], slug
        return sorted(p.rglob("*.json")), None
    return [], None


def build_frame(runs: list[dict]) -> pd.DataFrame:
    """Build a DataFrame indexed by (test type, ID) with median metric values.

    If several run files are provided the median across runs is used for each
    (test type, ID) pair, which gives a robust baseline.
    """
    records = []
    for run in runs:
        for entry in run.get("benchmarks", []):
            extra = entry.get("extra_info", {})
            test_type = extra.get("test type")
            run_id = extra.get("ID")
            if not test_type or not run_id:
                continue
            row = {
                "test type": test_type,
                "ID": run_id,
                "Time Taken(Seconds)": entry.get("stats", {}).get("median"),
            }
            for metric in METRIC_COLUMNS[1:]:
                row[metric] = extra.get(metric)
            records.append(row)

    if not records:
        return pd.DataFrame(columns=_TABLE_COLUMNS)

    frame = pd.DataFrame(records)
    numeric = [c for c in METRIC_COLUMNS if c not in EXACT_METRICS]
    for col in numeric:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    for col in EXACT_METRICS:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")

    grouped = (
        frame.groupby(["test type", "ID"], sort=False)
        .median(numeric_only=True)
        .reset_index()
    )
    return grouped.set_index(["test type", "ID"])


def _safe_pct(current, previous) -> float | None:
    """Return the percentage change current vs previous, or None when unknown."""
    if previous is None or current is None or pd.isna(previous) or previous == 0 or pd.isna(current):
        return None
    return (float(current) - float(previous)) / float(previous) * 100.0


def evaluate(current: pd.DataFrame, previous: pd.DataFrame | None, thresholds: dict) -> pd.DataFrame:
    """Compare current vs previous applying the configured thresholds.

    Returns a DataFrame with one row per (scenario, metric) pair.
    """
    metrics_cfg = thresholds.get("metrics", {})
    exact = set(thresholds.get("exact_metrics", [])) | set(EXACT_METRICS)

    rows = []
    for (test_type, run_id) in current.index:
        cur = current.loc[(test_type, run_id)]
        if previous is None:
            prev = pd.Series(dtype=float)
            baseline_ok = False
        elif (test_type, run_id) in previous.index:
            prev = previous.loc[(test_type, run_id)]
            baseline_ok = True
        else:
            rows.append({"test type": test_type, "ID": run_id, "metric": "(no baseline)",
                         "baseline": None, "current": None, "delta %": None, "verdict": "N/A"})
            continue

        for metric in METRIC_COLUMNS:
            if metric not in _allowed_metrics(test_type):
                continue
            cur_val = cur.get(metric)
            prev_val = prev.get(metric) if baseline_ok else None
            if cur_val is None or pd.isna(cur_val):
                continue

            if metric in exact:
                if baseline_ok and prev_val is not None and not pd.isna(prev_val):
                    assert cur_val is not None
                    cur_int = int(float(cur_val))
                    prev_int = int(float(prev_val))
                    verdict = "WARN" if cur_int != prev_int else "PASS"
                    rows.append({"test type": test_type, "ID": run_id, "metric": metric,
                                 "baseline": prev_val, "current": cur_val, "delta %": None, "verdict": verdict})
                elif not baseline_ok:
                    rows.append({"test type": test_type, "ID": run_id, "metric": metric,
                                 "baseline": None, "current": cur_val, "delta %": None, "verdict": "N/A"})
                continue

            cfg = metrics_cfg.get(metric, {})
            threshold = float(cfg.get("threshold", 15.0))
            floor = float(cfg.get("floor", 0.0))
            pct = _safe_pct(cur_val, prev_val)

            if not baseline_ok or prev_val is None or pd.isna(prev_val):
                rows.append({"test type": test_type, "ID": run_id, "metric": metric,
                             "baseline": prev_val, "current": cur_val, "delta %": pct, "verdict": "N/A"})
                continue

            verdict = "PASS"
            if pct is not None and pct > threshold and float(cur_val) >= floor:
                verdict = "WARN"
            rows.append({"test type": test_type, "ID": run_id, "metric": metric,
                         "baseline": prev_val, "current": cur_val, "delta %": pct, "verdict": verdict})

    return pd.DataFrame(rows, columns=_TABLE_COLUMNS)


def environment_warning(current_runs: list[dict], previous_runs: list[dict]) -> str | None:
    """Compare machine/environment info between current and baseline runs."""
    if not current_runs or not previous_runs:
        return None
    cur = current_runs[0].get("machine_info", {})
    prev = previous_runs[0].get("machine_info", {})
    cur_cpu = (cur.get("cpu") or {}).get("brand_raw")
    prev_cpu = (prev.get("cpu") or {}).get("brand_raw")
    if cur_cpu and prev_cpu and cur_cpu != prev_cpu:
        return (f"Environment differs from baseline: current ran on `{cur_cpu}` "
                f"while the baseline ran on `{prev_cpu}`. Results may not be comparable.")
    return None


def render_markdown(report: pd.DataFrame, version: str, current_label: str,
                    previous_label: str | None, env_warning: str | None) -> str:
    """Render the report as a GitHub-flavored markdown summary."""
    lines = [f"# Autosubmit Performance Metrics - Version {version}",
             "", f"- **Current:** {current_label}", f"- **Baseline:** {previous_label or 'None'}"]
    if env_warning:
        lines += ["", f"> ⚠️ {env_warning}"]
    lines.append("")

    warnings = report[report["verdict"] == "WARN"]
    if not warnings.empty:
        lines.append("## ⚠️ Regressions detected")
        lines.append("")
        for _, row in warnings.iterrows():
            delta = f"{row['delta %']:+.1f}%" if pd.notna(row["delta %"]) else "changed"
            lines.append(f"- `{row['test type']}/{row['ID']}` **{row['metric']}**: "
                         f"{row['baseline']:.2f} -> {row['current']:.2f} ({delta})")
        lines.append("")
    else:
        lines.append("## ✔️ No regressions detected")
        lines.append("")

    for test_type, group in report.groupby("test type", sort=False):
        table = group.drop(columns=["test type"]).copy()
        for col in ["baseline", "current"]:
            table[col] = table[col].map(lambda v: f"{v:.2f}" if pd.notna(v) else "-")
        table["delta %"] = table["delta %"].map(lambda v: f"{v:+.1f}%" if pd.notna(v) else "-")
        lines.append(f"### {test_type}")
        lines.append("")
        lines.append(table.to_markdown(index=False))
        lines.append("")
    return "\n".join(lines)


def _abbreviate_id(run_id: str) -> str:
    """Shorten a scenario id like ``fc0_fc1_fc2_fc3_2_10_ftcs`` to ``4m/2c/10s·ftcs``."""
    parts = run_id.split("_")
    members = sum(1 for part in parts if part.startswith("fc"))
    rest = parts[members:]
    if len(rest) >= 2:
        label = f"{members}m/{rest[0]}c/{rest[1]}s"
        tail = "·".join(rest[2:])
        return f"{label}·{tail}" if tail else label
    return run_id


def render_heatmap(current: pd.DataFrame, previous: pd.DataFrame | None, report: pd.DataFrame,
                   version: str, output_dir: Path, cpu_label: str | None = None) -> Path:
    """Render a delta heatmap of every scenario x metric, current vs baseline.

    Cells show the percentage change against the (CPU-matched) baseline using a
    diverging scale centered at zero (red = regression). Cells without a
    baseline, or for metrics excluded from a test type, are left gray.
    """
    import matplotlib

    matplotlib.use("Agg")
    import numpy as np
    import matplotlib.pyplot as plt
    from matplotlib.colors import TwoSlopeNorm

    metrics = [c for c in _PLOT_METRICS if current[c].notna().any()]
    if not metrics:
        out = output_dir / f"summary_{version}.png"
        out.parent.mkdir(parents=True, exist_ok=True)
        return out

    order = []
    labels = []
    for test_type in current.index.get_level_values("test type").unique():
        for run_id in current.xs(test_type, level="test type").index:
            order.append((test_type, run_id))
            labels.append(f"{test_type} · {_abbreviate_id(run_id)}")

    pivot = report.pivot_table(index=["test type", "ID"], columns="metric",
                               values="delta %", aggfunc="first")
    matrix = pivot.reindex(index=order, columns=metrics).to_numpy(dtype=float)

    clip = 50.0
    norm = TwoSlopeNorm(vmin=-clip, vcenter=0, vmax=clip)
    fig, ax = plt.subplots(figsize=(7.5, max(3.5, 0.42 * len(order) + 1.5)))
    im = ax.imshow(np.ma.masked_invalid(matrix), cmap="RdYlGn_r", norm=norm, aspect="auto")

    for r in range(len(order)):
        for c in range(len(metrics)):
            value = matrix[r, c]
            if np.isnan(value):
                continue
            ax.text(c, r, f"{value:+.1f}%", ha="center", va="center", fontsize=8,
                    color="white" if abs(value) > clip * 0.6 else "black")

    ax.set_yticks(range(len(order)))
    ax.set_yticklabels(labels, fontsize=7)
    ax.set_xticks(range(len(metrics)))
    ax.set_xticklabels([_SHORT_METRICS.get(m, m) for m in metrics], fontsize=9)

    prev_type = None
    for r, (test_type, _) in enumerate(order):
        if prev_type is not None and test_type != prev_type:
            ax.axhline(r - 0.5, color="gray", lw=0.9)
        prev_type = test_type

    title = f"Autosubmit Performance Metrics - Version {version}"
    if cpu_label:
        title += f" · {cpu_label}"
    ax.set_title(title, fontsize=13)
    cb = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cb.set_label("delta % vs baseline")

    fig.tight_layout()
    out = output_dir / f"summary_{version}.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=110, bbox_inches="tight")
    plt.close(fig)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current", required=True, help="Current run file or directory of JSON runs.")
    parser.add_argument("--previous", default=None, help="Baseline run file or directory of JSON runs.")
    parser.add_argument("--thresholds", default=str(Path(__file__).parent / "thresholds.yml"),
                        help="Path to the thresholds YAML file.")
    parser.add_argument("--version", default=None, help="Autosubmit version, used to name the output files.")
    parser.add_argument("--output-dir", default=str(Path(__file__).parent / "artifacts"),
                        help="Directory where the report files are written.")
    parser.add_argument("--current-label", default="Current run", help="Label for the current run.")
    parser.add_argument("--previous-label", default="Baseline", help="Label for the baseline.")
    args = parser.parse_args()

    if not args.version:
        from importlib.metadata import version as pkg_version

        try:
            args.version = pkg_version("autosubmit")
        except Exception:
            args.version = (Path(__file__).parent.parent / "VERSION").read_text().strip()

    current_files = _iter_run_files(args.current, latest_only=True)
    if not current_files:
        print(f"[ERROR] No benchmark runs found under --current {args.current}", file=sys.stderr)
        return 1

    current_runs = _load_runs(current_files)
    current_cpu = _current_cpu(current_runs)

    previous_files, baseline_slug = _select_previous(args.previous, current_cpu)
    previous_runs = _load_runs(previous_files) if previous_files else []
    if previous_runs and current_cpu:
        baseline_cpu = _current_cpu(previous_runs)
        if baseline_cpu and baseline_cpu != current_cpu:
            print(f"[WARNING] Baseline CPU `{baseline_cpu}` differs from current `{current_cpu}`; "
                  f"ignoring baseline.")
            previous_runs = []

    current_frame = build_frame(current_runs)
    previous_frame = build_frame(previous_runs) if previous_runs else None

    thresholds = _load_thresholds(Path(args.thresholds))
    report = evaluate(current_frame, previous_frame, thresholds)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    prev_label = args.previous_label if previous_runs else None
    baseline_cpu = _current_cpu(previous_runs) if previous_runs else ""
    if prev_label and baseline_cpu:
        prev_label = f"{prev_label} · {baseline_cpu}"

    env_warning = environment_warning(current_runs, previous_runs)
    if args.previous and not previous_runs and current_cpu:
        note = f"No baseline yet for CPU `{current_cpu}` - this run establishes it (shown without comparison)."
        env_warning = f"{env_warning}\n\n{note}" if env_warning else note

    markdown = render_markdown(report, args.version, args.current_label, prev_label, env_warning)
    markdown_path = output_dir / f"summary_{args.version}.md"
    markdown_path.write_text(markdown, encoding="UTF-8")
    print(f"Saved performance comparison markdown to {markdown_path}")

    plot_path = render_heatmap(current_frame, previous_frame, report, args.version, output_dir,
                               cpu_label=current_cpu or None)
    print(f"Saved performance comparison plot to {plot_path}")

    n_warnings = int((report["verdict"] == "WARN").sum())
    verdict = {
        "version": args.version,
        "regressions_detected": n_warnings > 0,
        "n_regressions": n_warnings,
        "n_scenarios": int(len(current_frame)),
        "cpu": current_cpu,
        "cpu_slug": _cpu_slug(current_cpu) if current_cpu else "",
    }
    verdict_path = output_dir / f"report_{args.version}.json"
    verdict_path.write_text(json.dumps(verdict), encoding="UTF-8")
    print(f"Saved comparison verdict to {verdict_path}")
    print(f"Detected {n_warnings} regression warning(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
