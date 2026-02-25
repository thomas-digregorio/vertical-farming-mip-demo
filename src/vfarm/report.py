from __future__ import annotations

import json
from pathlib import Path

import matplotlib
import pandas as pd

from .config import load_config
from .utils import resolve_paths

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def generate_report(config_path: str | Path, make_plots: bool = True) -> dict[str, float | int]:
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)

    harvest = pd.read_csv(paths.outputs_dir / "solution_harvest.csv")
    utilization = pd.read_csv(paths.outputs_dir / "utilization_weekly.csv")
    with (paths.outputs_dir / "cost_breakdown.json").open("r", encoding="utf-8") as f:
        cost_breakdown = json.load(f)

    demand_weekly = pd.read_parquet(paths.agg_dir / "demand_weekly.parquet")
    demand_total = float(demand_weekly["demand_lbs"].sum())
    produced_total = float(harvest["produced_lbs"].sum())
    waste_total = float(harvest["waste_lbs"].sum())

    summary: dict[str, float | int] = {
        "weeks": int(config.weeks),
        "num_crops": int(len(config.crops)),
        "total_demand_lbs": demand_total,
        "total_produced_lbs": produced_total,
        "total_waste_lbs": waste_total,
        "avg_rack_utilization": float(utilization["rack_utilization"].mean()),
        "avg_labor_utilization": float(utilization["labor_utilization"].mean()),
        "avg_process_utilization": float(utilization["process_utilization"].mean()),
        "total_cost": float(cost_breakdown["total_cost"]),
    }

    with (paths.outputs_dir / "report_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    summary_md = [
        "# Vertical Farming Optimization Report",
        "",
        f"- Weeks: {summary['weeks']}",
        f"- Crops: {summary['num_crops']}",
        f"- Total demand (lbs): {summary['total_demand_lbs']:.2f}",
        f"- Total produced (lbs): {summary['total_produced_lbs']:.2f}",
        f"- Total waste (lbs): {summary['total_waste_lbs']:.2f}",
        f"- Average rack utilization: {summary['avg_rack_utilization']:.2%}",
        f"- Average labor utilization: {summary['avg_labor_utilization']:.2%}",
        f"- Average process utilization: {summary['avg_process_utilization']:.2%}",
        f"- Total cost: ${summary['total_cost']:.2f}",
        "",
        "## Cost Breakdown",
        "",
    ]
    for key, value in cost_breakdown.items():
        summary_md.append(f"- {key}: ${float(value):.2f}")

    with (paths.outputs_dir / "report_summary.md").open("w", encoding="utf-8") as f:
        f.write("\n".join(summary_md))

    if make_plots:
        _make_plots(paths.outputs_dir, harvest, utilization)

    return summary


def _make_plots(outputs_dir: Path, harvest: pd.DataFrame, utilization: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(utilization["week_id"], utilization["rack_utilization"], label="Rack")
    ax.plot(utilization["week_id"], utilization["labor_utilization"], label="Labor")
    ax.plot(utilization["week_id"], utilization["process_utilization"], label="Process")
    ax.set_title("Weekly Utilization")
    ax.set_xlabel("Week")
    ax.set_ylabel("Utilization")
    ax.set_ylim(0, 1.1)
    ax.legend()
    fig.tight_layout()
    fig.savefig(outputs_dir / "utilization_weekly.png", dpi=140)
    plt.close(fig)

    by_week = (
        harvest.groupby("week_id", as_index=False)[["produced_lbs", "demand_lbs"]]
        .sum()
        .sort_values("week_id")
    )
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(by_week["week_id"], by_week["produced_lbs"], marker="o", label="Produced")
    ax.plot(by_week["week_id"], by_week["demand_lbs"], marker="o", label="Demand")
    ax.set_title("Production vs Demand (Total Lbs)")
    ax.set_xlabel("Week")
    ax.set_ylabel("Lbs")
    ax.legend()
    fig.tight_layout()
    fig.savefig(outputs_dir / "production_vs_demand.png", dpi=140)
    plt.close(fig)
