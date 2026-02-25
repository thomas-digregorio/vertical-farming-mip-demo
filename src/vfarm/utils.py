from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from .config import AppConfig, infer_project_root


@dataclass(frozen=True)
class ResolvedPaths:
    root_dir: Path
    data_dir: Path
    raw_dir: Path
    agg_dir: Path
    outputs_dir: Path


def resolve_paths(config: AppConfig, config_path: str | Path) -> ResolvedPaths:
    config_file = Path(config_path)
    root_dir = infer_project_root(config_file)

    data_dir = _resolve_path(root_dir, config.paths.data_dir)
    raw_dir = _resolve_path(data_dir, config.paths.raw_subdir)
    agg_dir = _resolve_path(data_dir, config.paths.agg_subdir)
    outputs_dir = _resolve_path(root_dir, config.paths.outputs_dir)

    raw_dir.mkdir(parents=True, exist_ok=True)
    agg_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)

    return ResolvedPaths(
        root_dir=root_dir,
        data_dir=data_dir,
        raw_dir=raw_dir,
        agg_dir=agg_dir,
        outputs_dir=outputs_dir,
    )


def _resolve_path(base: Path, path_like: str) -> Path:
    path = Path(path_like)
    if path.is_absolute():
        return path
    return (base / path).resolve()


def make_rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def build_calendar(start_date: pd.Timestamp, weeks: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    total_days = weeks * 7
    for day_idx in range(total_days):
        current = start_date + timedelta(days=day_idx)
        week_id = (day_idx // 7) + 1
        rows.append(
            {
                "date": current.date().isoformat(),
                "week_id": week_id,
                "day_of_week": int(current.day_of_week),
            }
        )
    return pd.DataFrame(rows)


def generate_feasible_weekly_demand(
    config: AppConfig, rng: np.random.Generator
) -> pd.DataFrame:
    weeks = config.weeks
    crops = config.crops

    avg_grow_weeks = float(
        np.mean(
            [
                (config.crop_params[c].g_min_weeks + config.crop_params[c].g_max_weeks) / 2.0
                for c in crops
            ]
        )
    )
    avg_total_labor_hours = float(
        np.mean(
            [
                config.crop_params[c].plant_hours_per_tray
                + config.crop_params[c].harvest_hours_per_tray
                for c in crops
            ]
        )
    )

    rack_limit = config.capacity.rack_capacity_trays_per_week / max(avg_grow_weeks, 1.0)
    process_limit = float(config.capacity.base_process_capacity_trays_per_week)
    labor_limit = (
        config.capacity.base_labor_hours_per_week + sum(config.capacity.overtime_block_hours)
    ) / max(avg_total_labor_hours, 1e-6)

    tray_limit = max(1.0, min(rack_limit, process_limit, labor_limit))
    target_total_trays = max(8, int(round(tray_limit * config.demand.utilization_target)))

    base_weights = rng.random(len(crops))
    base_weights = base_weights / base_weights.sum()
    crop_to_weight = {crop: base_weights[idx] for idx, crop in enumerate(crops)}

    rows: list[dict[str, object]] = []

    for week_id in range(1, weeks + 1):
        eligible_crops = [crop for crop in crops if week_id > config.crop_params[crop].g_min_weeks]

        if eligible_crops:
            seasonal = 1.0 + config.demand.seasonality_amplitude * np.sin(
                2.0 * np.pi * week_id / max(weeks, 1)
            )
            noise = max(0.55, 1.0 + rng.normal(0.0, config.demand.noise_std))
            total_trays = int(round(target_total_trays * seasonal * noise))
            total_trays = max(0, min(total_trays, int(round(tray_limit * 0.92))))

            jitter = rng.uniform(0.8, 1.2, size=len(eligible_crops))
            weights = np.array([crop_to_weight[crop] for crop in eligible_crops]) * jitter
            weights = weights / weights.sum()
            trays_by_crop = rng.multinomial(total_trays, weights)
            tray_lookup = {crop: int(trays_by_crop[idx]) for idx, crop in enumerate(eligible_crops)}
        else:
            tray_lookup = {}

        for crop in crops:
            trays = tray_lookup.get(crop, 0)
            net_yield = config.crop_params[crop].effective_yield_lbs
            demand_lbs = float(trays * net_yield * config.demand.safety_factor)
            rows.append(
                {
                    "crop": crop,
                    "week_id": week_id,
                    "demand_lbs": demand_lbs,
                    "demand_trays_est": trays,
                }
            )

    return pd.DataFrame(rows)
