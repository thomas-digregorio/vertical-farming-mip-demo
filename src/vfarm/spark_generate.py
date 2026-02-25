from __future__ import annotations

from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from .config import AppConfig, load_config
from .utils import (
    build_calendar,
    build_spark_session,
    generate_feasible_weekly_demand,
    make_rng,
    resolve_paths,
)


def generate_data(config_path: str | Path) -> dict[str, str]:
    config = load_config(config_path)
    if config.data_mode not in {"counts", "rows"}:
        raise ValueError("data_mode must be one of: counts, rows")

    paths = resolve_paths(config, config_path)
    rng = make_rng(config.seed)

    calendar = build_calendar(pd.Timestamp(config.start_date), config.weeks)
    demand_weekly = generate_feasible_weekly_demand(config, rng)

    demand_daily = _generate_demand_daily(calendar, demand_weekly, rng)
    energy_daily = _generate_energy_daily(config, calendar, rng)
    crop_params_df = _build_crop_params_df(config)

    telemetry_counts = _generate_telemetry_counts(config, calendar, rng)
    if config.data_mode == "rows":
        telemetry_daily = _expand_telemetry_rows(config, telemetry_counts, rng)
    else:
        telemetry_daily = telemetry_counts

    telemetry_path = paths.raw_dir / "telemetry_tray_daily.parquet"
    demand_path = paths.raw_dir / "demand_daily.parquet"
    prices_path = paths.raw_dir / "prices_energy_daily.parquet"
    crop_params_path = paths.raw_dir / "crop_params.parquet"

    spark = None
    try:
        spark = build_spark_session("vfarm-generate")
    except Exception:
        spark = None

    if spark is not None:
        try:
            spark.createDataFrame(telemetry_daily).write.mode("overwrite").parquet(
                str(telemetry_path)
            )
            spark.createDataFrame(demand_daily).write.mode("overwrite").parquet(str(demand_path))
            spark.createDataFrame(energy_daily).write.mode("overwrite").parquet(str(prices_path))
            spark.createDataFrame(crop_params_df).write.mode("overwrite").parquet(
                str(crop_params_path)
            )
        finally:
            spark.stop()
    else:
        # Fallback for environments without Java/Spark runtime.
        _write_parquet_pandas(telemetry_daily, telemetry_path)
        _write_parquet_pandas(demand_daily, demand_path)
        _write_parquet_pandas(energy_daily, prices_path)
        _write_parquet_pandas(crop_params_df, crop_params_path)

    return {
        "telemetry_tray_daily": str(telemetry_path),
        "demand_daily": str(demand_path),
        "prices_energy_daily": str(prices_path),
        "crop_params": str(crop_params_path),
    }


def _build_crop_params_df(config: AppConfig) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for crop in config.crops:
        p = config.crop_params[crop]
        rows.append(
            {
                "crop": crop,
                "g_min_weeks": p.g_min_weeks,
                "g_max_weeks": p.g_max_weeks,
                "yield_lbs_per_tray": p.yield_lbs_per_tray,
                "shrink": p.shrink,
                "plant_hours_per_tray": p.plant_hours_per_tray,
                "harvest_hours_per_tray": p.harvest_hours_per_tray,
                "seed_cost_per_tray": p.seed_cost_per_tray,
                "nutrient_cost_per_tray_week": p.nutrient_cost_per_tray_week,
                "energy_kwh_per_tray_week": p.energy_kwh_per_tray_week,
            }
        )
    return pd.DataFrame(rows)


def _generate_demand_daily(
    calendar: pd.DataFrame, demand_weekly: pd.DataFrame, rng: np.random.Generator
) -> pd.DataFrame:
    dates_by_week = {
        int(week_id): week_frame["date"].tolist()
        for week_id, week_frame in calendar.groupby("week_id", sort=True)
    }

    rows: list[dict[str, object]] = []
    for rec in demand_weekly.itertuples(index=False):
        week_id = int(rec.week_id)
        crop = str(rec.crop)
        week_demand = float(rec.demand_lbs)
        week_dates = dates_by_week[week_id]

        if week_demand <= 0:
            shares = np.zeros(len(week_dates), dtype=float)
        else:
            shares = rng.dirichlet(np.ones(len(week_dates), dtype=float))

        for idx, current_date in enumerate(week_dates):
            rows.append(
                {
                    "date": current_date,
                    "week_id": week_id,
                    "crop": crop,
                    "demand_lbs": float(week_demand * shares[idx]),
                }
            )

    return pd.DataFrame(rows)


def _generate_energy_daily(
    config: AppConfig, calendar: pd.DataFrame, rng: np.random.Generator
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    weeks = config.weeks

    for rec in calendar.itertuples(index=False):
        week_id = int(rec.week_id)
        seasonal = 1.0 + config.energy_price.seasonality_amplitude * np.sin(
            2.0 * np.pi * week_id / max(weeks, 1)
        )
        noisy = 1.0 + rng.normal(0.0, config.energy_price.noise_std)
        price = config.energy_price.base_per_kwh * seasonal * noisy
        price = max(0.02, float(price))

        rows.append(
            {
                "date": rec.date,
                "week_id": week_id,
                "energy_price_per_kwh": price,
            }
        )

    return pd.DataFrame(rows)


def _generate_telemetry_counts(
    config: AppConfig, calendar: pd.DataFrame, rng: np.random.Generator
) -> pd.DataFrame:
    crops = config.crops
    weeks = config.weeks
    base_weights = rng.random(len(crops))
    base_weights = base_weights / base_weights.sum()

    weekly_active: dict[int, dict[str, int]] = {}
    for week_id in range(1, weeks + 1):
        seasonal = 0.55 + 0.10 * np.sin(2.0 * np.pi * week_id / max(weeks, 1))
        noise = max(0.35, 1.0 + rng.normal(0.0, 0.08))
        total_active = int(
            max(
                40,
                min(
                    config.capacity.rack_capacity_trays_per_week,
                    round(config.capacity.rack_capacity_trays_per_week * seasonal * noise),
                ),
            )
        )

        jitter = rng.uniform(0.85, 1.15, size=len(crops))
        w = base_weights * jitter
        w = w / w.sum()
        split = rng.multinomial(total_active, w)
        weekly_active[week_id] = {crop: int(split[idx]) for idx, crop in enumerate(crops)}

    rows: list[dict[str, object]] = []
    for rec in calendar.itertuples(index=False):
        week_id = int(rec.week_id)
        date = str(rec.date)
        for crop in crops:
            active_trays = max(0, int(round(weekly_active[week_id][crop] * rng.uniform(0.93, 1.07))))
            max_age_days = config.crop_params[crop].g_max_weeks * 7
            stage_age_days = int(rng.integers(1, max(max_age_days, 2)))
            per_tray_kwh_day = config.crop_params[crop].energy_kwh_per_tray_week / 7.0
            total_kwh = float(active_trays * per_tray_kwh_day * rng.uniform(0.9, 1.1))

            rows.append(
                {
                    "date": date,
                    "week_id": week_id,
                    "crop": crop,
                    "batch_id": f"{crop}_{date}",
                    "active_trays": active_trays,
                    "stage_age_days": stage_age_days,
                    "kwh_used": total_kwh,
                    "is_active": active_trays > 0,
                }
            )

    return pd.DataFrame(rows)


def _expand_telemetry_rows(
    config: AppConfig, telemetry_counts: pd.DataFrame, rng: np.random.Generator
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for rec in telemetry_counts.itertuples(index=False):
        date = str(rec.date)
        week_id = int(rec.week_id)
        crop = str(rec.crop)
        active_trays = int(rec.active_trays)
        per_tray_kwh_day = config.crop_params[crop].energy_kwh_per_tray_week / 7.0
        max_age_days = config.crop_params[crop].g_max_weeks * 7

        for idx in range(active_trays):
            rows.append(
                {
                    "date": date,
                    "week_id": week_id,
                    "crop": crop,
                    "tray_id": f"{crop}_{date.replace('-', '')}_{idx:05d}",
                    "stage_age_days": int(rng.integers(1, max(max_age_days, 2))),
                    "kwh_used": float(max(0.01, per_tray_kwh_day * rng.normal(1.0, 0.12))),
                    "is_active": True,
                }
            )

    return pd.DataFrame(rows)


def _write_parquet_pandas(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    df.to_parquet(path, index=False)
