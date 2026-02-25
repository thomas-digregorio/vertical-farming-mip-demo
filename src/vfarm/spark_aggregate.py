from __future__ import annotations

from pathlib import Path
import shutil

import pandas as pd
from pyspark.sql import functions as F

from .config import load_config
from .utils import build_spark_session, resolve_paths


def aggregate_data(config_path: str | Path) -> dict[str, str]:
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)

    demand_weekly_path = paths.agg_dir / "demand_weekly.parquet"
    energy_weekly_path = paths.agg_dir / "energy_price_weekly.parquet"
    crop_params_path = paths.agg_dir / "crop_params.parquet"
    telemetry_weekly_path = paths.agg_dir / "telemetry_weekly.parquet"

    spark = None
    try:
        spark = build_spark_session("vfarm-aggregate")
    except Exception:
        spark = None

    if spark is not None:
        try:
            demand_daily = spark.read.parquet(str(paths.raw_dir / "demand_daily.parquet"))
            prices_daily = spark.read.parquet(str(paths.raw_dir / "prices_energy_daily.parquet"))
            crop_params = spark.read.parquet(str(paths.raw_dir / "crop_params.parquet"))
            telemetry_daily = spark.read.parquet(str(paths.raw_dir / "telemetry_tray_daily.parquet"))

            demand_weekly = (
                demand_daily.groupBy("crop", "week_id")
                .agg(F.sum("demand_lbs").alias("demand_lbs"))
                .orderBy("crop", "week_id")
            )

            energy_price_weekly = (
                prices_daily.groupBy("week_id")
                .agg(F.avg("energy_price_per_kwh").alias("price_per_kwh"))
                .orderBy("week_id")
            )

            if "tray_id" in telemetry_daily.columns:
                telemetry_weekly = (
                    telemetry_daily.groupBy("crop", "week_id")
                    .agg(
                        F.count("tray_id").alias("active_tray_rows"),
                        F.avg("stage_age_days").alias("avg_stage_age_days"),
                        F.sum("kwh_used").alias("kwh_used"),
                    )
                    .orderBy("crop", "week_id")
                )
            else:
                telemetry_weekly = (
                    telemetry_daily.groupBy("crop", "week_id")
                    .agg(
                        F.sum("active_trays").alias("active_trays"),
                        F.avg("stage_age_days").alias("avg_stage_age_days"),
                        F.sum("kwh_used").alias("kwh_used"),
                    )
                    .orderBy("crop", "week_id")
                )

            demand_weekly.write.mode("overwrite").parquet(str(demand_weekly_path))
            energy_price_weekly.write.mode("overwrite").parquet(str(energy_weekly_path))
            crop_params.dropDuplicates(["crop"]).write.mode("overwrite").parquet(
                str(crop_params_path)
            )
            telemetry_weekly.write.mode("overwrite").parquet(str(telemetry_weekly_path))

            # Small CSV artifacts for easier local inspection.
            demand_weekly.toPandas().to_csv(paths.agg_dir / "demand_weekly.csv", index=False)
            energy_price_weekly.toPandas().to_csv(
                paths.agg_dir / "energy_price_weekly.csv", index=False
            )
            crop_params.dropDuplicates(["crop"]).toPandas().to_csv(
                paths.agg_dir / "crop_params.csv", index=False
            )
        finally:
            spark.stop()
    else:
        demand_daily = pd.read_parquet(paths.raw_dir / "demand_daily.parquet")
        prices_daily = pd.read_parquet(paths.raw_dir / "prices_energy_daily.parquet")
        crop_params = pd.read_parquet(paths.raw_dir / "crop_params.parquet")
        telemetry_daily = pd.read_parquet(paths.raw_dir / "telemetry_tray_daily.parquet")

        demand_weekly_pd = (
            demand_daily.groupby(["crop", "week_id"], as_index=False)["demand_lbs"].sum()
        ).sort_values(["crop", "week_id"])
        energy_weekly_pd = (
            prices_daily.groupby("week_id", as_index=False)["energy_price_per_kwh"]
            .mean()
            .rename(columns={"energy_price_per_kwh": "price_per_kwh"})
            .sort_values("week_id")
        )
        crop_params_pd = crop_params.drop_duplicates(subset=["crop"])

        if "tray_id" in telemetry_daily.columns:
            telemetry_weekly_pd = (
                telemetry_daily.groupby(["crop", "week_id"], as_index=False)
                .agg(
                    active_tray_rows=("tray_id", "count"),
                    avg_stage_age_days=("stage_age_days", "mean"),
                    kwh_used=("kwh_used", "sum"),
                )
                .sort_values(["crop", "week_id"])
            )
        else:
            telemetry_weekly_pd = (
                telemetry_daily.groupby(["crop", "week_id"], as_index=False)
                .agg(
                    active_trays=("active_trays", "sum"),
                    avg_stage_age_days=("stage_age_days", "mean"),
                    kwh_used=("kwh_used", "sum"),
                )
                .sort_values(["crop", "week_id"])
            )

        _write_parquet_pandas(demand_weekly_pd, demand_weekly_path)
        _write_parquet_pandas(energy_weekly_pd, energy_weekly_path)
        _write_parquet_pandas(crop_params_pd, crop_params_path)
        _write_parquet_pandas(telemetry_weekly_pd, telemetry_weekly_path)

        demand_weekly_pd.to_csv(paths.agg_dir / "demand_weekly.csv", index=False)
        energy_weekly_pd.to_csv(paths.agg_dir / "energy_price_weekly.csv", index=False)
        crop_params_pd.to_csv(paths.agg_dir / "crop_params.csv", index=False)

    return {
        "demand_weekly": str(demand_weekly_path),
        "energy_price_weekly": str(energy_weekly_path),
        "crop_params": str(crop_params_path),
        "telemetry_weekly": str(telemetry_weekly_path),
    }


def _write_parquet_pandas(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    df.to_parquet(path, index=False)
