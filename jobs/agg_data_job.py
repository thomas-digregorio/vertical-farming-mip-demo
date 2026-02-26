from __future__ import annotations

import argparse
from pathlib import Path

from vfarm.config import load_config
from vfarm.spark_generate import generate_data
from vfarm.spark_aggregate import aggregate_data
from vfarm.utils import resolve_paths


def _raw_inputs_exist(config_path: str) -> bool:
    cfg = load_config(config_path)
    paths = resolve_paths(cfg, config_path)
    required = (
        Path(paths.raw_dir / "telemetry_tray_daily.parquet"),
        Path(paths.raw_dir / "demand_daily.parquet"),
        Path(paths.raw_dir / "prices_energy_daily.parquet"),
        Path(paths.raw_dir / "crop_params.parquet"),
    )
    return all(p.exists() for p in required)


def main() -> None:
    parser = argparse.ArgumentParser(description="Dataproc job: aggregate vfarm weekly inputs")
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    if not _raw_inputs_exist(args.config):
        # Dataproc batches are isolated; bootstrap raw data when agg runs standalone.
        generate_data(args.config)

    result = aggregate_data(args.config)
    print(result)


if __name__ == "__main__":
    main()
