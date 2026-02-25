from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pandas as pd
import pytest
import yaml

from vfarm.optimize import solve_optimization
from vfarm.spark_aggregate import aggregate_data
from vfarm.spark_generate import generate_data


REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_CONFIG = REPO_ROOT / "configs" / "base.yaml"


def _write_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False)


@pytest.fixture(scope="module")
def prepared_config(tmp_path_factory: pytest.TempPathFactory) -> Path:
    tmp_root = tmp_path_factory.mktemp("vfarm_demo")

    with BASE_CONFIG.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    cfg["paths"]["data_dir"] = str(tmp_root / "data")
    cfg["paths"]["outputs_dir"] = str(tmp_root / "outputs")
    cfg["data_mode"] = "rows"

    config_path = tmp_root / "base.yaml"
    _write_yaml(config_path, cfg)

    generate_data(config_path)
    aggregate_data(config_path)
    return config_path


def test_gen_data_outputs_and_columns(prepared_config: Path) -> None:
    with prepared_config.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    data_dir = Path(cfg["paths"]["data_dir"]) / cfg["paths"]["raw_subdir"]

    telemetry_path = data_dir / "telemetry_tray_daily.parquet"
    demand_path = data_dir / "demand_daily.parquet"
    prices_path = data_dir / "prices_energy_daily.parquet"
    crop_params_path = data_dir / "crop_params.parquet"

    assert telemetry_path.exists()
    assert demand_path.exists()
    assert prices_path.exists()
    assert crop_params_path.exists()

    telemetry = pd.read_parquet(telemetry_path)
    demand = pd.read_parquet(demand_path)
    prices = pd.read_parquet(prices_path)
    crop_params = pd.read_parquet(crop_params_path)

    assert {"date", "week_id", "crop", "tray_id", "stage_age_days", "kwh_used", "is_active"}.issubset(
        telemetry.columns
    )
    assert {"date", "week_id", "crop", "demand_lbs"}.issubset(demand.columns)
    assert {"date", "week_id", "energy_price_per_kwh"}.issubset(prices.columns)
    assert {
        "crop",
        "g_min_weeks",
        "g_max_weeks",
        "yield_lbs_per_tray",
        "shrink",
        "plant_hours_per_tray",
        "harvest_hours_per_tray",
        "seed_cost_per_tray",
        "nutrient_cost_per_tray_week",
        "energy_kwh_per_tray_week",
    }.issubset(crop_params.columns)

    assert (demand["demand_lbs"] >= 0).all()
    assert (prices["energy_price_per_kwh"] > 0).all()


def test_default_config_solves_and_meets_demand(prepared_config: Path) -> None:
    result = solve_optimization(prepared_config, tee=False, feasibility_only=False)

    assert result.feasible is True
    assert result.objective_value is not None
    assert result.termination_condition.lower() in {"optimal", "locallyoptimal"}

    with prepared_config.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    outputs_dir = Path(cfg["paths"]["outputs_dir"])
    harvest = pd.read_csv(outputs_dir / "solution_harvest.csv")

    assert ((harvest["produced_lbs"] + 1e-6) >= harvest["demand_lbs"]).all()


def test_objective_nonincreasing_with_more_rack_capacity(prepared_config: Path) -> None:
    with prepared_config.open("r", encoding="utf-8") as f:
        base_cfg = yaml.safe_load(f)

    low_cap_cfg = deepcopy(base_cfg)
    low_cap_cfg["capacity"]["rack_capacity_trays_per_week"] = 1000
    low_cap_cfg["paths"]["outputs_dir"] = str(Path(base_cfg["paths"]["outputs_dir"]).parent / "outputs_low")

    high_cap_cfg = deepcopy(base_cfg)
    high_cap_cfg["capacity"]["rack_capacity_trays_per_week"] = 1400
    high_cap_cfg["paths"]["outputs_dir"] = str(Path(base_cfg["paths"]["outputs_dir"]).parent / "outputs_high")

    low_cfg_path = prepared_config.parent / "low_cap.yaml"
    high_cfg_path = prepared_config.parent / "high_cap.yaml"
    _write_yaml(low_cfg_path, low_cap_cfg)
    _write_yaml(high_cfg_path, high_cap_cfg)

    low_result = solve_optimization(low_cfg_path, tee=False, feasibility_only=False)
    high_result = solve_optimization(high_cfg_path, tee=False, feasibility_only=False)

    assert low_result.objective_value is not None
    assert high_result.objective_value is not None
    assert high_result.objective_value <= low_result.objective_value + 1e-6
