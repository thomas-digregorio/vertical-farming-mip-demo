from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class CropParam:
    g_min_weeks: int
    g_max_weeks: int
    yield_lbs_per_tray: float
    shrink: float
    plant_hours_per_tray: float
    harvest_hours_per_tray: float
    seed_cost_per_tray: float
    nutrient_cost_per_tray_week: float
    energy_kwh_per_tray_week: float

    @property
    def effective_yield_lbs(self) -> float:
        return self.yield_lbs_per_tray * (1.0 - self.shrink)


@dataclass(frozen=True)
class CapacityConfig:
    rack_capacity_trays_per_week: int = 1200
    base_labor_hours_per_week: float = 220.0
    base_process_capacity_trays_per_week: int = 240
    overtime_block_hours: tuple[float, ...] = (40.0, 40.0)
    overtime_premium_per_hour: float = 12.0
    base_wage_per_hour: float = 24.0
    shift_fixed_cost_per_week: float = 180.0
    shift_labor_reduction_pct: float = 0.15


@dataclass(frozen=True)
class DemandConfig:
    seasonality_amplitude: float = 0.12
    noise_std: float = 0.08
    utilization_target: float = 0.70
    safety_factor: float = 0.90


@dataclass(frozen=True)
class EnergyPriceConfig:
    base_per_kwh: float = 0.13
    seasonality_amplitude: float = 0.08
    noise_std: float = 0.01


@dataclass(frozen=True)
class CostConfig:
    disposal_cost_per_lb_waste: float = 0.35


@dataclass(frozen=True)
class PathConfig:
    data_dir: str = "data"
    raw_subdir: str = "raw"
    agg_subdir: str = "agg"
    outputs_dir: str = "outputs"


@dataclass(frozen=True)
class SolverConfig:
    name: str = "highs"


@dataclass(frozen=True)
class AppConfig:
    seed: int
    weeks: int
    start_date: date
    data_mode: str
    crops: list[str]
    crop_params: dict[str, CropParam]
    capacity: CapacityConfig = field(default_factory=CapacityConfig)
    demand: DemandConfig = field(default_factory=DemandConfig)
    energy_price: EnergyPriceConfig = field(default_factory=EnergyPriceConfig)
    costs: CostConfig = field(default_factory=CostConfig)
    paths: PathConfig = field(default_factory=PathConfig)
    solver: SolverConfig = field(default_factory=SolverConfig)
    initial_wip: dict[str, dict[int, int]] = field(default_factory=dict)

    @property
    def max_age_weeks(self) -> int:
        return max(self.crop_params[c].g_max_weeks for c in self.crops)


def infer_project_root(config_path: Path) -> Path:
    config_path = config_path.resolve()
    if config_path.parent.name == "configs":
        return config_path.parent.parent
    return config_path.parent


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as f:
        raw: dict[str, Any] = yaml.safe_load(f)

    crop_params_raw = raw.get("crop_params", {})
    crop_params: dict[str, CropParam] = {
        crop: CropParam(**params) for crop, params in crop_params_raw.items()
    }

    crops = raw.get("crops", list(crop_params.keys()))
    if not crops:
        raise ValueError("Config must include at least one crop.")

    missing = [crop for crop in crops if crop not in crop_params]
    if missing:
        raise ValueError(f"Missing crop_params entries for crops: {missing}")

    initial_wip = _normalize_initial_wip(raw.get("initial_wip", {}))

    return AppConfig(
        seed=int(raw.get("seed", 20260225)),
        weeks=int(raw.get("weeks", 12)),
        start_date=date.fromisoformat(raw.get("start_date", "2026-01-05")),
        data_mode=str(raw.get("data_mode", "rows")).strip().lower(),
        crops=list(crops),
        crop_params=crop_params,
        capacity=CapacityConfig(**raw.get("capacity", {})),
        demand=DemandConfig(**raw.get("demand", {})),
        energy_price=EnergyPriceConfig(**raw.get("energy_price", {})),
        costs=CostConfig(**raw.get("costs", {})),
        paths=PathConfig(**raw.get("paths", {})),
        solver=SolverConfig(**raw.get("solver", {})),
        initial_wip=initial_wip,
    )


def _normalize_initial_wip(raw_initial_wip: dict[str, Any]) -> dict[str, dict[int, int]]:
    normalized: dict[str, dict[int, int]] = {}
    for crop, by_age in raw_initial_wip.items():
        if not isinstance(by_age, dict):
            raise ValueError(f"initial_wip[{crop}] must be a dict of age->quantity.")
        normalized[crop] = {int(age): int(qty) for age, qty in by_age.items()}
    return normalized
