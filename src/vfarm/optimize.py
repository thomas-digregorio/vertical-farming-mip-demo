from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd
import pyomo.environ as pyo

from .config import AppConfig, load_config
from .utils import resolve_paths


@dataclass
class SolveResult:
    objective_value: float | None
    solver_status: str
    termination_condition: str
    feasible: bool
    outputs_dir: str


@dataclass
class OptimizationInputs:
    demand_lbs: dict[tuple[str, int], float]
    energy_price: dict[int, float]


def solve_optimization(
    config_path: str | Path, tee: bool = False, feasibility_only: bool = False
) -> SolveResult:
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)

    inputs = _load_inputs(config, paths.agg_dir)
    issues = run_feasibility_checks(config, inputs)

    if issues:
        raise RuntimeError(
            "Feasibility pre-check failed.\n"
            + "\n".join(f"- {issue}" for issue in issues)
        )

    if feasibility_only:
        return SolveResult(
            objective_value=None,
            solver_status="not_run",
            termination_condition="feasible_precheck_passed",
            feasible=True,
            outputs_dir=str(paths.outputs_dir),
        )

    model = build_model(config, inputs)

    solver = pyo.SolverFactory(config.solver.name)
    if solver is None or not solver.available(False):
        raise RuntimeError(
            "HiGHS solver is not available. Install with: pip install highspy pyomo"
        )

    results = solver.solve(model, tee=tee)
    solver_status = str(results.solver.status)
    termination = str(results.solver.termination_condition)

    optimal_conditions = {
        pyo.TerminationCondition.optimal,
        pyo.TerminationCondition.locallyOptimal,
    }
    if results.solver.termination_condition not in optimal_conditions:
        raise RuntimeError(
            "Optimization did not reach an optimal solution. "
            f"status={solver_status}, termination={termination}"
        )

    _write_solution_outputs(config, inputs, model, paths.outputs_dir)

    return SolveResult(
        objective_value=float(pyo.value(model.total_cost)),
        solver_status=solver_status,
        termination_condition=termination,
        feasible=True,
        outputs_dir=str(paths.outputs_dir),
    )


def _load_inputs(config: AppConfig, agg_dir: Path) -> OptimizationInputs:
    demand_df = pd.read_parquet(agg_dir / "demand_weekly.parquet")
    energy_df = pd.read_parquet(agg_dir / "energy_price_weekly.parquet")

    full_index = pd.MultiIndex.from_product(
        [config.crops, range(1, config.weeks + 1)], names=["crop", "week_id"]
    )
    demand_series = (
        demand_df.groupby(["crop", "week_id"], as_index=True)["demand_lbs"]
        .sum()
        .reindex(full_index, fill_value=0.0)
    )

    energy_series = (
        energy_df.groupby("week_id", as_index=True)["price_per_kwh"]
        .mean()
        .reindex(range(1, config.weeks + 1))
    )
    if energy_series.isna().any():
        fill_val = float(energy_series.mean()) if not energy_series.dropna().empty else 0.13
        energy_series = energy_series.fillna(fill_val)

    demand_lbs = {(str(crop), int(week)): float(val) for (crop, week), val in demand_series.items()}
    energy_price = {int(week): float(val) for week, val in energy_series.items()}

    return OptimizationInputs(demand_lbs=demand_lbs, energy_price=energy_price)


def run_feasibility_checks(config: AppConfig, inputs: OptimizationInputs) -> list[str]:
    issues: list[str] = []

    # If no initial WIP is given, demand before earliest harvest week is impossible.
    has_initial_wip = any(config.initial_wip.values())
    if not has_initial_wip:
        for crop in config.crops:
            g_min = config.crop_params[crop].g_min_weeks
            for week in range(1, g_min + 1):
                demand = inputs.demand_lbs[(crop, week)]
                if demand > 1e-6:
                    issues.append(
                        f"Demand for crop={crop}, week={week} is {demand:.2f} lbs but "
                        "no initial WIP is configured and harvest cannot occur that early."
                    )

    process_cap = float(config.capacity.base_process_capacity_trays_per_week)
    labor_cap = float(
        config.capacity.base_labor_hours_per_week + sum(config.capacity.overtime_block_hours)
    )
    shift_reduction = float(config.capacity.shift_labor_reduction_pct)

    for week in range(1, config.weeks + 1):
        required_trays = 0.0
        required_labor = 0.0

        for crop in config.crops:
            cp = config.crop_params[crop]
            demand = inputs.demand_lbs[(crop, week)]
            if cp.effective_yield_lbs <= 0:
                issues.append(f"Crop={crop} has non-positive effective yield.")
                continue

            trays = demand / cp.effective_yield_lbs
            required_trays += trays
            required_labor += trays * (
                cp.plant_hours_per_tray + cp.harvest_hours_per_tray * (1.0 - shift_reduction)
            )

        if required_trays > process_cap + 1e-6:
            issues.append(
                f"Week {week}: required harvested trays {required_trays:.1f} exceeds "
                f"process cap {process_cap:.1f}."
            )

        if required_labor > labor_cap + 1e-6:
            issues.append(
                f"Week {week}: required labor {required_labor:.1f} hours exceeds "
                f"max labor cap {labor_cap:.1f}."
            )

    return issues


def build_model(config: AppConfig, inputs: OptimizationInputs) -> pyo.ConcreteModel:
    model = pyo.ConcreteModel("vertical_farming_mip")

    crops = list(config.crops)
    weeks = list(range(1, config.weeks + 1))
    ages = list(range(0, config.max_age_weeks + 1))
    blocks = list(range(1, len(config.capacity.overtime_block_hours) + 1))

    g_min = {crop: config.crop_params[crop].g_min_weeks for crop in crops}
    g_max = {crop: config.crop_params[crop].g_max_weeks for crop in crops}
    eff_yield = {crop: config.crop_params[crop].effective_yield_lbs for crop in crops}
    plant_hours = {crop: config.crop_params[crop].plant_hours_per_tray for crop in crops}
    harvest_hours = {crop: config.crop_params[crop].harvest_hours_per_tray for crop in crops}
    seed_cost = {crop: config.crop_params[crop].seed_cost_per_tray for crop in crops}
    nutrient_cost = {
        crop: config.crop_params[crop].nutrient_cost_per_tray_week for crop in crops
    }
    energy_kwh = {crop: config.crop_params[crop].energy_kwh_per_tray_week for crop in crops}

    initial_wip = config.initial_wip
    rack_capacity = float(config.capacity.rack_capacity_trays_per_week)
    process_cap = float(config.capacity.base_process_capacity_trays_per_week)
    base_labor = float(config.capacity.base_labor_hours_per_week)
    shift_reduction = float(config.capacity.shift_labor_reduction_pct)
    ot_block_hours = {
        b: float(config.capacity.overtime_block_hours[b - 1]) for b in blocks
    }

    model.C = pyo.Set(initialize=crops, ordered=True)
    model.T = pyo.Set(initialize=weeks, ordered=True)
    model.A = pyo.Set(initialize=ages, ordered=True)
    model.B = pyo.Set(initialize=blocks, ordered=True)

    model.x = pyo.Var(model.C, model.T, domain=pyo.NonNegativeIntegers)
    model.h = pyo.Var(model.C, model.T, model.A, domain=pyo.NonNegativeIntegers)
    model.inv_wip = pyo.Var(model.C, model.T, model.A, domain=pyo.NonNegativeIntegers)
    model.waste_lbs = pyo.Var(model.C, model.T, domain=pyo.NonNegativeReals)
    model.yOT = pyo.Var(model.B, model.T, domain=pyo.Binary)
    model.ot_hours = pyo.Var(model.T, domain=pyo.NonNegativeReals)
    model.yShift = pyo.Var(model.T, domain=pyo.Binary)
    model.h_shift = pyo.Var(model.C, model.T, model.A, domain=pyo.NonNegativeIntegers)

    def inv_age_limit_rule(m: pyo.ConcreteModel, c: str, t: int, a: int):
        if a > g_max[c]:
            return m.inv_wip[c, t, a] == 0
        return pyo.Constraint.Skip

    model.inv_age_limit = pyo.Constraint(model.C, model.T, model.A, rule=inv_age_limit_rule)

    def harvest_window_rule(m: pyo.ConcreteModel, c: str, t: int, a: int):
        if a < g_min[c] or a > g_max[c]:
            return m.h[c, t, a] == 0
        return pyo.Constraint.Skip

    model.harvest_window = pyo.Constraint(model.C, model.T, model.A, rule=harvest_window_rule)

    def inv_dynamics_rule(m: pyo.ConcreteModel, c: str, t: int, a: int):
        if a > g_max[c]:
            return pyo.Constraint.Skip

        if a == 0:
            return m.inv_wip[c, t, 0] == m.x[c, t]

        source = (
            int(initial_wip.get(c, {}).get(a - 1, 0))
            if t == 1
            else m.inv_wip[c, t - 1, a - 1]
        )

        if g_min[c] <= a <= g_max[c]:
            return m.inv_wip[c, t, a] == source - m.h[c, t, a]
        return m.inv_wip[c, t, a] == source

    model.inv_dynamics = pyo.Constraint(model.C, model.T, model.A, rule=inv_dynamics_rule)

    def harvest_available_rule(m: pyo.ConcreteModel, c: str, t: int, a: int):
        if a < g_min[c] or a > g_max[c]:
            return pyo.Constraint.Skip

        source = (
            int(initial_wip.get(c, {}).get(a - 1, 0))
            if t == 1
            else m.inv_wip[c, t - 1, a - 1]
        )
        return m.h[c, t, a] <= source

    model.harvest_available = pyo.Constraint(model.C, model.T, model.A, rule=harvest_available_rule)

    def force_harvest_at_max_age_rule(m: pyo.ConcreteModel, c: str, t: int):
        return m.inv_wip[c, t, g_max[c]] == 0

    model.force_harvest_at_max_age = pyo.Constraint(
        model.C, model.T, rule=force_harvest_at_max_age_rule
    )

    def demand_balance_rule(m: pyo.ConcreteModel, c: str, t: int):
        produced = eff_yield[c] * sum(m.h[c, t, a] for a in ages if g_min[c] <= a <= g_max[c])
        demand = inputs.demand_lbs[(c, t)]
        return produced == demand + m.waste_lbs[c, t]

    model.demand_balance = pyo.Constraint(model.C, model.T, rule=demand_balance_rule)

    def rack_capacity_rule(m: pyo.ConcreteModel, t: int):
        return (
            sum(m.inv_wip[c, t, a] for c in crops for a in ages if a <= g_max[c])
            <= rack_capacity
        )

    model.rack_capacity = pyo.Constraint(model.T, rule=rack_capacity_rule)

    def process_capacity_rule(m: pyo.ConcreteModel, t: int):
        return sum(m.h[c, t, a] for c in crops for a in ages) <= process_cap

    model.process_capacity = pyo.Constraint(model.T, rule=process_capacity_rule)

    def shift_link_to_harvest_rule(m: pyo.ConcreteModel, c: str, t: int, a: int):
        return m.h_shift[c, t, a] <= m.h[c, t, a]

    model.shift_link_to_harvest = pyo.Constraint(
        model.C, model.T, model.A, rule=shift_link_to_harvest_rule
    )

    def shift_activation_rule(m: pyo.ConcreteModel, t: int):
        return sum(m.h_shift[c, t, a] for c in crops for a in ages) <= process_cap * m.yShift[t]

    model.shift_activation = pyo.Constraint(model.T, rule=shift_activation_rule)

    def labor_used_rule(m: pyo.ConcreteModel, t: int):
        plant_component = sum(plant_hours[c] * m.x[c, t] for c in crops)
        harvest_component = sum(harvest_hours[c] * m.h[c, t, a] for c in crops for a in ages)
        reduction_component = shift_reduction * sum(
            harvest_hours[c] * m.h_shift[c, t, a] for c in crops for a in ages
        )
        return plant_component + harvest_component - reduction_component

    model.labor_used = pyo.Expression(model.T, rule=labor_used_rule)

    def labor_capacity_rule(m: pyo.ConcreteModel, t: int):
        return m.labor_used[t] <= base_labor + m.ot_hours[t]

    model.labor_capacity = pyo.Constraint(model.T, rule=labor_capacity_rule)

    if blocks:

        def overtime_hours_rule(m: pyo.ConcreteModel, t: int):
            return m.ot_hours[t] <= sum(ot_block_hours[b] * m.yOT[b, t] for b in blocks)

        model.overtime_hours = pyo.Constraint(model.T, rule=overtime_hours_rule)

        ordering_pairs = [(b, t) for b in blocks[1:] for t in weeks]
        model.ot_order_pairs = pyo.Set(initialize=ordering_pairs, dimen=2)

        def overtime_order_rule(m: pyo.ConcreteModel, b: int, t: int):
            return m.yOT[b, t] <= m.yOT[b - 1, t]

        model.overtime_order = pyo.Constraint(model.ot_order_pairs, rule=overtime_order_rule)
    else:

        def overtime_hours_zero_rule(m: pyo.ConcreteModel, t: int):
            return m.ot_hours[t] == 0

        model.overtime_hours = pyo.Constraint(model.T, rule=overtime_hours_zero_rule)

    model.cost_seed = pyo.Expression(
        expr=sum(seed_cost[c] * model.x[c, t] for c in crops for t in weeks)
    )
    model.cost_nutrients = pyo.Expression(
        expr=sum(
            nutrient_cost[c] * model.inv_wip[c, t, a]
            for c in crops
            for t in weeks
            for a in ages
            if a <= g_max[c]
        )
    )
    model.cost_energy = pyo.Expression(
        expr=sum(
            inputs.energy_price[t]
            * sum(
                energy_kwh[c]
                * sum(model.inv_wip[c, t, a] for a in ages if a <= g_max[c])
                for c in crops
            )
            for t in weeks
        )
    )
    model.cost_labor_base = pyo.Expression(
        expr=config.capacity.base_wage_per_hour * sum(model.labor_used[t] for t in weeks)
    )
    model.cost_overtime_premium = pyo.Expression(
        expr=config.capacity.overtime_premium_per_hour * sum(model.ot_hours[t] for t in weeks)
    )
    model.cost_shift = pyo.Expression(
        expr=config.capacity.shift_fixed_cost_per_week * sum(model.yShift[t] for t in weeks)
    )
    model.cost_disposal = pyo.Expression(
        expr=config.costs.disposal_cost_per_lb_waste
        * sum(model.waste_lbs[c, t] for c in crops for t in weeks)
    )

    model.total_cost = pyo.Objective(
        expr=model.cost_seed
        + model.cost_nutrients
        + model.cost_energy
        + model.cost_labor_base
        + model.cost_overtime_premium
        + model.cost_shift
        + model.cost_disposal,
        sense=pyo.minimize,
    )

    return model


def _write_solution_outputs(
    config: AppConfig,
    inputs: OptimizationInputs,
    model: pyo.ConcreteModel,
    outputs_dir: Path,
) -> None:
    crops = list(config.crops)
    weeks = list(range(1, config.weeks + 1))
    ages = list(range(0, config.max_age_weeks + 1))
    blocks = list(range(1, len(config.capacity.overtime_block_hours) + 1))

    g_min = {crop: config.crop_params[crop].g_min_weeks for crop in crops}
    g_max = {crop: config.crop_params[crop].g_max_weeks for crop in crops}
    eff_yield = {crop: config.crop_params[crop].effective_yield_lbs for crop in crops}

    planting_rows: list[dict[str, object]] = []
    harvest_rows: list[dict[str, object]] = []
    utilization_rows: list[dict[str, object]] = []

    for crop in crops:
        for week in weeks:
            planting_rows.append(
                {
                    "crop": crop,
                    "week_id": week,
                    "trays_planted": int(round(pyo.value(model.x[crop, week]))),
                }
            )

            trays_harvested = sum(
                pyo.value(model.h[crop, week, age])
                for age in ages
                if g_min[crop] <= age <= g_max[crop]
            )
            produced_lbs = eff_yield[crop] * trays_harvested

            harvest_rows.append(
                {
                    "crop": crop,
                    "week_id": week,
                    "trays_harvested": int(round(trays_harvested)),
                    "produced_lbs": float(produced_lbs),
                    "demand_lbs": float(inputs.demand_lbs[(crop, week)]),
                    "waste_lbs": float(pyo.value(model.waste_lbs[crop, week])),
                }
            )

    for week in weeks:
        rack_used = sum(
            pyo.value(model.inv_wip[crop, week, age])
            for crop in crops
            for age in ages
            if age <= g_max[crop]
        )
        process_used = sum(
            pyo.value(model.h[crop, week, age]) for crop in crops for age in ages
        )
        labor_used = float(pyo.value(model.labor_used[week]))
        ot_hours = float(pyo.value(model.ot_hours[week]))
        labor_capacity = float(config.capacity.base_labor_hours_per_week + ot_hours)
        energy_cost_week = inputs.energy_price[week] * sum(
            config.crop_params[crop].energy_kwh_per_tray_week
            * sum(
                pyo.value(model.inv_wip[crop, week, age])
                for age in ages
                if age <= g_max[crop]
            )
            for crop in crops
        )

        utilization_rows.append(
            {
                "week_id": week,
                "rack_used_trays": float(rack_used),
                "rack_capacity_trays": float(config.capacity.rack_capacity_trays_per_week),
                "rack_utilization": float(
                    rack_used / max(config.capacity.rack_capacity_trays_per_week, 1)
                ),
                "labor_used_hours": labor_used,
                "labor_capacity_hours": labor_capacity,
                "labor_utilization": labor_used / max(labor_capacity, 1e-6),
                "process_used_trays": float(process_used),
                "process_capacity_trays": float(config.capacity.base_process_capacity_trays_per_week),
                "process_utilization": float(
                    process_used / max(config.capacity.base_process_capacity_trays_per_week, 1)
                ),
                "overtime_hours": ot_hours,
                "overtime_blocks_enabled": int(
                    sum(round(pyo.value(model.yOT[b, week])) for b in blocks)
                ),
                "shift_on": int(round(pyo.value(model.yShift[week]))),
                "energy_cost": float(energy_cost_week),
            }
        )

    pd.DataFrame(planting_rows).to_csv(outputs_dir / "solution_planting.csv", index=False)
    pd.DataFrame(harvest_rows).to_csv(outputs_dir / "solution_harvest.csv", index=False)
    pd.DataFrame(utilization_rows).to_csv(outputs_dir / "utilization_weekly.csv", index=False)

    cost_breakdown = {
        "seed_cost": float(pyo.value(model.cost_seed)),
        "nutrient_cost": float(pyo.value(model.cost_nutrients)),
        "energy_cost": float(pyo.value(model.cost_energy)),
        "labor_base_cost": float(pyo.value(model.cost_labor_base)),
        "overtime_premium_cost": float(pyo.value(model.cost_overtime_premium)),
        "shift_fixed_cost": float(pyo.value(model.cost_shift)),
        "disposal_cost": float(pyo.value(model.cost_disposal)),
        "total_cost": float(pyo.value(model.total_cost)),
    }

    with (outputs_dir / "cost_breakdown.json").open("w", encoding="utf-8") as f:
        json.dump(cost_breakdown, f, indent=2)

    with (outputs_dir / "solve_summary.json").open("w", encoding="utf-8") as f:
        json.dump(asdict(SolveResult(
            objective_value=float(pyo.value(model.total_cost)),
            solver_status="ok",
            termination_condition="optimal",
            feasible=True,
            outputs_dir=str(outputs_dir),
        )), f, indent=2)
