# Vertical Farming MIP Demo (Spark + Pyomo + HiGHS)

Synthetic indoor vertical farm planning demo over a weekly horizon. The project:

- Generates realistic synthetic daily telemetry/demand/price data with local PySpark.
- Aggregates to weekly optimization input tables.
- Solves a mixed-integer planning model in Pyomo with HiGHS.
- Produces solution CSV/JSON outputs and a simple report with plots.

## What The Optimization Includes

- Weekly horizon (default 12 weeks)
- 6 leafy green crops, all output in lbs
- Hard demand via production balance: `produced_lbs = demand_lbs + waste_lbs`
- Harvest window per crop: `g_min..g_max` weeks after planting
- Integer planting and harvest decisions
- Age-indexed WIP inventory dynamics
- Rack capacity on total WIP trays
- Labor capacity with binary overtime blocks + per-hour overtime premium
- Binary shift variable (`yShift[t]`) that reduces harvest-processing labor per tray (no extra processing capacity)
- Weekly energy cost proportional to WIP trays

## Requirements

- Python 3.11+
- Java installed (for local Spark)

Install dependencies:

```bash
pip install -r requirements.txt
```

## Project Layout

```text
.
├── configs/
│   └── base.yaml
├── src/vfarm/
│   ├── __init__.py
│   ├── __main__.py
│   ├── cli.py
│   ├── config.py
│   ├── optimize.py
│   ├── report.py
│   ├── spark_aggregate.py
│   ├── spark_generate.py
│   └── utils.py
├── tests/
│   ├── conftest.py
│   └── test_pipeline.py
├── data/                  # generated parquet (gitignored)
├── outputs/               # solver/report outputs (gitignored)
├── requirements.txt
└── README.md
```

## Configuration

Primary config is YAML: `configs/base.yaml`.

Main knobs:

- `weeks`, `crops`
- `data_mode`: `rows` (default) or `counts`
- Growth windows: `g_min_weeks`, `g_max_weeks`
- Yield/shrink/crop labor/energy/cost parameters
- Capacities: rack, labor, process
- Overtime blocks/hours/premium
- Shift fixed cost and labor reduction percentage
- Demand/energy synthetic process parameters
- Seed for reproducibility

## Run Commands

Because source is under `src/`, use `PYTHONPATH=src`:

```bash
PYTHONPATH=src python -m vfarm.cli gen-data --config configs/base.yaml
PYTHONPATH=src python -m vfarm.cli agg-data --config configs/base.yaml
PYTHONPATH=src python -m vfarm.cli solve --config configs/base.yaml
PYTHONPATH=src python -m vfarm.cli report --config configs/base.yaml
```

Or run everything:

```bash
PYTHONPATH=src python -m vfarm.cli run-all --config configs/base.yaml
```

Optional flags:

- Solver log streaming: `solve --tee`
- Feasibility check only (no solve): `solve --feasibility-only`
- Skip plots in report: `report --no-plots`

## Generated Data (Raw)

Written to `data/raw/` parquet:

- `telemetry_tray_daily.parquet`
- `demand_daily.parquet`
- `prices_energy_daily.parquet`
- `crop_params.parquet`

## Aggregated Inputs (Weekly)

Written to `data/agg/` parquet (and convenience CSV):

- `demand_weekly`
- `energy_price_weekly`
- `crop_params`
- `telemetry_weekly`

## Optimization Outputs

Written to `outputs/`:

- `solution_planting.csv`
- `solution_harvest.csv`
- `utilization_weekly.csv`
- `cost_breakdown.json`
- `solve_summary.json`
- `report_summary.json`
- `report_summary.md`
- `utilization_weekly.png`
- `production_vs_demand.png`

## Tests

Run:

```bash
PYTHONPATH=src pytest -q
```

Tests cover:

- Data generation outputs and required columns
- Default config solves and satisfies demand
- Objective is non-increasing when rack capacity is increased (same demand data)

## Feasibility Diagnostics

`solve` runs a pre-check before the MIP solve. It reports clear diagnostics for common infeasibility causes:

- Early demand before harvest can be possible (when no initial WIP)
- Weekly demand requiring more trays than process capacity
- Weekly demand implying labor beyond max labor (base + overtime blocks)

No slack variables are used in the model.

## Scaling Guidance

To scale up later, increase:

- `weeks`
- number of crops
- demand magnitude/utilization target
- row-granularity telemetry (`data_mode: rows`)

Typical laptop-fast defaults are in `configs/base.yaml`. You can tune capacities and scenario sizes there.

## Dataproc / GCP Note

Spark generation and aggregation can be moved to Dataproc later:

1. Upload project code/config to GCS.
2. Submit PySpark job(s) (`spark_generate.py`, `spark_aggregate.py`) on Dataproc.
3. Write parquet outputs to GCS.
4. Run optimization (`optimize.py`) on a small VM or Cloud Run job that reads aggregated parquet and writes outputs.

This repo does not implement cloud deployment directly; it documents the path and keeps Spark/optimization decoupled for that transition.
