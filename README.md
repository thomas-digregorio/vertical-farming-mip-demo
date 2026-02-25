# Vertical Farming MIP Demo (Spark + Pyomo + HiGHS + Streamlit + GCP)

Synthetic indoor vertical-farm planning demo over a weekly horizon.

This repo supports:
- Local run: generate synthetic data, aggregate, optimize, report.
- GCP run: Dataproc Serverless Spark jobs for generation/aggregation.
- Interactive dashboard: tweak config, run workflows, inspect outputs, show tech stack.

## Core Optimization Features

- Weekly horizon (default 12)
- 6 crops (lbs output)
- Harvest windows (`g_min..g_max`)
- Integer planting/harvest decisions
- Age-indexed WIP tray dynamics
- Rack/labor/process constraints
- Overtime blocks (binary) + per-hour premium
- Shift binary that reduces harvest labor (no process capacity increase)
- Energy cost proportional to WIP
- Hard demand through balance: `produced = demand + waste` (no slack variables)

## Requirements

- Python 3.11+
- Java runtime for local Spark (optional; code falls back to pandas parquet path if Java is unavailable)
- `gcloud` CLI for GCP commands

Install Python deps:

```bash
pip install -r requirements.txt
```

## Project Layout

```text
.
├── configs/
│   └── base.yaml
├── jobs/
│   ├── agg_data_job.py
│   ├── gen_data_job.py
│   └── run_all_spark_job.py
├── src/vfarm/
│   ├── cli.py
│   ├── config.py
│   ├── dashboard_app.py
│   ├── gcp.py
│   ├── optimize.py
│   ├── report.py
│   ├── spark_aggregate.py
│   ├── spark_generate.py
│   └── utils.py
├── dashboard.py
├── tests/
├── data/         # gitignored generated parquet
├── outputs/      # gitignored outputs/reports/manifests
└── requirements.txt
```

## Configuration

Main config is `configs/base.yaml`.

Important knobs:
- Scale: `weeks`, `crops`, `data_mode`
- Capacities and costs under `capacity`
- Crop windows/yields under `crop_params`
- Demand and energy generation under `demand`, `energy_price`
- GCP under `gcp`:
  - `project_id`, `region`, `bucket`, `bucket_location`
  - `dataproc_runtime_version`
  - `budget_usd` and budget metadata

## Local CLI Usage

```bash
PYTHONPATH=src python -m vfarm.cli gen-data --config configs/base.yaml
PYTHONPATH=src python -m vfarm.cli agg-data --config configs/base.yaml
PYTHONPATH=src python -m vfarm.cli solve --config configs/base.yaml
PYTHONPATH=src python -m vfarm.cli report --config configs/base.yaml
PYTHONPATH=src python -m vfarm.cli run-all --config configs/base.yaml
```

Optional flags:
- `solve --tee`
- `solve --feasibility-only`
- `report --no-plots`

## Streamlit Dashboard

Run:

```bash
PYTHONPATH=src streamlit run dashboard.py
```

Dashboard tabs:
- `Config`: interactive YAML parameter editing
- `Run Local`: run `gen/agg/solve/report/run-all`
- `Run GCP`: setup/upload/submit/status for Dataproc
- `Results`: cost metrics, utilization charts, schedule tables
- `Tech Stack`: architecture + commands for demo presentation

## GCP Rollout Commands

Make sure auth is configured first:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project vertical-farming-mip-demo
```

Then from this repo:

```bash
PYTHONPATH=src python -m vfarm.cli gcp-setup --config configs/base.yaml
PYTHONPATH=src python -m vfarm.cli gcp-upload --config configs/base.yaml
PYTHONPATH=src python -m vfarm.cli gcp-submit-gen --config configs/base.yaml --wait
PYTHONPATH=src python -m vfarm.cli gcp-submit-agg --config configs/base.yaml --wait
# preferred single-batch path:
PYTHONPATH=src python -m vfarm.cli gcp-submit-run-all --config configs/base.yaml --wait
```

Check a batch:

```bash
PYTHONPATH=src python -m vfarm.cli gcp-batch-status --config configs/base.yaml --batch-id <BATCH_ID>
```

What `gcp-setup` does:
- Enables required APIs
- Ensures target GCS bucket exists
- Creates project budget (default `$50`) if enabled and not already present

## Outputs

Local outputs are in `outputs/`:
- `solution_planting.csv`
- `solution_harvest.csv`
- `utilization_weekly.csv`
- `cost_breakdown.json`
- `report_summary.json`
- `report_summary.md`
- plot PNGs
- `gcp_artifacts.json` (GCS artifact URIs for Dataproc jobs)

## Tests

```bash
PYTHONPATH=src pytest -q
```

Current tests validate:
- data generation files/columns/sanity
- default config solve feasibility and demand satisfaction
- objective monotonicity with increased rack capacity

## Demo Storyline (for Portfolio)

1. Open Streamlit dashboard.
2. Show config tweaks (capacity, demand, costs).
3. Run local optimization and review utilization/cost impacts.
4. Trigger GCP setup/upload and Dataproc submissions.
5. Explain stack split: Spark on Dataproc, optimization with Pyomo/HiGHS.
