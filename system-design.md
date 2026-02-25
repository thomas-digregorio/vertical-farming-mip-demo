You are Codex CLI. Build a complete, runnable Python project (local Spark + Pyomo + HiGHS) for a synthetic indoor vertical farming operations optimization demo.



GOAL

Create a vertical-farm planning MIP over a 12-week horizon that minimizes total cost while meeting hard demand (no unmet demand allowed). Use PySpark locally to generate realistic synthetic data and aggregate it into coefficient tables. Then build a Pyomo MIP solved with HiGHS. Make it “laptop fast” by default, but fully configurable to scale up later (more weeks, more crops, more scenarios, finer granularity, more candidate facilities, etc.).



KEY REQUIREMENTS (MUST)

1\) Time granularity: weekly; horizon = 12 weeks by default.

2\) Crops: 6 leafy greens; output units = pounds (lbs) only (no package types).

3\) Demand: one aggregate demand time series per crop per week. Demand must be satisfied exactly (hard constraint). If infeasible, solver should report infeasibility with helpful diagnostics (and the project should include a feasibility-check mode and/or slack-adding debug option).

4\) Harvest window: harvest can occur in a window \[g\_min\[c], g\_max\[c]] weeks after planting (not fixed age).

5\) Constraints to include: space/rack capacity (WIP trays occupying racks while growing), labor hours, and energy cost.

6\) Energy modeling: weekly energy cost proportional to WIP trays (no time-of-use).

7\) Integer/binary structure: 

&nbsp;  - integer tray planting decisions

&nbsp;  - binary overtime blocks (piecewise labor capacity/cost)

&nbsp;  - binary packaging shift on/off (even though output is lbs; interpret as “processing/harvest-pack line shift” enabling extra throughput or adding fixed cost; keep simple but meaningful)

8\) Solver: HiGHS. Use Pyomo with HiGHS; ensure it runs with pip install. Provide clear solver install notes if needed.

9\) Reproducibility: deterministic random seed; config file controlling all scales.

10\) Deliverables: 

&nbsp;   - data generation pipeline (PySpark)

&nbsp;   - optimization module (Pyomo)

&nbsp;   - run scripts/CLI

&nbsp;   - write outputs to parquet/csv and produce a small report (cost breakdown, utilization, planting/harvest schedule).

11\) Tests: include lightweight unit tests (pytest) that validate:

&nbsp;   - generated data dimensions and sanity ranges

&nbsp;   - feasibility for default config

&nbsp;   - objective decreases when increasing capacities (basic monotonicity sanity check)

12\) Documentation: README with step-by-step instructions to run locally; include scaling knobs and how to deploy Spark part later to GCP Dataproc.



PROJECT STRUCTURE

Create a repo with:

\- pyproject.toml (or requirements.txt) + README.md

\- src/vfarm/

&nbsp;   - config.py (pydantic or dataclasses)

&nbsp;   - spark\_generate.py (PySpark data generator)

&nbsp;   - spark\_aggregate.py (PySpark aggregator to optimization inputs)

&nbsp;   - optimize.py (Pyomo model builder + solver)

&nbsp;   - report.py (summary tables + optional matplotlib plots)

&nbsp;   - cli.py (typer or argparse entrypoints)

&nbsp;   - utils.py

\- tests/

\- data/ (gitignored)

\- outputs/ (gitignored)



CONFIG (DEFAULTS)

\- weeks: 12

\- crops: \["arugula","basil","kale","romaine","spinach","mixed\_greens"]

\- trays are discrete; each tray yields lbs at harvest.

\- growth window per crop: specify g\_min and g\_max in weeks (e.g., arugula 3-4, basil 4-5, kale 5-6, romaine 5-6, spinach 4-5, mixed 3-4)

\- yield\_lbs\_per\_tray\[c]: realistic ranges (e.g., 0.8–2.5 lbs depending on crop) configurable.

\- shrink/spoilage factor per crop applied at harvest (e.g., 2–8%) configurable.

\- rack\_capacity\_trays\_per\_week: e.g., 1200 trays (laptop fast) configurable.

\- labor requirements (hours):

&nbsp;   - plant\_hours\_per\_tray\[c]

&nbsp;   - harvest\_process\_hours\_per\_tray\[c]

\- base labor hours per week: e.g., 220 hours

\- overtime blocks: e.g., 0/1/2 blocks, each adds +40 hours with increasing cost

\- packaging/processing shift binary: if on, adds extra processing capacity or reduces processing hours (pick a clean formulation). Include fixed cost per week when shift=1.

\- processing capacity: optionally cap harvest/processing trays per week; increased if shift is on.

\- energy\_kwh\_per\_tray\_week: e.g., 2.5–5.5 kWh per tray-week (configurable)

\- energy\_price\_per\_kwh\[t]: synthetic weekly price series with small noise (configurable)

\- other costs:

&nbsp;   - seed\_cost\_per\_tray\[c]

&nbsp;   - nutrient\_cost\_per\_tray\_week\[c] (or per tray planted)

&nbsp;   - disposal\_cost\_per\_lb\_waste

&nbsp;   - overtime wage multiplier or block-specific cost

\- initial conditions: allow initial WIP trays by crop and their ages (default none for simplicity, but include the feature)



DATA GENERATION (SPARK)

Use PySpark locally (SparkSession in local\[\*] mode). Generate “realistic synthetic telemetry” tables at DAILY or HOURLY resolution, then aggregate to weekly.

Tables to generate (write to data/ as parquet):

1\) telemetry\_tray\_daily:

&nbsp;  - date, week\_id, crop, tray\_id (or batch\_id), stage\_age\_days, kwh\_used, is\_active

&nbsp;  Keep it simple: you can simulate WIP trays counts rather than per-tray rows by default for laptop fast. BUT add a config flag:

&nbsp;  - mode="counts" generates aggregated counts per crop/day

&nbsp;  - mode="rows" generates per-tray rows (bigger, for Spark demo later)

2\) demand\_daily:

&nbsp;  - date, week\_id, crop, demand\_lbs

&nbsp;  Generate demand with seasonality + noise; ensure it is feasible given default capacities.

3\) prices\_energy\_daily:

&nbsp;  - date, week\_id, energy\_price\_per\_kwh

4\) crop\_params:

&nbsp;  - crop, g\_min\_weeks, g\_max\_weeks, yield\_lbs\_per\_tray, shrink, plant\_hours\_per\_tray, harvest\_hours\_per\_tray, seed\_cost, nutrient\_cost\_per\_tray\_week, energy\_kwh\_per\_tray\_week



Then aggregate to weekly optimization inputs (write parquet):

\- demand\_weekly(crop, week\_id, demand\_lbs)

\- energy\_price\_weekly(week\_id, price\_per\_kwh)

\- crop\_params (as above)

Optionally produce scenario tables (future extension) but keep default single-scenario.



OPTIMIZATION MODEL (PYOMO MIP)

Decision variables:

\- x\[c,t] ∈ Z\_+ : trays planted at week t

\- h\[c,t,age] ∈ Z\_+ : trays harvested at week t that were planted age weeks ago, where age ∈ \[g\_min\[c], g\_max\[c]]

&nbsp; (This is the key to harvest window.)

\- inv\_wip\[c,t,age] ∈ Z\_+ : trays of crop c at week t with age=age (age 0..g\_max)

\- yOT\[b,t] ∈ {0,1}: overtime block b used in week t (b=1..B)

\- yShift\[t] ∈ {0,1}: processing shift on in week t



Core constraints:

1\) WIP aging dynamics (inventory-by-age):

&nbsp;  - inv\_wip\[c,t,0] = x\[c,t]

&nbsp;  - inv\_wip\[c,t,age] = inv\_wip\[c,t-1,age-1] - harvest\_from\_that\_age

&nbsp;    where harvest\_from\_that\_age is h\[c,t,age] for allowed ages; for ages not allowed, harvest=0.

&nbsp;  Use t indexing carefully; handle t=1 base case with 0 initial WIP unless config provides it.

2\) Harvest window:

&nbsp;  - h\[c,t,age] can be >0 only if age in \[g\_min, g\_max]

3\) Cannot harvest more than available:

&nbsp;  - h\[c,t,age] ≤ inv\_wip\[c,t,age]  (or inv\_wip from prior before subtract; ensure consistent)

4\) Demand satisfaction (hard):

&nbsp;  For each crop c, week t:

&nbsp;  - (1 - shrink\[c]) \* yield\[c] \* sum\_{age in window} h\[c,t,age]  >= demand\[c,t]

&nbsp;  Use equality if you want to prevent overproduction; otherwise allow >= with disposal variable.

&nbsp;  Add disposal variable waste\_lbs\[c,t] >= 0 and enforce:

&nbsp;  - produced\_lbs = demand + waste

&nbsp;  so model can overproduce but pays disposal cost.

&nbsp;  Because demand is hard, keep produced >= demand always.

5\) Rack capacity (space) as total WIP trays in system:

&nbsp;  - sum\_{c,age} inv\_wip\[c,t,age] ≤ rack\_capacity\[t]

6\) Labor capacity:

&nbsp;  - labor\_used\[t] = sum\_c plant\_hours\[c]\*x\[c,t] + sum\_{c,age} harvest\_hours\[c]\*h\[c,t,age]

&nbsp;  - labor\_used\[t] ≤ base\_labor\_hours\[t] + sum\_b add\_hours\[b]\*yOT\[b,t]

&nbsp;  Also include overtime costs with increasing marginal cost; enforce ordering:

&nbsp;  - yOT\[2,t] ≤ yOT\[1,t], yOT\[3,t] ≤ yOT\[2,t], etc.

7\) Processing/pack shift constraint:

&nbsp;  Introduce a weekly processing capacity in trays harvested:

&nbsp;  - sum\_{c,age} h\[c,t,age] ≤ base\_process\_cap\[t] + shift\_extra\_cap \* yShift\[t]

&nbsp;  And include fixed\_cost\_shift\_per\_week \* yShift\[t] in objective.

8\) Energy cost proportional to WIP trays:

&nbsp;  - wip\_total\_tray\_weeks\[t] = sum\_{c,age} inv\_wip\[c,t,age]

&nbsp;  - energy\_cost\[t] = energy\_price\[t] \* sum\_c energy\_kwh\_per\_tray\_week\[c] \* sum\_age inv\_wip\[c,t,age]

&nbsp;  (If you want crop-specific intensity, include inside sum.)

9\) Optional: initial WIP:

&nbsp;  - allow config initial\_wip\[c,age] at t=1; incorporate into inv\_wip for t=1 base.



Objective: Minimize total cost:

\- seeds: sum\_{c,t} seed\_cost\[c]\*x\[c,t]

\- nutrients: sum\_{c,t,age} nutrient\_cost\[c]\*inv\_wip\[c,t,age]

\- labor base: base\_wage \* labor\_used\_base + overtime block costs (either per hour or per block)

&nbsp; Simpler: include cost\_per\_ot\_block\[b]\*yOT\[b,t] + base\_wage\_per\_hour \* labor\_used\[t]

\- shift fixed cost: shift\_cost\[t]\*yShift\[t]

\- energy: as above

\- disposal waste cost: disposal\_cost\_per\_lb \* waste\_lbs\[c,t]



IMPORTANT: Ensure model is feasible under default config. The generator must tune demand to be within capacity, or provide an automatic “scale demand down” step to ensure feasibility when seed is random.



SOLVER INTEGRATION

\- Use Pyomo SolverFactory with highs.

\- If highs is unavailable, print actionable instructions to install it.

\- Add a CLI flag --tee to show solver output.

\- Provide warm-start off by default.



CLI COMMANDS

Implement `python -m vfarm.cli ...` or console script `vfarm` with commands:

\- vfarm gen-data --config configs/base.yaml

\- vfarm agg-data --config ...

\- vfarm solve --config ...

\- vfarm report --config ...

\- vfarm run-all --config ...



OUTPUTS

Write:

\- outputs/solution\_planting.csv (x)

\- outputs/solution\_harvest.csv (h aggregated to lbs)

\- outputs/utilization\_weekly.csv (rack usage, labor usage, process usage, energy cost)

\- outputs/cost\_breakdown.json

\- Optional plots (matplotlib): rack utilization, labor utilization, production vs demand



SCALING KNOBS (document + implement)

\- weeks, crops count

\- data mode: counts vs rows

\- number of demand points/scenarios (future)

\- rack capacity, labor capacity, processing cap

\- restrict harvest window widths

\- keep only top N ages, etc.



TESTS

\- Test that gen-data produces required parquet files with expected columns.

\- Test that default config solves to optimality and meets demand within tolerance.

\- Test that increasing rack capacity decreases or does not increase objective (monotonicity sanity check).



GCP NOTE (README)

Explain that Spark job can be run on GCP Dataproc by:

\- uploading code to GCS

\- submitting PySpark job

\- writing parquet back to GCS

Optimization can run on a small VM or Cloud Run job reading aggregated parquet (but do not implement deployment, just explain).



IMPLEMENTATION DETAILS

\- Use Python 3.11+.

\- Use pyspark, pyomo, pandas, numpy, pydantic (optional), typer (optional), matplotlib (optional), pytest.

\- Keep code clean, typed, and well-commented.

\- Avoid heavy dependencies beyond these.

\- Ensure everything runs with simple local commands and produces outputs.



NOW DO THE WORK

1\) Create all files.

2\) Implement data generation + aggregation in Spark.

3\) Implement Pyomo model exactly with harvest window using age-indexed WIP.

4\) Implement CLI, report, tests, and README.

5\) Run unit tests and provide any fixes to ensure passing.



Return the final repository tree and the commands to run it.

