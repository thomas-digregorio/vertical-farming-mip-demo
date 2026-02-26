from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import traceback
from typing import Any

import pandas as pd
import streamlit as st
import yaml

from .config import AppConfig, load_config
from .gcp import describe_batch, setup_gcp_resources, submit_gcp_batch, submit_gcp_run_all, upload_gcp_artifacts
from .optimize import solve_optimization
from .report import generate_report
from .spark_aggregate import aggregate_data
from .spark_generate import generate_data
from .utils import resolve_paths


DEFAULT_CONFIG_PATH = Path("configs/base.yaml")


def run_dashboard() -> None:
    st.set_page_config(
        page_title="Vertical Farming MIP Demo",
        page_icon="🌱",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    _init_state()

    with st.sidebar:
        st.header("Config")
        config_path_input = st.text_input(
            "Config path",
            value=st.session_state["config_path"],
            help="Path to a YAML config file",
        )
        st.session_state["config_path"] = config_path_input

        col_a, col_b = st.columns(2)
        if col_a.button("Load", use_container_width=True):
            _load_config_into_state(Path(config_path_input))
        if col_b.button("Save", use_container_width=True):
            _save_state_config(Path(config_path_input))

    st.title("Vertical Farming MIP Demo")
    st.caption("Tweak config, run local/GCP workflows, and inspect outputs")

    tabs = st.tabs(["Config", "Run", "Results", "Tech Stack"])

    with tabs[0]:
        _render_config_editor()

    with tabs[1]:
        _render_run_tab()

    with tabs[2]:
        _render_results_tab()

    with tabs[3]:
        _render_tech_stack_tab()

    _render_status_panel()


def _init_state() -> None:
    if "config_path" not in st.session_state:
        st.session_state["config_path"] = str(DEFAULT_CONFIG_PATH)

    if "cfg" not in st.session_state:
        _load_config_into_state(Path(st.session_state["config_path"]))

    if "last_result" not in st.session_state:
        st.session_state["last_result"] = None
    if "last_error" not in st.session_state:
        st.session_state["last_error"] = None


def _load_config_into_state(config_path: Path) -> None:
    try:
        with config_path.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        st.session_state["cfg"] = cfg
        st.session_state["last_error"] = None
        st.session_state["last_result"] = {"message": f"Loaded config: {config_path}"}
    except Exception as exc:  # pragma: no cover - UI path
        st.session_state["last_error"] = f"Failed to load config: {exc}"


def _save_state_config(config_path: Path) -> None:
    try:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with config_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(st.session_state["cfg"], f, sort_keys=False)
        st.session_state["last_error"] = None
        st.session_state["last_result"] = {"message": f"Saved config: {config_path}"}
    except Exception as exc:  # pragma: no cover - UI path
        st.session_state["last_error"] = f"Failed to save config: {exc}"


def _render_config_editor() -> None:
    cfg = st.session_state["cfg"]

    c1, c2, c3, c4 = st.columns(4)
    cfg["seed"] = int(c1.number_input("Seed", value=int(cfg.get("seed", 20260225)), step=1))
    cfg["weeks"] = int(c2.number_input("Weeks", min_value=1, value=int(cfg.get("weeks", 12))))
    cfg["start_date"] = c3.text_input("Start date", value=str(cfg.get("start_date", "2026-01-05")))
    cfg["data_mode"] = c4.selectbox(
        "Data mode",
        options=["rows", "counts"],
        index=0 if str(cfg.get("data_mode", "rows")) == "rows" else 1,
    )

    st.subheader("Capacity")
    cap = cfg.setdefault("capacity", {})
    c1, c2, c3, c4 = st.columns(4)
    cap["rack_capacity_trays_per_week"] = int(
        c1.number_input(
            "Rack cap (trays/week)",
            min_value=1,
            value=int(cap.get("rack_capacity_trays_per_week", 1200)),
        )
    )
    cap["base_labor_hours_per_week"] = float(
        c2.number_input(
            "Base labor hours/week",
            min_value=1.0,
            value=float(cap.get("base_labor_hours_per_week", 220.0)),
        )
    )
    cap["base_process_capacity_trays_per_week"] = int(
        c3.number_input(
            "Process cap (trays/week)",
            min_value=1,
            value=int(cap.get("base_process_capacity_trays_per_week", 240)),
        )
    )
    cap["overtime_premium_per_hour"] = float(
        c4.number_input(
            "OT premium/hour",
            min_value=0.0,
            value=float(cap.get("overtime_premium_per_hour", 12.0)),
        )
    )

    c1, c2, c3 = st.columns(3)
    cap["base_wage_per_hour"] = float(
        c1.number_input(
            "Base wage/hour",
            min_value=0.0,
            value=float(cap.get("base_wage_per_hour", 24.0)),
        )
    )
    cap["shift_fixed_cost_per_week"] = float(
        c2.number_input(
            "Shift fixed cost/week",
            min_value=0.0,
            value=float(cap.get("shift_fixed_cost_per_week", 180.0)),
        )
    )
    cap["shift_labor_reduction_pct"] = float(
        c3.slider(
            "Shift labor reduction %",
            min_value=0.0,
            max_value=0.5,
            value=float(cap.get("shift_labor_reduction_pct", 0.15)),
            step=0.01,
        )
    )

    overtime_raw = cap.get("overtime_block_hours", [40.0, 40.0])
    overtime_text = st.text_input(
        "Overtime block hours (comma separated)",
        value=", ".join(str(v) for v in overtime_raw),
    )
    try:
        cap["overtime_block_hours"] = [float(x.strip()) for x in overtime_text.split(",") if x.strip()]
    except ValueError:
        st.warning("Overtime blocks must be numeric; keeping previous values.")

    st.subheader("Demand + Energy")
    demand = cfg.setdefault("demand", {})
    energy = cfg.setdefault("energy_price", {})
    c1, c2, c3, c4 = st.columns(4)
    demand["seasonality_amplitude"] = float(
        c1.slider(
            "Demand seasonality",
            min_value=0.0,
            max_value=0.5,
            value=float(demand.get("seasonality_amplitude", 0.12)),
            step=0.01,
        )
    )
    demand["noise_std"] = float(
        c2.slider(
            "Demand noise std",
            min_value=0.0,
            max_value=0.5,
            value=float(demand.get("noise_std", 0.08)),
            step=0.01,
        )
    )
    demand["utilization_target"] = float(
        c3.slider(
            "Demand utilization target",
            min_value=0.1,
            max_value=1.0,
            value=float(demand.get("utilization_target", 0.70)),
            step=0.01,
        )
    )
    demand["safety_factor"] = float(
        c4.slider(
            "Demand safety factor",
            min_value=0.1,
            max_value=1.0,
            value=float(demand.get("safety_factor", 0.90)),
            step=0.01,
        )
    )

    c1, c2, c3 = st.columns(3)
    energy["base_per_kwh"] = float(
        c1.number_input("Base energy $/kWh", min_value=0.01, value=float(energy.get("base_per_kwh", 0.13)))
    )
    energy["seasonality_amplitude"] = float(
        c2.slider(
            "Energy seasonality",
            min_value=0.0,
            max_value=0.5,
            value=float(energy.get("seasonality_amplitude", 0.08)),
            step=0.01,
        )
    )
    energy["noise_std"] = float(
        c3.slider(
            "Energy noise std",
            min_value=0.0,
            max_value=0.2,
            value=float(energy.get("noise_std", 0.01)),
            step=0.005,
        )
    )

    st.subheader("Crop Parameters")
    crop_params = cfg.setdefault("crop_params", {})
    crops = cfg.setdefault("crops", list(crop_params.keys()))

    crop_df = (
        pd.DataFrame.from_dict(crop_params, orient="index")
        .reset_index()
        .rename(columns={"index": "crop"})
    )
    crop_df = crop_df[crop_df["crop"].isin(crops)]

    edited = st.data_editor(
        crop_df,
        use_container_width=True,
        hide_index=True,
        disabled=["crop"],
        num_rows="fixed",
    )

    rebuilt: dict[str, dict[str, Any]] = {}
    for row in edited.to_dict(orient="records"):
        crop = str(row.pop("crop"))
        rebuilt[crop] = {
            "g_min_weeks": int(row["g_min_weeks"]),
            "g_max_weeks": int(row["g_max_weeks"]),
            "yield_lbs_per_tray": float(row["yield_lbs_per_tray"]),
            "shrink": float(row["shrink"]),
            "plant_hours_per_tray": float(row["plant_hours_per_tray"]),
            "harvest_hours_per_tray": float(row["harvest_hours_per_tray"]),
            "seed_cost_per_tray": float(row["seed_cost_per_tray"]),
            "nutrient_cost_per_tray_week": float(row["nutrient_cost_per_tray_week"]),
            "energy_kwh_per_tray_week": float(row["energy_kwh_per_tray_week"]),
        }
    cfg["crop_params"] = rebuilt
    cfg["crops"] = [c for c in crops if c in rebuilt]


def _render_run_tab() -> None:
    st.subheader("Run Workflows")
    st.caption(
        "Use local actions for end-to-end optimization outputs (solve/report). "
        "GCP actions submit Spark jobs for cloud execution."
    )
    _render_local_run_tab()
    st.divider()
    _render_gcp_tab()


def _render_local_run_tab() -> None:
    st.markdown("Run Local Spark/Pandas Pipeline and Optimization")

    c1, c2, c3, c4, c5 = st.columns(5)
    if c1.button("gen-data", use_container_width=True):
        _run_action("gen-data", lambda p: generate_data(p))
    if c2.button("agg-data", use_container_width=True):
        _run_action("agg-data", lambda p: aggregate_data(p))
    if c3.button("solve", use_container_width=True):
        _run_action("solve", lambda p: solve_optimization(p, tee=False, feasibility_only=False).__dict__)
    if c4.button("report", use_container_width=True):
        _run_action("report", lambda p: generate_report(p, make_plots=True))
    if c5.button("run-all", use_container_width=True):
        _run_action("run-all", _run_all_local)


def _render_gcp_tab() -> None:
    st.markdown("Run GCP Setup and Dataproc Serverless Jobs")
    st.caption(
        "These actions call GCP APIs directly using Application Default Credentials "
        "(no local gcloud binary required). Job submits are async; use batch status to track progress."
    )

    c1, c2, c3 = st.columns(3)
    if c1.button("gcp-setup", use_container_width=True):
        _run_action("gcp-setup", lambda p: setup_gcp_resources(p, create_budget=True))
    if c2.button("gcp-upload", use_container_width=True):
        _run_action("gcp-upload", lambda p: upload_gcp_artifacts(p))
    if c3.button("gcp-run-all", use_container_width=True):
        _run_action("gcp-submit-run-all", lambda p: submit_gcp_run_all(p, wait=False))

    c1, c2 = st.columns(2)
    if c1.button("gcp-submit-gen", use_container_width=True):
        _run_action("gcp-submit-gen", lambda p: submit_gcp_batch(p, job_type="gen", wait=False))
    if c2.button("gcp-submit-agg", use_container_width=True):
        _run_action("gcp-submit-agg", lambda p: submit_gcp_batch(p, job_type="agg", wait=False))

    batch_id = st.text_input("Batch ID for status lookup", value="")
    if st.button("Check batch status"):
        if not batch_id.strip():
            st.warning("Provide a batch ID")
        else:
            _run_action("gcp-batch-status", lambda p: describe_batch(p, batch_id=batch_id.strip()))


def _render_results_tab() -> None:
    config_path = _persist_runtime_config()

    try:
        app_config: AppConfig = load_config(config_path)
        paths = resolve_paths(app_config, config_path)
    except Exception as exc:  # pragma: no cover - UI path
        st.error(f"Failed to resolve paths: {exc}")
        return

    cost_path = paths.outputs_dir / "cost_breakdown.json"
    util_path = paths.outputs_dir / "utilization_weekly.csv"
    harvest_path = paths.outputs_dir / "solution_harvest.csv"
    plant_path = paths.outputs_dir / "solution_planting.csv"

    if not cost_path.exists():
        st.info("No outputs yet. Run local pipeline first.")
        return

    with cost_path.open("r", encoding="utf-8") as f:
        cost = json.load(f)

    util = pd.read_csv(util_path) if util_path.exists() else pd.DataFrame()
    harvest = pd.read_csv(harvest_path) if harvest_path.exists() else pd.DataFrame()
    planting = pd.read_csv(plant_path) if plant_path.exists() else pd.DataFrame()

    c1, c2, c3 = st.columns(3)
    c1.metric("Total cost", f"${float(cost.get('total_cost', 0.0)):,.2f}")
    c2.metric("Seed cost", f"${float(cost.get('seed_cost', 0.0)):,.2f}")
    c3.metric("Energy cost", f"${float(cost.get('energy_cost', 0.0)):,.2f}")

    st.subheader("Cost breakdown")
    st.json(cost)

    if not util.empty:
        st.subheader("Weekly utilization")
        st.line_chart(
            util.set_index("week_id")[["rack_utilization", "labor_utilization", "process_utilization"]]
        )

    if not harvest.empty:
        st.subheader("Production vs demand")
        by_week = harvest.groupby("week_id", as_index=True)[["produced_lbs", "demand_lbs"]].sum()
        st.line_chart(by_week)
        st.dataframe(harvest, use_container_width=True)

    if not planting.empty:
        st.subheader("Planting schedule")
        st.dataframe(planting, use_container_width=True)


def _render_tech_stack_tab() -> None:
    st.subheader("Tech Stack")
    st.markdown(
        """
- **Python 3.11+** for orchestration and modeling
- **PySpark** for synthetic telemetry generation and weekly aggregation
- **Pyomo + HiGHS** for mixed-integer optimization
- **Pandas + PyArrow** for table IO and local analysis
- **Matplotlib** for report plots
- **Streamlit** for interactive config/run/results dashboard
- **GCP Dataproc Serverless + GCS** for scalable Spark execution
- **Google Cloud APIs (ADC)** for infra setup, job submit, and budget controls
        """
    )

    st.subheader("Local commands")
    st.code(
        "\n".join(
            [
                "pip install -r requirements.txt",
                "PYTHONPATH=src python -m vfarm.cli run-all --config configs/base.yaml",
                "PYTHONPATH=src streamlit run dashboard.py",
            ]
        ),
        language="bash",
    )

    st.subheader("GCP commands")
    st.code(
        "\n".join(
            [
                "PYTHONPATH=src python -m vfarm.cli gcp-setup --config configs/base.yaml",
                "PYTHONPATH=src python -m vfarm.cli gcp-upload --config configs/base.yaml",
                "PYTHONPATH=src python -m vfarm.cli gcp-submit-run-all --config configs/base.yaml --wait",
            ]
        ),
        language="bash",
    )


def _render_status_panel() -> None:
    if st.session_state.get("last_error"):
        st.error(st.session_state["last_error"])

    if st.session_state.get("last_result"):
        with st.expander("Last action result", expanded=True):
            st.json(st.session_state["last_result"])


def _run_all_local(config_path: Path) -> dict[str, Any]:
    result = {
        "gen_data": generate_data(config_path),
        "agg_data": aggregate_data(config_path),
        "solve": solve_optimization(config_path, tee=False, feasibility_only=False).__dict__,
        "report": generate_report(config_path, make_plots=True),
    }
    return result


def _run_action(name: str, action_fn) -> None:  # type: ignore[no-untyped-def]
    config_path = _persist_runtime_config()
    started = datetime.utcnow().isoformat() + "Z"

    with st.spinner(f"Running {name}..."):
        try:
            result = action_fn(config_path)
            st.session_state["last_result"] = {
                "action": name,
                "started_at": started,
                "result": result,
            }
            st.session_state["last_error"] = None
        except Exception:  # pragma: no cover - UI path
            st.session_state["last_error"] = traceback.format_exc()


def _persist_runtime_config() -> Path:
    config_path = Path(st.session_state["config_path"]).resolve()
    cfg = st.session_state["cfg"]

    root_dir = config_path.parent.parent if config_path.parent.name == "configs" else config_path.parent
    runtime_path = root_dir / "configs" / "dashboard_runtime.yaml"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)

    with runtime_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)

    return runtime_path
