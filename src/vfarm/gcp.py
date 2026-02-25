from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time
from typing import Any
import zipfile

from .config import AppConfig, load_config
from .utils import resolve_paths


class GCPError(RuntimeError):
    """Raised for gcloud-related failures."""


def setup_gcp_resources(config_path: str | Path, create_budget: bool = True) -> dict[str, Any]:
    config = load_config(config_path)
    project_id = config.gcp.project_id
    bucket_uri = _bucket_uri(config)

    _run_gcloud(
        [
            "services",
            "enable",
            *config.gcp.required_apis,
            f"--project={project_id}",
        ]
    )

    if not _bucket_exists(bucket_uri, project_id):
        _run_gcloud(
            [
                "storage",
                "buckets",
                "create",
                bucket_uri,
                f"--project={project_id}",
                f"--location={config.gcp.bucket_location}",
                "--uniform-bucket-level-access",
            ]
        )

    budget_summary: dict[str, Any] = {
        "budget_configured": False,
        "budget_display_name": config.gcp.budget_display_name,
        "budget_usd": config.gcp.budget_usd,
    }

    if create_budget and config.gcp.enable_budget:
        budget_summary = _ensure_budget(config)

    return {
        "project_id": project_id,
        "region": config.gcp.region,
        "bucket": config.gcp.bucket,
        "bucket_location": config.gcp.bucket_location,
        "services_enabled": list(config.gcp.required_apis),
        **budget_summary,
    }


def upload_gcp_artifacts(config_path: str | Path) -> dict[str, str]:
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    local_artifacts_dir = paths.root_dir / ".artifacts"
    local_artifacts_dir.mkdir(parents=True, exist_ok=True)

    source_zip_path = local_artifacts_dir / f"vfarm_src_{ts}.zip"
    _build_source_zip(paths.root_dir, source_zip_path)

    run_prefix = _gcs_prefix(config, ts)
    source_zip_uri = f"{run_prefix}/vfarm_src.zip"
    config_uri = f"{run_prefix}/base.yaml"
    gen_script_uri = f"{run_prefix}/gen_data_job.py"
    agg_script_uri = f"{run_prefix}/agg_data_job.py"
    run_all_script_uri = f"{run_prefix}/run_all_spark_job.py"

    _run_gcloud(["storage", "cp", str(source_zip_path), source_zip_uri])
    _run_gcloud(["storage", "cp", str(Path(config_path).resolve()), config_uri])
    _run_gcloud(["storage", "cp", str(paths.root_dir / "jobs" / "gen_data_job.py"), gen_script_uri])
    _run_gcloud(["storage", "cp", str(paths.root_dir / "jobs" / "agg_data_job.py"), agg_script_uri])
    _run_gcloud(
        [
            "storage",
            "cp",
            str(paths.root_dir / "jobs" / "run_all_spark_job.py"),
            run_all_script_uri,
        ]
    )

    manifest = {
        "uploaded_at_utc": ts,
        "project_id": config.gcp.project_id,
        "region": config.gcp.region,
        "bucket": config.gcp.bucket,
        "run_prefix": run_prefix,
        "source_zip_uri": source_zip_uri,
        "config_uri": config_uri,
        "gen_script_uri": gen_script_uri,
        "agg_script_uri": agg_script_uri,
        "run_all_script_uri": run_all_script_uri,
    }

    manifest_path = paths.outputs_dir / "gcp_artifacts.json"
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    return manifest


def submit_gcp_batch(
    config_path: str | Path,
    job_type: str,
    batch_id: str | None = None,
    wait: bool = False,
) -> dict[str, Any]:
    config = load_config(config_path)
    paths = resolve_paths(config, config_path)

    if job_type not in {"gen", "agg", "run_all"}:
        raise ValueError("job_type must be one of: gen, agg, run_all")

    artifacts = _load_artifacts_or_upload(config_path, paths.outputs_dir / "gcp_artifacts.json")

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    if batch_id is None:
        safe_job_type = job_type.replace("_", "-")
        batch_id = f"vfarm-{safe_job_type}-{run_ts}".lower()

    if job_type == "gen":
        script_uri = artifacts["gen_script_uri"]
    elif job_type == "agg":
        script_uri = artifacts["agg_script_uri"]
    else:
        script_uri = artifacts["run_all_script_uri"]
    config_name = Path(artifacts["config_uri"]).name

    cmd = [
        "dataproc",
        "batches",
        "submit",
        "pyspark",
        script_uri,
        f"--project={config.gcp.project_id}",
        f"--region={config.gcp.region}",
        f"--batch={batch_id}",
        "--async",
    ]

    if config.gcp.service_account:
        cmd.append(f"--service-account={config.gcp.service_account}")
    if config.gcp.subnet:
        cmd.append(f"--subnet={config.gcp.subnet}")

    if config.gcp.spark_properties:
        cmd.append(f"--properties={','.join(config.gcp.spark_properties)}")

    cmd.extend(
        [
            f"--version={config.gcp.dataproc_runtime_version}",
            f"--ttl={config.gcp.batch_ttl}",
            f"--deps-bucket={config.gcp.bucket}",
            f"--py-files={artifacts['source_zip_uri']}",
            f"--files={artifacts['config_uri']}",
            "--labels=app=vfarm,env=demo",
            "--",
            "--config",
            config_name,
        ]
    )

    stdout = _run_gcloud(cmd)
    result = {
        "batch_id": batch_id,
        "job_type": job_type,
        "submit_output": stdout.strip(),
        "project_id": config.gcp.project_id,
        "region": config.gcp.region,
    }

    if wait:
        final_status = wait_for_batch(
            config_path=config_path,
            batch_id=batch_id,
            timeout_seconds=7200,
        )
        result["final_status"] = final_status
        if str(final_status.get("state", "")) != "SUCCEEDED":
            raise GCPError(
                f"Batch '{batch_id}' failed with state={final_status.get('state')} "
                f"message={final_status.get('stateMessage', '')}"
            )

    return result


def submit_gcp_run_all(config_path: str | Path, wait: bool = True) -> dict[str, Any]:
    run_result = submit_gcp_batch(config_path, job_type="run_all", wait=wait)
    return {"run_all": run_result}


def describe_batch(config_path: str | Path, batch_id: str) -> dict[str, Any]:
    config = load_config(config_path)
    stdout = _run_gcloud(
        [
            "dataproc",
            "batches",
            "describe",
            batch_id,
            f"--project={config.gcp.project_id}",
            f"--region={config.gcp.region}",
            "--format=json",
        ]
    )
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise GCPError(f"Failed to parse batch describe output as JSON: {exc}") from exc


def wait_for_batch(
    config_path: str | Path,
    batch_id: str,
    poll_seconds: int = 15,
    timeout_seconds: int = 7200,
) -> dict[str, Any]:
    terminal_states = {"SUCCEEDED", "FAILED", "CANCELLED"}
    start = time.time()

    while True:
        desc = describe_batch(config_path, batch_id)
        state = str(desc.get("state", "UNKNOWN"))

        if state in terminal_states:
            return desc

        if time.time() - start > timeout_seconds:
            raise GCPError(
                f"Timed out waiting for batch '{batch_id}' after {timeout_seconds} seconds"
            )

        time.sleep(max(1, poll_seconds))


def _ensure_budget(config: AppConfig) -> dict[str, Any]:
    project_id = config.gcp.project_id
    billing_account = config.gcp.billing_account or _discover_billing_account(project_id)
    if not billing_account:
        return {
            "budget_configured": False,
            "budget_reason": "No billing account linked to project",
            "budget_display_name": config.gcp.budget_display_name,
            "budget_usd": config.gcp.budget_usd,
        }

    account_id = billing_account.split("/")[-1]
    existing = _run_gcloud_json(
        [
            "billing",
            "budgets",
            "list",
            f"--billing-account={account_id}",
            "--format=json",
        ]
    )

    if any(str(b.get("displayName", "")) == config.gcp.budget_display_name for b in existing):
        return {
            "budget_configured": True,
            "budget_exists": True,
            "budget_display_name": config.gcp.budget_display_name,
            "budget_usd": config.gcp.budget_usd,
            "billing_account": account_id,
        }

    _run_gcloud(
        [
            "billing",
            "budgets",
            "create",
            f"--billing-account={account_id}",
            f"--display-name={config.gcp.budget_display_name}",
            f"--budget-amount={config.gcp.budget_usd}USD",
            f"--filter-projects=projects/{project_id}",
            "--threshold-rule=percent=0.5",
            "--threshold-rule=percent=0.9",
            "--threshold-rule=percent=1.0",
        ]
    )

    return {
        "budget_configured": True,
        "budget_exists": False,
        "budget_display_name": config.gcp.budget_display_name,
        "budget_usd": config.gcp.budget_usd,
        "billing_account": account_id,
    }


def _discover_billing_account(project_id: str) -> str | None:
    account_name = _run_gcloud(
        [
            "billing",
            "projects",
            "describe",
            project_id,
            "--format=value(billingAccountName)",
        ]
    ).strip()
    return account_name or None


def _load_artifacts_or_upload(config_path: str | Path, manifest_path: Path) -> dict[str, str]:
    if manifest_path.exists():
        with manifest_path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
        required = {
            "source_zip_uri",
            "config_uri",
            "gen_script_uri",
            "agg_script_uri",
            "run_all_script_uri",
        }
        if required.issubset(raw.keys()):
            return {k: str(v) for k, v in raw.items()}

    return {k: str(v) for k, v in upload_gcp_artifacts(config_path).items()}


def _bucket_exists(bucket_uri: str, project_id: str) -> bool:
    try:
        _run_gcloud(
            [
                "storage",
                "buckets",
                "describe",
                bucket_uri,
                f"--project={project_id}",
            ]
        )
        return True
    except GCPError:
        return False


def _bucket_uri(config: AppConfig) -> str:
    return f"gs://{config.gcp.bucket}"


def _gcs_prefix(config: AppConfig, timestamp: str) -> str:
    prefix = config.gcp.staging_prefix.strip("/")
    return f"{_bucket_uri(config)}/{prefix}/{timestamp}"


def _build_source_zip(root_dir: Path, zip_path: Path) -> None:
    src_root = root_dir / "src"
    if not src_root.exists():
        raise GCPError(f"Source directory not found: {src_root}")

    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in src_root.rglob("*.py"):
            arcname = file_path.relative_to(src_root)
            zf.write(file_path, arcname.as_posix())


def _run_gcloud_json(args: list[str]) -> Any:
    out = _run_gcloud(args)
    try:
        return json.loads(out)
    except json.JSONDecodeError as exc:
        raise GCPError(f"Failed to parse JSON from gcloud output: {exc}") from exc


def _run_gcloud(args: list[str]) -> str:
    cmd = ["gcloud", *args]
    proc = subprocess.run(cmd, check=False, text=True, capture_output=True)
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        details = stderr if stderr else stdout
        raise GCPError(f"Command failed ({' '.join(cmd)}): {details}")
    return proc.stdout
