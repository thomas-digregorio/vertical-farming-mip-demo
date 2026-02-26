from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any
import zipfile

from google.auth import default
from google.auth.transport.requests import AuthorizedSession, Request
from google.cloud import storage

from .config import AppConfig, load_config
from .utils import resolve_paths


class GCPError(RuntimeError):
    """Raised for GCP API integration failures."""


def setup_gcp_resources(config_path: str | Path, create_budget: bool = True) -> dict[str, Any]:
    config = load_config(config_path)
    session = _authed_session()

    _enable_services(session, config)
    _ensure_bucket(config)

    budget_summary: dict[str, Any] = {
        "budget_configured": False,
        "budget_display_name": config.gcp.budget_display_name,
        "budget_usd": config.gcp.budget_usd,
    }

    if create_budget and config.gcp.enable_budget:
        budget_summary = _ensure_budget(session, config)

    return {
        "project_id": config.gcp.project_id,
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

    client = storage.Client(project=config.gcp.project_id)
    _upload_file(client, source_zip_uri, source_zip_path)
    _upload_file(client, config_uri, Path(config_path).resolve())
    _upload_file(client, gen_script_uri, paths.root_dir / "jobs" / "gen_data_job.py")
    _upload_file(client, agg_script_uri, paths.root_dir / "jobs" / "agg_data_job.py")
    _upload_file(client, run_all_script_uri, paths.root_dir / "jobs" / "run_all_spark_job.py")

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
    session = _authed_session()

    if job_type not in {"gen", "agg", "run_all"}:
        raise ValueError("job_type must be one of: gen, agg, run_all")

    artifacts = _load_artifacts_or_upload(config_path, paths.outputs_dir / "gcp_artifacts.json")

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    if batch_id is None:
        safe_job_type = job_type.replace("_", "-")
        batch_id = f"vfarm-{safe_job_type}-{run_ts}".lower()

    if wait:
        _wait_for_active_batches(session, config, timeout_seconds=3600)
        _wait_for_quota_headroom(session, config, timeout_seconds=900)
    else:
        active = _list_active_batches(session, config)
        if active:
            active_ids = ", ".join(_batch_id_from_batch(rec) for rec in active)
            return {
                "batch_id": batch_id,
                "job_type": job_type,
                "project_id": config.gcp.project_id,
                "region": config.gcp.region,
                "skipped": True,
                "reason": f"Active batch(es) detected: {active_ids}. Wait for completion then retry.",
            }
        quota_reason = _current_quota_shortfall(session, config)
        if quota_reason is not None:
            return {
                "batch_id": batch_id,
                "job_type": job_type,
                "project_id": config.gcp.project_id,
                "region": config.gcp.region,
                "skipped": True,
                "reason": quota_reason,
            }

    if job_type == "gen":
        script_uri = artifacts["gen_script_uri"]
    elif job_type == "agg":
        script_uri = artifacts["agg_script_uri"]
    else:
        script_uri = artifacts["run_all_script_uri"]

    config_name = Path(artifacts["config_uri"]).name

    body: dict[str, Any] = {
        "pysparkBatch": {
            "mainPythonFileUri": script_uri,
            "pythonFileUris": [artifacts["source_zip_uri"]],
            "fileUris": [artifacts["config_uri"]],
            "args": ["--config", config_name],
        },
        "runtimeConfig": {
            "version": config.gcp.dataproc_runtime_version,
            "properties": _parse_spark_properties(config.gcp.spark_properties),
        },
        "labels": {
            "app": "vfarm",
            "env": "demo",
        },
        "environmentConfig": {
            "executionConfig": {
                "ttl": _normalize_ttl(config.gcp.batch_ttl),
            }
        },
    }

    if not body["runtimeConfig"]["properties"]:
        del body["runtimeConfig"]["properties"]

    exec_cfg = body["environmentConfig"]["executionConfig"]
    if config.gcp.service_account:
        exec_cfg["serviceAccount"] = config.gcp.service_account
    if config.gcp.subnet:
        exec_cfg["subnetworkUri"] = config.gcp.subnet

    submit_url = (
        f"https://dataproc.googleapis.com/v1/projects/{config.gcp.project_id}"
        f"/locations/{config.gcp.region}/batches"
    )

    max_attempts = 3 if wait else 1
    for attempt in range(1, max_attempts + 1):
        attempt_batch_id = _with_retry_suffix(batch_id, attempt)
        try:
            submitted = _api_request(
                session=session,
                method="POST",
                url=submit_url,
                params={"batchId": attempt_batch_id},
                json_body=body,
                expected_status=(200,),
            )
        except GCPError as exc:
            detail = str(exc)
            if _is_quota_failure(detail):
                if wait and attempt < max_attempts:
                    _wait_for_quota_headroom(session, config, timeout_seconds=900)
                    time.sleep(20 * attempt)
                    continue
                if not wait:
                    return {
                        "batch_id": attempt_batch_id,
                        "job_type": job_type,
                        "project_id": config.gcp.project_id,
                        "region": config.gcp.region,
                        "skipped": True,
                        "reason": (
                            "Dataproc quota is currently unavailable. "
                            "Wait for running batches to finish and retry."
                        ),
                        "error_detail": detail,
                    }
            raise

        result: dict[str, Any] = {
            "batch_id": attempt_batch_id,
            "job_type": job_type,
            "submit_output": json.dumps(submitted, indent=2),
            "project_id": config.gcp.project_id,
            "region": config.gcp.region,
        }

        if not wait:
            return result

        final_status = wait_for_batch(
            config_path=config_path,
            batch_id=attempt_batch_id,
            timeout_seconds=7200,
        )
        result["final_status"] = final_status

        if str(final_status.get("state", "")) == "SUCCEEDED":
            return result

        state_message = str(final_status.get("stateMessage", ""))
        if _is_quota_failure(state_message) and attempt < max_attempts:
            _wait_for_quota_headroom(session, config, timeout_seconds=900)
            time.sleep(20 * attempt)
            continue

        raise GCPError(
            f"Batch '{attempt_batch_id}' failed with state={final_status.get('state')} "
            f"message={state_message}"
        )

    raise GCPError("Unexpected retry loop termination while submitting Dataproc batch")


def submit_gcp_run_all(config_path: str | Path, wait: bool = True) -> dict[str, Any]:
    run_result = submit_gcp_batch(config_path, job_type="run_all", wait=wait)
    return {"run_all": run_result}


def describe_batch(config_path: str | Path, batch_id: str) -> dict[str, Any]:
    config = load_config(config_path)
    session = _authed_session()

    url = (
        f"https://dataproc.googleapis.com/v1/projects/{config.gcp.project_id}"
        f"/locations/{config.gcp.region}/batches/{batch_id}"
    )
    return _api_request(session=session, method="GET", url=url, expected_status=(200,))


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


def _enable_services(session: AuthorizedSession, config: AppConfig) -> None:
    if not config.gcp.required_apis:
        return

    url = (
        f"https://serviceusage.googleapis.com/v1/projects/{config.gcp.project_id}"
        "/services:batchEnable"
    )
    op = _api_request(
        session=session,
        method="POST",
        url=url,
        json_body={"serviceIds": list(config.gcp.required_apis)},
        expected_status=(200,),
    )

    op_name = str(op.get("name", ""))
    if op_name:
        _wait_operation(session, f"https://serviceusage.googleapis.com/v1/{op_name}")


def _ensure_bucket(config: AppConfig) -> None:
    client = storage.Client(project=config.gcp.project_id)
    bucket = client.bucket(config.gcp.bucket)

    if not bucket.exists(client=client):
        bucket = client.create_bucket(bucket_or_name=config.gcp.bucket, location=config.gcp.bucket_location)

    if not bucket.iam_configuration.uniform_bucket_level_access_enabled:
        bucket.iam_configuration.uniform_bucket_level_access_enabled = True
        bucket.patch()


def _ensure_budget(session: AuthorizedSession, config: AppConfig) -> dict[str, Any]:
    try:
        project_id = config.gcp.project_id
        billing_account = config.gcp.billing_account or _discover_billing_account(session, project_id)
        if not billing_account:
            return {
                "budget_configured": False,
                "budget_reason": "No billing account linked to project",
                "budget_display_name": config.gcp.budget_display_name,
                "budget_usd": config.gcp.budget_usd,
            }

        existing = _list_budgets(session, billing_account)
        if any(str(b.get("displayName", "")) == config.gcp.budget_display_name for b in existing):
            return {
                "budget_configured": True,
                "budget_exists": True,
                "budget_display_name": config.gcp.budget_display_name,
                "budget_usd": config.gcp.budget_usd,
                "billing_account": billing_account.split("/")[-1],
            }

        _create_budget(session, config, billing_account)
        return {
            "budget_configured": True,
            "budget_exists": False,
            "budget_display_name": config.gcp.budget_display_name,
            "budget_usd": config.gcp.budget_usd,
            "billing_account": billing_account.split("/")[-1],
        }
    except GCPError as exc:
        return {
            "budget_configured": False,
            "budget_reason": str(exc),
            "budget_display_name": config.gcp.budget_display_name,
            "budget_usd": config.gcp.budget_usd,
        }


def _discover_billing_account(session: AuthorizedSession, project_id: str) -> str | None:
    url = f"https://cloudbilling.googleapis.com/v1/projects/{project_id}/billingInfo"
    info = _api_request(session=session, method="GET", url=url, expected_status=(200,))
    account_name = str(info.get("billingAccountName", "")).strip()
    return account_name or None


def _list_budgets(session: AuthorizedSession, billing_account: str) -> list[dict[str, Any]]:
    url = f"https://billingbudgets.googleapis.com/v1/{billing_account}/budgets"
    budgets: list[dict[str, Any]] = []
    page_token = ""

    while True:
        params: dict[str, str] = {}
        if page_token:
            params["pageToken"] = page_token

        payload = _api_request(
            session=session,
            method="GET",
            url=url,
            params=params,
            expected_status=(200,),
        )
        budgets.extend(payload.get("budgets", []))
        page_token = str(payload.get("nextPageToken", ""))
        if not page_token:
            break

    return budgets


def _create_budget(session: AuthorizedSession, config: AppConfig, billing_account: str) -> None:
    whole = int(config.gcp.budget_usd)
    nanos = int(round((config.gcp.budget_usd - whole) * 1_000_000_000))

    body = {
        "displayName": config.gcp.budget_display_name,
        "budgetFilter": {
            "projects": [f"projects/{config.gcp.project_id}"],
        },
        "amount": {
            "specifiedAmount": {
                "currencyCode": "USD",
                "units": str(whole),
                "nanos": nanos,
            }
        },
        "thresholdRules": [
            {"thresholdPercent": 0.5},
            {"thresholdPercent": 0.9},
            {"thresholdPercent": 1.0},
        ],
    }

    url = f"https://billingbudgets.googleapis.com/v1/{billing_account}/budgets"
    _api_request(session=session, method="POST", url=url, json_body=body, expected_status=(200,))


def _wait_operation(
    session: AuthorizedSession,
    operation_url: str,
    timeout_seconds: int = 900,
    poll_seconds: int = 2,
) -> dict[str, Any]:
    start = time.time()

    while True:
        op = _api_request(session=session, method="GET", url=operation_url, expected_status=(200,))
        if op.get("done"):
            if op.get("error"):
                raise GCPError(f"Operation failed: {op['error']}")
            return op

        if time.time() - start > timeout_seconds:
            raise GCPError(f"Timed out waiting for operation: {operation_url}")

        time.sleep(max(1, poll_seconds))


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


def _wait_for_active_batches(
    session: AuthorizedSession,
    config: AppConfig,
    timeout_seconds: int,
) -> None:
    start = time.time()
    while True:
        active = _list_active_batches(session, config)
        if not active:
            return

        if time.time() - start > timeout_seconds:
            ids = ", ".join(_batch_id_from_batch(rec) for rec in active)
            raise GCPError(f"Timed out waiting for active batch(es) to finish: {ids}")

        time.sleep(15)


def _wait_for_quota_headroom(
    session: AuthorizedSession,
    config: AppConfig,
    timeout_seconds: int,
    required_cpus: float = 12.0,
    required_disks_gb: float = 1200.0,
) -> None:
    start = time.time()
    while True:
        shortfall = _current_quota_shortfall(
            session,
            config,
            required_cpus=required_cpus,
            required_disks_gb=required_disks_gb,
        )
        if shortfall is None:
            return

        if time.time() - start > timeout_seconds:
            raise GCPError(
                f"Timed out waiting for Dataproc quota headroom: {shortfall}"
            )

        time.sleep(15)


def _list_active_batches(session: AuthorizedSession, config: AppConfig) -> list[dict[str, Any]]:
    all_batches = _list_batches(session, config)
    active_states = {"PENDING", "RUNNING", "CANCELLING"}
    results: list[dict[str, Any]] = []

    for rec in all_batches:
        state = str(rec.get("state", ""))
        labels = rec.get("labels", {})
        if state in active_states and labels.get("app") == "vfarm":
            results.append(rec)

    return results


def _list_batches(session: AuthorizedSession, config: AppConfig) -> list[dict[str, Any]]:
    base_url = (
        f"https://dataproc.googleapis.com/v1/projects/{config.gcp.project_id}"
        f"/locations/{config.gcp.region}/batches"
    )

    batches: list[dict[str, Any]] = []
    page_token = ""

    while True:
        params: dict[str, str] = {"pageSize": "50"}
        if page_token:
            params["pageToken"] = page_token

        payload = _api_request(
            session=session,
            method="GET",
            url=base_url,
            params=params,
            expected_status=(200,),
        )

        batches.extend(payload.get("batches", []))
        page_token = str(payload.get("nextPageToken", ""))
        if not page_token:
            break

    return batches


def _batch_id_from_batch(batch: dict[str, Any]) -> str:
    name = str(batch.get("name", ""))
    if "/batches/" in name:
        return name.rsplit("/batches/", maxsplit=1)[-1]
    return name or "unknown"


def _get_quota_metric(
    session: AuthorizedSession,
    config: AppConfig,
    metric: str,
) -> dict[str, Any] | None:
    try:
        url = f"https://compute.googleapis.com/compute/v1/projects/{config.gcp.project_id}"
        payload = _api_request(session=session, method="GET", url=url, expected_status=(200,))
        for rec in payload.get("quotas", []):
            if str(rec.get("metric", "")) == metric:
                return rec
    except GCPError:
        return None
    return None


def _current_quota_shortfall(
    session: AuthorizedSession,
    config: AppConfig,
    required_cpus: float = 12.0,
    required_disks_gb: float = 1200.0,
) -> str | None:
    cpu = _get_quota_metric(session, config, metric="CPUS_ALL_REGIONS")
    if cpu is not None:
        cpu_available = float(cpu.get("limit", 0.0)) - float(cpu.get("usage", 0.0))
        if cpu_available < required_cpus:
            return (
                "Insufficient CPUS_ALL_REGIONS quota headroom. "
                f"available={cpu_available:.1f}, required={required_cpus:.1f}"
            )

    disks = _get_quota_metric(session, config, metric="DISKS_TOTAL_GB")
    if disks is not None:
        disk_available = float(disks.get("limit", 0.0)) - float(disks.get("usage", 0.0))
        if disk_available < required_disks_gb:
            return (
                "Insufficient DISKS_TOTAL_GB quota headroom. "
                f"available={disk_available:.1f}, required={required_disks_gb:.1f}"
            )

    return None


def _with_retry_suffix(base_batch_id: str, attempt: int) -> str:
    if attempt <= 1:
        return base_batch_id
    suffix = f"-r{attempt}"
    if len(base_batch_id) + len(suffix) <= 63:
        return base_batch_id + suffix
    return base_batch_id[: 63 - len(suffix)] + suffix


def _is_quota_failure(state_message: str) -> bool:
    txt = state_message.lower()
    return "insufficient 'cpus_all_regions' quota" in txt or "insufficient 'disks_total_gb' quota" in txt


def _parse_spark_properties(raw_props: tuple[str, ...]) -> dict[str, str]:
    parsed: dict[str, str] = {}

    for item in raw_props:
        if "=" not in item:
            continue
        key, val = item.split("=", maxsplit=1)
        key = key.strip()
        val = val.strip()
        if not key:
            continue
        if ":" not in key:
            key = f"spark:{key}"
        parsed[key] = val

    return parsed


def _normalize_ttl(ttl: str) -> str:
    value = ttl.strip().lower()
    if value.endswith("s"):
        return value
    if value.endswith("m"):
        return f"{int(value[:-1]) * 60}s"
    if value.endswith("h"):
        return f"{int(value[:-1]) * 3600}s"
    if value.isdigit():
        return f"{value}s"
    return ttl


def _authed_session() -> AuthorizedSession:
    credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    credentials.refresh(Request())
    return AuthorizedSession(credentials)


def _api_request(
    session: AuthorizedSession,
    method: str,
    url: str,
    expected_status: tuple[int, ...],
    json_body: dict[str, Any] | None = None,
    params: dict[str, str] | None = None,
) -> dict[str, Any]:
    response = session.request(
        method=method,
        url=url,
        json=json_body,
        params=params,
        timeout=120,
    )

    if response.status_code not in expected_status:
        detail = response.text.strip()
        raise GCPError(
            f"API request failed: {method} {url} status={response.status_code} detail={detail}"
        )

    if not response.content:
        return {}

    try:
        return response.json()
    except ValueError as exc:
        raise GCPError(f"Non-JSON response from {method} {url}: {response.text[:500]}") from exc


def _bucket_uri(config: AppConfig) -> str:
    return f"gs://{config.gcp.bucket}"


def _gcs_prefix(config: AppConfig, timestamp: str) -> str:
    prefix = config.gcp.staging_prefix.strip("/")
    return f"{_bucket_uri(config)}/{prefix}/{timestamp}"


def _upload_file(client: storage.Client, destination_uri: str, local_file: Path) -> None:
    bucket_name, object_path = _split_gs_uri(destination_uri)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_filename(str(local_file))


def _split_gs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise GCPError(f"Invalid GCS URI: {uri}")

    raw = uri[5:]
    parts = raw.split("/", maxsplit=1)
    if len(parts) != 2:
        raise GCPError(f"Invalid GCS URI (missing object path): {uri}")

    return parts[0], parts[1]


def _build_source_zip(root_dir: Path, zip_path: Path) -> None:
    src_root = root_dir / "src"
    if not src_root.exists():
        raise GCPError(f"Source directory not found: {src_root}")

    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in src_root.rglob("*.py"):
            arcname = file_path.relative_to(src_root)
            zf.write(file_path, arcname.as_posix())
