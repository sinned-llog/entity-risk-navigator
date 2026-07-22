from datetime import datetime, timezone
from typing import Any


def extract_load_date_from_object_key(object_key: str) -> str | None:
    parts = object_key.split("/")

    for part in parts:
        if part.startswith("load_date="):
            return part.replace("load_date=", "")

    return None


def find_latest_successful_manifest(
    minio,
    manifest_prefix: str,
    manifest_filename: str,
    allowed_statuses: set[str] | None = None,
    
) -> tuple[str, str, dict[str, Any]]:
    if allowed_statuses is None:
        allowed_statuses = {"success", "success_with_warnings"}

    object_keys = minio.list_objects(manifest_prefix)

    manifest_keys = [
        key
        for key in object_keys
        if key.endswith(manifest_filename)
    ]

    if not manifest_keys:
        raise RuntimeError(
            f"No manifest files found under prefix: {manifest_prefix}"
        )

    candidates = []

    for manifest_key in manifest_keys:
        load_date = extract_load_date_from_object_key(manifest_key)

        if not load_date:
            print(f"Skipping manifest without load_date partition: {manifest_key}")
            continue

        try:
            manifest = minio.get_json_object(manifest_key)
            manifest_status = manifest.get("status")

            if manifest_status in allowed_statuses:
                candidates.append(
                    {
                        "load_date": load_date,
                        "manifest_key": manifest_key,
                        "manifest": manifest,
                        "status": manifest_status,
                    }
                )

        except Exception as exc:
            print(f"Skipping unreadable manifest {manifest_key}: {exc}")

    if not candidates:
        raise RuntimeError(
            f"No successful manifest files found under prefix: {manifest_prefix}"
        )

    latest = sorted(
        candidates,
        key=lambda item: item["load_date"],
        reverse=True,
    )[0]

    return latest["manifest_key"], latest["load_date"], latest["manifest"]


def evaluate_snapshot_freshness(
    effective_load_date: str,
    max_age_days: int,
    policy: str,
) -> dict[str, Any]:
    effective_date = datetime.strptime(effective_load_date, "%Y-%m-%d").date()
    today = datetime.now(timezone.utc).date()

    snapshot_age_days = (today - effective_date).days
    snapshot_is_stale = snapshot_age_days > max_age_days

    if not snapshot_is_stale:
        freshness_status = "fresh"
    elif policy == "fail":
        freshness_status = "stale_failed"
    elif policy == "warn":
        freshness_status = "stale_warning"
    else:
        freshness_status = "stale_allowed"

    return {
        "effective_load_date": effective_load_date,
        "snapshot_age_days": snapshot_age_days,
        "max_snapshot_age_days": max_age_days,
        "snapshot_is_stale": snapshot_is_stale,
        "stale_snapshot_policy": policy,
        "freshness_status": freshness_status,
    }


def handle_stale_snapshot(
    freshness: dict[str, Any],
    source_name: str,
) -> None:
    if not freshness["snapshot_is_stale"]:
        return

    message = (
        f"{source_name} snapshot is stale: "
        f"effective_load_date={freshness['effective_load_date']}, "
        f"age={freshness['snapshot_age_days']} days, "
        f"max_allowed={freshness['max_snapshot_age_days']} days, "
        f"policy={freshness['stale_snapshot_policy']}"
    )

    if freshness["stale_snapshot_policy"] == "fail":
        raise RuntimeError(message)

    if freshness["stale_snapshot_policy"] == "warn":
        print("WARNING:", message)