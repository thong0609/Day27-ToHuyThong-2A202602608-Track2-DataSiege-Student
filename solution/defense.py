"""
Streaming defense for the Data Siege lab.

Each handler makes one metered query for the event currently being dispatched
and compares the returned measurements with the published clean baseline.
"""
from api import Verdict


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def _bad(result):
    return not isinstance(result, dict) or "error" in result


def _outside(value, low=None, high=None):
    return (low is not None and value < low) or (high is not None and value > high)


def check_data_batch(payload, ctx):
    result = ctx.tools.batch_profile(payload["batch_id"])
    if _bad(result):
        return Verdict(alert=False, pillar="checks", reason="profile unavailable")

    baseline = ctx.baseline
    null_rate = result.get("null_rate", {}).get("customer_id")
    alert = (
        _outside(result.get("row_count"), baseline.get("row_count_min"), baseline.get("row_count_max"))
        or null_rate is None or null_rate > baseline.get("null_rate_max")
        or _outside(result.get("mean_amount"), baseline.get("mean_amount_min"), baseline.get("mean_amount_max"))
        or result.get("staleness_min", 0) > baseline.get("staleness_min_max")
    )
    return Verdict(alert=bool(alert), pillar="checks", reason="batch profile outside baseline" if alert else "within baseline")


def check_contract_checkpoint(payload, ctx):
    result = ctx.tools.contract_diff(payload["contract_id"], payload["checkpoint_batch_id"])
    if _bad(result):
        return Verdict(alert=False, pillar="contracts", reason="contract diff unavailable")

    baseline = ctx.baseline
    violations = result.get("violations", [])
    alert = bool(violations) or result.get("freshness_delay_min", 0) > baseline.get("freshness_delay_max_min")
    return Verdict(alert=alert, pillar="contracts", reason="contract violation or stale checkpoint" if alert else "within baseline")


def check_lineage_run(payload, ctx):
    result = ctx.tools.lineage_graph_slice(payload["run_id"])
    if _bad(result):
        return Verdict(alert=False, pillar="lineage", reason="lineage slice unavailable")

    # Compare the reported graph with the event's declared inputs/outputs,
    # then apply the published runtime bound.
    actual_upstream = set(result.get("actual_upstream") or [])
    declared_downstream_count = len(payload.get("outputs", []))
    actual_downstream_count = result.get("actual_downstream_count")
    profile_key = payload.get("job", "__default__")
    profiles = ctx.state.setdefault("lineage_profiles", {})
    expected_upstream = profiles.get(profile_key)
    if expected_upstream is None and actual_upstream:
        profiles[profile_key] = sorted(actual_upstream)
    alert = (
        result.get("duration_ms", 0) > ctx.baseline.get("lineage_duration_ms_max")
        or (expected_upstream is not None and actual_upstream != set(expected_upstream))
        or actual_downstream_count != declared_downstream_count
    )
    return Verdict(alert=bool(alert), pillar="lineage", reason="lineage structure or runtime anomalous" if alert else "within baseline")


def check_feature_materialization(payload, ctx):
    result = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if _bad(result):
        return Verdict(alert=False, pillar="ai_infra", reason="feature drift unavailable")

    alert = result.get("mean_shift_sigma", 0) > ctx.baseline.get("feature_mean_shift_sigma_max")
    return Verdict(alert=bool(alert), pillar="ai_infra", reason="feature distribution drift" if alert else "within baseline")


def check_embedding_batch(payload, ctx):
    result = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if _bad(result):
        return Verdict(alert=False, pillar="ai_infra", reason="embedding drift unavailable")

    baseline = ctx.baseline
    alert = (
        result.get("centroid_shift", 0) > baseline.get("embedding_centroid_shift_max")
        or result.get("avg_doc_age_days", 0) > baseline.get("corpus_avg_doc_age_days_max")
    )
    return Verdict(alert=bool(alert), pillar="ai_infra", reason="embedding drift or stale corpus" if alert else "within baseline")
