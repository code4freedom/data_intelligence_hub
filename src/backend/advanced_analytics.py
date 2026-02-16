from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.backend.intelligence import compute_manifest_intelligence


def _safe_num(v, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, str) and not v.strip():
            return default
        return float(v)
    except Exception:
        return default


def _linear_forecast(series: List[Tuple[float, float]], horizon: int = 3) -> List[Dict]:
    """Simple linear forecast without external ML dependencies."""
    if len(series) < 2:
        return []
    xs = [x for x, _ in series]
    ys = [y for _, y in series]
    n = len(xs)
    sx = sum(xs)
    sy = sum(ys)
    sxx = sum(x * x for x in xs)
    sxy = sum(x * y for x, y in series)
    denom = (n * sxx - sx * sx)
    if denom == 0:
        return []
    slope = (n * sxy - sx * sy) / denom
    intercept = (sy - slope * sx) / n

    step = 1.0
    if len(xs) >= 2:
        step = max(1e-6, (xs[-1] - xs[0]) / (len(xs) - 1))
    out = []
    base_x = xs[-1]
    for i in range(1, horizon + 1):
        fx = base_x + step * i
        out.append({"step": i, "value": round(intercept + slope * fx, 2)})
    return out


def _zscore_anomalies(values: List[float], threshold: float = 2.0) -> List[int]:
    if len(values) < 3:
        return []
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / len(values)
    std = math.sqrt(max(var, 1e-9))
    idx = []
    for i, v in enumerate(values):
        z = abs((v - mean) / std)
        if z >= threshold:
            idx.append(i)
    return idx


def _load_manifest(manifest_path: Path) -> Dict:
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _resolve_chunk_path(ch: Dict, chunks_dir: Path) -> Optional[Path]:
    lp = Path(ch.get("local_path", ""))
    if lp.exists():
        return lp
    name = ch.get("name")
    if not name:
        return None
    p = chunks_dir / name
    return p if p.exists() else None


def _load_latest_df(manifest_path: Path, chunks_dir: Path) -> pd.DataFrame:
    m = _load_manifest(manifest_path)
    frames = []
    for ch in m.get("chunks", []):
        p = _resolve_chunk_path(ch, chunks_dir)
        if not p:
            continue
        try:
            frames.append(pd.read_parquet(p))
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _find_col(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    low = {str(c).lower(): c for c in df.columns}
    for a in aliases:
        a_l = a.lower()
        if a_l in low:
            return low[a_l]
    for c_l, c_o in low.items():
        for a in aliases:
            if a.lower() in c_l:
                return c_o
    return None


def _infer_app(name: str) -> str:
    n = str(name).lower()
    rules = [
        ("database", ["sql", "oracle", "postgres", "mysql", "db"]),
        ("web", ["web", "nginx", "apache", "iis"]),
        ("application", ["app", "api", "svc", "service"]),
        ("identity", ["ad", "domain", "dns", "dhcp"]),
        ("backup", ["backup", "veeam", "commvault"]),
        ("vdi", ["vdi", "citrix", "horizon"]),
    ]
    for label, keys in rules:
        if any(k in n for k in keys):
            return label
    return "unclassified"


def _days_to_threshold(last: float, monthly_growth: float, threshold: float) -> Optional[int]:
    if monthly_growth <= 0:
        return None
    if last >= threshold:
        return 0
    months = (threshold - last) / monthly_growth
    return int(max(0, round(months * 30)))


def compute_advanced_analytics(
    manifest_path: str,
    manifests_dir: str,
    chunks_dir: str,
    forecast_horizon: int = 3,
    what_if_growth_pct: float = 20.0,
    consolidation_target_vcpu_pcpu: float = 4.0,
) -> Dict:
    mp = Path(manifest_path)
    mdir = Path(manifests_dir)
    cdir = Path(chunks_dir)
    if not mp.exists():
        raise FileNotFoundError(manifest_path)

    intel = compute_manifest_intelligence(manifest_path, manifests_dir, chunks_dir)
    trends = intel.get("trends", [])
    summary = intel.get("summary", {})
    exec_comp = intel.get("executive_components", {})
    perf = intel.get("performance", {})
    consolidation = intel.get("consolidation", {})

    # Capacity forecasting.
    x = list(range(len(trends)))
    vm_series = [(float(i), _safe_num(t.get("total_vms"))) for i, t in zip(x, trends)]
    mem_series = [(float(i), _safe_num(t.get("total_memory_tb"))) for i, t in zip(x, trends)]
    host_series = [(float(i), _safe_num(t.get("total_hosts"))) for i, t in zip(x, trends)]
    vm_forecast = _linear_forecast(vm_series, forecast_horizon)
    mem_forecast = _linear_forecast(mem_series, forecast_horizon)
    host_forecast = _linear_forecast(host_series, forecast_horizon)

    vm_growth_monthly = 0.0
    mem_growth_monthly = 0.0
    if len(vm_series) >= 2:
        vm_growth_monthly = vm_series[-1][1] - vm_series[-2][1]
    if len(mem_series) >= 2:
        mem_growth_monthly = mem_series[-1][1] - mem_series[-2][1]

    latest_vms = _safe_num(summary.get("total_vms"))
    latest_mem_tb = _safe_num(summary.get("total_memory_tb"))
    days_to_10k_vms = _days_to_threshold(latest_vms, vm_growth_monthly, 10000)
    days_to_20tb = _days_to_threshold(latest_mem_tb, mem_growth_monthly, 20.0)

    # Anomalies on historical series.
    vm_vals = [_safe_num(t.get("total_vms")) for t in trends]
    host_vals = [_safe_num(t.get("total_hosts")) for t in trends]
    eos_vals = [_safe_num(t.get("eos_risk_vms")) for t in trends]
    vm_anom_idx = _zscore_anomalies(vm_vals)
    host_anom_idx = _zscore_anomalies(host_vals)
    eos_anom_idx = _zscore_anomalies(eos_vals)
    anomaly_events = []
    for idx in sorted(set(vm_anom_idx + host_anom_idx + eos_anom_idx)):
        if idx < len(trends):
            anomaly_events.append(
                {
                    "ingest_id": trends[idx].get("ingest_id"),
                    "timestamp": trends[idx].get("timestamp"),
                    "total_vms": trends[idx].get("total_vms"),
                    "total_hosts": trends[idx].get("total_hosts"),
                    "eos_risk_vms": trends[idx].get("eos_risk_vms"),
                }
            )

    # Latest DF for per-VM analytics.
    latest_df = _load_latest_df(mp, cdir)
    vm_col = _find_col(latest_df, ["VM", "Name", "VM name"])
    cpu_col = _find_col(latest_df, ["CPUs", "NumCPU", "vCPU", "CPU"])
    mem_col = _find_col(latest_df, ["Memory", "MemoryMB"])
    host_col = _find_col(latest_df, ["Host", "Hostname"])
    cluster_col = _find_col(latest_df, ["Cluster"])
    pwr_col = _find_col(latest_df, ["Powerstate", "Power State"])
    os_col = _find_col(latest_df, ["OS according to the configuration file", "OS"])

    # Right-sizing recommender (top items).
    right_size_recs = []
    if not latest_df.empty and vm_col and cpu_col and mem_col:
        d = latest_df.copy()
        d["_cpu"] = pd.to_numeric(d[cpu_col], errors="coerce").fillna(0)
        d["_mem"] = pd.to_numeric(d[mem_col], errors="coerce").fillna(0)
        if pwr_col:
            d["_on"] = d[pwr_col].astype(str).str.lower().eq("poweredon")
        else:
            d["_on"] = True
        d = d[(d["_cpu"] >= 8) & (d["_mem"] >= 16384) & (d["_on"])]
        d = d.sort_values(["_cpu", "_mem"], ascending=False).head(25)
        for _, r in d.iterrows():
            vm_name = str(r.get(vm_col))
            cur_cpu = int(_safe_num(r.get("_cpu")))
            cur_mem = int(_safe_num(r.get("_mem")))
            tgt_cpu = max(2, int(round(cur_cpu * 0.75)))
            tgt_mem = max(4096, int(round(cur_mem * 0.8)))
            right_size_recs.append(
                {
                    "vm": vm_name,
                    "current_cpu": cur_cpu,
                    "target_cpu": tgt_cpu,
                    "current_memory_mb": cur_mem,
                    "target_memory_mb": tgt_mem,
                    "confidence": 0.62,
                }
            )

    # EOS prioritization.
    eos_priority = []
    if not latest_df.empty and vm_col and os_col:
        d = latest_df.copy()
        os_text = d[os_col].astype(str).str.lower()
        risky = os_text.str.contains("2003|2008|2012|rhel 5|rhel 6|centos 6|ubuntu 14|ubuntu 16|sles 11", regex=True)
        d = d[risky]
        if not d.empty:
            d["_cpu"] = pd.to_numeric(d[cpu_col], errors="coerce").fillna(0) if cpu_col else 1
            d["_mem"] = pd.to_numeric(d[mem_col], errors="coerce").fillna(0) if mem_col else 1024
            d["_score"] = d["_cpu"] * 1.5 + d["_mem"] / 4096.0
            d = d.sort_values("_score", ascending=False).head(50)
            for _, r in d.iterrows():
                eos_priority.append(
                    {
                        "vm": str(r.get(vm_col)),
                        "os": str(r.get(os_col)),
                        "priority_score": round(_safe_num(r.get("_score")), 2),
                        "recommended_action": "Upgrade or migrate OS within next planning cycle",
                    }
                )

    # Consolidation optimization (heuristic scenario).
    hosts = max(1.0, _safe_num(summary.get("total_hosts")))
    total_vcpu = _safe_num(summary.get("total_vcpu"))
    est_pcpu_per_host = 32.0
    target_total_hosts = max(1.0, math.ceil(total_vcpu / (consolidation_target_vcpu_pcpu * est_pcpu_per_host)))
    retire_hosts = max(0, int(hosts - target_total_hosts))
    consolidation_optimization = {
        "current_hosts": int(hosts),
        "target_hosts": int(target_total_hosts),
        "retireable_hosts_estimate": retire_hosts,
        "assumed_pcpu_per_host": int(est_pcpu_per_host),
        "target_vcpu_pcpu_ratio": consolidation_target_vcpu_pcpu,
    }

    # Application dependency mining (co-location graph by cluster).
    dependency_edges = []
    dependency_nodes = set()
    if not latest_df.empty and vm_col:
        d = latest_df.copy()
        d["_app"] = d[vm_col].astype(str).map(_infer_app)
        if cluster_col:
            grouped = d.groupby(cluster_col)["_app"].apply(list).tolist()
            pair_count = {}
            for apps in grouped:
                uniq = sorted(set(a for a in apps if a))
                for i in range(len(uniq)):
                    for j in range(i + 1, len(uniq)):
                        key = (uniq[i], uniq[j])
                        pair_count[key] = pair_count.get(key, 0) + 1
            for (a, b), w in sorted(pair_count.items(), key=lambda kv: kv[1], reverse=True)[:25]:
                dependency_edges.append({"source": a, "target": b, "weight": w})
                dependency_nodes.add(a)
                dependency_nodes.add(b)
    dependency_graph = {
        "nodes": [{"id": n, "type": "app_group"} for n in sorted(dependency_nodes)],
        "edges": dependency_edges,
    }

    # Storage efficiency trajectory.
    storage_eff = {
        "overprovision_ratio": _safe_num(exec_comp.get("storage_overprov_ratio")),
        "estimated_reclaim_memory_gb": _safe_num(perf.get("estimated_reclaim_memory_gb")),
        "estimated_reclaim_vcpu": _safe_num(perf.get("estimated_reclaim_vcpu")),
    }

    # Drift & governance from last two snapshots.
    drift = {"available": False, "changes": []}
    if len(trends) >= 2:
        a = trends[-2]
        b = trends[-1]
        drift["available"] = True
        for key in ("total_vms", "total_hosts", "total_vcpu", "total_memory_tb", "eos_risk_vms"):
            av = _safe_num(a.get(key))
            bv = _safe_num(b.get(key))
            if av != bv:
                drift["changes"].append(
                    {"metric": key, "from": av, "to": bv, "delta": round(bv - av, 2)}
                )

    # What-if simulation.
    growth_factor = 1.0 + (what_if_growth_pct / 100.0)
    what_if = {
        "growth_pct": what_if_growth_pct,
        "projected_total_vms": round(latest_vms * growth_factor, 2),
        "projected_total_vcpu": round(_safe_num(summary.get("total_vcpu")) * growth_factor, 2),
        "projected_total_memory_tb": round(_safe_num(summary.get("total_memory_tb")) * growth_factor, 2),
        "projected_vm_per_host": round(_safe_num(consolidation.get("vm_per_host")) * growth_factor, 2),
    }

    # Operational health scorecards.
    risk_score = _safe_num(intel.get("executive_score"))
    efficiency_score = max(0.0, 100.0 - abs(_safe_num(exec_comp.get("vcpu_pcpu_ratio")) - 3.0) * 15.0)
    lifecycle_score = max(0.0, 100.0 - _safe_num(intel.get("lifecycle", {}).get("eos_risk_pct")))
    growth_hygiene_score = 100.0
    if len(trends) >= 2 and abs(vm_growth_monthly) > 1000:
        growth_hygiene_score = 65.0
    scorecard = {
        "risk_score": round(100.0 - risk_score, 1),
        "efficiency_score": round(efficiency_score, 1),
        "lifecycle_score": round(lifecycle_score, 1),
        "growth_hygiene_score": round(growth_hygiene_score, 1),
    }

    # Prioritized action backlog.
    backlog = []
    if eos_priority:
        backlog.append(
            {
                "priority": "P1",
                "title": "Remediate end-of-support VMs",
                "count": len(eos_priority),
            }
        )
    if right_size_recs:
        backlog.append(
            {
                "priority": "P2",
                "title": "Apply right-sizing recommendations",
                "count": len(right_size_recs),
            }
        )
    if retire_hosts > 0:
        backlog.append(
            {
                "priority": "P2",
                "title": "Consolidate underutilized hosts",
                "count": retire_hosts,
            }
        )

    return {
        "forecasting": {
            "vm_forecast": vm_forecast,
            "host_forecast": host_forecast,
            "memory_tb_forecast": mem_forecast,
            "days_to_threshold": {
                "vms_10000": days_to_10k_vms,
                "memory_tb_20": days_to_20tb,
            },
        },
        "anomalies": {"events": anomaly_events},
        "right_sizing_recommendations": right_size_recs,
        "eos_prioritization": eos_priority,
        "consolidation_optimization": consolidation_optimization,
        "dependency_graph": dependency_graph,
        "storage_efficiency": storage_eff,
        "drift_governance": drift,
        "what_if_simulation": what_if,
        "operational_scorecard": scorecard,
        "action_backlog": backlog,
    }

