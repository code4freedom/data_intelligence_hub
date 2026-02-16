import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


def _to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def _find_col(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    cols = {str(c).lower(): c for c in df.columns}
    for a in aliases:
        if a.lower() in cols:
            return cols[a.lower()]
    for c_l, c_o in cols.items():
        for a in aliases:
            if a.lower() in c_l:
                return c_o
    return None


def _parse_manifest_time(manifest_path: Path, manifest_obj: Dict) -> datetime:
    ts = manifest_obj.get("generated_at_utc")
    if ts:
        try:
            return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            pass
    return datetime.fromtimestamp(manifest_path.stat().st_mtime, tz=timezone.utc)


def _load_app_mapping_csv(path: Optional[str]) -> List[Dict]:
    if not path:
        return []
    p = Path(path)
    if not p.exists():
        return []
    try:
        df = pd.read_csv(p)
    except Exception:
        return []
    if "pattern" not in df.columns or "application" not in df.columns:
        return []
    rows: List[Dict] = []
    for _, r in df.iterrows():
        pat = str(r["pattern"]).strip()
        app = str(r["application"]).strip()
        if not pat or not app:
            continue
        rows.append(
            {
                "pattern": pat,
                "application": app,
                "priority": int(float(r.get("priority", 100))) if str(r.get("priority", "")).strip() else 100,
                "owner": str(r.get("owner", "")).strip() or None,
                "criticality": str(r.get("criticality", "")).strip() or None,
            }
        )
    rows.sort(key=lambda x: x.get("priority", 100))
    return rows


def _infer_application(vm_name, mappings: List[Dict]) -> Dict:
    n = str(vm_name).strip().lower()
    if n in ("", "nan", "<na>", "none"):
        return {"application": "Unclassified", "source": "none", "owner": None, "criticality": None}
    for rule in mappings:
        pat = rule.get("pattern", "")
        app = rule.get("application", "Unclassified")
        try:
            if re.search(str(pat), str(vm_name), flags=re.IGNORECASE):
                return {
                    "application": app,
                    "source": "mapping",
                    "owner": rule.get("owner"),
                    "criticality": rule.get("criticality"),
                }
        except Exception:
            if str(pat).lower() in n:
                return {
                    "application": app,
                    "source": "mapping",
                    "owner": rule.get("owner"),
                    "criticality": rule.get("criticality"),
                }

    heuristics = [
        (r"(sql|oracle|postgres|mysql|db)", "Database"),
        (r"(web|nginx|apache|iis)", "Web"),
        (r"(app|api|svc|service)", "Application"),
        (r"(vdi|horizon|citrix)", "VDI"),
        (r"(backup|veeam|commvault)", "Backup"),
        (r"(k8s|kube|openshift)", "Container"),
        (r"(ad|domain|dns|dhcp)", "Identity"),
    ]
    for pat, app in heuristics:
        if re.search(pat, n):
            return {"application": app, "source": "heuristic", "owner": None, "criticality": None}
    return {"application": "Unclassified", "source": "heuristic", "owner": None, "criticality": None}


def _eos_from_os(os_val: str) -> int:
    v = str(os_val).lower()
    risky_patterns = [
        "windows 2003",
        "windows 2008",
        "windows 2012",
        "rhel 5",
        "rhel 6",
        "centos 6",
        "ubuntu 14",
        "ubuntu 16",
        "sles 11",
        "debian 8",
    ]
    return 1 if any(p in v for p in risky_patterns) else 0


def _safe_first(series: pd.Series):
    try:
        s = series.dropna()
        if len(s) == 0:
            return None
        v = s.iloc[0]
        if isinstance(v, str):
            vv = v.strip()
            return vv if vv else None
        return v
    except Exception:
        return None


def _mode_or_none(series: pd.Series):
    try:
        s = series.dropna().astype(str)
        if len(s) == 0:
            return None
        m = s.mode()
        if len(m) == 0:
            return None
        v = str(m.iloc[0]).strip()
        return v if v else None
    except Exception:
        return None


def _load_manifest_df_chunks(manifest_path: Path, chunks_dir: Path) -> List[pd.DataFrame]:
    m = json.loads(manifest_path.read_text(encoding="utf-8"))
    frames: List[pd.DataFrame] = []
    for ch in m.get("chunks", []):
        lp = Path(ch.get("local_path", ""))
        p = lp if lp.exists() else (chunks_dir / str(ch.get("name", "")))
        if not p.exists():
            continue
        try:
            frames.append(pd.read_parquet(p))
        except Exception:
            continue
    return frames


def compute_manifest_intelligence(
    manifest_path: str,
    manifests_dir: str,
    chunks_dir: str,
    app_map_csv: Optional[str] = None,
) -> Dict:
    mp = Path(manifest_path)
    if not mp.exists():
        raise FileNotFoundError(manifest_path)
    manifest = json.loads(mp.read_text(encoding="utf-8"))
    cdir = Path(chunks_dir)
    mdir = Path(manifests_dir)

    frames = _load_manifest_df_chunks(mp, cdir)
    if not frames:
        return {
            "summary": {
                "total_vms": manifest.get("total_rows", 0),
                "total_hosts": 0,
                "total_clusters": 0,
                "total_vcpu": 0,
                "total_memory_tb": 0.0,
            },
            "executive_score": 0,
            "executive_components": {},
            "consolidation": {},
            "lifecycle": {},
            "performance": {},
            "application": {},
            "trends": [],
            "insights": [],
        }

    df = pd.concat(frames, ignore_index=True)
    vm_col = _find_col(df, ["VM", "Name", "VM name"])
    host_col = _find_col(df, ["Host", "ESXi host", "Hostname"])
    cluster_col = _find_col(df, ["Cluster"])
    cpu_col = _find_col(df, ["CPUs", "NumCPU", "vCPU", "CPU"])
    mem_col = _find_col(df, ["Memory", "MemoryMB", "Mem"])
    pwr_col = _find_col(df, ["Powerstate", "Power State"])
    templ_col = _find_col(df, ["Template"])
    prov_col = _find_col(df, ["Provisioned MB", "Provisioned"])
    inuse_col = _find_col(df, ["In Use MB", "Used MB", "InUseMB"])
    os_cfg_col = _find_col(df, ["OS according to the configuration file", "OS"])
    os_tools_col = _find_col(df, ["OS according to the VMware Tools", "Guest OS"])

    total_vms = int(len(df))
    hosts = df[host_col].dropna().astype(str).nunique() if host_col else 0
    clusters = df[cluster_col].dropna().astype(str).nunique() if cluster_col else 0
    total_vcpu = int(_to_num(df[cpu_col]).sum()) if cpu_col else 0
    total_mem_mb = float(_to_num(df[mem_col]).sum()) if mem_col else 0.0
    total_mem_tb = total_mem_mb / (1024.0 * 1024.0) if total_mem_mb else 0.0

    power_off = 0
    if pwr_col:
        s = df[pwr_col].astype(str).str.lower()
        power_off = int(s.str.contains("off").sum())
    power_off_pct = (100.0 * power_off / total_vms) if total_vms else 0.0

    template_count = 0
    if templ_col:
        s = df[templ_col].astype(str).str.lower()
        template_count = int((s == "true").sum() + s.str.contains("template").sum())
    template_pct = (100.0 * template_count / total_vms) if total_vms else 0.0

    eos_count = 0
    if os_cfg_col:
        eos_count += int(df[os_cfg_col].astype(str).map(_eos_from_os).sum())
    if os_tools_col:
        eos_count += int(df[os_tools_col].astype(str).map(_eos_from_os).sum())
    eos_count = min(eos_count, total_vms)
    eos_pct = (100.0 * eos_count / total_vms) if total_vms else 0.0

    provisioned_mb = float(_to_num(df[prov_col]).sum()) if prov_col else 0.0
    inuse_mb = float(_to_num(df[inuse_col]).sum()) if inuse_col else 0.0
    storage_overprov_ratio = (provisioned_mb / inuse_mb) if inuse_mb > 0 else 0.0

    vm_per_host = (total_vms / hosts) if hosts else 0.0
    est_pcpu = hosts * 32
    est_pmem_mb = hosts * 262144
    vcpu_pcpu_ratio = (total_vcpu / est_pcpu) if est_pcpu else 0.0
    vmem_pmem_ratio = (total_mem_mb / est_pmem_mb) if est_pmem_mb else 0.0

    # Right-sizing opportunity heuristics.
    right_size_count = 0
    reclaim_vcpu = 0.0
    reclaim_mem_mb = 0.0
    if cpu_col and mem_col:
        oversized = df[
            (_to_num(df[cpu_col]) >= 8) &
            (_to_num(df[mem_col]) >= 16384) &
            (df[pwr_col].astype(str).str.lower() == "poweredon" if pwr_col else True)
        ]
        right_size_count = int(len(oversized))
        reclaim_vcpu = float((_to_num(oversized[cpu_col]) * 0.25).sum()) if len(oversized) else 0.0
        reclaim_mem_mb = float((_to_num(oversized[mem_col]) * 0.20).sum()) if len(oversized) else 0.0

    # Top cluster density and pressure.
    cluster_density = []
    if cluster_col and host_col:
        by_cluster = (
            df.groupby(cluster_col)
            .agg(vms=(vm_col if vm_col else host_col, "count"), hosts=(host_col, "nunique"))
            .reset_index()
        )
        by_cluster["vm_per_host"] = by_cluster["vms"] / by_cluster["hosts"].replace(0, 1)
        by_cluster = by_cluster.sort_values("vm_per_host", ascending=False)
        for _, r in by_cluster.head(10).iterrows():
            cluster_density.append(
                {
                    "cluster": str(r[cluster_col]),
                    "vms": int(r["vms"]),
                    "hosts": int(r["hosts"]),
                    "vm_per_host": round(float(r["vm_per_host"]), 2),
                }
            )

    # Application intelligence.
    app_map = _load_app_mapping_csv(app_map_csv)
    app_summary: Dict[str, int] = {}
    top_apps: List[Dict] = []
    source_counts = {"mapping": 0, "heuristic": 0, "none": 0}
    owner_counts: Dict[str, int] = {}
    criticality_counts: Dict[str, int] = {}
    if vm_col:
        app_meta_series = df[vm_col].map(lambda v: _infer_application(v, app_map))
        app_series = app_meta_series.map(lambda x: x.get("application", "Unclassified"))
        src_series = app_meta_series.map(lambda x: x.get("source", "none"))
        own_series = app_meta_series.map(lambda x: x.get("owner"))
        crit_series = app_meta_series.map(lambda x: x.get("criticality"))
        for k, v in src_series.value_counts().to_dict().items():
            source_counts[str(k)] = int(v)
        owner_counts = {str(k): int(v) for k, v in own_series.dropna().value_counts().head(10).to_dict().items()}
        criticality_counts = {str(k): int(v) for k, v in crit_series.dropna().value_counts().to_dict().items()}
        vc = app_series.value_counts()
        app_summary = {str(k): int(v) for k, v in vc.to_dict().items()}
        top_apps = [{"application": str(k), "vm_count": int(v)} for k, v in vc.head(10).items()]

    # Historical trends across manifests.
    trend_rows: List[Dict] = []
    for manifest_file in sorted(mdir.glob("manifest_*.json")):
        try:
            mobj = json.loads(manifest_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(mobj.get("sheet", "")).lower() != "vinfo":
            continue
        frames_hist = _load_manifest_df_chunks(manifest_file, cdir)
        if not frames_hist:
            continue
        dfx = pd.concat(frames_hist, ignore_index=True)
        hc = _find_col(dfx, ["Host", "Hostname"])
        ccpu = _find_col(dfx, ["CPUs", "NumCPU", "vCPU", "CPU"])
        cmem = _find_col(dfx, ["Memory", "MemoryMB"])
        cos = _find_col(dfx, ["OS according to the configuration file", "OS"])
        eosx = int(dfx[cos].astype(str).map(_eos_from_os).sum()) if cos else 0
        t = _parse_manifest_time(manifest_file, mobj)
        trend_rows.append(
            {
                "ingest_id": str(mobj.get("ingest_id", manifest_file.stem)),
                "timestamp": t.isoformat(),
                "total_vms": int(len(dfx)),
                "total_hosts": int(dfx[hc].dropna().astype(str).nunique()) if hc else 0,
                "total_vcpu": int(_to_num(dfx[ccpu]).sum()) if ccpu else 0,
                "total_memory_tb": round(float(_to_num(dfx[cmem]).sum()) / (1024 * 1024), 2) if cmem else 0.0,
                "eos_risk_vms": eosx,
            }
        )
    trend_rows = sorted(trend_rows, key=lambda x: x["timestamp"])

    # vCenter -> Cluster -> Node logical view (best effort based on available columns).
    vcenter_col = _find_col(df, ["vCenter", "VC", "vCenter Server"])
    dc_col = _find_col(df, ["Datacenter", "Data Center"])
    host_model_col = _find_col(df, ["Model", "Hardware Model", "Server Model"])
    host_version_col = _find_col(df, ["Version", "ESX Version", "Host Version"])
    if not host_version_col:
        host_version_col = _find_col(df, ["vSphere Version", "ESXi Version"])

    root_col = vcenter_col or dc_col
    working = df.copy()
    if root_col:
        working["_root"] = working[root_col].astype(str).replace("nan", "Unspecified")
    else:
        working["_root"] = "vCenter-Unknown"
    working["_cluster"] = working[cluster_col].astype(str).replace("nan", "Unspecified-Cluster") if cluster_col else "Unspecified-Cluster"
    working["_host"] = working[host_col].astype(str).replace("nan", "Unspecified-Host") if host_col else "Unspecified-Host"
    working["_cpu"] = _to_num(working[cpu_col]) if cpu_col else 0
    working["_mem"] = _to_num(working[mem_col]) if mem_col else 0
    if pwr_col:
        working["_off"] = working[pwr_col].astype(str).str.lower().str.contains("off")
    else:
        working["_off"] = False

    host_group = working.groupby(["_root", "_cluster", "_host"], dropna=False)
    node_rows = []
    for (root_name, cluster_name, host_name), g in host_group:
        vm_count = int(len(g))
        alloc_vcpu = int(g["_cpu"].sum()) if cpu_col else 0
        alloc_mem_gb = round(float(g["_mem"].sum()) / 1024.0, 1) if mem_col else 0.0
        off_vms = int(g["_off"].sum())
        top_os = _mode_or_none(g[os_cfg_col]) if os_cfg_col else None
        ver = _safe_first(g[host_version_col]) if host_version_col else None
        model = _safe_first(g[host_model_col]) if host_model_col else None
        hardware_details = f"Observed VM Allocation: {alloc_vcpu} vCPU / {alloc_mem_gb} GB"
        if model:
            hardware_details = f"{model} | {hardware_details}"
        node_rows.append(
            {
                "vcenter": str(root_name),
                "cluster": str(cluster_name),
                "host": str(host_name),
                "vm_count": vm_count,
                "allocated_vcpu": alloc_vcpu,
                "allocated_memory_gb": alloc_mem_gb,
                "powered_off_vms": off_vms,
                "vsphere_version": str(ver) if ver is not None else "Not available in uploaded sheet",
                "hardware_details": hardware_details,
                "top_os_family": top_os or "Unknown",
            }
        )

    logical_vcenters: List[Dict] = []
    if node_rows:
        nd = pd.DataFrame(node_rows)
        for vc_name, vc_df in nd.groupby("vcenter", dropna=False):
            vc_clusters: List[Dict] = []
            for c_name, c_df in vc_df.groupby("cluster", dropna=False):
                vm_sum = int(c_df["vm_count"].sum())
                host_count = int(c_df["host"].nunique())
                vm_per_host = round(vm_sum / host_count, 2) if host_count else 0.0
                risk_band = "Normal"
                if vm_per_host >= 45:
                    risk_band = "High Density"
                elif vm_per_host >= 30:
                    risk_band = "Elevated"
                node_records = c_df.sort_values("vm_count", ascending=False).to_dict(orient="records")
                vc_clusters.append(
                    {
                        "name": str(c_name),
                        "host_count": host_count,
                        "vm_count": vm_sum,
                        "vm_per_host": vm_per_host,
                        "risk_band": risk_band,
                        "nodes": node_records[:10],
                        "nodes_hidden_count": max(0, len(node_records) - 10),
                    }
                )
            vc_clusters = sorted(vc_clusters, key=lambda x: x.get("vm_count", 0), reverse=True)
            logical_vcenters.append(
                {
                    "name": str(vc_name),
                    "cluster_count": len(vc_clusters),
                    "vm_count": int(vc_df["vm_count"].sum()),
                    "host_count": int(vc_df["host"].nunique()),
                    "clusters": vc_clusters,
                }
            )
        logical_vcenters = sorted(logical_vcenters, key=lambda x: x.get("vm_count", 0), reverse=True)

    growth = {}
    if len(trend_rows) >= 2:
        a = trend_rows[-2]
        b = trend_rows[-1]
        def _delta(k: str) -> Dict:
            av = float(a.get(k, 0))
            bv = float(b.get(k, 0))
            diff = bv - av
            pct = (diff / av * 100.0) if av else 0.0
            return {"delta": round(diff, 2), "pct": round(pct, 2)}

        growth = {
            "vms": _delta("total_vms"),
            "hosts": _delta("total_hosts"),
            "vcpu": _delta("total_vcpu"),
            "memory_tb": _delta("total_memory_tb"),
            "eos_risk_vms": _delta("eos_risk_vms"),
        }

    # Executive risk score.
    eos_component = min(eos_pct, 100.0) * 0.40
    density_component = min(max((vm_per_host - 30) * 2, 0), 100.0) * 0.20
    cpu_component = min(max((vcpu_pcpu_ratio - 4.0) * 20, 0), 100.0) * 0.20
    power_component = min(power_off_pct, 100.0) * 0.10
    storage_component = min(max((storage_overprov_ratio - 1.5) * 50, 0), 100.0) * 0.10
    executive_score = round(eos_component + density_component + cpu_component + power_component + storage_component, 1)

    insights: List[str] = []
    if eos_pct > 15:
        insights.append("High lifecycle risk: significant VM population appears on end-of-support guest OS versions.")
    if vcpu_pcpu_ratio > 6:
        insights.append("CPU oversubscription risk is elevated; review cluster headroom and noisy-neighbor exposure.")
    if storage_overprov_ratio > 2:
        insights.append("Storage over-provisioning is high; thin/thick policy and stale allocation cleanup recommended.")
    if right_size_count > 0:
        insights.append(
            f"Right-sizing opportunity identified on {right_size_count} VMs; estimated reclaim "
            f"{int(reclaim_vcpu)} vCPU and {reclaim_mem_mb / 1024.0:.1f} GB RAM."
        )
    if not insights:
        insights.append("No critical risk outliers detected from current RVTools snapshot.")

    return {
        "summary": {
            "total_vms": total_vms,
            "total_hosts": hosts,
            "total_clusters": clusters,
            "total_vcpu": total_vcpu,
            "total_memory_tb": round(total_mem_tb, 2),
            "powered_off_vms": power_off,
            "templates": template_count,
        },
        "executive_score": executive_score,
        "executive_components": {
            "eos_risk_pct": round(eos_pct, 2),
            "vm_density_vm_per_host": round(vm_per_host, 2),
            "vcpu_pcpu_ratio": round(vcpu_pcpu_ratio, 2),
            "vmem_pmem_ratio": round(vmem_pmem_ratio, 2),
            "powered_off_pct": round(power_off_pct, 2),
            "storage_overprov_ratio": round(storage_overprov_ratio, 2),
        },
        "consolidation": {
            "vm_per_host": round(vm_per_host, 2),
            "vcpu_pcpu_ratio": round(vcpu_pcpu_ratio, 2),
            "vmem_pmem_ratio": round(vmem_pmem_ratio, 2),
            "cluster_density_top": cluster_density,
        },
        "lifecycle": {
            "eos_risk_vms": int(eos_count),
            "eos_risk_pct": round(eos_pct, 2),
            "template_pct": round(template_pct, 2),
        },
        "performance": {
            "right_size_candidates": int(right_size_count),
            "estimated_reclaim_vcpu": round(reclaim_vcpu, 1),
            "estimated_reclaim_memory_gb": round(reclaim_mem_mb / 1024.0, 1),
            "storage_overprov_ratio": round(storage_overprov_ratio, 2),
        },
        "application": {
            "top_app_groups": top_apps,
            "app_group_counts": app_summary,
            "mapping_source": str(app_map_csv) if app_map_csv else None,
            "mapped_count": int(source_counts.get("mapping", 0)),
            "heuristic_count": int(source_counts.get("heuristic", 0)),
            "unclassified_count": int(app_summary.get("Unclassified", 0)),
            "mapping_coverage_pct": round((source_counts.get("mapping", 0) / total_vms * 100.0), 2) if total_vms else 0.0,
            "owner_distribution_top": owner_counts,
            "criticality_distribution": criticality_counts,
        },
        "logical_view": {
            "vcenters": logical_vcenters,
            "available_columns": {
                "vcenter": vcenter_col,
                "datacenter": dc_col,
                "cluster": cluster_col,
                "host": host_col,
                "host_version": host_version_col,
                "host_model": host_model_col,
            },
            "notes": [
                "vSphere version and hardware model depend on available columns in uploaded sheet.",
                "When host inventory columns are missing, allocation-based hardware details are inferred from VM footprint.",
            ],
        },
        "trends": trend_rows,
        "trend_growth": growth,
        "insights": insights,
    }
