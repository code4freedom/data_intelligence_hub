import os
import json
import logging
import re
import shutil
from pathlib import Path
from typing import List, Optional
import zipfile
import tempfile
from datetime import datetime, timedelta, timezone

import pandas as pd
from fastapi import FastAPI, File, Form, UploadFile, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from jinja2 import Template
from src.rvtools_parser import chunk_and_write
from src.backend.kpis import compute_full_kpis
from src.backend.intelligence import compute_manifest_intelligence
from src.backend.advanced_analytics import compute_advanced_analytics
from src.backend.projects import (
    DATA_DIR,
    ensure_project_dirs,
    get_project_app_mapping_path,
    get_project_settings,
    list_projects,
    normalize_project_name,
    project_dirs,
    update_project_settings,
)
from src.backend.anonymize import anonymize_manifest_chunks
from rq import Queue
from rq.job import Job
import redis
from src.backend import tasks
from src.backend.graph_api import router as graph_router
from src.backend.postgres_loader import load_manifest_into_postgres
from src.backend.neo4j_sync import sync_manifest_to_neo4j
from src.backend.auth import get_current_user, create_access_token, ACCESS_TOKEN_EXPIRE_MINUTES
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Upload constraints
# ---------------------------------------------------------------------------
MAX_UPLOAD_SIZE_MB = int(os.environ.get("MAX_UPLOAD_SIZE_MB", "100"))
MAX_UPLOAD_SIZE_BYTES = MAX_UPLOAD_SIZE_MB * 1024 * 1024
ALLOWED_UPLOAD_EXTENSIONS = {".xlsx", ".xls"}
_UNSAFE_FILENAME_RE = re.compile(r"[^a-zA-Z0-9._-]")


def _secure_filename(name: str) -> str:
    """Sanitize uploaded filename: strip path components, replace unsafe chars."""
    # Take only the final component (prevent path traversal)
    name = Path(name).name
    # Replace unsafe characters
    name = _UNSAFE_FILENAME_RE.sub("_", name)
    # Prevent empty or hidden filenames
    name = name.lstrip("._") or "upload"
    return name


# ---------------------------------------------------------------------------
# Directory setup
# ---------------------------------------------------------------------------
RAW_DIR = DATA_DIR / "raw"
CHUNKS_DIR = DATA_DIR / "chunks"
MANIFESTS_DIR = DATA_DIR / "manifests"
RAW_DIR.mkdir(parents=True, exist_ok=True)
CHUNKS_DIR.mkdir(parents=True, exist_ok=True)
MANIFESTS_DIR.mkdir(parents=True, exist_ok=True)
ensure_project_dirs("default")

# ---------------------------------------------------------------------------
# App initialization
# ---------------------------------------------------------------------------
app = FastAPI(title="VCF Intelligence Hub Backend")
app.mount("/static", StaticFiles(directory="/app/frontend"), name="static")
app.include_router(graph_router, prefix='/api')

# CORS middleware (#7)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Health endpoint (#6)
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ready")
def ready():
    """Readiness check — verifies data directory is accessible."""
    try:
        DATA_DIR.exists()
        return {"status": "ready"}
    except Exception as e:
        logger.error("Readiness check failed: %s", e)
        return JSONResponse(status_code=503, content={"status": "not ready", "error": str(e)})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _project_context(project: Optional[str]):
    return ensure_project_dirs(project or "default")


def _read_dirs_for_project(project: Optional[str]):
    """Use project dirs. For legacy default data, fallback if needed."""
    d = project_dirs(project or "default")
    if d["name"] == "default":
        has_project_data = d["manifests"].exists() and any(d["manifests"].glob("manifest_*.json"))
        has_legacy_data = any(MANIFESTS_DIR.glob("manifest_*.json"))
        if (not has_project_data) and has_legacy_data:
            return {"name": "default", "raw": RAW_DIR, "chunks": CHUNKS_DIR, "manifests": MANIFESTS_DIR, "exports": DATA_DIR / "exports"}
    return d


def _history_from_manifests_dir(manifests_dir: Path):
    items = []
    for mf in sorted(manifests_dir.glob("manifest_*.json"), key=lambda x: x.stat().st_mtime, reverse=True):
        try:
            m = json.loads(mf.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("Could not read manifest %s", mf)
            continue
        ts = m.get("generated_at_utc")
        if not ts:
            ts = datetime.fromtimestamp(mf.stat().st_mtime, tz=timezone.utc).isoformat()
        items.append(
            {
                "manifest_name": mf.name,
                "ingest_id": m.get("ingest_id"),
                "sheet": m.get("sheet"),
                "total_rows": m.get("total_rows"),
                "chunk_count": m.get("chunk_count"),
                "generated_at_utc": ts,
            }
        )
    return items


def _app_mapping_for_project(project: str) -> Optional[str]:
    p = get_project_app_mapping_path(project)
    if p.exists():
        return str(p)
    env_default = os.environ.get("APP_MAPPING_CSV")
    return env_default if env_default else None


def _normalize_export_result(result):
    if not isinstance(result, dict):
        return result
    out = dict(result)
    for key in ("pdf", "png", "pptx"):
        value = out.get(key)
        if not value:
            continue
        p = Path(value)
        if not p.exists():
            continue
        resolved = p.resolve()
        parts = resolved.parts
        if "projects" in parts and "exports" in parts:
            try:
                idx = parts.index("projects")
                project = parts[idx + 1]
                eidx = parts.index("exports")
                rel = Path(*parts[eidx + 1:])
                out[f"{key}_url"] = f"/projects/{project}/exports/{rel.as_posix()}"
                continue
            except Exception:
                logger.warning("Could not resolve export path for %s", value)
        try:
            rel = resolved.relative_to((DATA_DIR / "exports").resolve())
            out[f"{key}_url"] = f"/exports/{rel.as_posix()}"
        except Exception:
            continue
    return out


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    app_pwd = os.environ.get("APP_PASSWORD")
    if not app_pwd:
        # If no password set in env, auto-succeed for local dev convenience
        pass
    elif form_data.password != app_pwd:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": "admin"}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/", response_class=HTMLResponse)
async def index():
    return (Path("/app/frontend/index.html").read_text())


@app.get("/projects")
def get_projects(current_user: str = Depends(get_current_user)):
    return {"projects": list_projects()}


@app.post("/projects/create")
def create_project(name: str = Form(...), anonymize_default: bool = Form(False), current_user: str = Depends(get_current_user)):
    p = normalize_project_name(name)
    d = ensure_project_dirs(p)
    settings = update_project_settings(p, anonymize_default=anonymize_default)
    if not settings.get("anonymize_default", False):
        settings = get_project_settings(p)
    return {"project": p, "path": str(d["base"]), "settings": settings}


@app.get("/projects/{project}/history")
def project_history(project: str, current_user: str = Depends(get_current_user)):
    p = normalize_project_name(project)
    dirs = _read_dirs_for_project(p)
    return {"project": dirs["name"], "history": _history_from_manifests_dir(dirs["manifests"])}


@app.delete("/projects/{project}")
def delete_project(project: str, current_user: str = Depends(get_current_user)):
    p = normalize_project_name(project)
    dirs = project_dirs(p)
    if not dirs["base"].exists():
        return JSONResponse(status_code=404, content={"error": "project not found"})
    try:
        shutil.rmtree(dirs["base"], ignore_errors=False)
    except Exception as e:
        logger.error("Failed to delete project %s: %s", p, e)
        return JSONResponse(status_code=500, content={"error": f"project delete failed: {str(e)}"})
    return {"status": "deleted", "project": p}


@app.delete("/projects/{project}/datasets/{manifest_name}")
def delete_dataset(project: str, manifest_name: str, current_user: str = Depends(get_current_user)):
    p = normalize_project_name(project)
    dirs = _read_dirs_for_project(p)
    mf = dirs["manifests"] / manifest_name
    if not mf.exists():
        return JSONResponse(status_code=404, content={"error": "manifest not found"})

    try:
        m = json.loads(mf.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error("Cannot read manifest %s: %s", mf, e)
        return JSONResponse(status_code=500, content={"error": f"cannot read manifest: {str(e)}"})

    try:
        mf.unlink(missing_ok=True)
    except Exception as e:
        logger.error("Failed to delete manifest file %s: %s", mf, e)
        return JSONResponse(status_code=500, content={"error": f"manifest delete failed: {str(e)}"})

    ingest_id = str(m.get("ingest_id", "")).strip()
    deleted_exports = 0
    if ingest_id:
        exp_dir = dirs["exports"] / ingest_id
        if exp_dir.exists():
            try:
                shutil.rmtree(exp_dir, ignore_errors=False)
                deleted_exports += 1
            except Exception:
                logger.warning("Failed to delete export dir %s", exp_dir)

    referenced = set()
    for other in dirs["manifests"].glob("manifest_*.json"):
        try:
            om = json.loads(other.read_text(encoding="utf-8"))
        except Exception:
            continue
        for ch in om.get("chunks", []) or []:
            lp = str(ch.get("local_path", "")).strip()
            if lp:
                referenced.add(lp)

    deleted_chunks = 0
    for ch in m.get("chunks", []) or []:
        lp = str(ch.get("local_path", "")).strip()
        if not lp or lp in referenced:
            continue
        cp = Path(lp)
        if cp.exists() and cp.is_file():
            try:
                cp.unlink(missing_ok=True)
                deleted_chunks += 1
            except Exception:
                logger.warning("Failed to delete chunk file %s", cp)

    return {
        "status": "deleted",
        "project": dirs["name"],
        "manifest": manifest_name,
        "deleted_chunks": deleted_chunks,
        "deleted_export_dirs": deleted_exports,
    }


@app.get("/appendices")
def get_appendices(current_user: str = Depends(get_current_user)):
    return {"appendices": tasks.APPENDIX_CATALOG}


@app.get("/report-profiles")
def get_report_profiles(current_user: str = Depends(get_current_user)):
    return {"profiles": tasks.REPORT_PROFILES}


@app.get("/projects/{project}/app-mapping")
def get_project_app_mapping(project: str, current_user: str = Depends(get_current_user)):
    p = normalize_project_name(project)
    path = project_dirs(p)["app_mapping"]
    if not path.exists():
        return {"project": p, "path": str(path), "exists": False, "preview": []}
    try:
        df = pd.read_csv(path)
        preview = df.head(20).fillna("").to_dict(orient="records")
        return {
            "project": p,
            "path": str(path),
            "exists": True,
            "rows": int(len(df)),
            "columns": [str(c) for c in df.columns],
            "preview": preview,
        }
    except Exception as e:
        logger.error("Failed to read app mapping for %s: %s", p, e)
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/projects/{project}/app-mapping/upload")
async def upload_project_app_mapping(project: str, file: UploadFile = File(...), current_user: str = Depends(get_current_user)):
    p = normalize_project_name(project)
    if not file.filename.lower().endswith(".csv"):
        return JSONResponse(status_code=400, content={"error": "only .csv files are supported"})
    path = get_project_app_mapping_path(p)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = await file.read()
    path.write_bytes(data)
    return {"project": p, "path": str(path), "bytes": len(data)}


@app.get("/export/html")
def export_html(template: str = "vsphere", project: str = "default", current_user: str = Depends(get_current_user)):
    """Render a sample HTML report template populated with basic KPIs."""
    if template == "vsphere":
        tpl_path = Path("/app/frontend/templates/vsphere_template.html")
        if not tpl_path.exists():
            return JSONResponse(status_code=404, content={"error": "template not found"})
        dirs = _read_dirs_for_project(project)
        k = compute_full_kpis(str(dirs["manifests"]), str(dirs["chunks"]))
        context = {
            "total_vms": k.get("total_vms", 0),
            "total_hosts": k.get("total_hosts", 0),
            "total_compute": k.get("total_compute", 0),
            "total_memory": k.get("total_memory_tb", "--"),
            "eos_risk": k.get("eos_risk", 0),
        }
        tpl_text = tpl_path.read_text(encoding="utf-8")
        rendered = Template(tpl_text).render(**context)
        return HTMLResponse(content=rendered)
    return JSONResponse(status_code=400, content={"error": "unsupported template"})


@app.post("/upload")
async def upload(file: UploadFile = File(...), project: str = Form("default"), current_user: str = Depends(get_current_user)):
    # Validate extension (#4)
    original_name = file.filename or "upload.xlsx"
    ext = Path(original_name).suffix.lower()
    if ext not in ALLOWED_UPLOAD_EXTENSIONS:
        return JSONResponse(
            status_code=400,
            content={"error": f"Unsupported file type '{ext}'. Allowed: {', '.join(ALLOWED_UPLOAD_EXTENSIONS)}"},
        )

    # Sanitize filename (#4)
    safe_name = _secure_filename(original_name)

    dirs = _project_context(project)
    dest = dirs["raw"] / safe_name

    # Read with size limit (#4)
    content = await file.read()
    if len(content) > MAX_UPLOAD_SIZE_BYTES:
        return JSONResponse(
            status_code=413,
            content={"error": f"File too large ({len(content)} bytes). Max allowed: {MAX_UPLOAD_SIZE_MB} MB"},
        )

    with dest.open("wb") as f:
        f.write(content)

    logger.info("Uploaded file %s (%d bytes) to project %s", safe_name, len(content), dirs["name"])
    return {"filename": safe_name, "project": dirs["name"], "stored_path": str(dest)}


@app.post("/parse")
def parse(filename: str = Form(...), sheet: str = Form(...), chunk_size: int = Form(5000),
          upload_s3: bool = Form(False), s3_endpoint: Optional[str] = Form(None),
          s3_access_key: Optional[str] = Form(None), s3_secret_key: Optional[str] = Form(None),
          s3_bucket: Optional[str] = Form(None), ingest_id: Optional[str] = Form(None),
          project: str = Form("default"), anonymize: Optional[bool] = Form(None),
          current_user: str = Depends(get_current_user)):
    dirs = _project_context(project)

    # Sanitize the filename before using it (#3)
    safe_filename = _secure_filename(filename)
    src = dirs["raw"] / safe_filename
    if not src.exists():
        return JSONResponse(status_code=404, content={"error": "file not found"})
    if not ingest_id:
        ingest_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    sheets = [s.strip() for s in str(sheet).split(",") if s.strip()]
    if not sheets:
        return JSONResponse(status_code=400, content={"error": "at least one sheet is required"})

    run_results = []
    parse_warnings = []
    parse_errors = []
    project_settings = get_project_settings(dirs["name"])
    do_anonymize = bool(project_settings.get("anonymize_default", False)) if anonymize is None else bool(anonymize)

    for idx, sh in enumerate(sheets):
        # Call parser in-process instead of subprocess (#3)
        try:
            parsed = chunk_and_write(
                str(src), sh, str(dirs["chunks"]), chunk_size,
                upload_s3=upload_s3,
                s3_endpoint=s3_endpoint,
                s3_access_key=s3_access_key,
                s3_secret_key=s3_secret_key,
                s3_bucket=s3_bucket,
                ingest_id=ingest_id,
            )
        except Exception as e:
            logger.error("Parse failed for sheet %s: %s", sh, e)
            if idx == 0:
                return JSONResponse(status_code=500, content={"error": str(e)})
            parse_warnings.append({"sheet": sh, "error": str(e)})
            continue

        manifest_path = parsed.get("manifest_path")
        if not manifest_path:
            parse_errors.append({"sheet": sh, "error": "manifest path not returned"})
            continue

        mp = Path(manifest_path)
        if not mp.exists():
            parse_errors.append({"sheet": sh, "error": f"manifest file not found: {manifest_path}"})
            continue

        manifest_obj = json.loads(mp.read_text(encoding="utf-8"))
        if do_anonymize:
            manifest_obj = anonymize_manifest_chunks(manifest_obj, seed=f"{dirs['name']}:{manifest_obj.get('ingest_id', 'ingest')}:{sh}")
            mp.write_text(json.dumps(manifest_obj, indent=2), encoding="utf-8")

        dest = dirs["manifests"] / mp.name
        dest.write_text(json.dumps(manifest_obj, indent=2), encoding="utf-8")
        run_results.append(
            {
                "sheet": sh,
                "manifest_name": dest.name,
                "manifest_path": str(dest),
                "manifest": manifest_obj,
            }
        )

    if not run_results:
        return JSONResponse(status_code=500, content={"error": "no sheets parsed successfully", "details": parse_errors or parse_warnings})

    primary_manifest = run_results[0]["manifest"]

    return {
        "manifest": primary_manifest,
        "manifests": [{"sheet": r["sheet"], "manifest_name": r["manifest_name"]} for r in run_results],
        "warnings": parse_warnings,
        "errors": parse_errors,
        "project": dirs["name"],
        "anonymize": do_anonymize,
        "project_anonymize_default": bool(project_settings.get("anonymize_default", False)),
    }


# Redis + RQ setup
redis_url = os.environ.get('REDIS_URL', 'redis://redis:6379')
redis_conn = redis.from_url(redis_url)
q = Queue('default', connection=redis_conn)


@app.post('/jobs/create')
def create_job(
    manifest_name: str = Form(...),
    template: str = Form('vsphere'),
    output_format: str = Form('pdf'),
    project: str = Form("default"),
    appendices: Optional[str] = Form(None),
    report_profile: Optional[str] = Form(None),
    current_user: str = Depends(get_current_user)
):
    dirs = _read_dirs_for_project(project)
    manifest_path = str(dirs["manifests"] / manifest_name)
    if not Path(manifest_path).exists():
        return JSONResponse(status_code=404, content={'error': 'manifest not found'})
    fmt = (output_format or "pdf").lower()
    if fmt not in ("pdf", "pptx", "both"):
        return JSONResponse(status_code=400, content={'error': 'output_format must be pdf, pptx, or both'})
    job = q.enqueue(tasks.generate_report, manifest_path, template, fmt, appendices, report_profile)
    return {'job_id': job.get_id(), 'status': job.get_status(), 'output_format': fmt, 'project': dirs["name"], 'report_profile': report_profile or 'full'}


@app.post('/export/create')
def create_export(
    manifest_name: str = Form(...),
    template: str = Form('vsphere'),
    output_format: str = Form('pdf'),
    project: str = Form("default"),
    appendices: Optional[str] = Form(None),
    report_profile: Optional[str] = Form(None),
    current_user: str = Depends(get_current_user)
):
    """Generate export synchronously and return file immediately."""
    dirs = _read_dirs_for_project(project)
    manifest_path = str(dirs["manifests"] / manifest_name)
    if not Path(manifest_path).exists():
        return JSONResponse(status_code=404, content={'error': 'manifest not found'})

    fmt = (output_format or "pdf").lower()
    if fmt not in ("pdf", "pptx", "both"):
        return JSONResponse(status_code=400, content={'error': 'output_format must be pdf, pptx, or both'})

    try:
        result = tasks.generate_report(manifest_path, template, fmt, appendices, report_profile)
    except Exception as e:
        logger.error("Export generation failed: %s", e)
        return JSONResponse(status_code=500, content={'error': f'export generation failed: {str(e)}'})

    if fmt == "pdf":
        pdf_path = result.get("pdf")
        if not pdf_path or not Path(pdf_path).exists():
            return JSONResponse(status_code=500, content={'error': 'pdf export not generated'})
        return FileResponse(pdf_path, media_type="application/pdf", filename=Path(pdf_path).name)

    if fmt == "pptx":
        pptx_path = result.get("pptx")
        if not pptx_path or not Path(pptx_path).exists():
            return JSONResponse(status_code=500, content={'error': 'pptx export not generated'})
        return FileResponse(
            pptx_path,
            media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
            filename=Path(pptx_path).name,
        )

    # both -> return zip with PDF + PPTX
    pdf_path = result.get("pdf")
    pptx_path = result.get("pptx")
    if not pdf_path or not pptx_path or not Path(pdf_path).exists() or not Path(pptx_path).exists():
        return JSONResponse(status_code=500, content={'error': 'one or more export files not generated'})

    ingest_id = result.get("ingest_id", "ingest")
    out_dir = Path(pdf_path).parent if pdf_path else (dirs["exports"] / ingest_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / f"report_{template}_{ingest_id}.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(pdf_path, arcname=Path(pdf_path).name)
        zf.write(pptx_path, arcname=Path(pptx_path).name)

    return FileResponse(zip_path, media_type="application/zip", filename=zip_path.name)


@app.get('/jobs/{job_id}')
def get_job(job_id: str, current_user: str = Depends(get_current_user)):
    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except Exception:
        return JSONResponse(status_code=404, content={'error': 'job not found'})
    res = {'id': job.get_id(), 'status': job.get_status(), 'result': _normalize_export_result(job.result)}
    return res


@app.get("/exports/{export_path:path}")
def download_export(export_path: str, current_user: str = Depends(get_current_user)):
    base = (DATA_DIR / "exports").resolve()
    candidate = (base / export_path).resolve()
    try:
        candidate.relative_to(base)
    except Exception:
        return JSONResponse(status_code=400, content={"error": "invalid export path"})
    if not candidate.exists() or not candidate.is_file():
        return JSONResponse(status_code=404, content={"error": "export not found"})
    return FileResponse(str(candidate), filename=candidate.name)


@app.get("/projects/{project}/exports/{export_path:path}")
def download_export_for_project(project: str, export_path: str, current_user: str = Depends(get_current_user)):
    dirs = project_dirs(project)
    base = dirs["exports"].resolve()
    candidate = (base / export_path).resolve()
    try:
        candidate.relative_to(base)
    except Exception:
        return JSONResponse(status_code=400, content={"error": "invalid export path"})
    if not candidate.exists() or not candidate.is_file():
        return JSONResponse(status_code=404, content={"error": "export not found"})
    return FileResponse(str(candidate), filename=candidate.name)


@app.post('/load_manifest')
def load_manifest(manifest_name: str = Form(...), project: str = Form("default"), current_user: str = Depends(get_current_user)):
    dirs = _read_dirs_for_project(project)
    manifest_path = str(dirs["manifests"] / manifest_name)
    if not Path(manifest_path).exists():
        return JSONResponse(status_code=404, content={'error': 'manifest not found'})
    try:
        load_manifest_into_postgres(manifest_path)
    except Exception as e:
        logger.error("Postgres load failed: %s", e)
        return JSONResponse(status_code=500, content={'error': str(e)})
    try:
        sync_manifest_to_neo4j(manifest_path)
    except Exception as e:
        logger.error("Neo4j sync failed: %s", e)
        return JSONResponse(status_code=500, content={'error': 'neo4j sync failed: ' + str(e)})
    return {'status': 'ok'}


@app.get("/manifests")
def list_manifests(project: str = "default", current_user: str = Depends(get_current_user)):
    dirs = _read_dirs_for_project(project)
    paths = list(dirs["manifests"].glob("manifest_*.json"))
    return {"project": dirs["name"], "manifests": [p.name for p in sorted(paths, key=lambda x: x.stat().st_mtime, reverse=True)]}


@app.get("/manifests/{name}")
def get_manifest(name: str, project: str = "default", current_user: str = Depends(get_current_user)):
    dirs = _read_dirs_for_project(project)
    p = dirs["manifests"] / name
    if not p.exists():
        return JSONResponse(status_code=404, content={"error": "manifest not found"})
    return json.loads(p.read_text(encoding="utf-8"))


@app.get("/kpis")
def kpis(sheet: str = "vInfo", project: str = "default", current_user: str = Depends(get_current_user)):
    """Compute aggregated KPI metrics across all manifests and chunks."""
    try:
        dirs = _read_dirs_for_project(project)
        k = compute_full_kpis(str(dirs["manifests"]), str(dirs["chunks"]))
        return {
            "total_vms": k.get("total_vms", 0),
            "total_hosts": k.get("total_hosts", 0),
            "total_compute": k.get("total_compute", 0),
            "total_memory_tb": k.get("total_memory_tb", "0.00"),
            "eos_risk": k.get("eos_risk", 0),
        }
    except Exception as e:
        logger.error("KPI computation error: %s", e)
        return {
            "total_vms": 0,
            "total_hosts": 0,
            "total_compute": 0,
            "total_memory_tb": "0.00",
            "eos_risk": 0,
        }


@app.get("/kpis/enterprise")
def kpis_enterprise(manifest_name: Optional[str] = None, project: str = "default", current_user: str = Depends(get_current_user)):
    dirs = _read_dirs_for_project(project)
    manifests = sorted(dirs["manifests"].glob("manifest_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not manifests:
        return JSONResponse(status_code=404, content={"error": "no manifests found"})

    if manifest_name:
        target = dirs["manifests"] / manifest_name
        if not target.exists():
            return JSONResponse(status_code=404, content={"error": "manifest not found"})
    else:
        target = manifests[0]

    try:
        intelligence = compute_manifest_intelligence(
            str(target),
            str(dirs["manifests"]),
            str(dirs["chunks"]),
            app_map_csv=_app_mapping_for_project(dirs["name"]),
        )
        advanced = compute_advanced_analytics(
            str(target),
            str(dirs["manifests"]),
            str(dirs["chunks"]),
        )
    except Exception as e:
        logger.error("Enterprise KPI computation failed: %s", e)
        return JSONResponse(status_code=500, content={"error": str(e)})
    return {"project": dirs["name"], "manifest": target.name, "intelligence": intelligence, "advanced": advanced}


@app.get("/analytics/full")
def analytics_full(
    project: str = "default",
    manifest_name: Optional[str] = None,
    forecast_horizon: int = 3,
    current_user: str = Depends(get_current_user),
):
    dirs = _read_dirs_for_project(project)
    manifests = sorted(dirs["manifests"].glob("manifest_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not manifests:
        return JSONResponse(status_code=404, content={"error": "no manifests found"})
    if manifest_name:
        target = dirs["manifests"] / manifest_name
        if not target.exists():
            return JSONResponse(status_code=404, content={"error": "manifest not found"})
    else:
        target = manifests[0]
    try:
        result = compute_advanced_analytics(
            str(target),
            str(dirs["manifests"]),
            str(dirs["chunks"]),
            forecast_horizon=max(1, min(24, forecast_horizon)),
        )
    except Exception as e:
        logger.error("Full analytics computation failed: %s", e)
        return JSONResponse(status_code=500, content={"error": str(e)})
    return {"project": dirs["name"], "manifest": target.name, "analytics": result}


@app.get("/analytics/whatif")
def analytics_whatif(
    project: str = "default",
    manifest_name: Optional[str] = None,
    growth_pct: float = 20.0,
    target_vcpu_pcpu: float = 4.0,
    current_user: str = Depends(get_current_user),
):
    dirs = _read_dirs_for_project(project)
    manifests = sorted(dirs["manifests"].glob("manifest_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not manifests:
        return JSONResponse(status_code=404, content={"error": "no manifests found"})
    if manifest_name:
        target = dirs["manifests"] / manifest_name
        if not target.exists():
            return JSONResponse(status_code=404, content={"error": "manifest not found"})
    else:
        target = manifests[0]

    try:
        result = compute_advanced_analytics(
            str(target),
            str(dirs["manifests"]),
            str(dirs["chunks"]),
            what_if_growth_pct=growth_pct,
            consolidation_target_vcpu_pcpu=target_vcpu_pcpu,
        )
    except Exception as e:
        logger.error("What-if analysis failed: %s", e)
        return JSONResponse(status_code=500, content={"error": str(e)})
    return {
        "project": dirs["name"],
        "manifest": target.name,
        "what_if": result.get("what_if_simulation", {}),
        "consolidation_optimization": result.get("consolidation_optimization", {}),
    }


@app.get("/export/csv")
def export_csv(manifest: str, project: str = "default", current_user: str = Depends(get_current_user)):
    """Export the parsed datasets as raw CSV files in a ZIP archive."""
    dirs = _read_dirs_for_project(project)
    
    if not manifest.endswith(".json"):
        manifest += ".json"
        
    m_path = dirs["manifests"] / manifest
    if not m_path.exists():
        return JSONResponse(status_code=404, content={"error": "manifest not found"})
        
    try:
        m_data = json.loads(m_path.read_text(encoding="utf-8"))
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"invalid manifest: {str(e)}"})
        
    chunks = m_data.get("chunks", [])
    if not chunks:
        return JSONResponse(status_code=400, content={"error": "manifest has no chunks"})
        
    ingest_id = m_data.get("ingest_id", "dataset")
    sheet_name = m_data.get("sheet", "vInfo")
    
    out_dir = dirs["exports"] / ingest_id
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_filename = f"csv_export_{ingest_id}_{sheet_name}.zip"
    zip_path = out_dir / zip_filename
    
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for idx, chunk in enumerate(chunks):
                    cp = Path(chunk.get("local_path", ""))
                    if not cp.exists():
                        continue
                    try:
                        df = pd.read_parquet(cp)
                        csv_name = f"{sheet_name}_part{idx+1:03d}.csv"
                        csv_path = tmp_path / csv_name
                        df.to_csv(csv_path, index=False)
                        zf.write(csv_path, arcname=csv_name)
                    except Exception as e:
                        logger.warning("Failed to convert chunk %s to CSV: %s", cp.name, e)
    except Exception as e:
        logger.error("CSV export generation failed: %s", e)
        return JSONResponse(status_code=500, content={"error": f"CSV generation failed: {str(e)}"})
        
    if not zip_path.exists():
        return JSONResponse(status_code=500, content={"error": "failed to create zip file"})
        
    return FileResponse(zip_path, media_type="application/zip", filename=zip_filename)


@app.get("/export/pdf")
def export_pdf(sheet: str = "vInfo", current_user: str = Depends(get_current_user)):
    k = kpis(sheet)
    out_pdf = DATA_DIR / f"report_{sheet}.pdf"
    c = canvas.Canvas(str(out_pdf), pagesize=letter)
    c.setFont("Helvetica", 14)
    c.drawString(72, 720, "VCF Intelligence Hub Quick Report")
    c.setFont("Helvetica", 12)
    c.drawString(72, 700, f"Sheet: {sheet}")
    c.drawString(72, 680, f"Chunks: {k.get('chunks', 0)}")
    c.drawString(72, 660, f"Rows: {k.get('rows', 0)}")
    c.drawString(72, 640, f"Author: {os.environ.get('REPORT_AUTHOR', 'VCF Intelligence Hub')}")
    c.save()
    return FileResponse(str(out_pdf), media_type="application/pdf", filename=out_pdf.name)
