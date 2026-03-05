# VCF Intelligence Hub

Enterprise RVTools analytics platform with:
- FastAPI backend (Gunicorn + Uvicorn workers)
- React dashboard
- PDF/PPTX report export
- Project-based data isolation
- Docker-first runtime

## 1. Quick Start

### Prerequisites
- Docker Desktop (8 GB+ RAM allocated)
- Copy `.env.example` → `.env` and set real credentials

```bash
cp .env.example .env
# Edit .env with your passwords
docker compose up -d --build
```

### Services
- Dashboard (Vite): `http://localhost:5173`
- Backend API: `http://localhost:8000`
- API docs: `http://localhost:8000/docs`
- Health check: `http://localhost:8000/health`
- Neo4j: `http://localhost:7474`
- MinIO console: `http://localhost:9001`

### Stop
```bash
docker compose down
```

## 2. Typical Usage

1. Open dashboard: `http://localhost:5173`
2. Create/select customer project.
3. Upload RVTools XLSX and parse.
4. Optionally enable anonymization on upload.
5. Export PDF/PPTX report.

## 3. Data Layout

Runtime data is stored under:

```text
data/projects/<project>/
  raw/
  chunks/
  manifests/
  exports/
  config/
```

## 4. Useful API Endpoints

- `GET /health` — Liveness check
- `GET /ready` — Readiness check
- `GET /projects`
- `POST /projects/create`
- `DELETE /projects/{project}`
- `GET /projects/{project}/history`
- `DELETE /projects/{project}/datasets/{manifest_name}`
- `POST /upload`
- `POST /parse`
- `GET /manifests`
- `POST /export/create`
- `GET /kpis`
- `GET /kpis/enterprise`

## 5. Running Tests

```bash
pip install -r requirements.txt
pytest -q --cov=src
```

## 6. Security / Production

- **Authentication:** Add JWT/OAuth2 middleware or deploy behind an authenticating reverse proxy before exposing to the network.
- **Secrets:** All credentials are externalized via `.env`. Never commit `.env` to version control.
- **Uploads:** File uploads are validated for type (.xlsx/.xls only), size, and filename sanitization.
- **Data backups:** Use external managed volumes/backups for `/data`.

## 7. Notes For Mac

- First start will take longer (`npm install` inside frontend container).
- If dashboard shows stale assets, hard refresh browser (`Cmd+Shift+R`).
- If ports are occupied, change host ports in `docker-compose.yml`.
