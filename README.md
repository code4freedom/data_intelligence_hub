# VCF Intelligence Hub

Enterprise RVTools analytics platform with:
- FastAPI backend
- React dashboard
- PDF/PPTX report export
- Project-based data isolation
- Docker-first runtime

## 1. Publish To GitHub

From project root:

```bash
git init
git add .
git commit -m "Initial import: VCF Intelligence Hub"
git branch -M main
git remote add origin https://github.com/<your-org>/<your-repo>.git
git push -u origin main
```

## 2. Run On MacBook (Docker)

Prerequisites:
- Docker Desktop for Mac
- At least 8 GB RAM allocated to Docker

From cloned repo:

```bash
docker compose up -d --build
```

Services:
- Dashboard (Vite): `http://localhost:5173`
- Backend API: `http://localhost:8000`
- API docs: `http://localhost:8000/docs`
- Neo4j: `http://localhost:7474`
- MinIO console: `http://localhost:9001`

Stop:

```bash
docker compose down
```

## 3. Typical Usage

1. Open dashboard: `http://localhost:5173`
2. Create/select customer project.
3. Upload RVTools XLSX and parse.
4. Optionally enable anonymization on upload.
5. Export PDF/PPTX report.

## 4. Data Layout

Runtime data is stored under:

```text
data/projects/<project>/
  raw/
  chunks/
  manifests/
  exports/
  config/
```

## 5. Useful API Endpoints

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

## 6. Notes For Mac

- First start will take longer (`npm install` inside frontend container).
- If dashboard shows stale assets, hard refresh browser (`Cmd+Shift+R`).
- If ports are occupied, change host ports in `docker-compose.yml`.

## 7. Security / Production

Before production:
- Change default database and Neo4j credentials.
- Put the API behind auth/reverse proxy.
- Use external managed volumes/backups for `/data`.
