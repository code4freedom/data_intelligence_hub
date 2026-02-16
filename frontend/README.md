# React Frontend for RVTools Dashboard

A modern, responsive React application for visualizing and managing RVTools infrastructure data.

## Features

- **KPI Dashboard** — Real-time metrics: VMs, hosts, compute, memory, EOS risk
- **Dataset Management** — Browse and select RVTools manifests
- **Export Generation** — Queue PDF & PowerPoint reports with async job tracking
- **Responsive Design** — Built with Tailwind CSS for all screen sizes
- **Live Updates** — Auto-refresh KPIs and job status every 5 seconds

## Tech Stack

- **React 18** — Component framework
- **TypeScript** — Type safety
- **Vite** — Ultra-fast build tool
- **Tailwind CSS** — Utility-first styling
- **Axios** — HTTP client
- **Lucide React** — Icon library

## Development Setup

```bash
cd frontend

# Install dependencies
npm install

# Start dev server (http://localhost:5173)
# Backend at http://localhost:8000 is proxied via Vite config
npm run dev

# Build for production
npm run build

# Preview production build
npm run preview
```

## Proxy Configuration

During development, Vite proxies API requests to the backend:
- `http://localhost:5173/kpis` -> `http://localhost:8000/kpis`
- `http://localhost:5173/manifests` -> `http://localhost:8000/manifests`
- `http://localhost:5173/jobs/*` -> `http://localhost:8000/jobs/*`
- `http://localhost:5173/upload` -> `http://localhost:8000/upload`
- `http://localhost:5173/parse` -> `http://localhost:8000/parse`
- `http://localhost:5173/export/*` -> `http://localhost:8000/export/*`
- `http://localhost:5173/projects/*` -> `http://localhost:8000/projects/*`
- `http://localhost:5173/appendices` -> `http://localhost:8000/appendices`

## Production Build

The production build is optimized for minimal bundle size:

```bash
npm run build
# Output: frontend/dist/
```

To serve the built app, update `src/backend/app.py` to serve `frontend/dist` instead of `frontend`.

## Key Components

- **Dashboard** — Main KPI view + dataset selector + export panel
- **KPI Cards** — Visual metric tiles (VMs, Hosts, CPU, Memory, EOS Risk)
- **Job Tracker** — Real-time export task status
- **System Status** — Overall metrics and health

## Backend API Integration

The dashboard consumes:
- `GET /kpis` — Compute current KPIs
- `GET /manifests` — List available datasets  
- `POST /jobs/create` — Queue export jobs
- `GET /jobs/{job_id}` — Check export status

## Styling

Tailwind CSS with custom configuration in `tailwind.config.js`. Responsive grid layouts for mobile, tablet, and desktop.

## Future Enhancements

- VM data table with filtering/sorting
- Graph visualization (Neo4j integration)
- Real-time alerts for EOS risk
- User preferences & theming
- Dark mode support

