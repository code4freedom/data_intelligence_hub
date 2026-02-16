#!/bin/bash
# Build and integrate React frontend with FastAPI backend

set -e

cd /app/frontend

echo "Installing dependencies..."
npm install

echo "Building React app..."
npm run build

echo "React build complete. Output in: frontend/dist/"
echo ""
echo "To integrate with FastAPI:"
echo "1. Update app.py to serve frontend/dist instead of frontend"
echo "2. Rebuild the backend Docker image"
echo "3. Deploy to production"
