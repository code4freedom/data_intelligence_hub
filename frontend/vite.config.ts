import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

const apiProxyTarget = process.env.VITE_API_PROXY_TARGET || 'http://localhost:8000'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/kpis': {
        target: apiProxyTarget,
        changeOrigin: true
      },
      '/manifests': {
        target: apiProxyTarget,
        changeOrigin: true
      },
      '/jobs': {
        target: apiProxyTarget,
        changeOrigin: true
      },
      '/upload': {
        target: apiProxyTarget,
        changeOrigin: true
      },
      '/parse': {
        target: apiProxyTarget,
        changeOrigin: true
      },
      '/export': {
        target: apiProxyTarget,
        changeOrigin: true
      },
      '/projects': {
        target: apiProxyTarget,
        changeOrigin: true
      },
      '/appendices': {
        target: apiProxyTarget,
        changeOrigin: true
      },
      '/report-profiles': {
        target: apiProxyTarget,
        changeOrigin: true
      }
    }
  },
  build: {
    outDir: 'dist',
    sourcemap: false
  }
})
