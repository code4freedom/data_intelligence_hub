import { Download, Zap, Upload, AlertCircle } from 'lucide-react'
import { useEffect, useState } from 'react'
import axios from 'axios'
import toast from 'react-hot-toast'

interface KPI {
  total_vms: number
  total_hosts: number
  total_compute: number
  total_memory_tb: string
  eos_risk: number
}

interface Manifest {
  name: string
}

interface ProjectItem {
  name: string
  manifest_count: number
  anonymize_default?: boolean
}

interface HistoryItem {
  manifest_name: string
  ingest_id?: string
  sheet?: string
  total_rows?: number
  chunk_count?: number
  generated_at_utc?: string
}

interface EnterpriseData {
  executive_score?: number
  executive_components?: {
    eos_risk_pct?: number
    vcpu_pcpu_ratio?: number
    vmem_pmem_ratio?: number
  }
  performance?: {
    right_size_candidates?: number
    estimated_reclaim_vcpu?: number
    estimated_reclaim_memory_gb?: number
  }
  trend_growth?: {
    vms?: { delta?: number; pct?: number }
  }
  application?: {
    mapping_coverage_pct?: number
    mapped_count?: number
    heuristic_count?: number
    unclassified_count?: number
  }
}

interface AppendixItem {
  id: string
  title: string
}

interface ReportProfile {
  id: string
  title: string
  description?: string
}

interface AdvancedData {
  forecasting?: {
    days_to_threshold?: { vms_10000?: number | null; memory_tb_20?: number | null }
  }
  anomalies?: { events?: Array<any> }
  consolidation_optimization?: { retireable_hosts_estimate?: number; target_hosts?: number }
  operational_scorecard?: { risk_score?: number; efficiency_score?: number; lifecycle_score?: number }
}

function Dashboard() {
  const [kpis, setKpis] = useState<KPI | null>(null)
  const [enterprise, setEnterprise] = useState<EnterpriseData | null>(null)
  const [advanced, setAdvanced] = useState<AdvancedData | null>(null)
  const [projects, setProjects] = useState<ProjectItem[]>([])
  const [selectedProject, setSelectedProject] = useState<string>('default')
  const [newProject, setNewProject] = useState<string>('')
  const [newProjectAnonymize, setNewProjectAnonymize] = useState<boolean>(false)
  const [uploadAnonymize, setUploadAnonymize] = useState<boolean>(false)
  const [manifests, setManifests] = useState<Manifest[]>([])
  const [history, setHistory] = useState<HistoryItem[]>([])
  const [appendices, setAppendices] = useState<AppendixItem[]>([])
  const [selectedAppendices, setSelectedAppendices] = useState<string[]>([])
  const [profiles, setProfiles] = useState<ReportProfile[]>([])
  const [reportProfile, setReportProfile] = useState<string>('full')
  const [mappingFile, setMappingFile] = useState<File | null>(null)
  const [selectedManifest, setSelectedManifest] = useState<string>('')
  const [exporting, setExporting] = useState(false)
  const [exportFormat, setExportFormat] = useState<'pdf' | 'pptx' | 'both' | 'csv'>('pdf')
  const [ingesting, setIngesting] = useState(false)
  const [sheetName, setSheetName] = useState<string>('vInfo,vHost')
  const [chunkSize, setChunkSize] = useState<number>(5000)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    loadData()
    const interval = setInterval(loadData, 5000)
    return () => clearInterval(interval)
  }, [selectedProject])

  useEffect(() => {
    const selected = projects.find((p) => p.name === selectedProject)
    if (selected) {
      setUploadAnonymize(!!selected.anonymize_default)
    }
  }, [selectedProject, projects])

  const loadData = async () => {
    try {
      const projectRes = await axios.get('/projects')
      const projectList = projectRes.data?.projects || []
      setProjects(projectList)

      if (projectList.length > 0 && !projectList.some((p: ProjectItem) => p.name === selectedProject)) {
        setSelectedProject(projectList[0].name)
      }

      const kpiRes = await axios.get('/kpis', { params: { project: selectedProject } })
      setKpis(kpiRes.data)

      const manifestRes = await axios.get('/manifests', { params: { project: selectedProject } })
      const manifestList = manifestRes.data.manifests.map((m: string) => ({ name: m })) || []
      setManifests(manifestList)
      if (manifestList.length > 0 && !selectedManifest) {
        setSelectedManifest(manifestList[0].name)
      }
      if (manifestList.length === 0) {
        setSelectedManifest('')
      }

      try {
        const entRes = await axios.get('/kpis/enterprise', { params: { project: selectedProject } })
        setEnterprise(entRes.data?.intelligence || null)
        setAdvanced(entRes.data?.advanced || null)
      } catch {
        setEnterprise(null)
        setAdvanced(null)
      }

      try {
        const histRes = await axios.get(`/projects/${selectedProject}/history`)
        setHistory(histRes.data?.history || [])
      } catch {
        setHistory([])
      }

      try {
        const apxRes = await axios.get('/appendices')
        const list = apxRes.data?.appendices || []
        setAppendices(list)
        setSelectedAppendices((prev) => (prev.length > 0 ? prev : list.map((a: AppendixItem) => a.id)))
      } catch {
        setAppendices([])
      }

      try {
        const profRes = await axios.get('/report-profiles')
        const list = profRes.data?.profiles || []
        setProfiles(list)
        setReportProfile((prev) => prev || (list[0]?.id ?? 'full'))
      } catch {
        setProfiles([])
      }

      setError(null)
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e?.response?.data?.error || e?.message || 'Unknown error'
      setError(`Failed to load data: ${msg}`)
    } finally {
      setLoading(false)
    }
  }

  const toggleAppendix = (id: string) => {
    setSelectedAppendices((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]))
  }

  const handleMappingUpload = async () => {
    if (!mappingFile) {
      toast.error('Select a CSV mapping file first.')
      return
    }
    try {
      const form = new FormData()
      form.append('file', mappingFile)
      await axios.post(`/projects/${selectedProject}/app-mapping/upload`, form)
      toast.success('Application mapping uploaded.')
      setMappingFile(null)
      await loadData()
    } catch (e: any) {
      toast.error(`Mapping upload failed: ${e?.response?.data?.error || e?.message || 'Unknown error'}`)
    }
  }

  const handleCreateProject = async () => {
    const name = newProject.trim()
    if (!name) return
    try {
      const form = new FormData()
      form.append('name', name)
      form.append('anonymize_default', String(newProjectAnonymize))
      const res = await axios.post('/projects/create', form)
      const p = res.data?.project
      setNewProject('')
      setNewProjectAnonymize(false)
      if (p) {
        setSelectedProject(p)
      }
      toast.success(`Project ${p} created.`)
      await loadData()
    } catch (e: any) {
      toast.error(`Create project failed: ${e?.response?.data?.error || e?.message || 'Unknown error'}`)
    }
  }

  const handleFileIngest = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return
    setIngesting(true)
    try {
      const uploadForm = new FormData()
      uploadForm.append('file', file)
      uploadForm.append('project', selectedProject)
      const uploadRes = await axios.post('/upload', uploadForm)

      const parseForm = new FormData()
      parseForm.append('filename', uploadRes.data?.filename || file.name)
      parseForm.append('sheet', sheetName)
      parseForm.append('chunk_size', String(chunkSize))
      parseForm.append('project', selectedProject)
      parseForm.append('anonymize', String(uploadAnonymize))
      const parseRes = await axios.post('/parse', parseForm)

      const manifest = parseRes.data?.manifest
      const rows = manifest?.total_rows ?? 0
      const chunks = manifest?.chunk_count ?? 0
      const masked = !!manifest?.anonymized
      toast.success(`Ingest completed for project "${selectedProject}": ${rows} rows, ${chunks} chunk(s). Anonymized: ${masked ? 'Yes' : 'No'}.`)
      setTimeout(() => loadData(), 1000)
    } catch (e: any) {
      toast.error(`Ingest failed: ${e?.response?.data?.error || e?.message || 'Unknown error'}`)
    } finally {
      setIngesting(false)
    }
  }

  const handleDeleteDataset = async () => {
    if (!selectedManifest) return
    const ok = window.confirm(`Delete dataset "${selectedManifest}" from project "${selectedProject}"? This cannot be undone.`)
    if (!ok) return
    try {
      await axios.delete(`/projects/${selectedProject}/datasets/${encodeURIComponent(selectedManifest)}`)
      toast.success(`Deleted dataset: ${selectedManifest}`)
      setSelectedManifest('')
      await loadData()
    } catch (e: any) {
      toast.error(`Delete dataset failed: ${e?.response?.data?.error || e?.message || 'Unknown error'}`)
    }
  }

  const handleDeleteProject = async () => {
    const ok = window.confirm(`Delete customer project "${selectedProject}" and all its data? This cannot be undone.`)
    if (!ok) return
    try {
      await axios.delete(`/projects/${selectedProject}`)
      toast.success(`Deleted project: ${selectedProject}`)
      setSelectedProject('default')
      setSelectedManifest('')
      await loadData()
    } catch (e: any) {
      toast.error(`Delete project failed: ${e?.response?.data?.error || e?.message || 'Unknown error'}`)
    }
  }

  const handleExport = async () => {
    if (!selectedManifest) {
      toast.error('Please select a dataset')
      return
    }
    setExporting(true)

    if (exportFormat === 'csv') {
      try {
        const url = `/export/csv?project=${encodeURIComponent(selectedProject)}&manifest=manifest_${encodeURIComponent(selectedManifest.replace('manifest_', ''))}`
        const res = await axios.get(url, { responseType: 'blob' })
        const disposition = res.headers['content-disposition'] || ''
        const match = disposition.match(/filename="?([^"]+)"?/)
        const filename = match?.[1] || `csv_export_${selectedManifest.replace('.json', '')}.zip`
        const blobUrl = window.URL.createObjectURL(new Blob([res.data]))
        const link = document.createElement('a')
        link.href = blobUrl
        link.setAttribute('download', filename)
        document.body.appendChild(link)
        link.click()
        link.remove()
        window.URL.revokeObjectURL(blobUrl)
        toast.success('CSV dataset downloaded.')
      } catch (e: any) {
        if (e?.response?.data instanceof Blob) {
          try {
            const text = await e.response.data.text()
            const payload = JSON.parse(text)
            toast.error(`CSV export failed: ${payload?.error || text}`)
            return
          } catch { /* fall through */ }
        }
        toast.error(`CSV export failed: ${e?.response?.data?.error || e?.message || 'Unknown error'}`)
      } finally {
        setExporting(false)
      }
      return
    }

    try {
      const formData = new FormData()
      formData.append('manifest_name', selectedManifest)
      formData.append('template', 'vsphere')
      formData.append('output_format', exportFormat)
      formData.append('project', selectedProject)
      formData.append('appendices', selectedAppendices.join(','))
      formData.append('report_profile', reportProfile)

      const res = await axios.post('/export/create', formData, { responseType: 'blob' })
      const disposition = res.headers['content-disposition'] || ''
      const match = disposition.match(/filename="?([^"]+)"?/)
      const filename = match?.[1] || `report.${exportFormat === 'both' ? 'zip' : exportFormat}`
      const blobUrl = window.URL.createObjectURL(new Blob([res.data]))
      const link = document.createElement('a')
      link.href = blobUrl
      link.setAttribute('download', filename)
      document.body.appendChild(link)
      link.click()
      link.remove()
      window.URL.revokeObjectURL(blobUrl)
      toast.success('Report downloaded.')
    } catch (e: any) {
      if (e?.response?.data instanceof Blob) {
        try {
          const text = await e.response.data.text()
          const payload = JSON.parse(text)
          toast.error(`Export failed: ${payload?.error || text}`)
          return
        } catch {
          // fall through
        }
      }
      toast.error(`Export failed: ${e?.response?.data?.error || e?.message || 'Unknown error'}`)
    } finally {
      setExporting(false)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-screen bg-slate-50">
        <p className="text-slate-600">Loading dashboard...</p>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 p-4">
      <header className="bg-white shadow rounded-lg mb-6 p-6">
        <div className="flex items-center gap-3">
          <img src="/vcf-hub-logo.png" alt="VCF Intelligence Hub logo" className="w-20 h-20 object-contain" />
          <div>
            <h1 className="text-2xl font-bold text-slate-900">VCF Intelligence Hub</h1>
            <p className="text-sm text-slate-600">by Samir Roshan</p>
          </div>
        </div>
      </header>

      {error && (
        <div className="mb-6 bg-red-50 border border-red-200 rounded-lg p-4 flex gap-3">
          <AlertCircle className="w-5 h-5 text-red-600 flex-shrink-0" />
          <p className="text-red-800">{error}</p>
        </div>
      )}

      {!kpis && !loading && !error && (
        <div className="mb-8 p-12 bg-white rounded-lg shadow border border-slate-200 text-center">
          <h2 className="text-xl font-bold text-slate-800 mb-2">No Data Available</h2>
          <p className="text-slate-600">Upload a VMware RVTools XLSX payload to populate the dashboard.</p>
        </div>
      )}

      {kpis && kpis.total_vms === 0 && (
        <div className="mb-8 p-12 bg-white rounded-lg shadow border border-slate-200 text-center">
          <h2 className="text-xl font-bold text-slate-800 mb-2">Project is Empty</h2>
          <p className="text-slate-600">This project has no datasets. Upload an RVTools export to begin.</p>
        </div>
      )}

      {kpis && kpis.total_vms > 0 && (
        <div className="mb-8 space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-5 gap-3">
            <div className="bg-slate-100 border border-slate-300 rounded-lg p-4 text-center">
              <p className="text-[10px] tracking-widest uppercase text-slate-600 font-semibold">Total Hosts</p>
              <p className="text-4xl font-extrabold text-slate-900 mt-1">{kpis.total_hosts.toLocaleString()}</p>
            </div>
            <div className="bg-slate-100 border border-slate-300 rounded-lg p-4 text-center">
              <p className="text-[10px] tracking-widest uppercase text-slate-600 font-semibold">Total Compute</p>
              <p className="text-4xl font-extrabold text-slate-900 mt-1">{kpis.total_compute.toLocaleString()}</p>
              <p className="text-[10px] text-slate-500">cores</p>
            </div>
            <div className="bg-slate-100 border border-slate-300 rounded-lg p-4 text-center">
              <p className="text-[10px] tracking-widest uppercase text-slate-600 font-semibold">Total Memory</p>
              <p className="text-4xl font-extrabold text-slate-900 mt-1">{kpis.total_memory_tb}</p>
            </div>
            <div className="bg-slate-100 border border-slate-300 rounded-lg p-4 text-center">
              <p className="text-[10px] tracking-widest uppercase text-slate-600 font-semibold">Total VMs</p>
              <p className="text-4xl font-extrabold text-slate-900 mt-1">{kpis.total_vms.toLocaleString()}</p>
            </div>
            <div className="bg-rose-50 border border-rose-300 rounded-lg p-4 text-center">
              <p className="text-[10px] tracking-widest uppercase text-rose-700 font-semibold">EOS Risk</p>
              <p className="text-4xl font-extrabold text-rose-600 mt-1">{kpis.eos_risk.toLocaleString()}</p>
              <p className="text-[10px] text-rose-600">
                {(kpis.total_vms > 0 ? ((kpis.eos_risk / kpis.total_vms) * 100).toFixed(1) : '0.0')}% of VMs
              </p>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <div className="bg-green-50 border-2 border-green-600 rounded-lg p-4">
              <p className="text-[10px] tracking-widest uppercase text-green-700 font-bold">Tier 1: Resilient</p>
              <p className="text-3xl font-extrabold text-slate-900 mt-1">Platform Stability</p>
              <p className="text-xs text-slate-700 mt-1">Operational baseline with controlled risk posture.</p>
              <div className="grid grid-cols-3 gap-2 mt-3">
                <div className="bg-white rounded border border-slate-200 p-2 text-center"><p className="text-xl font-bold">{enterprise?.executive_score ?? 0}</p><p className="text-[9px] tracking-widest text-slate-500 uppercase">Score</p></div>
                <div className="bg-white rounded border border-slate-200 p-2 text-center"><p className="text-xl font-bold">{enterprise?.executive_components?.vcpu_pcpu_ratio ?? 0}x</p><p className="text-[9px] tracking-widest text-slate-500 uppercase">vCPU:pCPU</p></div>
                <div className="bg-white rounded border border-slate-200 p-2 text-center"><p className="text-xl font-bold">{advanced?.operational_scorecard?.efficiency_score ?? 0}</p><p className="text-[9px] tracking-widest text-slate-500 uppercase">Efficiency</p></div>
              </div>
            </div>

            <div className="bg-amber-50 border-2 border-amber-600 rounded-lg p-4">
              <p className="text-[10px] tracking-widest uppercase text-amber-700 font-bold">Tier 2: Optimization</p>
              <p className="text-3xl font-extrabold text-slate-900 mt-1">Capacity Pressure</p>
              <p className="text-xs text-slate-700 mt-1">Savings and consolidation opportunities identified.</p>
              <div className="grid grid-cols-3 gap-2 mt-3">
                <div className="bg-white rounded border border-slate-200 p-2 text-center"><p className="text-xl font-bold">{enterprise?.performance?.right_size_candidates ?? 0}</p><p className="text-[9px] tracking-widest text-slate-500 uppercase">Candidates</p></div>
                <div className="bg-white rounded border border-slate-200 p-2 text-center"><p className="text-xl font-bold">{advanced?.consolidation_optimization?.retireable_hosts_estimate ?? 0}</p><p className="text-[9px] tracking-widest text-slate-500 uppercase">Retirable Hosts</p></div>
                <div className="bg-white rounded border border-slate-200 p-2 text-center"><p className="text-xl font-bold">{enterprise?.performance?.estimated_reclaim_vcpu ?? 0}</p><p className="text-[9px] tracking-widest text-slate-500 uppercase">Reclaim vCPU</p></div>
              </div>
            </div>

            <div className="bg-rose-50 border-2 border-rose-600 rounded-lg p-4">
              <p className="text-[10px] tracking-widest uppercase text-rose-700 font-bold">Tier 3: Modernize</p>
              <p className="text-3xl font-extrabold text-slate-900 mt-1">Lifecycle Risk</p>
              <p className="text-xs text-slate-700 mt-1">Legacy workloads require remediation planning.</p>
              <div className="grid grid-cols-3 gap-2 mt-3">
                <div className="bg-white rounded border border-slate-200 p-2 text-center"><p className="text-xl font-bold">{kpis.eos_risk}</p><p className="text-[9px] tracking-widest text-slate-500 uppercase">EOS VMs</p></div>
                <div className="bg-white rounded border border-slate-200 p-2 text-center"><p className="text-xl font-bold">{advanced?.anomalies?.events?.length ?? 0}</p><p className="text-[9px] tracking-widest text-slate-500 uppercase">Anomalies</p></div>
                <div className="bg-white rounded border border-slate-200 p-2 text-center"><p className="text-xl font-bold">{advanced?.forecasting?.days_to_threshold?.vms_10000 ?? 'n/a'}</p><p className="text-[9px] tracking-widest text-slate-500 uppercase">Days to 10k</p></div>
              </div>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div className="bg-white border border-slate-300 rounded-lg p-4">
              <p className="text-xs tracking-widest uppercase text-slate-700 font-bold mb-3">Volume Distribution by Snapshot</p>
              <div className="space-y-2">
                {(history.slice(0, 5).length > 0 ? history.slice(0, 5) : [{ manifest_name: 'current', total_rows: kpis.total_vms }]).map((h) => {
                  const base = Math.max(kpis.total_vms, 1)
                  const rows = Number(h.total_rows || 0)
                  const pct = Math.min(100, Math.max(4, (rows / base) * 100))
                  return (
                    <div key={h.manifest_name}>
                      <div className="flex justify-between text-xs text-slate-700 mb-1">
                        <span>{h.ingest_id || h.manifest_name.replace('manifest_', '').replace('.json', '')}</span>
                        <span>{rows.toLocaleString()} rows</span>
                      </div>
                      <div className="w-full h-5 rounded bg-slate-200 overflow-hidden">
                        <div className="h-5 bg-emerald-600 text-white text-[10px] font-semibold flex items-center justify-center" style={{ width: `${pct}%` }}>
                          {pct.toFixed(0)}%
                        </div>
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>

            <div className="bg-white border border-slate-300 rounded-lg p-4">
              <p className="text-xs tracking-widest uppercase text-slate-700 font-bold mb-3">Compute Fabric Health Matrix</p>
              <div className="grid grid-cols-2 gap-2">
                <div className="border border-slate-200 rounded p-3 text-center"><p className="text-[10px] uppercase tracking-widest text-slate-500">vCPU:pCPU</p><p className="text-2xl font-extrabold text-slate-900">{enterprise?.executive_components?.vcpu_pcpu_ratio ?? 0}x</p></div>
                <div className="border border-slate-200 rounded p-3 text-center"><p className="text-[10px] uppercase tracking-widest text-slate-500">vMem:pMem</p><p className="text-2xl font-extrabold text-slate-900">{enterprise?.executive_components?.vmem_pmem_ratio ?? 0}x</p></div>
                <div className="border border-slate-200 rounded p-3 text-center"><p className="text-[10px] uppercase tracking-widest text-slate-500">Mapped Coverage</p><p className="text-2xl font-extrabold text-slate-900">{enterprise?.application?.mapping_coverage_pct ?? 0}%</p></div>
                <div className="border border-slate-200 rounded p-3 text-center"><p className="text-[10px] uppercase tracking-widest text-slate-500">Target Hosts</p><p className="text-2xl font-extrabold text-slate-900">{advanced?.consolidation_optimization?.target_hosts ?? 0}</p></div>
              </div>
            </div>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="space-y-6">
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold text-slate-900 mb-4">Project Workspace</h2>
            <div className="space-y-4">
              <div>
                <p className="text-xs text-slate-600 mb-1">Customer Project</p>
                <select
                  value={selectedProject}
                  onChange={(e) => {
                    setSelectedProject(e.target.value)
                    setSelectedManifest('')
                  }}
                  className="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm bg-white"
                >
                  {projects.map((p) => (
                    <option key={p.name} value={p.name}>{p.name}</option>
                  ))}
                  {projects.length === 0 && <option value="default">default</option>}
                </select>
              </div>
              <div>
                <p className="text-xs text-slate-600 mb-1">Create New Project</p>
                <div className="flex gap-2">
                  <input
                    value={newProject}
                    onChange={(e) => setNewProject(e.target.value)}
                    placeholder="e.g. acme-corp"
                    className="flex-1 border border-slate-300 rounded-lg px-3 py-2 text-sm"
                  />
                  <button
                    onClick={handleCreateProject}
                    className="bg-slate-800 text-white px-4 py-2 rounded-lg text-sm hover:bg-slate-700"
                  >
                    Create
                  </button>
                </div>
                <label className="mt-2 flex items-center gap-2 text-sm text-slate-700">
                  <input
                    type="checkbox"
                    checked={newProjectAnonymize}
                    onChange={(e) => setNewProjectAnonymize(e.target.checked)}
                  />
                  <span>Anonymize by default for this project</span>
                </label>
              </div>
              <div>
                <button
                  onClick={handleDeleteProject}
                  className="w-full bg-rose-700 text-white px-4 py-2 rounded-lg text-sm hover:bg-rose-600"
                >
                  Delete Selected Project
                </button>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold text-slate-900 mb-4 flex items-center gap-2"><Upload className="w-5 h-5 text-teal-600" />Upload + Parse (One Click)</h2>
            <div className="grid grid-cols-2 gap-2 mb-4">
              <div>
                <label className="block text-xs text-slate-600 mb-1">Sheet(s)</label>
                <input
                  value={sheetName}
                  onChange={(e) => setSheetName(e.target.value)}
                  className="w-full border border-slate-300 rounded-lg px-2 py-2 text-sm"
                />
                <p className="text-[10px] text-slate-500 mt-1">Use comma-separated sheets, e.g. <code>vInfo,vHost</code> for hardware enrichment.</p>
              </div>
              <div>
                <label className="block text-xs text-slate-600 mb-1">Chunk Size</label>
                <input
                  type="number"
                  min={100}
                  step={100}
                  value={chunkSize}
                  onChange={(e) => setChunkSize(Number(e.target.value || 5000))}
                  className="w-full border border-slate-300 rounded-lg px-2 py-2 text-sm"
                />
              </div>
            </div>
            <label className="mb-3 flex items-center gap-2 text-sm text-slate-700">
              <input
                type="checkbox"
                checked={uploadAnonymize}
                onChange={(e) => setUploadAnonymize(e.target.checked)}
              />
              <span>Anonymize uploaded RVTools data (mask names/identifiers)</span>
            </label>
            <label className="block cursor-pointer">
              <div className="border-2 border-dashed border-slate-300 rounded-lg p-6 text-center hover:border-blue-500">
                <Upload className="w-8 h-8 text-slate-400 mx-auto mb-2" />
                <p className="text-sm font-medium text-slate-700">{ingesting ? 'Processing...' : 'Click to upload and parse XLSX'}</p>
                <input type="file" accept=".xlsx,.xls" onChange={handleFileIngest} disabled={ingesting} className="hidden" />
              </div>
            </label>
          </div>
        </div>

        <div className="lg:col-span-2">
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold text-slate-900 mb-4 flex items-center gap-2"><Download className="w-5 h-5 text-green-600" />Export Reports</h2>
            {selectedManifest && (
              <div className="space-y-4">
                <p className="text-sm text-slate-600">Project: <span className="font-semibold text-indigo-600">{selectedProject}</span> | Selected: <span className="font-semibold text-blue-600">{selectedManifest}</span></p>
                <div>
                  <label className="block text-sm text-slate-700 mb-2">Manifest</label>
                  <div className="flex gap-2">
                    <select
                      value={selectedManifest}
                      onChange={(e) => setSelectedManifest(e.target.value)}
                      className="flex-1 border border-slate-300 rounded-lg px-3 py-2 text-sm bg-white"
                    >
                      {manifests.map((m) => (
                        <option key={m.name} value={m.name}>{m.name}</option>
                      ))}
                    </select>
                    <button
                      onClick={handleDeleteDataset}
                      className="bg-rose-700 text-white px-3 py-2 rounded-lg text-sm hover:bg-rose-600"
                    >
                      Delete Dataset
                    </button>
                  </div>
                </div>
                <div>
                  <label className="block text-sm text-slate-700 mb-2">Report Profile</label>
                  <select value={reportProfile} onChange={(e) => setReportProfile(e.target.value)} className="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm bg-white">
                    {profiles.map((p) => (
                      <option key={p.id} value={p.id}>{p.title}</option>
                    ))}
                    {profiles.length === 0 && <option value="full">Full Intelligence (Default)</option>}
                  </select>
                </div>
                <div>
                  <label className="block text-sm text-slate-700 mb-2">Export format</label>
                  <select value={exportFormat} onChange={(e) => setExportFormat(e.target.value as 'pdf' | 'pptx' | 'both' | 'csv')} className="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm bg-white">
                    <option value="pdf">PDF</option>
                    <option value="pptx">PowerPoint (PPTX)</option>
                    <option value="both">Both PDF + PPTX</option>
                    <option value="csv">Raw Data (CSV ZIP)</option>
                  </select>
                </div>
                <div>
                  <label className="block text-sm text-slate-700 mb-2">Appendix Pages</label>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                    {appendices.map((a) => (
                      <label key={a.id} className="flex items-center gap-2 text-sm">
                        <input
                          type="checkbox"
                          checked={selectedAppendices.includes(a.id)}
                          onChange={() => toggleAppendix(a.id)}
                        />
                        <span>{a.title}</span>
                      </label>
                    ))}
                  </div>
                </div>
                <button onClick={handleExport} disabled={exporting} className="w-full bg-blue-600 hover:bg-blue-700 disabled:bg-slate-300 text-white font-medium py-3 px-4 rounded-lg transition">
                  {exporting ? 'Generating report...' : 'Generate Report Now'}
                </button>
              </div>
            )}
            {!selectedManifest && (
              <p className="text-sm text-slate-500">No manifest found for this project. Upload and parse an XLSX file first.</p>
            )}
          </div>

          <div className="bg-white rounded-lg shadow p-6 mt-6">
            <h2 className="text-lg font-semibold text-slate-900 mb-3">Application Mapping</h2>
            <p className="text-sm text-slate-600 mb-3">Upload project-specific mapping CSV (`pattern,application,priority,owner,criticality`).</p>
            <div className="flex gap-2">
              <input
                type="file"
                accept=".csv"
                onChange={(e) => setMappingFile(e.target.files?.[0] || null)}
                className="flex-1 border border-slate-300 rounded-lg px-3 py-2 text-sm"
              />
              <button onClick={handleMappingUpload} className="bg-indigo-700 text-white px-4 py-2 rounded-lg text-sm hover:bg-indigo-600">
                Upload Mapping
              </button>
            </div>
          </div>
        </div>
      </div>

      <div className="mt-8 bg-white rounded-lg shadow p-6">
        <h2 className="text-lg font-semibold text-slate-900 mb-4 flex items-center gap-2"><Zap className="w-5 h-5 text-amber-600" />System Status</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div><p className="text-sm text-slate-600">Projects</p><p className="text-2xl font-bold">{projects.length}</p></div>
          <div><p className="text-sm text-slate-600">Datasets</p><p className="text-2xl font-bold">{manifests.length}</p></div>
          <div><p className="text-sm text-slate-600">API Status</p><p className="text-2xl font-bold text-green-600">Ready</p></div>
          <div><p className="text-sm text-slate-600">Export Mode</p><p className="text-2xl font-bold text-blue-600">Sync</p></div>
        </div>
      </div>

      <div className="mt-8 bg-white rounded-lg shadow p-6">
        <h2 className="text-lg font-semibold text-slate-900 mb-4">Historical Ingests ({selectedProject})</h2>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead>
              <tr className="border-b">
                <th className="text-left py-2 pr-4">Ingest</th>
                <th className="text-left py-2 pr-4">Manifest</th>
                <th className="text-left py-2 pr-4">Rows</th>
                <th className="text-left py-2 pr-4">Chunks</th>
                <th className="text-left py-2 pr-4">Timestamp (UTC)</th>
              </tr>
            </thead>
            <tbody>
              {history.map((h) => (
                <tr key={h.manifest_name} className="border-b border-slate-100">
                  <td className="py-2 pr-4">{h.ingest_id || '-'}</td>
                  <td className="py-2 pr-4">{h.manifest_name}</td>
                  <td className="py-2 pr-4">{h.total_rows ?? 0}</td>
                  <td className="py-2 pr-4">{h.chunk_count ?? 0}</td>
                  <td className="py-2 pr-4">{h.generated_at_utc || '-'}</td>
                </tr>
              ))}
              {history.length === 0 && (
                <tr>
                  <td className="py-3 text-slate-500" colSpan={5}>No historical data yet for this project.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

export default Dashboard
