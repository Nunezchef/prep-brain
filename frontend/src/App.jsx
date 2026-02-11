import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  AlertTriangle,
  BatteryCharging,
  BookOpenText,
  Clock3,
  Hand,
  Info,
  Layers3,
  Mail,
  MapPin,
  Phone,
  Play,
  Radio,
  RefreshCw,
  Settings2,
  Shield,
  Send,
  FileEdit,
  Thermometer,
  Trash2,
  Truck,
  Upload,
  Package,
  ClipboardList,
  Activity,
  Menu,
  X,
} from 'lucide-react'

const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8000'

const MenuEngineeringView = () => {
  const [data, setData] = useState({ items: [], averages: { margin: 0, count: 0 } })
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    setLoading(true)
    apiRequest('/api/menu-engineering')
      .then(setData)
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <div className="p-8 text-center text-zinc-400">Analyzing menu performance...</div>

  return (
    <div className="space-y-8">
      <div className="grid grid-cols-2 gap-4">
        <div className="rounded-xl bg-emerald-50 p-4 border border-emerald-100">
          <p className="text-xs font-bold text-emerald-800 uppercase tracking-widest mb-1">AVG MARGIN</p>
          <p className="text-2xl font-bold text-emerald-900">${(data.averages.margin || 0).toFixed(2)}</p>
        </div>
        <div className="rounded-xl bg-blue-50 p-4 border border-blue-100">
          <p className="text-xs font-bold text-blue-800 uppercase tracking-widest mb-1">AVG SALES QTY</p>
          <p className="text-2xl font-bold text-blue-900">{Math.round(data.averages.count || 0)}</p>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {['Star', 'Plowhorse', 'Puzzle', 'Dog'].map(type => {
          const items = data.items.filter(i => i.classification === type)
          const colors = {
            Star: 'bg-yellow-50 border-yellow-200 text-yellow-800',
            Plowhorse: 'bg-blue-50 border-blue-200 text-blue-800',
            Puzzle: 'bg-purple-50 border-purple-200 text-purple-800',
            Dog: 'bg-red-50 border-red-200 text-red-800'
          }[type]

          return (
            <div key={type} className={`rounded-xl border p-4 ${colors}`}>
              <h3 className="font-bold uppercase tracking-widest border-b border-black/10 pb-2 mb-3 flex justify-between">
                {type} <span className="text-xs opacity-60">({items.length})</span>
              </h3>
              <div className="space-y-2">
                {items.map(item => (
                  <div key={item.id} className="text-sm flex justify-between">
                    <span>{item.name}</span>
                    <span className="font-mono text-xs opacity-75">${item.margin.toFixed(2)} / {item.count}#</span>
                  </div>
                ))}
                {items.length === 0 && <p className="text-xs opacity-50 italic">No items.</p>}
              </div>
              <p className="mt-4 text-[10px] leading-tight opacity-70">
                {type === 'Star' && "High Profit, High Popularity. Promote & maintain."}
                {type === 'Plowhorse' && "Low Profit, High Popularity. Increase price or lower cost."}
                {type === 'Puzzle' && "High Profit, Low Popularity. Marketing & placement."}
                {type === 'Dog' && "Low Profit, Low Popularity. Remove or rethink."}
              </p>
            </div>
          )
        })}
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b border-zinc-200 text-zinc-500">
              <th className="py-2 font-bold text-[10px] uppercase tracking-wider">Item</th>
              <th className="py-2 font-bold text-[10px] uppercase tracking-wider text-right">Cost</th>
              <th className="py-2 font-bold text-[10px] uppercase tracking-wider text-right">Price</th>
              <th className="py-2 font-bold text-[10px] uppercase tracking-wider text-right">Margin</th>
              <th className="py-2 font-bold text-[10px] uppercase tracking-wider text-right">Sold</th>
              <th className="py-2 font-bold text-[10px] uppercase tracking-wider text-right">Status</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map(item => (
              <tr key={item.id} className="border-b border-zinc-100 hover:bg-zinc-50">
                <td className="py-2 font-medium">{item.name}</td>
                <td className="py-2 text-right text-zinc-500">${item.cost.toFixed(2)}</td>
                <td className="py-2 text-right">${item.price.toFixed(2)}</td>
                <td className="py-2 text-right font-medium text-emerald-600">${item.margin.toFixed(2)}</td>
                <td className="py-2 text-right">{item.count}</td>
                <td className="py-2 text-right">
                  <span className={`inline-block px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider
                                        ${item.classification === 'Star' ? 'bg-yellow-100 text-yellow-800' : ''}
                                        ${item.classification === 'Plowhorse' ? 'bg-blue-100 text-blue-800' : ''}
                                        ${item.classification === 'Puzzle' ? 'bg-purple-100 text-purple-800' : ''}
                                        ${item.classification === 'Dog' ? 'bg-red-100 text-red-800' : ''}
                                    `}>
                    {item.classification}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

const PrepListView = () => {
  const [recipes, setRecipes] = useState([])
  const [mode, setMode] = useState('audit') // audit | plan

  const loadData = useCallback(() => {
    apiRequest('/api/recipes').then(res => setRecipes(res.recipes || []))
  }, [])

  useEffect(() => { loadData() }, [loadData])

  const saveCounts = async () => {
    const updates = {}
    recipes.forEach(r => {
      updates[r.id] = parseFloat(r.on_hand || 0)
    })
    await apiRequest('/api/prep-update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(updates)
    })
    setMode('plan')
  }

  const needs = recipes.filter(r => (r.par_level || 0) > (r.on_hand || 0))

  return (
    <div className="space-y-6">
      <div className="flex justify-center gap-4 mb-8">
        <button
          onClick={() => setMode('audit')}
          className={`px-6 py-2 rounded-full text-sm font-bold tracking-widest transition-colors ${mode === 'audit' ? 'bg-black text-white' : 'bg-zinc-100 text-zinc-400'}`}
        >
          1. AUDIT
        </button>
        <button
          onClick={() => setMode('plan')}
          className={`px-6 py-2 rounded-full text-sm font-bold tracking-widest transition-colors ${mode === 'plan' ? 'bg-black text-white' : 'bg-zinc-100 text-zinc-400'}`}
        >
          2. PLAN
        </button>
      </div>

      {mode === 'audit' && (
        <div className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {recipes.filter(r => (r.par_level || 0) > 0).map(recipe => (
              <div key={recipe.id} className="flex items-center justify-between p-4 rounded-xl border border-hairline bg-white">
                <div>
                  <p className="font-bold">{recipe.name}</p>
                  <p className="text-xs text-zinc-400">Par: {recipe.par_level} {recipe.yield_unit}</p>
                </div>
                <div className="flex items-center gap-2">
                  <label className="text-[10px] font-bold text-zinc-400">ON HAND</label>
                  <input
                    type="number"
                    className="w-20 rounded border border-hairline bg-zinc-50 px-2 py-1 text-right font-mono"
                    value={recipe.on_hand || ''}
                    onChange={(e) => {
                      const val = e.target.value
                      setRecipes(recipes.map(r => r.id === recipe.id ? { ...r, on_hand: val } : r))
                    }}
                  />
                </div>
              </div>
            ))}
          </div>
          {recipes.filter(r => (r.par_level || 0) > 0).length === 0 && (
            <p className="text-center text-zinc-400 italic py-10">No recipes have Par Levels set. Edit recipes to enable Prep-List.</p>
          )}
          <div className="flex justify-end pt-4">
            <button onClick={saveCounts} className="bg-obsidian text-white px-6 py-3 rounded-lg font-bold text-sm tracking-widest hover:bg-zinc-800">
              GENERATE PREP LIST →
            </button>
          </div>
        </div>
      )}

      {mode === 'plan' && (
        <div className="space-y-4">
          <h3 className="text-center text-lg font-bold mb-6">Prep Requirements</h3>
          <div className="space-y-2">
            {needs.map(recipe => {
              const need = (recipe.par_level || 0) - (recipe.on_hand || 0)
              return (
                <div key={recipe.id} className="p-4 rounded-xl bg-orange-50 border border-orange-100 flex justify-between items-center">
                  <div>
                    <p className="font-bold text-orange-900">{recipe.name}</p>
                    <p className="text-xs text-orange-700/60">Par: {recipe.par_level} | On Hand: {recipe.on_hand}</p>
                  </div>
                  <div className="text-right">
                    <p className="text-xs font-bold text-orange-800 uppercase tracking-widest">TO PREP</p>
                    <p className="text-3xl font-bold text-orange-900">{need < 0 ? 0 : Number(need.toFixed(2))} <span className="text-sm font-normal text-orange-800/60">{recipe.yield_unit}</span></p>
                  </div>
                </div>
              )
            })}
            {needs.length === 0 && (
              <div className="text-center py-12">
                <p className="text-emerald-600 font-bold mb-2">ALL PREP COMPLETE</p>
                <p className="text-zinc-400 text-sm">Par levels satisfied.</p>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

const AutonomyView = ({ status, logs, onRefresh }) => {
  return (
    <div className="space-y-8">
      <SectionTitle
        overline="BACKGROUND INTELLIGENCE"
        title="Continuous Autonomy Engine"
        subtitle="Monitor background enrichment, reconciliation, and safety audits."
      />

      <Panel className="p-5">
        <div className="mb-4 flex items-center justify-between">
          <p className="tactical-label">WORKER STATUS (ALWAYS ON)</p>
          <RefreshCw size={15} className={`text-zinc-400 ${status.running ? 'animate-spin' : ''}`} />
        </div>
        <div className="rounded-xl border border-hairline bg-bone/35 px-3 py-3">
          <div className="flex items-center justify-between">
            <p className="tactical-label">AUTONOMY</p>
            <p className={`text-xs font-semibold ${status.running ? 'text-emerald-600' : 'text-amber-600'}`}>
              {status.status?.toUpperCase() || 'UNKNOWN'}
            </p>
          </div>
          <div className="mt-3 space-y-1 text-xs text-zinc-500">
            <p>Last tick: {status.last_tick_at || '-'}</p>
            <p>Current task: {status.last_action || '-'}</p>
            <p>Queue: {status.queue_pending_ingests || 0} ingest | {status.queue_pending_drafts || 0} drafts</p>
            <p>Error count: {status.error_count || 0}</p>
            {status.last_promoted_recipe_name && (
              <p>Last promoted: {status.last_promoted_recipe_name} ({status.last_promoted_at || '-'})</p>
            )}
            {status.last_error && (
              <p className="text-amber-700">Last error: {status.last_error}</p>
            )}
          </div>
        </div>
      </Panel>

      <Panel className="p-5">
        <div className="mb-4 flex items-center justify-between">
          <p className="tactical-label">AUTONOMY LOGS (LATEST 50)</p>
          <button
            onClick={onRefresh}
            className="rounded-full border border-hairline px-2 py-1 text-[9px] font-bold tracking-[0.18em] text-zinc-500"
          >
            REFRESH
          </button>
        </div>
        <div className="max-h-96 space-y-3 overflow-y-auto pr-1">
          {logs.length === 0 && <p className="text-sm text-zinc-400">No autonomy events recorded.</p>}
          {logs.map((log, idx) => (
            <div key={idx} className="flex flex-col gap-1 border-b border-hairline/70 pb-3 last:border-b-0">
              <div className="flex justify-between items-center">
                <span className="tactical-text text-[11px] text-zinc-400">{log.created_at}</span>
                <span className="text-[10px] bg-zinc-100 px-1.5 py-0.5 rounded font-bold text-zinc-500 uppercase tracking-widest">{log.action}</span>
              </div>
              <p className="text-sm text-obsidian/90 font-medium">{log.detail}</p>
              {log.target_type && (
                <p className="text-[10px] text-zinc-400">Target: {log.target_type} ({log.target_id})</p>
              )}
            </div>
          ))}
        </div>
      </Panel>
    </div>
  )
}

const TABS = [
  { id: 'overview', label: 'CONTROL' },
  { id: 'autonomy', label: 'AUTONOMY' },
  { id: 'sessions', label: 'SESSIONS' },
  { id: 'vendors', label: 'VENDORS' },
  { id: 'inventory', label: 'INVENTORY' },
  { id: 'recipes', label: 'RECIPES' },
  { id: 'menu-engineering', label: 'MENU ENG' },
  { id: 'prep-list', label: 'PREP-LIST' },
  { id: 'knowledge', label: 'KNOWLEDGE' },
  { id: 'test-lab', label: 'TEST LAB' },
  { id: 'settings', label: 'SETTINGS' },
  { id: 'about', label: 'ABOUT' },
]

const iconProps = { size: 15, strokeWidth: 1.5 }

const defaultStatus = {
  bot: { status: 'Unknown', running: false, pid: null },
  ollama: { status: 'Unknown', running: false, pids: [] },
  telemetry: {
    battery: null,
    core_temp: null,
    core_temp_estimated: false,
    signal: 0,
    position: 'KITCHEN A2',
  },
  uptime_seconds: 0,
  processing: false,
}

function formatUptime(totalSeconds) {
  const safe = Number.isFinite(totalSeconds) ? totalSeconds : 0
  const hours = String(Math.floor(safe / 3600)).padStart(2, '0')
  const minutes = String(Math.floor((safe % 3600) / 60)).padStart(2, '0')
  const seconds = String(safe % 60).padStart(2, '0')
  return `${hours}:${minutes}:${seconds}`
}

async function apiRequest(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      ...(options.headers || {}),
    },
  })

  const contentType = response.headers.get('content-type') || ''
  const payload = contentType.includes('application/json') ? await response.json() : await response.text()

  if (!response.ok) {
    const detail = typeof payload === 'string' ? payload : payload?.detail || payload?.message || 'Request failed'
    throw new Error(detail)
  }

  return payload
}

function Panel({ className = '', children }) {
  return <section className={`panel animate-in ${className}`}>{children}</section>
}

function SectionTitle({ overline, title, subtitle }) {
  return (
    <div className="animate-in space-y-3">
      <p className="tactical-label">{overline}</p>
      <h2 className="luxe-serif text-4xl leading-tight text-obsidian sm:text-5xl">{title}</h2>
      <p className="max-w-xl text-sm text-zinc-400 sm:text-base">{subtitle}</p>
    </div>
  )
}

function statusPillClass(isRunning) {
  return isRunning
    ? 'border-obsidian text-obsidian bg-white'
    : 'border-hairline text-zinc-400 bg-white'
}

function ServiceSwitch({ label, running, status, onToggle }) {
  return (
    <div className="flex items-center justify-between rounded-xl border border-hairline bg-bone/35 px-3 py-3">
      <div className="space-y-1">
        <p className="tactical-label">{label}</p>
        <p className={`text-xs font-semibold ${running ? 'text-emerald-600' : 'text-red-500'}`}>
          {status}
        </p>
      </div>

      <button
        type="button"
        role="switch"
        aria-checked={running}
        onClick={onToggle}
        className={`relative h-7 w-14 rounded-full border transition-all duration-300 ease-atelier ${running
          ? 'border-emerald-500 bg-emerald-500/20'
          : 'border-red-300 bg-red-500/10'
          }`}
      >
        <span
          className={`absolute top-1 h-5 w-5 rounded-full transition-all duration-300 ease-atelier ${running ? 'left-8 bg-emerald-600' : 'left-1 bg-red-500'
            }`}
        />
      </button>
    </div>
  )
}

function App() {
  const [activeTab, setActiveTab] = useState('overview')
  const [isSidebarOpen, setIsSidebarOpen] = useState(false)
  const [statusData, setStatusData] = useState(defaultStatus)
  const [autonomyStatus, setAutonomyStatus] = useState({
    status: 'Waiting for heartbeat',
    running: false,
    last_tick_at: null,
    last_action: null,
    queue_pending_ingests: 0,
    queue_pending_drafts: 0,
    error_count: 0,
  })
  const [autonomyLogs, setAutonomyLogs] = useState([])
  const [logs, setLogs] = useState([])
  const [logFilter, setLogFilter] = useState('all')
  const [sessions, setSessions] = useState([])
  const [selectedSessionId, setSelectedSessionId] = useState(null)
  const [sessionMessages, setSessionMessages] = useState([])
  const [knowledge, setKnowledge] = useState([])
  const [configData, setConfigData] = useState(null)
  const [configDraft, setConfigDraft] = useState(null)
  const [aboutInfo, setAboutInfo] = useState(null)

  // Feature 1 & 3 State
  const [vendors, setVendors] = useState([])
  const [editingVendor, setEditingVendor] = useState(null)
  const [editingDraft, setEditingDraft] = useState(null)

  const [recipes, setRecipes] = useState([])
  const [editingRecipe, setEditingRecipe] = useState(null)

  const [vendorItems, setVendorItems] = useState([])
  const [editingItem, setEditingItem] = useState(null)
  const [activeVendorForItems, setActiveVendorForItems] = useState(null)

  const [inventorySheets, setInventorySheets] = useState([])
  const [inventoryCounts, setInventoryCounts] = useState({})
  const [isCountingMode, setIsCountingMode] = useState(false)


  const [brainPrompt, setBrainPrompt] = useState('Suggest a compact prep plan for 60-cover lunch with one oven down.')
  const [brainAnswer, setBrainAnswer] = useState('')
  const [audioFile, setAudioFile] = useState(null)
  const [transcript, setTranscript] = useState('')

  const [uploadFile, setUploadFile] = useState(null)
  const [extractImages, setExtractImages] = useState(false)
  const [visionDescriptions, setVisionDescriptions] = useState(false)

  const [notice, setNotice] = useState('')
  const [error, setError] = useState('')
  const [processingAction, setProcessingAction] = useState(false)

  const uptime = useMemo(() => formatUptime(statusData.uptime_seconds), [statusData.uptime_seconds])

  const telemetryCards = useMemo(
    () => [
      {
        id: 'battery',
        label: 'BATTERY',
        value: statusData.telemetry.battery !== null ? `${statusData.telemetry.battery}%` : '--',
        detail: 'POWER CELL / NOMINAL',
        Icon: BatteryCharging,
      },
      {
        id: 'core-temp',
        label: 'CORE TEMP',
        value: statusData.telemetry.core_temp !== null ? `${statusData.telemetry.core_temp}C` : '--',
        detail: statusData.telemetry.core_temp_estimated ? 'ESTIMATED / SENSOR FALLBACK' : 'THERMAL SAFE RANGE',
        Icon: Thermometer,
      },
      {
        id: 'signal',
        label: 'SIGNAL',
        value: `${statusData.telemetry.signal || 0}%`,
        detail: 'LINK STABLE / LOW LATENCY',
        Icon: Radio,
      },
      {
        id: 'position',
        label: 'POSITION',
        value: statusData.telemetry.position || 'KITCHEN A2',
        detail: 'SERVICE LINE READY',
        Icon: MapPin,
      },
    ],
    [statusData.telemetry],
  )

  const isProcessing = processingAction || statusData.processing

  useEffect(() => {
    if (!isSidebarOpen) {
      return undefined
    }
    const handleEscape = (event) => {
      if (event.key === 'Escape') {
        setIsSidebarOpen(false)
      }
    }
    window.addEventListener('keydown', handleEscape)
    return () => window.removeEventListener('keydown', handleEscape)
  }, [isSidebarOpen])

  const clearFlash = () => {
    setError('')
    setNotice('')
  }

  const emitError = useCallback((message) => {
    setError(message)
    setNotice('')
  }, [])

  const emitNotice = useCallback((message) => {
    setNotice(message)
    setError('')
  }, [])

  const appendClientLog = useCallback((message) => {
    const ts = new Date().toLocaleTimeString('en-US', { hour12: false })
    const row = { ts, message, raw: `${ts} - ${message}` }
    setLogs((current) => [row, ...current].slice(0, 120))
  }, [])

  const loadStatus = useCallback(async () => {
    try {
      const payload = await apiRequest('/api/status')
      setStatusData(payload)
    } catch (err) {
      emitError(`Status load failed: ${err.message}`)
    }
  }, [emitError])

  const loadAutonomyStatus = useCallback(async () => {
    try {
      const payload = await apiRequest('/api/autonomy/status')
      setAutonomyStatus(payload)
    } catch (err) {
      emitError(`Autonomy status failed: ${err.message}`)
    }
  }, [emitError])

  const loadAutonomyLogs = useCallback(async () => {
    try {
      const payload = await apiRequest('/api/autonomy/logs')
      setAutonomyLogs(payload.items || [])
    } catch (err) {
      emitError(`Autonomy logs failed: ${err.message}`)
    }
  }, [emitError])

  const loadLogs = useCallback(async (filterValue) => {
    const level = filterValue || logFilter
    try {
      const payload = await apiRequest(`/api/logs?lines=120&level=${level}`)
      setLogs(payload.items || [])
    } catch (err) {
      emitError(`Logs load failed: ${err.message}`)
    }
  }, [emitError, logFilter])

  const loadSessions = useCallback(async () => {
    try {
      const payload = await apiRequest('/api/sessions')
      const rows = payload.items || []
      setSessions(rows)
      if (rows.length > 0 && !selectedSessionId) {
        setSelectedSessionId(rows[0].id)
      }
    } catch (err) {
      emitError(`Sessions load failed: ${err.message}`)
    }
  }, [emitError, selectedSessionId])

  const loadSessionMessages = useCallback(async (sessionId) => {
    if (!sessionId) {
      setSessionMessages([])
      return
    }
    try {
      const payload = await apiRequest(`/api/sessions/${sessionId}/messages?limit=10`)
      setSessionMessages(payload.items || [])
    } catch (err) {
      emitError(`Session messages load failed: ${err.message}`)
    }
  }, [emitError])

  const loadKnowledge = useCallback(async () => {
    try {
      const payload = await apiRequest('/api/knowledge')
      setKnowledge(payload.items || [])
    } catch (err) {
      emitError(`Knowledge load failed: ${err.message}`)
    }
  }, [emitError])

  const loadConfig = useCallback(async () => {
    try {
      const payload = await apiRequest('/api/config')
      const cfg = payload.config || {}
      setConfigData(cfg)
      setConfigDraft({
        model: cfg?.ollama?.model || 'llama3.1:8b',
        temperature: String(cfg?.ollama?.temperature ?? 0.7),
        maxTokens: String(cfg?.ollama?.max_tokens ?? 1000),
        topK: String(cfg?.rag?.top_k ?? 3),
        ragEnabled: Boolean(cfg?.rag?.enabled),
        ocrEnabled: Boolean(cfg?.rag?.ocr?.enabled ?? true),
        extractImages: Boolean(cfg?.rag?.image_processing?.extract_images),
        visionEnabled: Boolean(cfg?.rag?.vision?.enabled),
        visionModel: cfg?.rag?.vision?.model || '',
      })
    } catch (err) {
      emitError(`Config load failed: ${err.message}`)
    }
  }, [emitError])

  const loadAbout = useCallback(async () => {
    try {
      const payload = await apiRequest('/api/system/info')
      setAboutInfo(payload)
    } catch (err) {
      emitError(`System info load failed: ${err.message}`)
    }
  }, [emitError])

  useEffect(() => {
    clearFlash()
    loadStatus()
    loadLogs('all')
  }, [loadLogs, loadStatus])

  useEffect(() => {
    const timer = setInterval(() => {
      loadStatus()
      if (activeTab === 'overview') {
        loadLogs(logFilter)
      }
      if (activeTab === 'autonomy') {
        loadAutonomyStatus()
        loadAutonomyLogs()
      }
    }, 5000)

    return () => clearInterval(timer)
  }, [activeTab, logFilter, loadLogs, loadStatus])

  const loadVendors = useCallback(async () => {
    try {
      const payload = await apiRequest('/api/vendors')
      setVendors(payload.items || [])
    } catch (err) {
      emitError(`Vendors load failed: ${err.message}`)
    }
  }, [emitError])

  const saveVendor = async (vendor) => {
    try {
      const isNew = !vendor.id
      const url = isNew ? '/api/vendors' : `/api/vendors/${vendor.id}`
      const method = isNew ? 'POST' : 'PUT'

      await apiRequest(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(vendor),
      })

      await loadVendors()
      setEditingVendor(null)
      emitNotice(`Vendor ${isNew ? 'created' : 'updated'}.`)
    } catch (err) {
      emitError(`Save vendor failed: ${err.message}`)
    }
  }

  const deleteVendor = async (vendorId) => {
    if (!confirm('Are you sure you want to delete this vendor?')) return
    try {
      await apiRequest(`/api/vendors/${vendorId}`, { method: 'DELETE' })
      await loadVendors()
      emitNotice('Vendor deleted.')
    } catch (err) {
      emitError(`Delete vendor failed: ${err.message}`)
    }
  }

  const generateDraft = async () => {
    if (!editingDraft || !editingDraft.context) {
      emitError("Please enter order details first.")
      return
    }

    setProcessingAction(true)
    try {
      const payload = await apiRequest('/api/composer/draft', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ vendor_id: editingDraft.vendorId, context: editingDraft.context }),
      })

      setEditingDraft({
        ...editingDraft,
        subject: payload.subject,
        body: payload.body,
        vendorEmail: payload.vendor_email
      })
      emitNotice("Draft generated.")
    } catch (err) {
      emitError(`Draft generation failed: ${err.message}`)
    } finally {
      setProcessingAction(false)
    }
  }

  const openMailClient = () => {
    if (!editingDraft || !editingDraft.vendorEmail) {
      emitError("Missing vendor email.")
      return
    }

    const subject = encodeURIComponent(editingDraft.subject || "")
    const body = encodeURIComponent(editingDraft.body || "")
    window.open(`mailto:${editingDraft.vendorEmail}?subject=${subject}&body=${body}`, '_blank')
    setEditingDraft(null)
    emitNotice("Mail client opened.")
  }

  const loadVendorItems = async (vendorId) => {
    try {
      const payload = await apiRequest(`/api/vendors/${vendorId}/items`)
      setVendorItems(payload.items || [])
    } catch (err) {
      emitError(`Items load failed: ${err.message}`)
    }
  }

  const saveVendorItem = async (item) => {
    if (!activeVendorForItems) return
    try {
      const isNew = !item.id
      const url = isNew ? `/api/vendors/${activeVendorForItems.id}/items` : `/api/vendors/items/${item.id}`
      const method = isNew ? 'POST' : 'PUT'

      await apiRequest(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(item),
      })

      await loadVendorItems(activeVendorForItems.id)
      setEditingItem(null)
      emitNotice(`Item ${isNew ? 'added' : 'updated'}.`)
    } catch (err) {
      emitError(`Save item failed: ${err.message}`)
    }
  }

  const deleteVendorItem = async (itemId) => {
    if (!confirm('Delete item from guide?')) return
    try {
      await apiRequest(`/api/vendors/items/${itemId}`, { method: 'DELETE' })
      if (activeVendorForItems) await loadVendorItems(activeVendorForItems.id)
      emitNotice('Item deleted.')
    } catch (err) {
      emitError(`Delete item failed: ${err.message}`)
    }
  }

  const loadInventorySheets = useCallback(async () => {
    try {
      const payload = await apiRequest('/api/inventory/sheets')
      setInventorySheets(payload.categories || [])
    } catch (err) {
      emitError(`Inventory sheets load failed: ${err.message}`)
    }
  }, [emitError])

  const loadRecipes = useCallback(async () => {
    try {
      const payload = await apiRequest('/api/recipes')
      setRecipes(payload.recipes || [])
    } catch (err) {
      emitError(`Recipes load failed: ${err.message}`)
    }
  }, [emitError])

  const saveRecipe = async (recipe) => {
    try {
      const isNew = !recipe.id
      const url = isNew ? '/api/recipes' : `/api/recipes/${recipe.id}`
      const method = isNew ? 'POST' : 'PUT'

      await apiRequest(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(recipe),
      })

      await loadRecipes()
      setEditingRecipe(null)
      emitNotice(`Recipe ${isNew ? 'created' : 'updated'}.`)
    } catch (err) {
      emitError(`Save recipe failed: ${err.message}`)
    }
  }

  const deleteRecipe = async (recipeId) => {
    if (!confirm('Are you sure you want to delete this recipe?')) return
    try {
      await apiRequest(`/api/recipes/${recipeId}`, { method: 'DELETE' })
      await loadRecipes()
      emitNotice('Recipe deleted.')
    } catch (err) {
      emitError(`Delete recipe failed: ${err.message}`)
    }
  }

  useEffect(() => {
    if (activeTab === 'autonomy') {
      loadAutonomyStatus()
      loadAutonomyLogs()
    }
    if (activeTab === 'sessions') {
      loadSessions()
    }
    if (activeTab === 'knowledge') {
      loadKnowledge()
    }
    if (activeTab === 'vendors') {
      loadVendors()
    }
    if (activeTab === 'inventory') {
      loadInventorySheets()
    }
    if (activeTab === 'recipes') {
      loadRecipes()
    }
    if (activeTab === 'settings') {
      loadConfig()
    }
    if (activeTab === 'about') {
      loadAbout()
    }
  }, [activeTab, loadAbout, loadConfig, loadKnowledge, loadSessions, loadVendors, loadInventorySheets, loadRecipes])

  useEffect(() => {
    if (activeTab === 'sessions' && selectedSessionId) {
      loadSessionMessages(selectedSessionId)
    }
  }, [activeTab, selectedSessionId, loadSessionMessages])

  const controlBot = async (action) => {
    try {
      const payload = await apiRequest(`/api/control/bot/${action}`, { method: 'POST' })
      await loadStatus()
      await loadLogs(logFilter)
      emitNotice(payload.message || `Bot ${action} executed.`)
    } catch (err) {
      emitError(`Bot ${action} failed: ${err.message}`)
    }
  }

  const controlOllama = async (action) => {
    try {
      const payload = await apiRequest(`/api/control/ollama/${action}`, { method: 'POST' })
      await loadStatus()
      emitNotice(payload.message || `Ollama ${action} executed.`)
    } catch (err) {
      emitError(`Ollama ${action} failed: ${err.message}`)
    }
  }

  const handleManualOverride = () => {
    appendClientLog('Manual override engaged. Operator controlling sequence plan.')
    emitNotice('Manual override noted in diagnostics.')
  }

  const handleExecuteSequence = async () => {
    setProcessingAction(true)
    appendClientLog('Execute sequence accepted. Starting local services and restart cycle.')

    try {
      await apiRequest('/api/control/ollama/start', { method: 'POST' })
      await apiRequest('/api/control/bot/restart', { method: 'POST' })
      await loadStatus()
      await loadLogs(logFilter)
      emitNotice('Execute sequence completed.')
    } catch (err) {
      emitError(`Execute sequence failed: ${err.message}`)
    } finally {
      setProcessingAction(false)
    }
  }

  const handleEmergencyStop = async () => {
    setProcessingAction(false)
    appendClientLog('Emergency stop command issued. Halting automation.')
    await controlBot('stop')
  }

  const clearSelectedSession = async () => {
    if (!selectedSessionId) {
      return
    }

    try {
      const payload = await apiRequest(`/api/sessions/${selectedSessionId}/messages`, { method: 'DELETE' })
      emitNotice(`Cleared ${payload.deleted || 0} messages from session ${selectedSessionId}.`)
      await loadSessionMessages(selectedSessionId)
      await loadSessions()
    } catch (err) {
      emitError(`Clear session failed: ${err.message}`)
    }
  }

  const toggleKnowledgeSource = async (sourceId, active) => {
    try {
      await apiRequest(`/api/knowledge/${sourceId}/toggle`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ active }),
      })
      await loadKnowledge()
      emitNotice(`Source ${active ? 'enabled' : 'disabled'}.`)
    } catch (err) {
      emitError(`Toggle source failed: ${err.message}`)
    }
  }

  const deleteKnowledgeSource = async (sourceId) => {
    try {
      await apiRequest(`/api/knowledge/${sourceId}`, { method: 'DELETE' })
      await loadKnowledge()
      emitNotice('Source removed successfully.')
    } catch (err) {
      emitError(`Delete source failed: ${err.message}`)
    }
  }

  const uploadKnowledgeSource = async () => {
    if (!uploadFile) {
      emitError('Select a file before ingestion.')
      return
    }

    const formData = new FormData()
    formData.append('file', uploadFile)
    formData.append('extract_images', String(extractImages))
    formData.append('vision_descriptions', String(visionDescriptions))

    try {
      const payload = await apiRequest('/api/knowledge/upload', {
        method: 'POST',
        body: formData,
      })
      emitNotice(`Ingested ${payload.num_chunks} chunks from ${payload.source_title}.`)
      setUploadFile(null)
      await loadKnowledge()
    } catch (err) {
      emitError(`Ingestion failed: ${err.message}`)
    }
  }

  const runBrainTest = async () => {
    try {
      const payload = await apiRequest('/api/test/brain', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: brainPrompt }),
      })
      setBrainAnswer(payload.answer || '')
      emitNotice('Brain test completed.')
    } catch (err) {
      emitError(`Brain test failed: ${err.message}`)
    }
  }

  const runTranscriptionTest = async () => {
    if (!audioFile) {
      emitError('Select an audio file first.')
      return
    }

    const formData = new FormData()
    formData.append('file', audioFile)

    try {
      const payload = await apiRequest('/api/test/transcribe', {
        method: 'POST',
        body: formData,
      })
      setTranscript(payload.text || '')
      emitNotice('Transcription test completed.')
    } catch (err) {
      emitError(`Transcription failed: ${err.message}`)
    }
  }

  const saveSettings = async () => {
    if (!configData || !configDraft) {
      return
    }

    const next = JSON.parse(JSON.stringify(configData))
    next.ollama = next.ollama || {}
    next.rag = next.rag || {}
    next.rag.ocr = next.rag.ocr || {}
    next.rag.image_processing = next.rag.image_processing || {}
    next.rag.vision = next.rag.vision || {}

    next.ollama.model = configDraft.model
    next.ollama.temperature = Number(configDraft.temperature)
    next.ollama.max_tokens = Number(configDraft.maxTokens)
    next.rag.top_k = Number(configDraft.topK)
    next.rag.enabled = Boolean(configDraft.ragEnabled)
    next.rag.ocr.enabled = Boolean(configDraft.ocrEnabled)
    next.rag.image_processing.extract_images = Boolean(configDraft.extractImages)
    next.rag.vision.enabled = Boolean(configDraft.visionEnabled)
    next.rag.vision.model = configDraft.visionModel

    try {
      const payload = await apiRequest('/api/config', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(next),
      })
      setConfigData(payload.config)
      emitNotice('Configuration saved.')
      await loadStatus()
    } catch (err) {
      emitError(`Save config failed: ${err.message}`)
    }
  }

  return (
    <div className="relative min-h-screen bg-bone text-obsidian">
      <div className="noise-overlay" />

      <div className="relative z-10">
        <div
          className={`fixed inset-0 z-40 bg-black/40 transition-opacity duration-300 ${isSidebarOpen ? 'opacity-100' : 'pointer-events-none opacity-0'
            }`}
          onClick={() => setIsSidebarOpen(false)}
          aria-hidden={!isSidebarOpen}
        />
        <aside
          className={`fixed inset-y-0 left-0 z-50 w-[280px] border-r border-hairline/90 bg-white/95 shadow-2xl backdrop-blur-2xl transition-transform duration-300 ${isSidebarOpen ? 'translate-x-0' : '-translate-x-full'
            }`}
        >
          <div className="flex h-full flex-col">
            <div className="flex items-center justify-between border-b border-hairline/90 px-4 py-4">
              <div>
                <p className="tactical-label">NAVIGATION</p>
                <p className="text-sm font-semibold tracking-tight">Prep-Brain Dashboard</p>
              </div>
              <button
                type="button"
                onClick={() => setIsSidebarOpen(false)}
                className="rounded-lg border border-hairline bg-white p-2 text-zinc-500 transition-colors hover:text-obsidian"
                aria-label="Close menu"
              >
                <X size={16} />
              </button>
            </div>

            <div className="flex-1 overflow-y-auto p-3">
              <div className="mb-4 rounded-xl border border-hairline bg-bone/70 p-3">
                <p className="tactical-label">SYSTEM</p>
                <div className="mt-2 flex flex-wrap gap-2">
                  <span className={`rounded-full border px-2 py-1 text-[10px] font-semibold tracking-[0.18em] ${statusPillClass(statusData.bot.running)}`}>
                    BOT {statusData.bot.status.toUpperCase()}
                  </span>
                  <span className={`rounded-full border px-2 py-1 text-[10px] font-semibold tracking-[0.18em] ${statusPillClass(statusData.ollama.running)}`}>
                    OLLAMA {statusData.ollama.status.toUpperCase()}
                  </span>
                </div>
              </div>

              <nav className="space-y-1">
                {TABS.map((tab) => (
                  <button
                    key={tab.id}
                    type="button"
                    onClick={() => {
                      clearFlash()
                      setActiveTab(tab.id)
                      setIsSidebarOpen(false)
                    }}
                    className={`w-full rounded-xl border px-3 py-2 text-left text-[11px] font-bold tracking-[0.2em] transition-all duration-300 ease-atelier ${activeTab === tab.id
                      ? 'border-obsidian bg-obsidian text-white'
                      : 'border-hairline bg-white text-zinc-500 hover:border-zinc-300 hover:text-obsidian'
                      }`}
                  >
                    {tab.label}
                  </button>
                ))}
              </nav>
            </div>
          </div>
        </aside>

        <header className="sticky top-0 z-40 border-b border-hairline/90 bg-white/65 backdrop-blur-2xl">
          <div className="mx-auto max-w-[800px] px-4 py-4 sm:px-6">
            <div className="flex items-center justify-between gap-3">
              <div className="animate-in flex items-center gap-3">
                <button
                  type="button"
                  onClick={() => setIsSidebarOpen((open) => !open)}
                  className="rounded-lg border border-hairline bg-white p-2 text-zinc-500 transition-colors hover:text-obsidian"
                  aria-label="Open menu"
                >
                  <Menu size={16} />
                </button>
                <h1 className="text-base font-semibold tracking-tight">Prep-Brain Dashboard</h1>
              </div>

              <div className="animate-in text-right">
                <p className="tactical-label">UPTIME</p>
                <div className="tactical-value text-sm font-semibold text-zinc-500">{uptime}</div>
              </div>
            </div>

            <div className="animate-in mt-3 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span className={`rounded-full border px-2 py-1 text-[10px] font-semibold tracking-[0.18em] ${statusPillClass(statusData.bot.running)}`}>
                  BOT {statusData.bot.status.toUpperCase()}
                </span>
                <span className={`rounded-full border px-2 py-1 text-[10px] font-semibold tracking-[0.18em] ${statusPillClass(statusData.ollama.running)}`}>
                  OLLAMA {statusData.ollama.status.toUpperCase()}
                </span>
              </div>
              <p className="tactical-text text-[10px] text-zinc-400">API {API_BASE}</p>
            </div>
          </div>
        </header>

        <main className="mx-auto max-w-[800px] space-y-16 px-4 pb-44 pt-12 sm:px-6">
          {(notice || error) && (
            <Panel className={`p-4 ${error ? 'border-red-200 bg-red-50/50' : ''}`}>
              <p className={`text-sm ${error ? 'text-red-600' : 'text-zinc-600'}`}>{error || notice}</p>
            </Panel>
          )}

          {activeTab === 'overview' && (
            <>
              <SectionTitle
                overline="PRIMARY OBJECTIVE"
                title={statusData.bot.running ? 'System active and standby' : 'System idle and awaiting command'}
                subtitle="Maintain clean control over local bot operations while indexing kitchen intelligence with explicit OCR integrity."
              />

              <div className="animate-in space-y-2">
                <p className="tactical-label">BOT PROCESSING</p>
                {isProcessing ? <div className="animate-progress" /> : <div className="h-[1px] w-full bg-hairline" />}
              </div>

              <Panel className="p-5">
                <div className="mb-4 flex items-center justify-between">
                  <p className="tactical-label">CONTROL ACTIONS</p>
                  <RefreshCw {...iconProps} className="text-zinc-400" />
                </div>
                <div className="space-y-3">
                  <ServiceSwitch
                    label="BOT"
                    running={statusData.bot.running}
                    status={statusData.bot.running ? 'RUNNING' : 'STOPPED'}
                    onToggle={() => controlBot(statusData.bot.running ? 'stop' : 'start')}
                  />
                  <ServiceSwitch
                    label="OLLAMA"
                    running={statusData.ollama.running}
                    status={statusData.ollama.running ? 'RUNNING' : 'STOPPED'}
                    onToggle={() => controlOllama(statusData.ollama.running ? 'stop' : 'start')}
                  />
                  <div className="pt-1">
                    <button
                      type="button"
                      onClick={() => controlBot('restart')}
                      className="rounded-xl border border-hairline px-3 py-2 text-[10px] font-bold tracking-[0.2em] hover:border-zinc-300"
                    >
                      RESTART BOT
                    </button>
                  </div>
                </div>
              </Panel>

              <section className="grid animate-in grid-cols-1 gap-4 sm:grid-cols-2">
                {telemetryCards.map((card) => (
                  <Panel key={card.id} className="p-5">
                    <div className="mb-6 flex items-start justify-between">
                      <p className="tactical-label">{card.label}</p>
                      <Shield {...iconProps} className="text-zinc-400" />
                    </div>
                    <div className="flex items-end justify-between">
                      <div>
                        <p className="tactical-value text-xl font-bold tracking-tight">{card.value}</p>
                        <p className="mt-1 text-xs text-zinc-400">{card.detail}</p>
                      </div>
                      <card.Icon {...iconProps} className="text-zinc-400" />
                    </div>
                  </Panel>
                ))}
              </section>

              <Panel className="p-5">
                <div className="mb-4 flex items-center justify-between">
                  <p className="tactical-label">DIAGNOSTIC LOG</p>
                  <div className="flex items-center gap-2">
                    {['all', 'warnings', 'errors'].map((level) => (
                      <button
                        key={level}
                        type="button"
                        onClick={() => {
                          setLogFilter(level)
                          loadLogs(level)
                        }}
                        className={`rounded-full border px-2 py-1 text-[9px] font-bold tracking-[0.18em] ${logFilter === level
                          ? 'border-obsidian bg-obsidian text-white'
                          : 'border-hairline text-zinc-500'
                          }`}
                      >
                        {level.toUpperCase()}
                      </button>
                    ))}
                    <button
                      type="button"
                      onClick={() => loadLogs(logFilter)}
                      className="rounded-full border border-hairline px-2 py-1 text-[9px] font-bold tracking-[0.18em] text-zinc-500"
                    >
                      REFRESH
                    </button>
                  </div>
                </div>

                <div className="max-h-80 space-y-3 overflow-y-auto pr-1">
                  {logs.length === 0 && <p className="text-sm text-zinc-400">No log entries available.</p>}
                  {logs.map((entry, index) => (
                    <div key={`${entry.ts}-${index}`} className="flex flex-col gap-1 border-b border-hairline/70 pb-3 last:border-b-0">
                      <span className="tactical-text text-[11px] text-zinc-400">{entry.ts || '--:--:--'}</span>
                      <p className="text-sm text-obsidian/90">{entry.message || entry.raw}</p>
                    </div>
                  ))}
                </div>
              </Panel>
            </>
          )}

          {activeTab === 'autonomy' && (
            <AutonomyView
              status={autonomyStatus}
              logs={autonomyLogs}
              onRefresh={loadAutonomyLogs}
            />
          )}

          {activeTab === 'sessions' && (
            <>
              <SectionTitle
                overline="SESSION CONTROL"
                title="Operational memory lanes"
                subtitle="Track active and archived service sessions with direct access to stored message context."
              />

              <Panel className="overflow-hidden p-0">
                <div className="grid grid-cols-5 border-b border-hairline px-5 py-3">
                  {['SESSION', 'VENUE', 'MODE', 'UPDATED', 'STATUS'].map((column) => (
                    <div key={column} className="tactical-label">
                      {column}
                    </div>
                  ))}
                </div>

                {sessions.length === 0 && <p className="p-5 text-sm text-zinc-400">No sessions available yet.</p>}

                {sessions.map((row) => (
                  <button
                    key={row.id}
                    type="button"
                    onClick={() => setSelectedSessionId(row.id)}
                    className={`grid w-full grid-cols-5 items-center border-b border-hairline/60 px-5 py-4 text-left last:border-b-0 ${selectedSessionId === row.id ? 'bg-zinc-50/80' : 'bg-white'
                      }`}
                  >
                    <p className="tactical-text text-xs font-bold">S-{row.id}</p>
                    <p className="text-sm">{row.display_name}</p>
                    <p className="text-sm text-zinc-500">{row.message_count} MSG</p>
                    <p className="tactical-text text-xs text-zinc-500">{row.created_at}</p>
                    <span className={`inline-flex w-fit rounded-full border px-2 py-1 text-[10px] font-semibold tracking-[0.18em] ${row.is_active ? 'border-obsidian text-obsidian' : 'border-hairline text-zinc-400'}`}>
                      {row.is_active ? 'ACTIVE' : 'ARCHIVED'}
                    </span>
                  </button>
                ))}
              </Panel>

              <Panel className="p-5">
                <div className="mb-4 flex items-center justify-between">
                  <p className="tactical-label">SESSION MESSAGES</p>
                  <button
                    type="button"
                    onClick={clearSelectedSession}
                    disabled={!selectedSessionId}
                    className="rounded-full border border-hairline px-3 py-1 text-[9px] font-bold tracking-[0.18em] text-zinc-500 disabled:opacity-50"
                  >
                    CLEAR HISTORY
                  </button>
                </div>

                <p className="mb-3 text-xs text-zinc-400">Showing the latest 10 messages.</p>
                <div className="max-h-96 space-y-3 overflow-y-auto pr-1">
                  {sessionMessages.length === 0 && <p className="text-sm text-zinc-400">No messages in selected session.</p>}
                  {sessionMessages.map((message) => (
                    <div key={message.id} className="rounded-xl border border-hairline p-3">
                      <p className="tactical-label">{message.role}</p>
                      <p className="mt-2 text-sm text-zinc-700">{message.content}</p>
                      <p className="mt-2 tactical-text text-[10px] text-zinc-400">{message.created_at}</p>
                    </div>
                  ))}
                </div>
              </Panel>
            </>
          )}

          {activeTab === 'vendors' && (
            <>
              <SectionTitle
                overline="PROVIDER DIRECTORY"
                title="Supply Chain Network"
                subtitle="Manage vendor relationships, ordering windows, and contact points."
              />

              {!editingVendor && !editingDraft && !activeVendorForItems && (
                <Panel className="p-5">
                  <div className="mb-6 flex items-center justify-between">
                    <p className="tactical-label">ACTIVE VENDORS</p>
                    <button
                      type="button"
                      onClick={() => setEditingVendor({ name: '', contact_name: '', email: '', phone: '', ordering_window: '', preferred_method: 'email', notes: '' })}
                      className="rounded-full border border-obsidian bg-obsidian px-3 py-1 text-[10px] font-bold tracking-[0.18em] text-white"
                    >
                      ADD VENDOR
                    </button>
                  </div>

                  <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                    {vendors.length === 0 && <p className="text-sm text-zinc-400 col-span-2">No vendors defined.</p>}
                    {vendors.map((vendor) => (
                      <div key={vendor.id} className="group relative rounded-xl border border-hairline bg-white p-4 transition-all hover:border-zinc-300">
                        <div className="flex items-start justify-between">
                          <div>
                            <h3 className="font-semibold text-obsidian">{vendor.name}</h3>
                            <p className="text-xs text-zinc-500">{vendor.ordering_window || 'No schedule'}</p>
                          </div>
                          <div className="flex gap-2 opacity-0 transition-opacity group-hover:opacity-100">
                            <button
                              onClick={() => setEditingDraft({ vendorId: vendor.id, vendorName: vendor.name, context: '', subject: '', body: '' })}
                              className="text-zinc-400 hover:text-obsidian"
                              title="Compose Email"
                            >
                              <FileEdit size={14} />
                            </button>
                            <button
                              onClick={() => {
                                setActiveVendorForItems(vendor)
                                loadVendorItems(vendor.id)
                              }}
                              className="text-zinc-400 hover:text-obsidian"
                              title="Order Guide"
                            >
                              <Package size={14} />
                            </button>
                            <button onClick={() => setEditingVendor(vendor)} className="text-zinc-400 hover:text-obsidian">
                              <Settings2 size={14} />
                            </button>
                            <button onClick={() => deleteVendor(vendor.id)} className="text-zinc-400 hover:text-red-600">
                              <Trash2 size={14} />
                            </button>
                          </div>
                        </div>

                        <div className="mt-4 space-y-2">
                          <div className="flex items-center gap-2 text-xs text-zinc-600">
                            <Truck size={12} className="text-zinc-400" />
                            <span>{vendor.contact_name || 'No contact'}</span>
                          </div>
                          <div className="flex items-center gap-2 text-xs text-zinc-600">
                            <Mail size={12} className="text-zinc-400" />
                            <a href={`mailto:${vendor.email}`} className="hover:underline">{vendor.email || '--'}</a>
                          </div>
                          <div className="flex items-center gap-2 text-xs text-zinc-600">
                            <Phone size={12} className="text-zinc-400" />
                            <a href={`tel:${vendor.phone}`} className="hover:underline">{vendor.phone || '--'}</a>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </Panel>
              )}

              {activeVendorForItems && (
                <Panel className="p-5">
                  <div className="mb-6 flex items-center justify-between">
                    <p className="tactical-label">ORDER GUIDE / {activeVendorForItems.name.toUpperCase()}</p>
                    <button
                      type="button"
                      onClick={() => {
                        setActiveVendorForItems(null)
                        setEditingItem(null)
                      }}
                      className="text-[10px] font-bold tracking-[0.18em] text-zinc-400 hover:text-obsidian"
                    >
                      CLOSE
                    </button>
                  </div>

                  {!editingItem && (
                    <div className="space-y-4">
                      <div className="flex justify-end">
                        <button
                          onClick={() => setEditingItem({ name: '', item_code: '', unit: '', price: '', category: 'Produce', is_active: true })}
                          className="flex items-center gap-2 rounded-full border border-hairline px-3 py-1 text-[10px] font-bold tracking-[0.18em] hover:bg-zinc-50"
                        >
                          <Package size={12} /> ADD ITEM
                        </button>
                      </div>

                      {vendorItems.length === 0 && <p className="text-sm text-zinc-400">No items in guide.</p>}
                      <div className="space-y-2">
                        {vendorItems.map((item) => (
                          <div key={item.id} className="flex items-center justify-between rounded-lg border border-hairline bg-white p-3">
                            <div>
                              <p className="font-semibold text-sm">{item.name}</p>
                              <p className="text-xs text-zinc-400">{item.item_code ? `SKU: ${item.item_code}` : ''} {item.unit ? `(${item.unit})` : ''}</p>
                            </div>
                            <div className="flex items-center gap-4">
                              <p className="text-sm font-medium">{item.price ? `$${item.price.toFixed(2)}` : '--'}</p>
                              <div className="flex gap-2">
                                <button onClick={() => setEditingItem(item)} className="text-zinc-400 hover:text-obsidian"><Settings2 size={14} /></button>
                                <button onClick={() => deleteVendorItem(item.id)} className="text-zinc-400 hover:text-red-600"><Trash2 size={14} /></button>
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {editingItem && (
                    <form
                      onSubmit={(e) => {
                        e.preventDefault()
                        saveVendorItem(editingItem)
                      }}
                      className="space-y-4 rounded-xl border border-hairline bg-bone/30 p-4"
                    >
                      <div className="grid grid-cols-2 gap-3">
                        <div className="space-y-1">
                          <label className="text-[10px] font-bold tracking-wider text-zinc-500">ITEM NAME</label>
                          <input
                            required
                            className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm"
                            value={editingItem.name}
                            onChange={(e) => setEditingItem({ ...editingItem, name: e.target.value })}
                          />
                        </div>
                        <div className="space-y-1">
                          <label className="text-[10px] font-bold tracking-wider text-zinc-500">SKU / CODE</label>
                          <input
                            className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm"
                            value={editingItem.item_code}
                            onChange={(e) => setEditingItem({ ...editingItem, item_code: e.target.value })}
                          />
                        </div>
                        <div className="space-y-1">
                          <label className="text-[10px] font-bold tracking-wider text-zinc-500">UNIT</label>
                          <input
                            placeholder="e.g. Case, lb"
                            className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm"
                            value={editingItem.unit}
                            onChange={(e) => setEditingItem({ ...editingItem, unit: e.target.value })}
                          />
                        </div>
                        <div className="space-y-1">
                          <label className="text-[10px] font-bold tracking-wider text-zinc-500">PRICE</label>
                          <input
                            type="number"
                            step="0.01"
                            className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm"
                            value={editingItem.price}
                            onChange={(e) => setEditingItem({ ...editingItem, price: e.target.value })}
                          />
                        </div>
                      </div>

                      <div className="flex justify-end gap-3 pt-2">
                        <button
                          type="button"
                          onClick={() => setEditingItem(null)}
                          className="text-[10px] font-bold tracking-[0.18em] text-zinc-400 hover:text-obsidian"
                        >
                          CANCEL
                        </button>
                        <button
                          type="submit"
                          className="rounded-lg border border-obsidian bg-obsidian px-4 py-2 text-[10px] font-bold tracking-[0.2em] text-white"
                        >
                          SAVE ITEM
                        </button>
                      </div>
                    </form>
                  )}
                </Panel>
              )}

              {editingDraft && (
                <Panel className="p-5">
                  <div className="mb-6 flex items-center justify-between">
                    <p className="tactical-label">COMPOSE / {editingDraft.vendorName.toUpperCase()}</p>
                    <button
                      type="button"
                      onClick={() => setEditingDraft(null)}
                      className="text-[10px] font-bold tracking-[0.18em] text-zinc-400 hover:text-obsidian"
                    >
                      CANCEL
                    </button>
                  </div>

                  <div className="space-y-4">
                    <div className="space-y-1">
                      <label className="text-[10px] font-bold tracking-wider text-zinc-500">REQUIREMENTS / CONTEXT</label>
                      <textarea
                        rows={2}
                        placeholder="e.g. Order 5kg salmon and 3 cases of lemons for Friday delivery."
                        className="w-full rounded-lg border border-hairline bg-bone/50 px-3 py-2 text-sm"
                        value={editingDraft.context}
                        onChange={(e) => setEditingDraft({ ...editingDraft, context: e.target.value })}
                      />
                    </div>

                    <button
                      onClick={generateDraft}
                      disabled={isProcessing}
                      className="rounded-xl border border-hairline bg-white px-4 py-2 text-[10px] font-bold tracking-[0.2em] hover:bg-zinc-50 disabled:opacity-50"
                    >
                      {isProcessing ? 'GENERATING...' : 'GENERATE DRAFT'}
                    </button>

                    {editingDraft.body && (
                      <div className="animate-in space-y-3 border-t border-hairline pt-4">
                        <div className="space-y-1">
                          <label className="text-[10px] font-bold tracking-wider text-zinc-500">SUBJECT</label>
                          <input
                            className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm font-medium"
                            value={editingDraft.subject}
                            onChange={(e) => setEditingDraft({ ...editingDraft, subject: e.target.value })}
                          />
                        </div>
                        <div className="space-y-1">
                          <label className="text-[10px] font-bold tracking-wider text-zinc-500">BODY</label>
                          <textarea
                            rows={8}
                            className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm"
                            value={editingDraft.body}
                            onChange={(e) => setEditingDraft({ ...editingDraft, body: e.target.value })}
                          />
                        </div>

                        <div className="flex justify-end pt-2">
                          <button
                            onClick={openMailClient}
                            className="flex items-center gap-2 rounded-xl border border-obsidian bg-obsidian px-6 py-2 text-[10px] font-bold tracking-[0.2em] text-white"
                          >
                            <Send size={12} />
                            OPEN MAIL APP
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                </Panel>
              )}

              {editingVendor && (
                <Panel className="p-5">
                  <div className="mb-6 flex items-center justify-between">
                    <p className="tactical-label">{editingVendor.id ? 'EDIT VENDOR' : 'NEW VENDOR'}</p>
                    <button
                      type="button"
                      onClick={() => setEditingVendor(null)}
                      className="text-[10px] font-bold tracking-[0.18em] text-zinc-400 hover:text-obsidian"
                    >
                      CANCEL
                    </button>
                  </div>

                  <form
                    onSubmit={(e) => {
                      e.preventDefault()
                      saveVendor(editingVendor)
                    }}
                    className="space-y-4"
                  >
                    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                      <div className="space-y-1">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">PROVIDER NAME</label>
                        <input
                          required
                          className="w-full rounded-lg border border-hairline bg-bone/50 px-3 py-2 text-sm"
                          value={editingVendor.name}
                          onChange={(e) => setEditingVendor({ ...editingVendor, name: e.target.value })}
                        />
                      </div>
                      <div className="space-y-1">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">CONTACT NAME</label>
                        <input
                          className="w-full rounded-lg border border-hairline bg-bone/50 px-3 py-2 text-sm"
                          value={editingVendor.contact_name}
                          onChange={(e) => setEditingVendor({ ...editingVendor, contact_name: e.target.value })}
                        />
                      </div>
                      <div className="space-y-1">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">EMAIL</label>
                        <input
                          type="email"
                          className="w-full rounded-lg border border-hairline bg-bone/50 px-3 py-2 text-sm"
                          value={editingVendor.email}
                          onChange={(e) => setEditingVendor({ ...editingVendor, email: e.target.value })}
                        />
                      </div>
                      <div className="space-y-1">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">PHONE</label>
                        <input
                          type="tel"
                          className="w-full rounded-lg border border-hairline bg-bone/50 px-3 py-2 text-sm"
                          value={editingVendor.phone}
                          onChange={(e) => setEditingVendor({ ...editingVendor, phone: e.target.value })}
                        />
                      </div>
                      <div className="space-y-1">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">ORDER WINDOW</label>
                        <input
                          placeholder="e.g. Mon-Fri 8am-2pm"
                          className="w-full rounded-lg border border-hairline bg-bone/50 px-3 py-2 text-sm"
                          value={editingVendor.ordering_window}
                          onChange={(e) => setEditingVendor({ ...editingVendor, ordering_window: e.target.value })}
                        />
                      </div>
                      <div className="space-y-1">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">METHOD</label>
                        <select
                          className="w-full rounded-lg border border-hairline bg-bone/50 px-3 py-2 text-sm"
                          value={editingVendor.preferred_method}
                          onChange={(e) => setEditingVendor({ ...editingVendor, preferred_method: e.target.value })}
                        >
                          <option value="email">Email</option>
                          <option value="text">Text/SMS</option>
                          <option value="portal">Web Portal</option>
                          <option value="phone">Phone Call</option>
                        </select>
                      </div>
                    </div>

                    <div className="space-y-1">
                      <label className="text-[10px] font-bold tracking-wider text-zinc-500">NOTES</label>
                      <textarea
                        rows={3}
                        className="w-full rounded-lg border border-hairline bg-bone/50 px-3 py-2 text-sm"
                        value={editingVendor.notes}
                        onChange={(e) => setEditingVendor({ ...editingVendor, notes: e.target.value })}
                      />
                    </div>

                    <div className="mt-4 flex justify-end gap-3">
                      <button
                        type="submit"
                        className="rounded-xl border border-obsidian bg-obsidian px-6 py-2 text-[10px] font-bold tracking-[0.2em] text-white"
                      >
                        SAVE VENDOR
                      </button>
                    </div>
                  </form>
                </Panel>
              )}
            </>
          )}

          {activeTab === 'inventory' && (
            <>
              <style>{`
                  @media print {
                    body * { visibility: hidden; }
                    #inventory-print-area, #inventory-print-area * { visibility: visible; }
                    #inventory-print-area { position: absolute; left: 0; top: 0; width: 100%; padding: 20px; background: white; color: black; }
                    .no-print { display: none !important; }
                  }
               `}</style>
              <div className="no-print">
                <SectionTitle
                  overline="INVENTORY MANAGEMENT"
                  title="Physical Count Sheets"
                  subtitle="Generated from active vendor order guides. Use for weekly or daily stock takes."
                />
              </div>

              <Panel className="p-0 overflow-hidden bg-white">
                <div className="no-print flex items-center justify-between border-b border-hairline px-6 py-4">
                  <div className="flex items-center gap-4">
                    <p className="tactical-label">SHEET PREVIEW</p>
                    <button
                      onClick={() => setIsCountingMode(!isCountingMode)}
                      className={`text-[10px] font-bold tracking-[0.18em] ${isCountingMode ? 'text-obsidian' : 'text-zinc-400 hover:text-obsidian'}`}
                    >
                      {isCountingMode ? 'CANCEL COUNT' : 'ENTER COUNTS'}
                    </button>
                  </div>

                  {isCountingMode ? (
                    <button
                      onClick={saveInventoryCounts}
                      className="flex items-center gap-2 rounded-full border border-emerald-600 bg-emerald-600 px-4 py-1.5 text-[10px] font-bold tracking-[0.2em] text-white hover:bg-emerald-700"
                    >
                      <ClipboardList size={12} /> SAVE COUNTS
                    </button>
                  ) : (
                    <button
                      onClick={() => window.print()}
                      className="flex items-center gap-2 rounded-full border border-obsidian bg-obsidian px-4 py-1.5 text-[10px] font-bold tracking-[0.2em] text-white hover:bg-zinc-800"
                    >
                      <ClipboardList size={12} /> PRINT SHEET
                    </button>
                  )}
                </div>

                <div id="inventory-print-area" className="p-8 space-y-8">
                  <div className="mb-6 border-b-2 border-black pb-4">
                    <h1 className="text-3xl font-bold uppercase tracking-tight">Inventory Count Sheet</h1>
                    <p className="mt-1 text-sm text-zinc-500">Generated: {new Date().toLocaleDateString()} {new Date().toLocaleTimeString()}</p>
                  </div>

                  {inventorySheets.length === 0 && <p className="text-zinc-400 italic">No inventory items found. Add items to Vendor Order Guides first.</p>}

                  <div className="space-y-8">
                    {inventorySheets.map((cat) => (
                      <div key={cat.name} break-inside="avoid">
                        <h2 className="mb-3 text-lg font-bold uppercase tracking-wider border-b border-black/20 pb-1">{cat.name}</h2>
                        <table className="w-full text-left text-sm">
                          <thead>
                            <tr className="border-b border-black/10">
                              <th className="py-2 font-semibold w-[40%]">ITEM</th>
                              <th className="py-2 font-semibold w-[20%]">UNIT</th>
                              <th className="py-2 font-semibold w-[20%]">EXPECTED</th>
                              <th className="py-2 font-semibold w-[20%]">ACTUAL</th>
                            </tr>
                          </thead>
                          <tbody>
                            {cat.items.map((item) => (
                              <tr key={item.id} className="border-b border-black/5 last:border-0">
                                <td className="py-3 pr-2 font-medium">{item.name} <span className="text-xs text-zinc-400 font-normal">{item.item_code}</span></td>
                                <td className="py-3 pr-2 text-zinc-600">{item.unit || '-'}</td>
                                <td className="py-3 pr-2"></td>
                                <td className={`py-3 ${!isCountingMode ? 'border-l border-black/10 bg-zinc-50/50' : ''}`}>
                                  {isCountingMode ? (
                                    <input
                                      type="number"
                                      step="0.1"
                                      className="w-20 rounded border border-hairline bg-white px-2 py-1 text-right"
                                      placeholder="0.0"
                                      value={inventoryCounts[item.id] || ''}
                                      onChange={(e) => setInventoryCounts({ ...inventoryCounts, [item.id]: e.target.value })}
                                    />
                                  ) : (
                                    <div className="h-6"></div>
                                  )}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ))}
                  </div>
                </div>
              </Panel>
            </>
          )}

          {activeTab === 'recipes' && (
            <>
              <SectionTitle
                overline="KITCHEN INTELLIGENCE"
                title="Recipe Database"
                subtitle="Standardized recipes, yields, and preparation methods."
              />

              <Panel className="p-5 min-h-[500px]">
                <div className="mb-6 flex items-center justify-between">
                  <p className="tactical-label">ACTIVE RECIPES</p>
                  {!editingRecipe && (
                    <button
                      onClick={() => setEditingRecipe({ name: '', yield_amount: 1, yield_unit: 'portion', ingredients: [], instructions: '', is_active: true })}
                      className="flex items-center gap-2 rounded-full border border-hairline px-3 py-1 text-[10px] font-bold tracking-[0.18em] hover:bg-zinc-50"
                    >
                      <BookOpenText size={12} /> NEW RECIPE
                    </button>
                  )}
                  {editingRecipe && (
                    <button
                      onClick={() => setEditingRecipe(null)}
                      className="text-[10px] font-bold tracking-[0.18em] text-zinc-400 hover:text-obsidian"
                    >
                      CANCEL
                    </button>
                  )}
                </div>

                {!editingRecipe ? (
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {recipes.length === 0 && <p className="text-sm text-zinc-400 col-span-full italic">No recipes found.</p>}
                    {recipes.map(recipe => (
                      <div key={recipe.id} className="rounded-xl border border-hairline bg-white p-4 hover:shadow-sm transition-shadow">
                        <div className="flex justify-between items-start mb-2">
                          <h3 className="font-bold text-lg leading-tight">{recipe.name}</h3>
                          <div className="flex gap-2">
                            <button onClick={() => {
                              // Parse ingredients if string
                              let parsedIng = []
                              try { parsedIng = typeof recipe.ingredients === 'string' ? JSON.parse(recipe.ingredients) : recipe.ingredients } catch (e) { }
                              setEditingRecipe({ ...recipe, ingredients: parsedIng })
                            }} className="text-zinc-400 hover:text-obsidian"><FileEdit size={14} /></button>
                            <button onClick={() => deleteRecipe(recipe.id)} className="text-zinc-400 hover:text-red-500"><Trash2 size={14} /></button>
                          </div>
                        </div>
                        <p className="text-xs text-zinc-500 uppercase tracking-wider mb-2">Yield: {recipe.yield_amount} {recipe.yield_unit}</p>
                        <p className="text-sm font-bold text-emerald-600 mb-4">Est. Cost: ${(recipe.estimated_cost || 0).toFixed(2)}</p>
                        <div className="text-sm text-zinc-600 line-clamp-3 whitespace-pre-wrap font-serif">
                          {recipe.instructions || <span className="italic text-zinc-300">No instructions</span>}
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <form
                    onSubmit={(e) => { e.preventDefault(); saveRecipe(editingRecipe); }}
                    className="space-y-6 max-w-2xl mx-auto"
                  >
                    <div className="grid grid-cols-2 gap-4">
                      <div className="col-span-2 space-y-1">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">RECIPE NAME</label>
                        <input
                          required
                          className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm"
                          value={editingRecipe.name}
                          onChange={(e) => setEditingRecipe({ ...editingRecipe, name: e.target.value })}
                        />
                      </div>
                      <div className="space-y-1">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">YIELD AMOUNT</label>
                        <input
                          type="number"
                          step="0.01"
                          className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm"
                          value={editingRecipe.yield_amount}
                          onChange={(e) => setEditingRecipe({ ...editingRecipe, yield_amount: e.target.value })}
                        />
                      </div>
                      <div className="space-y-1">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">UNIT</label>
                        <input
                          className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm"
                          value={editingRecipe.yield_unit}
                          onChange={(e) => setEditingRecipe({ ...editingRecipe, yield_unit: e.target.value })}
                        />
                      </div>
                    </div>

                    <div className="space-y-2">
                      <div className="flex justify-between items-center">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">INGREDIENTS</label>
                        <button type="button" onClick={() => {
                          const newIngs = [...(editingRecipe.ingredients || []), { item: '', qty: '', unit: '' }]
                          setEditingRecipe({ ...editingRecipe, ingredients: newIngs })
                        }} className="text-xs text-obsidian font-bold hover:underline">+ ADD ROW</button>
                      </div>
                      <div className="space-y-2 bg-zinc-50/50 p-4 rounded-xl border border-hairline">
                        {(editingRecipe.ingredients || []).length === 0 && <p className="text-xs text-center text-zinc-400 italic">No ingredients added.</p>}
                        {(editingRecipe.ingredients || []).map((ing, idx) => (
                          <div key={idx} className="flex gap-2">
                            <input
                              placeholder="Item"
                              className="flex-1 rounded border border-hairline px-2 py-1 text-sm"
                              value={ing.item}
                              onChange={(e) => {
                                const newIngs = [...editingRecipe.ingredients]
                                newIngs[idx].item = e.target.value
                                setEditingRecipe({ ...editingRecipe, ingredients: newIngs })
                              }}
                            />
                            <input
                              placeholder="Qty"
                              className="w-20 rounded border border-hairline px-2 py-1 text-sm"
                              value={ing.qty}
                              onChange={(e) => {
                                const newIngs = [...editingRecipe.ingredients]
                                newIngs[idx].qty = e.target.value
                                setEditingRecipe({ ...editingRecipe, ingredients: newIngs })
                              }}
                            />
                            <input
                              placeholder="Unit"
                              className="w-20 rounded border border-hairline px-2 py-1 text-sm"
                              value={ing.unit}
                              onChange={(e) => {
                                const newIngs = [...editingRecipe.ingredients]
                                newIngs[idx].unit = e.target.value
                                setEditingRecipe({ ...editingRecipe, ingredients: newIngs })
                              }}
                            />
                            <button type="button" onClick={() => {
                              const newIngs = editingRecipe.ingredients.filter((_, i) => i !== idx)
                              setEditingRecipe({ ...editingRecipe, ingredients: newIngs })
                            }} className="text-zinc-400 hover:text-red-500"><Trash2 size={14} /></button>
                          </div>
                        ))}
                      </div>
                    </div>

                    <div className="space-y-1">
                      <label className="text-[10px] font-bold tracking-wider text-zinc-500">METHOD / INSTRUCTIONS</label>
                      <textarea
                        className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm h-32 font-serif"
                        value={editingRecipe.instructions}
                        onChange={(e) => setEditingRecipe({ ...editingRecipe, instructions: e.target.value })}
                      />
                    </div>

                    <div className="flex justify-end pt-4">
                      <button
                        type="submit"
                        className="rounded-xl border border-obsidian bg-obsidian px-6 py-2 text-[10px] font-bold tracking-[0.2em] text-white"
                      >
                        SAVE RECIPE
                      </button>
                    </div>
                  </form>
                )}
              </Panel>
            </>
          )}

          {activeTab === 'recipes' && (
            <>
              <SectionTitle
                overline="KITCHEN INTELLIGENCE"
                title="Recipe Database"
                subtitle="Standardized recipes, yields, and preparation methods."
              />

              <Panel className="p-5 min-h-[500px]">
                <div className="mb-6 flex items-center justify-between">
                  <p className="tactical-label">ACTIVE RECIPES</p>
                  {!editingRecipe && (
                    <button
                      onClick={() => setEditingRecipe({ name: '', yield_amount: 1, yield_unit: 'portion', ingredients: [], instructions: '', is_active: true })}
                      className="flex items-center gap-2 rounded-full border border-hairline px-3 py-1 text-[10px] font-bold tracking-[0.18em] hover:bg-zinc-50"
                    >
                      <BookOpenText size={12} /> NEW RECIPE
                    </button>
                  )}
                  {editingRecipe && (
                    <button
                      onClick={() => setEditingRecipe(null)}
                      className="text-[10px] font-bold tracking-[0.18em] text-zinc-400 hover:text-obsidian"
                    >
                      CANCEL
                    </button>
                  )}
                </div>

                {!editingRecipe ? (
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {recipes.length === 0 && <p className="text-sm text-zinc-400 col-span-full italic">No recipes found.</p>}
                    {recipes.map(recipe => (
                      <div key={recipe.id} className="rounded-xl border border-hairline bg-white p-4 hover:shadow-sm transition-shadow">
                        <div className="flex justify-between items-start mb-2">
                          <h3 className="font-bold text-lg leading-tight">{recipe.name}</h3>
                          <div className="flex gap-2">
                            <button onClick={() => {
                              // Parse ingredients if string
                              let parsedIng = []
                              try { parsedIng = typeof recipe.ingredients === 'string' ? JSON.parse(recipe.ingredients) : recipe.ingredients } catch (e) { }
                              setEditingRecipe({ ...recipe, ingredients: parsedIng })
                            }} className="text-zinc-400 hover:text-obsidian"><FileEdit size={14} /></button>
                            <button onClick={() => deleteRecipe(recipe.id)} className="text-zinc-400 hover:text-red-500"><Trash2 size={14} /></button>
                          </div>
                        </div>
                        <p className="text-xs text-zinc-500 uppercase tracking-wider mb-4">Yield: {recipe.yield_amount} {recipe.yield_unit}</p>
                        <div className="text-sm text-zinc-600 line-clamp-3 whitespace-pre-wrap font-serif">
                          {recipe.instructions || <span className="italic text-zinc-300">No instructions</span>}
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <form
                    onSubmit={(e) => { e.preventDefault(); saveRecipe(editingRecipe); }}
                    className="space-y-6 max-w-2xl mx-auto"
                  >
                    <div className="grid grid-cols-2 gap-4">
                      <div className="col-span-2 space-y-1">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">RECIPE NAME</label>
                        <input
                          required
                          className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm"
                          value={editingRecipe.name}
                          onChange={(e) => setEditingRecipe({ ...editingRecipe, name: e.target.value })}
                        />
                      </div>
                      <div className="space-y-1">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">YIELD AMOUNT</label>
                        <input
                          type="number"
                          step="0.01"
                          className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm"
                          value={editingRecipe.yield_amount}
                          onChange={(e) => setEditingRecipe({ ...editingRecipe, yield_amount: e.target.value })}
                        />
                      </div>
                      <div className="space-y-1">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">UNIT</label>
                        <input
                          className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm"
                          value={editingRecipe.yield_unit}
                          onChange={(e) => setEditingRecipe({ ...editingRecipe, yield_unit: e.target.value })}
                        />
                      </div>
                      <div className="space-y-1">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">MENU PRICE ($)</label>
                        <input
                          type="number"
                          step="0.01"
                          className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm"
                          value={editingRecipe.sales_price || 0}
                          onChange={(e) => setEditingRecipe({ ...editingRecipe, sales_price: e.target.value })}
                        />
                      </div>
                      <div className="space-y-1">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">WKLY SALES (QTY)</label>
                        <input
                          type="number"
                          className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm"
                          value={editingRecipe.recent_sales_count || 0}
                          onChange={(e) => setEditingRecipe({ ...editingRecipe, recent_sales_count: e.target.value })}
                        />
                      </div>
                      <div className="space-y-1">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">PAR LEVEL (QTY)</label>
                        <input
                          type="number"
                          step="0.1"
                          className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm"
                          value={editingRecipe.par_level || 0}
                          onChange={(e) => setEditingRecipe({ ...editingRecipe, par_level: e.target.value })}
                        />
                      </div>
                    </div>

                    <div className="space-y-2">
                      <div className="flex justify-between items-center">
                        <label className="text-[10px] font-bold tracking-wider text-zinc-500">INGREDIENTS</label>
                        <button type="button" onClick={() => {
                          const newIngs = [...(editingRecipe.ingredients || []), { item: '', qty: '', unit: '' }]
                          setEditingRecipe({ ...editingRecipe, ingredients: newIngs })
                        }} className="text-xs text-obsidian font-bold hover:underline">+ ADD ROW</button>
                      </div>
                      <div className="space-y-2 bg-zinc-50/50 p-4 rounded-xl border border-hairline">
                        {(editingRecipe.ingredients || []).length === 0 && <p className="text-xs text-center text-zinc-400 italic">No ingredients added.</p>}
                        {(editingRecipe.ingredients || []).map((ing, idx) => (
                          <div key={idx} className="flex gap-2">
                            <input
                              placeholder="Item"
                              className="flex-1 rounded border border-hairline px-2 py-1 text-sm"
                              value={ing.item}
                              onChange={(e) => {
                                const newIngs = [...editingRecipe.ingredients]
                                newIngs[idx].item = e.target.value
                                setEditingRecipe({ ...editingRecipe, ingredients: newIngs })
                              }}
                            />
                            <input
                              placeholder="Qty"
                              className="w-20 rounded border border-hairline px-2 py-1 text-sm"
                              value={ing.qty}
                              onChange={(e) => {
                                const newIngs = [...editingRecipe.ingredients]
                                newIngs[idx].qty = e.target.value
                                setEditingRecipe({ ...editingRecipe, ingredients: newIngs })
                              }}
                            />
                            <input
                              placeholder="Unit"
                              className="w-20 rounded border border-hairline px-2 py-1 text-sm"
                              value={ing.unit}
                              onChange={(e) => {
                                const newIngs = [...editingRecipe.ingredients]
                                newIngs[idx].unit = e.target.value
                                setEditingRecipe({ ...editingRecipe, ingredients: newIngs })
                              }}
                            />
                            <button type="button" onClick={() => {
                              const newIngs = editingRecipe.ingredients.filter((_, i) => i !== idx)
                              setEditingRecipe({ ...editingRecipe, ingredients: newIngs })
                            }} className="text-zinc-400 hover:text-red-500"><Trash2 size={14} /></button>
                          </div>
                        ))}
                      </div>
                    </div>

                    <div className="space-y-1">
                      <label className="text-[10px] font-bold tracking-wider text-zinc-500">METHOD / INSTRUCTIONS</label>
                      <textarea
                        className="w-full rounded-lg border border-hairline bg-white px-3 py-2 text-sm h-32 font-serif"
                        value={editingRecipe.instructions}
                        onChange={(e) => setEditingRecipe({ ...editingRecipe, instructions: e.target.value })}
                      />
                    </div>

                    <div className="flex justify-end pt-4">
                      <button
                        type="submit"
                        className="rounded-xl border border-obsidian bg-obsidian px-6 py-2 text-[10px] font-bold tracking-[0.2em] text-white"
                      >
                        SAVE RECIPE
                      </button>
                    </div>
                  </form>
                )}
              </Panel>
            </>
          )}

          {activeTab === 'menu-engineering' && (
            <>
              <SectionTitle
                overline="KITCHEN INTELLIGENCE"
                title="Menu Engineering"
                subtitle="Profitability vs. Popularity Analysis."
              />

              <Panel className="p-5 min-h-[500px]">
                <MenuEngineeringView />
              </Panel>
            </>
          )}

          {activeTab === 'prep-list' && (
            <>
              <SectionTitle
                overline="PREP OPERATIONS"
                title="Prep-List"
                subtitle="Station-centric prep audit and production requirements from par levels."
              />

              <Panel className="p-5 min-h-[500px]">
                <PrepListView />
              </Panel>
            </>
          )}

          {activeTab === 'knowledge' && (
            <>
              <SectionTitle
                overline="KNOWLEDGE CONTROL"
                title="Grounded sources with OCR truth"
                subtitle="Ingestion, OCR status, and source activation are directly managed from this control plane."
              />

              <Panel className="p-5">
                <div className="mb-4 flex items-center justify-between">
                  <p className="tactical-label">UPLOAD SOURCE</p>
                  <Upload {...iconProps} className="text-zinc-400" />
                </div>

                <div className="space-y-4">
                  <input
                    type="file"
                    accept=".pdf,.txt,.docx"
                    onChange={(event) => setUploadFile(event.target.files?.[0] || null)}
                    className="w-full rounded-xl border border-hairline bg-bone/60 p-3 text-sm"
                  />

                  <div className="flex flex-wrap gap-4">
                    <label className="flex items-center gap-2 text-xs text-zinc-600">
                      <input type="checkbox" checked={extractImages} onChange={(event) => setExtractImages(event.target.checked)} />
                      Extract images
                    </label>
                    <label className="flex items-center gap-2 text-xs text-zinc-600">
                      <input
                        type="checkbox"
                        checked={visionDescriptions}
                        onChange={(event) => setVisionDescriptions(event.target.checked)}
                      />
                      Vision descriptions (manual)
                    </label>
                  </div>

                  <button
                    type="button"
                    onClick={uploadKnowledgeSource}
                    className="rounded-xl border border-obsidian bg-obsidian px-4 py-2 text-[10px] font-bold tracking-[0.2em] text-white"
                  >
                    INGEST FILE
                  </button>

                  <p className="text-xs text-zinc-400">Recommended OCR preprocess: <code>ocrmypdf --skip-text input.pdf output_ocr.pdf</code></p>
                </div>
              </Panel>

              <div className="space-y-4">
                {knowledge.length === 0 && (
                  <Panel className="p-5">
                    <p className="text-sm text-zinc-400">No ingested sources yet.</p>
                  </Panel>
                )}

                {knowledge.map((source) => (
                  <Panel key={source.id} className="p-5">
                    {(() => {
                      const extractedChars = Number(source.extracted_text_chars || source.text_chars_after_ocr || source.text_chars_before_ocr || 0)
                      const tableChars = Number(source.extracted_from_tables_chars || 0)
                      const paragraphChars = Number(source.extracted_from_paragraphs_chars || 0)
                      const textProfile =
                        source.text_profile_label ||
                        (source.image_rich
                          ? 'IMAGE-RICH'
                          : extractedChars >= 20000
                          ? 'TEXT-RICH'
                          : tableChars > paragraphChars && tableChars > 0
                          ? 'TABLES-ONLY'
                          : 'LOW TEXT')
                      const textProfileClass =
                        textProfile === 'IMAGE-RICH'
                          ? 'border-red-200 text-red-500'
                          : textProfile === 'TEXT-RICH'
                          ? 'border-hairline text-zinc-500'
                          : textProfile === 'TABLES-ONLY'
                          ? 'border-amber-300 text-amber-600'
                          : 'border-orange-300 text-orange-600'
                      return (
                    <>
                    <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                      <div>
                        <p className="text-base font-semibold tracking-tight">{source.title || source.source_name}</p>
                        <p className="tactical-text mt-1 text-[11px] text-zinc-400">{source.chunk_count || 0} CHUNKS</p>
                      </div>

                      <div className="flex flex-wrap gap-2">
                        <span className={`rounded-full border px-3 py-1 text-[10px] font-semibold tracking-[0.18em] ${textProfileClass}`}>
                          {textProfile}
                        </span>
                        <span className={`rounded-full border px-3 py-1 text-[10px] font-semibold tracking-[0.18em] ${source.ocr_applied ? 'border-obsidian text-obsidian' : source.ocr_required ? 'border-red-300 text-red-500' : 'border-hairline text-zinc-500'}`}>
                          OCR {source.ocr_applied ? 'APPLIED' : source.ocr_required ? 'REQUIRED' : 'N/A'}
                        </span>
                        <span className={`rounded-full border px-3 py-1 text-[10px] font-semibold tracking-[0.18em] ${source.status === 'active' ? 'border-obsidian text-obsidian' : 'border-hairline text-zinc-400'}`}>
                          {(source.status || 'disabled').toUpperCase()}
                        </span>
                        <span className="rounded-full border border-hairline px-3 py-1 text-[10px] font-semibold tracking-[0.18em] text-zinc-500">
                          INGEST {(source.ingest_status || 'unknown').toUpperCase()}
                        </span>
                      </div>
                    </div>

                    <div className="mt-4 flex flex-wrap gap-2">
                      <button
                        type="button"
                        disabled={!source.can_toggle}
                        onClick={() => toggleKnowledgeSource(source.id, source.status !== 'active')}
                        className={`rounded-xl border px-3 py-2 text-[10px] font-bold tracking-[0.2em] ${source.can_toggle ? 'border-hairline' : 'border-hairline text-zinc-300 cursor-not-allowed'}`}
                      >
                        {source.status === 'active' ? 'DISABLE' : 'ENABLE'}
                      </button>

                      <button
                        type="button"
                        onClick={() => deleteKnowledgeSource(source.id)}
                        className="rounded-xl border border-red-300 px-3 py-2 text-[10px] font-bold tracking-[0.2em] text-red-500"
                      >
                        REMOVE
                      </button>
                    </div>

                    {Array.isArray(source.warnings) && source.warnings.length > 0 && (
                      <div className="mt-4 space-y-1 rounded-xl border border-hairline bg-zinc-50/60 p-3">
                        <p className="tactical-label">WARNINGS</p>
                        {source.warnings.slice(0, 3).map((warning, idx) => (
                          <p key={`${source.id}-warn-${idx}`} className="text-xs text-zinc-500">
                            - {warning}
                          </p>
                        ))}
                      </div>
                    )}
                    </>
                      )
                    })()}
                  </Panel>
                ))}
              </div>
            </>
          )}

          {activeTab === 'test-lab' && (
            <>
              <SectionTitle
                overline="TEST LAB"
                title="Controlled sequence simulation"
                subtitle="Run direct backend calls for brain responses and transcription without leaving the command surface."
              />

              <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                <Panel className="p-5">
                  <p className="tactical-label">BRAIN PROMPT TEST</p>
                  <textarea
                    value={brainPrompt}
                    onChange={(event) => setBrainPrompt(event.target.value)}
                    className="mt-4 h-40 w-full resize-none rounded-xl border border-hairline bg-bone/60 p-3 text-sm outline-none"
                  />
                  <button
                    type="button"
                    onClick={runBrainTest}
                    className="mt-4 inline-flex items-center gap-2 rounded-full border border-obsidian bg-obsidian px-4 py-2 text-[10px] font-bold tracking-[0.24em] text-white"
                  >
                    <Play {...iconProps} /> RUN SCENARIO
                  </button>

                  {brainAnswer && (
                    <div className="mt-4 rounded-xl border border-hairline p-3">
                      <p className="tactical-label">RESPONSE</p>
                      <p className="mt-2 text-sm text-zinc-700">{brainAnswer}</p>
                    </div>
                  )}
                </Panel>

                <Panel className="p-5">
                  <p className="tactical-label">VOICE PIPELINE CHECK</p>
                  <p className="mt-4 text-sm text-zinc-500">Upload sample audio and run backend transcription.</p>
                  <input
                    type="file"
                    accept="audio/*"
                    onChange={(event) => setAudioFile(event.target.files?.[0] || null)}
                    className="mt-4 w-full rounded-xl border border-hairline bg-bone/60 p-3 text-sm"
                  />
                  <button
                    type="button"
                    onClick={runTranscriptionTest}
                    className="mt-4 inline-flex items-center gap-2 rounded-full border border-hairline px-4 py-2 text-[10px] font-bold tracking-[0.24em]"
                  >
                    <Clock3 {...iconProps} /> TRANSCRIBE
                  </button>

                  {transcript && (
                    <div className="mt-4 rounded-xl border border-hairline p-3">
                      <p className="tactical-label">TRANSCRIPT</p>
                      <p className="mt-2 text-sm text-zinc-700">{transcript}</p>
                    </div>
                  )}
                </Panel>
              </div>
            </>
          )}

          {activeTab === 'settings' && (
            <>
              <SectionTitle
                overline="SYSTEM SETTINGS"
                title="Runtime behavior controls"
                subtitle="Update model, retrieval, and OCR defaults directly in config.yaml through the API."
              />

              <Panel className="p-5">
                {!configDraft && <p className="text-sm text-zinc-400">Loading configuration...</p>}

                {configDraft && (
                  <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                    <label className="text-sm text-zinc-600">
                      Model
                      <input
                        value={configDraft.model}
                        onChange={(event) => setConfigDraft((prev) => ({ ...prev, model: event.target.value }))}
                        className="mt-1 w-full rounded-xl border border-hairline bg-bone/60 p-2"
                      />
                    </label>

                    <label className="text-sm text-zinc-600">
                      Temperature
                      <input
                        type="number"
                        step="0.1"
                        value={configDraft.temperature}
                        onChange={(event) => setConfigDraft((prev) => ({ ...prev, temperature: event.target.value }))}
                        className="mt-1 w-full rounded-xl border border-hairline bg-bone/60 p-2"
                      />
                    </label>

                    <label className="text-sm text-zinc-600">
                      Max Tokens
                      <input
                        type="number"
                        value={configDraft.maxTokens}
                        onChange={(event) => setConfigDraft((prev) => ({ ...prev, maxTokens: event.target.value }))}
                        className="mt-1 w-full rounded-xl border border-hairline bg-bone/60 p-2"
                      />
                    </label>

                    <label className="text-sm text-zinc-600">
                      RAG Top K
                      <input
                        type="number"
                        value={configDraft.topK}
                        onChange={(event) => setConfigDraft((prev) => ({ ...prev, topK: event.target.value }))}
                        className="mt-1 w-full rounded-xl border border-hairline bg-bone/60 p-2"
                      />
                    </label>

                    <label className="flex items-center gap-2 text-sm text-zinc-600">
                      <input
                        type="checkbox"
                        checked={configDraft.ragEnabled}
                        onChange={(event) => setConfigDraft((prev) => ({ ...prev, ragEnabled: event.target.checked }))}
                      />
                      RAG Enabled
                    </label>

                    <label className="flex items-center gap-2 text-sm text-zinc-600">
                      <input
                        type="checkbox"
                        checked={configDraft.ocrEnabled}
                        onChange={(event) => setConfigDraft((prev) => ({ ...prev, ocrEnabled: event.target.checked }))}
                      />
                      OCR Enabled
                    </label>

                    <label className="flex items-center gap-2 text-sm text-zinc-600">
                      <input
                        type="checkbox"
                        checked={configDraft.extractImages}
                        onChange={(event) => setConfigDraft((prev) => ({ ...prev, extractImages: event.target.checked }))}
                      />
                      Extract Images by Default
                    </label>

                    <label className="flex items-center gap-2 text-sm text-zinc-600">
                      <input
                        type="checkbox"
                        checked={configDraft.visionEnabled}
                        onChange={(event) => setConfigDraft((prev) => ({ ...prev, visionEnabled: event.target.checked }))}
                      />
                      Vision Descriptions Enabled
                    </label>

                    <label className="text-sm text-zinc-600 sm:col-span-2">
                      Vision Model
                      <input
                        value={configDraft.visionModel}
                        onChange={(event) => setConfigDraft((prev) => ({ ...prev, visionModel: event.target.value }))}
                        className="mt-1 w-full rounded-xl border border-hairline bg-bone/60 p-2"
                      />
                    </label>
                  </div>
                )}

                <div className="mt-5 flex gap-2">
                  <button
                    type="button"
                    onClick={saveSettings}
                    className="rounded-xl border border-obsidian bg-obsidian px-4 py-2 text-[10px] font-bold tracking-[0.2em] text-white"
                  >
                    SAVE SETTINGS
                  </button>
                  <button
                    type="button"
                    onClick={loadConfig}
                    className="rounded-xl border border-hairline px-4 py-2 text-[10px] font-bold tracking-[0.2em]"
                  >
                    RELOAD
                  </button>
                </div>
              </Panel>
            </>
          )}

          {activeTab === 'about' && (
            <>
              <SectionTitle
                overline="SYSTEM PROFILE"
                title="Atelier Tactical interface"
                subtitle="A local-first operational surface wired directly to the backend controller API."
              />

              <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
                <Panel className="p-5">
                  <Layers3 {...iconProps} className="text-zinc-400" />
                  <p className="mt-4 tactical-label">ARCHITECTURE</p>
                  <p className="mt-3 text-sm text-zinc-500">React + Tailwind frontend with FastAPI control plane and local Python services.</p>
                </Panel>
                <Panel className="p-5">
                  <BookOpenText {...iconProps} className="text-zinc-400" />
                  <p className="mt-4 tactical-label">DATA LAYER</p>
                  <p className="mt-3 text-sm text-zinc-500">SQLite sessions, ChromaDB knowledge sources, and explicit OCR/source flags.</p>
                </Panel>
                <Panel className="p-5">
                  <Info {...iconProps} className="text-zinc-400" />
                  <p className="mt-4 tactical-label">RUNTIME</p>
                  <p className="mt-3 text-sm text-zinc-500">Backend endpoints for control, ingestion, logs, test lab, and live config updates.</p>
                </Panel>
              </div>

              <Panel className="p-5">
                <p className="tactical-label">SYSTEM INFO</p>
                <div className="mt-4 space-y-2 text-sm text-zinc-500">
                  <p>Python: {aboutInfo?.python_version || '--'}</p>
                  <p>Platform: {aboutInfo?.platform || '--'}</p>
                  <p>API Started At (epoch): {aboutInfo?.api_started_at || '--'}</p>
                  <p>Workspace: {aboutInfo?.cwd || '--'}</p>
                </div>
              </Panel>
            </>
          )}
        </main>

        <div className="pointer-events-none fixed inset-x-0 bottom-4 z-50 flex justify-center px-4">
          <div className="panel pointer-events-auto flex w-full max-w-[820px] items-center justify-between gap-2 rounded-2xl border border-hairline/90 bg-white/85 p-2 backdrop-blur-xl">
            <button
              type="button"
              onClick={handleManualOverride}
              className="flex flex-1 items-center justify-center gap-2 rounded-xl border border-hairline bg-white px-4 py-3 text-[10px] font-bold tracking-[0.24em] text-obsidian hover:border-zinc-300"
            >
              <Hand {...iconProps} /> [ MANUAL OVERRIDE ]
            </button>

            <button
              type="button"
              onClick={handleExecuteSequence}
              className="flex flex-1 items-center justify-center gap-2 rounded-xl border border-obsidian bg-obsidian px-4 py-3 text-[10px] font-bold tracking-[0.24em] text-white hover:opacity-95"
            >
              <Play {...iconProps} /> [ EXECUTE SEQUENCE ]
            </button>

            <button
              type="button"
              onClick={handleEmergencyStop}
              className="flex flex-1 items-center justify-center gap-2 rounded-xl border border-red-400 bg-white px-4 py-3 text-[10px] font-bold tracking-[0.24em] text-red-500 hover:bg-red-50"
            >
              <AlertTriangle {...iconProps} /> [ EMERGENCY_STOP ]
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}

export default App
