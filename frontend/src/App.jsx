import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  AlertTriangle,
  BatteryCharging,
  BookOpenText,
  Clock3,
  Hand,
  Info,
  Layers3,
  MapPin,
  Play,
  Radio,
  RefreshCw,
  Settings2,
  Shield,
  Thermometer,
  Trash2,
  Upload,
} from 'lucide-react'

const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8000'

const TABS = [
  { id: 'overview', label: 'CONTROL' },
  { id: 'sessions', label: 'SESSIONS' },
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
        className={`relative h-7 w-14 rounded-full border transition-all duration-300 ease-atelier ${
          running
            ? 'border-emerald-500 bg-emerald-500/20'
            : 'border-red-300 bg-red-500/10'
        }`}
      >
        <span
          className={`absolute top-1 h-5 w-5 rounded-full transition-all duration-300 ease-atelier ${
            running ? 'left-8 bg-emerald-600' : 'left-1 bg-red-500'
          }`}
        />
      </button>
    </div>
  )
}

function App() {
  const [activeTab, setActiveTab] = useState('overview')
  const [statusData, setStatusData] = useState(defaultStatus)
  const [logs, setLogs] = useState([])
  const [logFilter, setLogFilter] = useState('all')
  const [sessions, setSessions] = useState([])
  const [selectedSessionId, setSelectedSessionId] = useState(null)
  const [sessionMessages, setSessionMessages] = useState([])
  const [knowledge, setKnowledge] = useState([])
  const [configData, setConfigData] = useState(null)
  const [configDraft, setConfigDraft] = useState(null)
  const [aboutInfo, setAboutInfo] = useState(null)

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
    }, 5000)

    return () => clearInterval(timer)
  }, [activeTab, logFilter, loadLogs, loadStatus])

  useEffect(() => {
    if (activeTab === 'sessions') {
      loadSessions()
    }
    if (activeTab === 'knowledge') {
      loadKnowledge()
    }
    if (activeTab === 'settings') {
      loadConfig()
    }
    if (activeTab === 'about') {
      loadAbout()
    }
  }, [activeTab, loadAbout, loadConfig, loadKnowledge, loadSessions])

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
        <header className="sticky top-0 z-40 border-b border-hairline/90 bg-white/65 backdrop-blur-2xl">
          <div className="mx-auto max-w-[800px] px-4 py-4 sm:px-6">
            <div className="flex items-center justify-between gap-3">
              <div className="animate-in flex items-baseline gap-3">
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

            <nav className="animate-in mt-4 flex gap-2 overflow-x-auto pb-1">
              {TABS.map((tab) => (
                <button
                  key={tab.id}
                  type="button"
                  onClick={() => {
                    clearFlash()
                    setActiveTab(tab.id)
                  }}
                  className={`whitespace-nowrap rounded-full border px-3 py-1.5 text-[10px] font-bold tracking-[0.28em] transition-all duration-300 ease-atelier ${
                    activeTab === tab.id
                      ? 'border-obsidian bg-obsidian text-white'
                      : 'border-hairline bg-white text-zinc-400 hover:border-zinc-300 hover:text-obsidian'
                  }`}
                >
                  {tab.label}
                </button>
              ))}
            </nav>
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
                        className={`rounded-full border px-2 py-1 text-[9px] font-bold tracking-[0.18em] ${
                          logFilter === level
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
                    className={`grid w-full grid-cols-5 items-center border-b border-hairline/60 px-5 py-4 text-left last:border-b-0 ${
                      selectedSessionId === row.id ? 'bg-zinc-50/80' : 'bg-white'
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
                    accept=".pdf,.txt"
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
                    <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                      <div>
                        <p className="text-base font-semibold tracking-tight">{source.title || source.source_name}</p>
                        <p className="tactical-text mt-1 text-[11px] text-zinc-400">{source.chunk_count || 0} CHUNKS</p>
                      </div>

                      <div className="flex flex-wrap gap-2">
                        <span className={`rounded-full border px-3 py-1 text-[10px] font-semibold tracking-[0.18em] ${source.image_rich ? 'border-red-200 text-red-500' : 'border-hairline text-zinc-500'}`}>
                          {source.image_rich ? 'IMAGE-RICH' : 'TEXT-RICH'}
                        </span>
                        <span className={`rounded-full border px-3 py-1 text-[10px] font-semibold tracking-[0.18em] ${source.ocr_applied ? 'border-obsidian text-obsidian' : source.ocr_required ? 'border-red-300 text-red-500' : 'border-hairline text-zinc-500'}`}>
                          OCR {source.ocr_applied ? 'APPLIED' : source.ocr_required ? 'REQUIRED' : 'N/A'}
                        </span>
                        <span className={`rounded-full border px-3 py-1 text-[10px] font-semibold tracking-[0.18em] ${source.status === 'active' ? 'border-obsidian text-obsidian' : 'border-hairline text-zinc-400'}`}>
                          {(source.status || 'disabled').toUpperCase()}
                        </span>
                      </div>
                    </div>

                    <div className="mt-4 flex flex-wrap gap-2">
                      <button
                        type="button"
                        onClick={() => toggleKnowledgeSource(source.id, source.status !== 'active')}
                        className="rounded-xl border border-hairline px-3 py-2 text-[10px] font-bold tracking-[0.2em]"
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
