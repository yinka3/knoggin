import { useState, useEffect, useRef, useCallback } from 'react'
import { Trash2, Pause, Play, Search, RefreshCw, Radio, Eye, EyeOff } from 'lucide-react'
import { createDebugConnection } from '@/api/debug'
import { listSessions } from '@/api/sessions'
import { useSession } from '../context/SessionContext'

const COMPONENT_COLORS = {
  pipeline: 'text-blue-400',
  agent: 'text-purple-400',
  resolver: 'text-amber-400',
  job: 'text-emerald-400',
  system: 'text-rose-400',
}

const STORAGE_KEY = 'knoggin_debug_events'
const MAX_EVENTS = 500

function saveEvents(sessionId, events) {
  try {
    const data = { sessionId, events: events.slice(-MAX_EVENTS), ts: Date.now() }
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(data))
  } catch {
    // storage full, ignore
  }
}

function loadEvents(sessionId) {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY)
    if (!raw) return []
    const data = JSON.parse(raw)
    // Only restore if same session and less than 1 hour old
    if (data.sessionId === sessionId && Date.now() - data.ts < 3600000) {
      return data.events || []
    }
  } catch {}
  return []
}

function EventRow({ event }) {
  const time = new Date(event.ts).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    fractionalSecondDigits: 3,
  })

  const color = COMPONENT_COLORS[event.component] || 'text-muted-foreground'
  const dataStr = JSON.stringify(event.data)

  return (
    <div className="flex items-start gap-0 py-px px-3 hover:bg-white/[0.02] font-mono text-[11px] leading-5 group">
      <span className="text-neutral-600 shrink-0 w-[90px] select-none">{time}</span>
      <span className={`shrink-0 w-[72px] ${color}`}>{event.component}</span>
      <span className="text-neutral-400 shrink-0 w-[200px] truncate">{event.event}</span>
      <span className="text-neutral-500 truncate flex-1 group-hover:text-neutral-300 transition-colors">
        {dataStr === '{}' ? '' : dataStr}
      </span>
    </div>
  )
}

export default function DebugPage() {
  const { currentSessionId } = useSession()

  const [sessions, setSessions] = useState([])
  const [selectedSession, setSelectedSession] = useState(null)
  const [events, setEvents] = useState([])
  const [paused, setPaused] = useState(false)
  const [verbose, setVerbose] = useState(false)
  const [filter, setFilter] = useState('')
  const [connected, setConnected] = useState(false)
  const [eventCount, setEventCount] = useState(0)

  const pausedRef = useRef(false)
  const verboseRef = useRef(false)
  const connectionRef = useRef(null)
  const eventsBufferRef = useRef([])
  const scrollRef = useRef(null)
  const reconnectTimeoutRef = useRef(null)
  const reconnectAttemptsRef = useRef(0)
  const disposedRef = useRef(false)
  const saveTimerRef = useRef(null)
  const MAX_RECONNECT = 5

  // Keep refs in sync
  useEffect(() => {
    pausedRef.current = paused
  }, [paused])
  useEffect(() => {
    verboseRef.current = verbose
  }, [verbose])

  // Debounced save to sessionStorage
  useEffect(() => {
    if (!selectedSession || events.length === 0) return
    if (saveTimerRef.current) clearTimeout(saveTimerRef.current)
    saveTimerRef.current = setTimeout(() => {
      saveEvents(selectedSession, events)
    }, 500)
    return () => {
      if (saveTimerRef.current) clearTimeout(saveTimerRef.current)
    }
  }, [events, selectedSession])

  // Fetch sessions
  const loadSessions = useCallback(async () => {
    try {
      const data = await listSessions()
      setSessions(data.sessions || [])
      return data.sessions || []
    } catch (err) {
      console.error('Failed to load sessions:', err)
      return []
    }
  }, [])

  useEffect(() => {
    loadSessions().then(loaded => {
      if (currentSessionId && loaded.some(s => s.session_id === currentSessionId)) {
        setSelectedSession(currentSessionId)
      } else {
        const active = loaded.find(s => s.is_active)
        setSelectedSession(active?.session_id || loaded[0]?.session_id || null)
      }
    })
  }, [loadSessions, currentSessionId])

  // Restore events from sessionStorage when session selected
  useEffect(() => {
    if (selectedSession) {
      const restored = loadEvents(selectedSession)
      if (restored.length > 0) {
        setEvents(restored)
        setEventCount(restored.length)
      }
    }
  }, [selectedSession])

  // WebSocket connection — reconnects when session OR verbose changes
  useEffect(() => {
    if (!selectedSession) return

    disposedRef.current = false

    function connect() {
      if (connectionRef.current) {
        connectionRef.current.close()
      }

      eventsBufferRef.current = []

      const conn = createDebugConnection(
        selectedSession,
        event => {
          if (event.type === 'connected') return
          if (pausedRef.current) {
            eventsBufferRef.current.push(event)
          } else {
            setEvents(prev => [...prev, event].slice(-MAX_EVENTS))
            setEventCount(prev => prev + 1)
          }
        },
        () => {
          if (!disposedRef.current) setConnected(false)
        },
        () => {
          if (disposedRef.current) return
          setConnected(false)
          if (reconnectAttemptsRef.current < MAX_RECONNECT) {
            const delay = Math.min(1000 * 2 ** reconnectAttemptsRef.current, 30000)
            reconnectTimeoutRef.current = setTimeout(() => {
              if (disposedRef.current) return
              reconnectAttemptsRef.current += 1
              connect()
            }, delay)
          }
        },
        () => {
          if (!disposedRef.current) {
            setConnected(true)
            reconnectAttemptsRef.current = 0
          }
        },
        verbose
      )

      connectionRef.current = conn
    }

    reconnectAttemptsRef.current = 0
    connect()

    return () => {
      disposedRef.current = true
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current)
        reconnectTimeoutRef.current = null
      }
      if (connectionRef.current) {
        connectionRef.current.close()
        connectionRef.current = null
      }
      setConnected(false)
    }
  }, [selectedSession, verbose])

  // Auto-scroll
  useEffect(() => {
    if (!paused && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [events, paused])

  function handleResume() {
    const buffered = eventsBufferRef.current
    eventsBufferRef.current = []
    setEvents(prev => [...prev, ...buffered].slice(-MAX_EVENTS))
    setEventCount(prev => prev + buffered.length)
    setPaused(false)
  }

  function handleClear() {
    setEvents([])
    setEventCount(0)
    eventsBufferRef.current = []
    if (selectedSession) {
      sessionStorage.removeItem(STORAGE_KEY)
    }
  }

  function handleToggleVerbose() {
    setVerbose(v => !v)
    // Reconnect happens via the useEffect dependency on verbose
  }

  const filteredEvents = filter
    ? events.filter(
        e =>
          e.component?.includes(filter.toLowerCase()) ||
          e.event?.toLowerCase().includes(filter.toLowerCase()) ||
          JSON.stringify(e.data).toLowerCase().includes(filter.toLowerCase())
      )
    : events

  const bufferedCount = eventsBufferRef.current.length

  return (
    <div className="flex flex-col h-full bg-[#0c0c0c]">
      {/* Terminal header bar */}
      <div className="border-b border-neutral-800 px-4 py-2.5 bg-[#111111]">
        <div className="flex items-center justify-between">
          {/* Left: title + status */}
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
              <div
                className={`h-2 w-2 rounded-full ${connected ? 'bg-emerald-500 animate-pulse' : 'bg-neutral-600'}`}
              />
              <span className="font-mono text-xs text-neutral-300 font-medium">debug</span>
            </div>
            <span className="text-[10px] text-neutral-600 font-mono">
              {eventCount > 0 ? `${eventCount} events` : 'idle'}
            </span>
            {verbose && (
              <span className="text-[9px] text-amber-500/70 font-mono uppercase tracking-wider">
                verbose
              </span>
            )}
          </div>

          {/* Right: controls */}
          <div className="flex items-center gap-1.5">
            {/* Session selector */}
            <select
              value={selectedSession || ''}
              onChange={e => setSelectedSession(e.target.value)}
              className="bg-neutral-900 border border-neutral-800 rounded px-2 py-1 text-[11px] text-neutral-400 font-mono focus:outline-none focus:border-neutral-600"
            >
              {sessions.length === 0 && (
                <option value="" disabled>
                  No sessions
                </option>
              )}
              {sessions.map(s => (
                <option key={s.session_id} value={s.session_id}>
                  {s.session_id.slice(0, 8)}
                  {s.is_active ? ' ●' : ''}
                  {s.session_id === currentSessionId ? ' (current)' : ''}
                </option>
              ))}
            </select>

            <button
              onClick={loadSessions}
              title="Refresh sessions"
              className="p-1.5 rounded text-neutral-600 hover:text-neutral-300 hover:bg-neutral-800 transition-colors"
            >
              <RefreshCw size={12} />
            </button>

            <div className="w-px h-4 bg-neutral-800 mx-0.5" />

            {/* Filter */}
            <div className="relative">
              <Search
                size={11}
                className="absolute left-2 top-1/2 -translate-y-1/2 text-neutral-600"
              />
              <input
                value={filter}
                onChange={e => setFilter(e.target.value)}
                placeholder="filter"
                className="pl-6 pr-2 py-1 w-28 bg-neutral-900 border border-neutral-800 rounded text-[11px] text-neutral-400 font-mono placeholder:text-neutral-700 focus:outline-none focus:border-neutral-600"
              />
            </div>

            <div className="w-px h-4 bg-neutral-800 mx-0.5" />

            {/* Verbose toggle */}
            <button
              onClick={handleToggleVerbose}
              title={verbose ? 'Hide verbose events' : 'Show verbose events'}
              className={`p-1.5 rounded transition-colors ${
                verbose
                  ? 'text-amber-400 bg-amber-500/10 hover:bg-amber-500/20'
                  : 'text-neutral-600 hover:text-neutral-300 hover:bg-neutral-800'
              }`}
            >
              {verbose ? <Eye size={12} /> : <EyeOff size={12} />}
            </button>

            {/* Pause/Play */}
            <button
              onClick={() => (paused ? handleResume() : setPaused(true))}
              title={paused ? 'Resume' : 'Pause'}
              className={`p-1.5 rounded transition-colors ${
                paused
                  ? 'text-amber-400 bg-amber-500/10 hover:bg-amber-500/20'
                  : 'text-neutral-600 hover:text-neutral-300 hover:bg-neutral-800'
              }`}
            >
              {paused ? <Play size={12} /> : <Pause size={12} />}
            </button>

            {/* Clear */}
            <button
              onClick={handleClear}
              title="Clear"
              className="p-1.5 rounded text-neutral-600 hover:text-red-400 hover:bg-red-500/10 transition-colors"
            >
              <Trash2 size={12} />
            </button>
          </div>
        </div>
      </div>

      {/* Column headers */}
      <div className="flex items-center gap-0 px-3 py-1 bg-[#0e0e0e] border-b border-neutral-800/50 font-mono text-[10px] text-neutral-600 uppercase tracking-wider select-none">
        <span className="w-[90px]">time</span>
        <span className="w-[72px]">source</span>
        <span className="w-[200px]">event</span>
        <span className="flex-1">data</span>
      </div>

      {/* Event stream */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto overflow-x-hidden">
        {filteredEvents.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-20 gap-2">
            {connected ? (
              <>
                <div className="h-2 w-2 rounded-full bg-emerald-500/50 animate-pulse" />
                <p className="text-[11px] text-neutral-600 font-mono">listening...</p>
                <p className="text-[10px] text-neutral-700 font-mono">
                  send a message to see events
                </p>
              </>
            ) : selectedSession ? (
              <>
                <div className="h-2 w-2 rounded-full bg-neutral-700" />
                <p className="text-[11px] text-neutral-600 font-mono">connecting...</p>
              </>
            ) : (
              <>
                <div className="h-2 w-2 rounded-full bg-neutral-700" />
                <p className="text-[11px] text-neutral-600 font-mono">no sessions</p>
              </>
            )}
          </div>
        ) : (
          filteredEvents.map((event, idx) => <EventRow key={idx} event={event} />)
        )}
      </div>

      {/* Paused bar */}
      {paused && (
        <div className="flex items-center justify-between px-4 py-1.5 bg-amber-500/5 border-t border-amber-500/20">
          <span className="text-[11px] text-amber-500/70 font-mono">
            paused — {bufferedCount} buffered
          </span>
          <button
            onClick={handleResume}
            className="flex items-center gap-1 text-[11px] text-amber-400 font-mono hover:text-amber-300 transition-colors"
          >
            <Play size={10} />
            resume
          </button>
        </div>
      )}
    </div>
  )
}
