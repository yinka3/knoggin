/* eslint-disable react-refresh/only-export-components */
import { createContext, useContext, useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { toast } from 'sonner'
import { listSessions, createSession as apiCreateSession } from '../api/sessions'
import SessionConfigModal from '../components/session/SessionConfigModal'

const SessionContext = createContext(null)

export function SessionProvider({ children }) {
  const [sessions, setSessions] = useState([])
  const [currentSessionId, setCurrentSessionId] = useState(null)
  const [loading, setLoading] = useState(true)
  const [configModalOpen, setConfigModalOpen] = useState(false)
  const navigate = useNavigate()

  useEffect(() => {
    loadSessions()
  }, [])

  async function loadSessions() {
    setLoading(true)
    try {
      const data = await listSessions()
      setSessions(data.sessions || [])
    } catch (err) {
      console.error('Failed to load sessions:', err)
    } finally {
      setLoading(false)
    }
  }

  const updateSessionInList = (sessionId, updates) => {
    setSessions(prev =>
      prev.map(s => (s.id === sessionId ? { ...s, ...updates } : s))
    )
  }

  function openCreateSession() {
    setConfigModalOpen(true)
  }

  async function createSessionWithConfig(topicsConfig, agentId = null) {
    try {
      const data = await apiCreateSession(topicsConfig, agentId)
      await loadSessions()
      setCurrentSessionId(data.session_id)
      navigate(`/chat/${data.session_id}`)
      return data.session_id
    } catch (err) {
      console.error('Failed to create session:', err)
      toast.error('Failed to create session', {
        description: err.message,
      })
      return null
    }
  }

  function selectSession(sessionId) {
    setCurrentSessionId(sessionId)
    navigate(`/chat/${sessionId}`)
  }

  const value = {
    sessions,
    currentSessionId,
    setCurrentSessionId,
    loading,
    createSession: openCreateSession,
    updateSessionInList,
    loadSessions,
  }

  return (
    <SessionContext.Provider value={value}>
      {children}
      <SessionConfigModal
        open={configModalOpen}
        onOpenChange={setConfigModalOpen}
        sessions={sessions}
        onCreateSession={createSessionWithConfig}
      />
    </SessionContext.Provider>
  )
}

export function useSession() {
  const context = useContext(SessionContext)
  if (!context) {
    throw new Error('useSession must be used within SessionProvider')
  }
  return context
}
