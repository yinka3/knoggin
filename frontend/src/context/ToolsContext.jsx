/* eslint-disable react-refresh/only-export-components */
import { createContext, useContext, useEffect, useState, useCallback, useRef } from 'react'
import { getTools } from '@/api/config'
import { updateSession } from '@/api/sessions'

const ToolsContext = createContext(null)

export function ToolsProvider({ children }) {
  const [availableTools, setAvailableTools] = useState([])
  const [enabledTools, setEnabledTools] = useState([])
  const [loading, setLoading] = useState(true)
  const sessionIdRef = useRef(null)

  useEffect(() => {
    async function loadTools() {
      try {
        const data = await getTools()
        setAvailableTools(data.tools || [])

        const defaults = data.tools.filter(t => t.source === 'knoggin').map(t => t.id)

        setEnabledTools(prev => ((prev || []).length ? prev : defaults))
      } catch (err) {
        console.error('Failed to load tools:', err)
      } finally {
        setLoading(false)
      }
    }
    loadTools()
  }, [])

  // Persist enabled tools to the session (fire-and-forget)
  const persistTools = useCallback((newTools) => {
    const sid = sessionIdRef.current
    if (!sid) return
    updateSession(sid, { enabledTools: newTools }).catch(err =>
      console.error('Failed to persist tool toggles:', err)
    )
  }, [])

  const setSessionId = useCallback((id) => {
    sessionIdRef.current = id
  }, [])

  const toggleTool = useCallback(toolId => {
    setEnabledTools(prev => {
      const current = prev?.length ? prev : availableTools.map(t => t.id)
      const next = current.includes(toolId)
        ? current.filter(id => id !== toolId)
        : [...current, toolId]
      persistTools(next)
      return next
    })
  }, [persistTools, availableTools])

  const enableGroup = useCallback(
    serverName => {
      setEnabledTools(prev => {
        const current = prev?.length ? prev : availableTools.map(t => t.id)
        const toolsInGroup = availableTools
          .filter(t => t.server === serverName || t.source === serverName)
          .map(t => t.id)
        const next = Array.from(new Set([...current, ...toolsInGroup]))
        persistTools(next)
        return next
      })
    },
    [availableTools, persistTools]
  )

  const disableGroup = useCallback(
    serverName => {
      setEnabledTools(prev => {
        const current = prev?.length ? prev : availableTools.map(t => t.id)
        const toolsInGroup = availableTools
          .filter(t => t.server === serverName || t.source === serverName)
          .map(t => t.id)
        const next = current.filter(id => !toolsInGroup.includes(id))
        persistTools(next)
        return next
      })
    },
    [availableTools, persistTools]
  )

  const value = {
    availableTools,
    enabledTools,
    setEnabledTools,
    setSessionId,
    toggleTool,
    enableGroup,
    disableGroup,
    loading,
  }

  return <ToolsContext.Provider value={value}>{children}</ToolsContext.Provider>
}

export function useTools() {
  const context = useContext(ToolsContext)
  if (!context) throw new Error('useTools must be used within ToolsProvider')
  return context
}

