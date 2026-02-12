import { createContext, useContext, useEffect, useState, useCallback } from 'react'
import { getTools } from '@/api/config'
import { useSession } from './SessionContext'

const ToolsContext = createContext(null)

export function ToolsProvider({ children }) {
  const { currentSessionId } = useSession()
  const [availableTools, setAvailableTools] = useState([])
  const [enabledTools, setEnabledTools] = useState([])
  const [loading, setLoading] = useState(true)


  useEffect(() => {
    async function loadTools() {
      try {
        const data = await getTools()
        setAvailableTools(data.tools || [])

        const defaults = data.tools
          .filter(t => t.source === 'knoggin')
          .map(t => t.id)
        

        setEnabledTools(prev => (prev || []).length ? prev : defaults)
      } catch (err) {
        console.error('Failed to load tools:', err)
      } finally {
        setLoading(false)
      }
    }
    loadTools()
  }, [])

  const toggleTool = useCallback((toolId) => {
    setEnabledTools(prev => {
      if (prev.includes(toolId)) {
        return prev.filter(id => id !== toolId)
      } else {
        return [...prev, toolId]
      }
    })
  }, [])

  const enableGroup = useCallback((serverName) => {
    const toolsInGroup = availableTools
        .filter(t => t.server === serverName || t.source === serverName)
        .map(t => t.id)
    
    setEnabledTools(prev => {
        const next = new Set([...prev, ...toolsInGroup])
        return Array.from(next)
    })
  }, [availableTools])

  const disableGroup = useCallback((serverName) => {
     const toolsInGroup = availableTools
        .filter(t => t.server === serverName || t.source === serverName)
        .map(t => t.id)
     
     setEnabledTools(prev => prev.filter(id => !toolsInGroup.includes(id)))
  }, [availableTools])

  const value = {
    availableTools,
    enabledTools,
    setEnabledTools,
    toggleTool,
    enableGroup,
    disableGroup,
    loading
  }

  return (
    <ToolsContext.Provider value={value}>
      {children}
    </ToolsContext.Provider>
  )
}

export function useTools() {
  const context = useContext(ToolsContext)
  if (!context) throw new Error("useTools must be used within ToolsProvider")
  return context
}
