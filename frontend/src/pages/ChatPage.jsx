import { useEffect, useState } from 'react'
import { useParams, useNavigate, useLocation } from 'react-router-dom'
import { useSession } from '../context/SessionContext'
import { useChat } from '../hooks/useChat'
import { Skeleton } from '@/components/ui/skeleton'
import { getConfig } from '@/api/config'
import { getSession, updateSession } from '@/api/sessions'
import { createSession as apiCreateSession } from '@/api/sessions'
import { toast } from 'sonner'
import InputBar from '../components/chat/InputBar'
import MessageList from '../components/chat/MessageList'
import TopicsDrawer from '../components/chat/TopicsDrawer'
import TokenCounter from '../components/chat/TokenCounter'
import WelcomeState from '../components/chat/WelcomeState'
import AgentSelector from '../components/chat/AgentSelector'
import { listAgents } from '@/api/agents'

export default function ChatPage() {
  const { sessionId } = useParams()
  const { createSession, setCurrentSessionId, loadSessions } = useSession()
  const [showSkeleton, setShowSkeleton] = useState(false)
  const [enabledTools, setEnabledTools] = useState(null)
  const [currentAgentId, setCurrentAgentId] = useState(null)
  const [currentAgentName, setCurrentAgentName] = useState('Assistant')
  const [currentModel, setCurrentModel] = useState(null)
  const [userName, setUserName] = useState('')
  const {
    messages,
    loading,
    streaming,
    streamingContent,
    toolCalls,
    currentThinking,
    totalTokens,
    loadHistory,
    send,
  } = useChat(sessionId)

  const navigate = useNavigate()
  const location = useLocation()

  useEffect(() => {
    if (sessionId) {
      setCurrentSessionId(sessionId)

      getSession(sessionId)
        .then(async data => {
          setEnabledTools(data.enabled_tools || null)
          setCurrentAgentId(data.agent_id || null)
          setCurrentModel(data.model || null)
          if (data.agent_id) {
            const { agents } = await listAgents()
            const agent = agents?.find(a => a.id === data.agent_id)
            if (agent) setCurrentAgentName(agent.name)
          }
        })
        .catch(err => console.error('Failed to load session:', err))

      loadHistory().then(() => {
        if (location.state?.firstMessage) {
          send(location.state.firstMessage)
          navigate(location.pathname, { replace: true, state: {} })
        }
      })
    }
  }, [sessionId])

  useEffect(() => {
    getConfig().then(config => setUserName(config.user_name || ''))
  }, [])

  async function handleAgentChange(newAgentId) {
    const prevAgent = currentAgentId
    const prevName = currentAgentName
    setCurrentAgentId(newAgentId)
    try {
      await updateSession(sessionId, { agentId: newAgentId })
      const { agents } = await listAgents()
      const agent = agents?.find(a => a.id === newAgentId)
      if (agent) setCurrentAgentName(agent.name)
      toast.success('Agent switched')
    } catch (err) {
      console.error('Failed to switch agent:', err)
      toast.error('Failed to switch agent')
      setCurrentAgentId(prevAgent)
      setCurrentAgentName(prevName)
    }
  }

  async function handleModelChange(newModel) {
    const prev = currentModel
    const effectiveModel = newModel || null
    setCurrentModel(effectiveModel)
    try {
      await updateSession(sessionId, { model: effectiveModel })
      toast.success('Model updated')
    } catch (err) {
      console.error('Failed to switch model:', err)
      toast.error('Failed to switch model')
      setCurrentModel(prev)
    }
  }

  async function handleToolsChange(newEnabledTools) {
    const previousTools = enabledTools
    setEnabledTools(newEnabledTools)

    try {
      await updateSession(sessionId, { enabledTools: newEnabledTools })
    } catch (err) {
      console.error('Failed to update tools:', err)
      toast.error('Failed to save tool settings')
      setEnabledTools(previousTools)
    }
  }

  useEffect(() => {
    if (loading) {
      const timer = setTimeout(() => setShowSkeleton(true), 150)
      return () => clearTimeout(timer)
    }
    setShowSkeleton(false)
  }, [loading])

  async function handleFirstMessage(message) {
    try {
      const config = await getConfig()
      const topicsConfig = config.default_topics || null
      const data = await apiCreateSession(topicsConfig)

      if (data?.session_id) {
        setCurrentSessionId(data.session_id)
        await loadSessions()
        navigate(`/chat/${data.session_id}`, { state: { firstMessage: message } })
      }
    } catch (err) {
      console.error('Failed to create session:', err)
      toast.error('Failed to start conversation')
    }
  }

  return (
    <div className="flex flex-col h-full">
      {/* Header with Agent selector and Topics drawer */}
      {sessionId && (
        <div className="flex items-center justify-between px-4 py-2 border-b border-border">
          <div className="flex items-center gap-3">
            <span className="text-xs text-muted-foreground font-mono">
              {sessionId.slice(0, 8)}...
            </span>
            <AgentSelector
              currentAgentId={currentAgentId}
              onAgentChange={handleAgentChange}
              disabled={streaming}
            />
          </div>
          <div className="flex items-center gap-4">
            <TokenCounter value={totalTokens} />
            <TopicsDrawer sessionId={sessionId} />
          </div>
        </div>
      )}

      {/* Message area */}
      <div className="flex-1 overflow-y-auto p-4">
        {sessionId ? (
          loading && showSkeleton ? (
            <div className="space-y-4">
              <Skeleton className="h-12 w-3/4" />
              <Skeleton className="h-12 w-1/2" />
              <Skeleton className="h-12 w-2/3" />
            </div>
          ) : loading ? null : (
            <MessageList
              messages={messages}
              streaming={streaming}
              streamingContent={streamingContent}
              currentToolCalls={toolCalls}
              currentThinking={currentThinking}
              agentName={currentAgentName}
            />
          )
        ) : (
          <WelcomeState onFirstMessage={handleFirstMessage} userName={userName} />
        )}
      </div>

      {/* Input */}
      {sessionId && (
        <InputBar
          onSend={send}
          disabled={loading || streaming}
          enabledTools={enabledTools}
          onToolsChange={handleToolsChange}
          currentModel={currentModel}
          onModelChange={handleModelChange}
        />
      )}
    </div>
  )
}
