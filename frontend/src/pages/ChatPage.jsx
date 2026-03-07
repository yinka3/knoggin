import { useState, useCallback, useRef, useEffect } from 'react'
import { useParams, useNavigate, useLocation } from 'react-router-dom'
import { useSession } from '../context/SessionContext'
import { useChat } from '../hooks/useChat'
import { useSocket } from '@/context/SocketContext'
import { Skeleton } from '@/components/ui/skeleton'
import { getConfig } from '@/api/config'
import { getSession, updateSession, exportSession } from '@/api/sessions'
import { createSession as apiCreateSession } from '@/api/sessions'
import { getTopics } from '@/api/topics'
import { toast } from 'sonner'
import InputBar from '../components/chat/InputBar'
import MessageList from '../components/chat/MessageList'
import TopicsDrawer from '../components/chat/TopicsDrawer'
import { useTools } from '@/context/ToolsContext'
import WelcomeState from '../components/chat/WelcomeState'
import FilesDrawer from '../components/chat/FilesDrawer'
import AgentNotesDrawer from '../components/chat/AgentNotesDrawer'
import ChatHeader from '../components/chat/ChatHeader'
import { listAgents, addAgentMemory } from '@/api/agents'
import useDelayedLoading from '@/hooks/useDelayedLoading'
import ToolsDrawer from '../components/tools/ToolsDrawer'
import MergeInboxDrawer from '../components/chat/MergeInboxDrawer'

async function processSlashCommand(command, currentAgentId) {
  const match = command.match(/^\/(rules?|prefs?|icks?)\s+(.+)$/i)
  if (!match) return false
  
  if (!currentAgentId) {
    toast.error('Please select an agent first')
    return true
  }
  
  const rawCat = match[1].toLowerCase()
  let category = 'rules'
  if (rawCat.startsWith('pref')) category = 'preferences'
  if (rawCat.startsWith('ick')) category = 'icks'

  const content = match[2].trim()
  
  try {
    await addAgentMemory(currentAgentId, category, content)
    toast.success(`Saved to Agent ${category}`)
  } catch (err) {
    console.error('Failed to save memory:', err)
    toast.error(`Failed to save ${category}`)
  }
  return true
}

export default function ChatPage() {
  const { sessionId } = useParams()
  const { setCurrentSessionId, loadSessions } = useSession()
  const { setEnabledTools, setSessionId: setToolsSessionId } = useTools()
  const [currentAgentId, setCurrentAgentId] = useState(null)
  const [currentAgentName, setCurrentAgentName] = useState('Assistant')
  const [currentModel, setCurrentModel] = useState(null)
  const [userName, setUserName] = useState('')
  const [topicsOpen, setTopicsOpen] = useState(false)
  const [toolsOpen, setToolsOpen] = useState(false)
  const [filesOpen, setFilesOpen] = useState(false)
  const [inboxOpen, setInboxOpen] = useState(false)
  const [notesOpen, setNotesOpen] = useState(false)
  const [notesCount, setNotesCount] = useState(0)
  const [fileCount, setFileCount] = useState(0)
  const [inboxCount, setInboxCount] = useState(0)
  const [hotTopics, setHotTopics] = useState([])
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
  const showSkeleton = useDelayedLoading(loading)

  const pendingMessageRef = useRef(null)

  useEffect(() => {
    getConfig()
      .then(config => {
        setUserName(config.user_name || '')
      })
      .catch(err => console.error('Failed to get config:', err))
  }, [])

  // Pause gradient-bg animation during streaming to free GPU
  useEffect(() => {
    if (streaming) {
      document.documentElement.classList.add('streaming')
    } else {
      document.documentElement.classList.remove('streaming')
    }
    return () => {
      document.documentElement.classList.remove('streaming')
    }
  }, [streaming])

  const navigate = useNavigate()
  const location = useLocation()

  const handleProfileRefined = useCallback(data => {
    toast.success('Your profile has been refined', {
      description: `Updated ${data.data.facts_created} facts based on recent chat.`,
    })
  }, [])

  const handleFactsChanged = useCallback(data => {
    if (data.data.created > 0) {
      toast.info('Knowledge Graph Updated', {
        description: `Extracted ${data.data.created} new facts.`,
      })
    }
  }, [])

  const handleMergeJudgmentsComplete = useCallback(data => {
    if (data.data.hitl_count > 0) {
      toast.info('New Merge Proposals', {
        description: `Found ${data.data.hitl_count} potential merges needing human review.`,
      })
      setInboxCount(prev => prev + data.data.hitl_count)
    }
  }, [])

  useSocket('user_profile_refined', handleProfileRefined)
  useSocket('facts_changed', handleFactsChanged)
  useSocket('merge_judgments_complete', handleMergeJudgmentsComplete)

  useEffect(() => {
    if (sessionId) {
      setCurrentSessionId(sessionId)
      setToolsSessionId(sessionId)

      // Capture first message from navigation state immediately (before async work)
      if (location.state?.firstMessage) {
        pendingMessageRef.current = location.state.firstMessage
        // Clear location state right away to prevent re-sends on remount
        navigate(location.pathname, { replace: true, state: {} })
      }

      getSession(sessionId)
        .then(async data => {
          setEnabledTools(data.enabled_tools?.length ? data.enabled_tools : null)
          setCurrentAgentId(data.agent_id || null)
          setCurrentModel(data.model || null)
          if (data.agent_id) {
            const { agents } = await listAgents()
            const agent = agents?.find(a => a.id === data.agent_id)
            if (agent) setCurrentAgentName(agent.name)
          }
        })
        .catch(err => console.error('Failed to load session:', err))

      getTopics(sessionId)
        .then(data => {
          setHotTopics(data.hot_topics || [])
        })
        .catch(() => {})

      loadHistory().then(() => {
        // After history is loaded, send the pending message if one exists
        const pending = pendingMessageRef.current
        if (pending) {
          pendingMessageRef.current = null
          send(pending)
        }
      })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionId, loadHistory, send])

  // userName is already set by the getConfig() call above

  const handleAgentChange = useCallback(async (newAgentId) => {
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
  }, [currentAgentId, currentAgentName, sessionId])

  const handleModelChange = useCallback(async (newModel) => {
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
  }, [currentModel, sessionId])

  const handleFirstMessage = useCallback(async (message) => {
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
  }, [setCurrentSessionId, loadSessions, navigate])

  const handleSend = useCallback(async (message) => {
    const isCommand = await processSlashCommand(message, currentAgentId)
    if (isCommand) return

    send(message, hotTopics)
  }, [currentAgentId, send, hotTopics])

  const handleExport = useCallback(async () => {
    try {
      const data = await exportSession(sessionId)
      const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `knoggin_session_${sessionId.slice(0, 8)}.json`
      a.click()
      URL.revokeObjectURL(url)
      toast.success('Chat exported')
    } catch (err) {
      console.error('Export failed:', err)
      toast.error('Failed to export chat')
    }
  }, [sessionId])

  return (
    <div className="flex flex-col h-full">
      {/* Header with Agent selector and Session Settings */}
      <ChatHeader
        sessionId={sessionId}
        currentAgentId={currentAgentId}
        onAgentChange={handleAgentChange}
        disabled={streaming}
        totalTokens={totalTokens}
        fileCount={fileCount}
        onOpenTopics={() => setTopicsOpen(true)}
        onOpenTools={() => setToolsOpen(true)}
        onOpenFiles={() => setFilesOpen(true)}
        onOpenInbox={() => setInboxOpen(true)}
        onOpenNotes={() => setNotesOpen(true)}
        onExport={handleExport}
        inboxCount={inboxCount}
        notesCount={notesCount}
        isChatEmpty={messages.length === 0}
      />

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
              sessionId={sessionId}
            />
          )
        ) : (
          <WelcomeState onFirstMessage={handleFirstMessage} userName={userName} />
        )}
      </div>

      {/* Input */}
      {sessionId && (
        <InputBar
          onSend={handleSend}
          disabled={loading || streaming}
          currentModel={currentModel}
          onModelChange={handleModelChange}
        />
      )}

      {sessionId && (
        <>
          <TopicsDrawer sessionId={sessionId} open={topicsOpen} onOpenChange={setTopicsOpen} />
          <FilesDrawer
            sessionId={sessionId}
            open={filesOpen}
            onOpenChange={setFilesOpen}
            onCountChange={setFileCount}
          />
          <MergeInboxDrawer
            sessionId={sessionId}
            open={inboxOpen}
            onOpenChange={setInboxOpen}
            onCountChange={setInboxCount}
          />
          <AgentNotesDrawer
            sessionId={sessionId}
            open={notesOpen}
            onOpenChange={setNotesOpen}
            onCountChange={setNotesCount}
          />
        </>
      )}
      <ToolsDrawer open={toolsOpen} onOpenChange={setToolsOpen} />
    </div>
  )
}
