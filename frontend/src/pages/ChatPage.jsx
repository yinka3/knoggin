import { useEffect, useState, useCallback, useRef } from 'react'
import { useParams, useNavigate, useLocation } from 'react-router-dom'
import { useSession } from '../context/SessionContext'
import { useChat } from '../hooks/useChat'
import { useSocket } from '@/context/SocketContext'
import { Skeleton } from '@/components/ui/skeleton'
import { getConfig } from '@/api/config'
import { getSession, updateSession } from '@/api/sessions'
import { createSession as apiCreateSession } from '@/api/sessions'
import { toast } from 'sonner'
import InputBar from '../components/chat/InputBar'
import MessageList from '../components/chat/MessageList'
import TopicsDrawer from '../components/chat/TopicsDrawer'
import { useTools } from '@/context/ToolsContext'
import WelcomeState from '../components/chat/WelcomeState'
import FilesDrawer from '../components/chat/FilesDrawer'
import MemoryDrawer from '../components/chat/MemoryDrawer'
import ChatHeader from '../components/chat/ChatHeader'
import { listAgents, addAgentMemory } from '@/api/agents'
import useDelayedLoading from '@/hooks/useDelayedLoading'
import ToolsDrawer from '../components/tools/ToolsDrawer'
import MergeInboxDrawer from '../components/chat/MergeInboxDrawer'

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
  const [memoryOpen, setMemoryOpen] = useState(false)
  const [filesOpen, setFilesOpen] = useState(false)
  const [inboxOpen, setInboxOpen] = useState(false)
  const [memoryCount, setMemoryCount] = useState(0)
  const [fileCount, setFileCount] = useState(0)
  const [inboxCount, setInboxCount] = useState(0)
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

  // Ref to always hold the latest send function (avoids stale closure in effects)
  const sendRef = useRef(send)
  useEffect(() => { sendRef.current = send }, [send])
  const pendingMessageRef = useRef(null)


  useEffect(() => {
    getConfig().then(config => {
      setUserName(config.user_name || '')
    })
  }, [])

  // Pause gradient-bg animation during streaming to free GPU
  useEffect(() => {
    const bg = document.querySelector('.gradient-bg')
    if (bg) bg.classList.toggle('streaming', streaming)
    return () => {
      if (bg) bg.classList.remove('streaming')
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

      loadHistory().then(() => {
        // After history is loaded, send the pending message if one exists
        const pending = pendingMessageRef.current
        if (pending) {
          pendingMessageRef.current = null
          // Use a small timeout to let useChat hook sync the new sessionIdRef before sending
          setTimeout(() => {
            sendRef.current(pending)
          }, 50)
        }
      })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionId])

  // userName is already set by the getConfig() call above

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

  async function handleSend(message) {
    const match = message.match(/^\/(rules?|prefs?|icks?)\s+(.+)$/i)
    if (match) {
      if (!currentAgentId) {
        toast.error('Please select an agent first')
        return
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
      return
    }

    send(message)
  }

  return (
    <div className="flex flex-col h-full">
      {/* Header with Agent selector and Session Settings */}
      <ChatHeader
        sessionId={sessionId}
        currentAgentId={currentAgentId}
        onAgentChange={handleAgentChange}
        disabled={streaming}
        totalTokens={totalTokens}
        memoryCount={memoryCount}
        fileCount={fileCount}
        onOpenTopics={() => setTopicsOpen(true)}
        onOpenTools={() => setToolsOpen(true)}
        onOpenMemory={() => setMemoryOpen(true)}
        onOpenFiles={() => setFilesOpen(true)}
        onOpenInbox={() => setInboxOpen(true)}
        inboxCount={inboxCount}
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
          <MemoryDrawer
            sessionId={sessionId}
            open={memoryOpen}
            onOpenChange={setMemoryOpen}
            onCountChange={setMemoryCount}
          />
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
        </>
      )}
      <ToolsDrawer open={toolsOpen} onOpenChange={setToolsOpen} />
    </div>
  )
}
