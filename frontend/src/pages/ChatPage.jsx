import { useEffect, useState } from 'react'
import { useParams, useNavigate, useLocation } from 'react-router-dom'
import { useSession } from '../context/SessionContext'
import { useChat } from '../hooks/useChat'
import { Skeleton } from '@/components/ui/skeleton'
import { getConfig } from '@/api/config'
import { createSession as apiCreateSession } from '@/api/sessions'
import { toast } from 'sonner'
import InputBar from '../components/chat/InputBar'
import MessageList from '../components/chat/MessageList'
import TopicsDrawer from '../components/chat/TopicsDrawer'
import TokenCounter from '../components/chat/TokenCounter'
import WelcomeState from '../components/chat/WelcomeState'

export default function ChatPage() {
  const { sessionId } = useParams()
  const { createSession, setCurrentSessionId, loadSessions } = useSession()
  const [showSkeleton, setShowSkeleton] = useState(false)
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
      loadHistory().then(() => {
        if (location.state?.firstMessage) {
          send(location.state.firstMessage)
          navigate(location.pathname, { replace: true, state: {} })
        }
      })
    }
  }, [sessionId])

  useEffect(() => {
    if (loading) {
      const timer = setTimeout(() => setShowSkeleton(true), 150)
      return () => clearTimeout(timer)
    }
    setShowSkeleton(false)
  }, [loading])

  useEffect(() => {
    if (sessionId) {
      setCurrentSessionId(sessionId)
      loadHistory()
    }
  }, [sessionId, loadHistory])

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
      {/* Header with Topics drawer */}
      {sessionId && (
        <div className="flex items-center justify-between px-4 py-2 border-b border-border">
          <span className="text-xs text-muted-foreground font-mono">
            {sessionId.slice(0, 8)}...
          </span>
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
            />
          )
        ) : (
          <WelcomeState onFirstMessage={handleFirstMessage} />
        )}
      </div>

      {/* Input */}
      {sessionId && <InputBar onSend={send} disabled={loading || streaming} />}
    </div>
  )
}
