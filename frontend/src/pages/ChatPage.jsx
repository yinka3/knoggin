import { useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import { useSession } from '../context/SessionContext'
import { useChat } from '../hooks/useChat'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'

import InputBar from '../components/chat/InputBar'
import MessageList from '../components/chat/MessageList'
import TopicsDrawer from '../components/chat/TopicsDrawer'
import TokenCounter from '../components/chat/TokenCounter'

export default function ChatPage() {
  const { sessionId } = useParams()
  const { createSession, setCurrentSessionId } = useSession()
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
    send 
  } = useChat(sessionId)

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
          <div>
            <p className="mb-2 text-muted-foreground">No session selected</p>
            <Button
              variant="outline"
              onClick={createSession}
              className="border-primary text-primary hover:bg-primary hover:text-primary-foreground"
            >
              New Chat
            </Button>
          </div>
        )}
      </div>

      {/* Input */}
      {sessionId && (
        <InputBar onSend={send} disabled={loading || streaming} />
      )}
    </div>
  )
}