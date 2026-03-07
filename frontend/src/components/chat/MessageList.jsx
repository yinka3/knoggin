import { useEffect, useRef, useState, memo } from 'react'
import { ScrollArea } from '@/components/ui/scroll-area'
import { cn } from '@/lib/utils'
import { ArrowDown, Network, Loader2, Check, Info } from 'lucide-react'
import ThinkingBox from './ThinkingBox'
import MarkdownRenderer from './MarkdownRenderer'
import SourcesArtifact from './SourcesArtifact'
import { extractMessageFacts } from '@/api/chat'
import { toast } from 'sonner'
import { motion, AnimatePresence } from 'motion/react'

const MessageItem = memo(({ msg, agentName, sessionId }) => {
  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3, ease: 'easeOut' }}
      className={`flex flex-col gap-1 ${msg.role === 'user' ? 'items-end' : 'items-start'}`}
    >
      <div className="flex items-center gap-2 text-xs text-muted-foreground">
        <span className="font-medium">{msg.role === 'user' ? 'You' : agentName}</span>
        <span>{formatTimestamp(msg.timestamp)}</span>
      </div>

      <div
        className={
          msg.role === 'user'
            ? 'bg-primary/15 text-foreground rounded-2xl rounded-tr-sm px-4 py-2.5 max-w-[85%] leading-relaxed'
            : 'text-foreground leading-relaxed max-w-full'
        }
      >
        {msg.role === 'assistant' ? (
          <>
            {(msg.toolCalls || msg.tool_calls) && (
              <ThinkingBox
                toolCalls={msg.toolCalls || msg.tool_calls}
                streaming={false}
                currentThinking={null}
                defaultOpen={false}
                totalDuration={msg.total_duration}
              />
            )}
            <MarkdownRenderer content={msg.content} />
            {msg.sources && <SourcesArtifact sources={msg.sources} />}
            
            {sessionId && msg.msg_id && msg.content?.trim() && (
              <div className="flex items-center gap-2 mt-2 border-t border-border/10 pt-2">
                <ExtractFactsButton sessionId={sessionId} message={msg} />
              </div>
            )}
          </>
        ) : (
          <span className="whitespace-pre-wrap">{msg.content}</span>
        )}
      </div>
    </motion.div>
  )
}, (prev, next) => {
  return prev.msg.content === next.msg.content &&
         prev.msg.timestamp === next.msg.timestamp &&
         prev.agentName === next.agentName &&
         prev.sessionId === next.sessionId
})

export default function MessageList({
  messages,
  streaming,
  streamingContent,
  currentToolCalls,
  currentThinking,
  agentName = 'Assistant',
  sessionId,
}) {
  const bottomRef = useRef(null)
  const scrollAreaRef = useRef(null)
  const [showScrollButton, setShowScrollButton] = useState(false)
  const [userScrolled, setUserScrolled] = useState(false)

  const scrollToBottom = (instant = false) => {
    bottomRef.current?.scrollIntoView({
      behavior: instant ? 'auto' : 'smooth',
    })
    setUserScrolled(false)
  }

  useEffect(() => {
    if (!userScrolled) {
      const shouldBeInstant = streaming && streamingContent
      setTimeout(() => scrollToBottom(shouldBeInstant), 0)
    }
  }, [messages, streaming, currentToolCalls, currentThinking, streamingContent, userScrolled])

  useEffect(() => {
    const viewport = scrollAreaRef.current?.querySelector('[data-radix-scroll-area-viewport]')
    if (!viewport) return

    const handleScroll = () => {
      const { scrollTop, scrollHeight, clientHeight } = viewport
      const isNearBottom = scrollHeight - scrollTop - clientHeight < 100
      setShowScrollButton(!isNearBottom)

      if (!isNearBottom && !streaming) {
        setUserScrolled(true)
      }
      if (isNearBottom) {
        setUserScrolled(false)
      }
    }

    viewport.addEventListener('scroll', handleScroll)
    return () => viewport.removeEventListener('scroll', handleScroll)
  }, [streaming])

  useEffect(() => {
    if (streaming) {
      setTimeout(() => setUserScrolled(false), 0)
    }
  }, [streaming])

  return (
    <div className="relative h-full">
      <ScrollArea ref={scrollAreaRef} className="h-full pr-4">
        <div className="max-w-3xl mx-auto">
          <div className="space-y-4 pb-4">
            <AnimatePresence initial={false}>
              {messages
                .filter(msg => !(msg.role === 'assistant' && !msg.content?.trim()))
                .map((msg) => (
                  <MessageItem key={`${msg.role}-${msg.timestamp}`} msg={msg} agentName={agentName} sessionId={sessionId} />
              ))}
            </AnimatePresence>

              {/* Show simple loader when streaming but nothing else yet */}
              {streaming && !currentThinking && currentToolCalls?.length === 0 && !streamingContent && (
                <div className="flex flex-col gap-1 items-start">
                  <div className="flex items-center gap-2 text-xs text-muted-foreground">
                    <span className="font-medium">{agentName}</span>
                  </div>
                  <div className="flex items-center gap-2 py-3">
                    <Loader2 size={16} className="animate-spin text-muted-foreground" />
                    <span className="text-muted-foreground text-sm">Thinking...</span>
                  </div>
                </div>
              )}

              {/* Live streaming section */}
              <AnimatePresence>
              {streaming && (currentToolCalls?.length > 0 || currentThinking || streamingContent) && (
                <motion.div 
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, scale: 0.95 }}
                  className="flex flex-col gap-1 items-start"
                >
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                  <span className="font-medium">{agentName}</span>
                </div>

                {/* ThinkingBox — auto-collapses when streaming content arrives */}
                {(currentToolCalls?.length > 0 || currentThinking) && (
                  <ThinkingBox
                    toolCalls={currentToolCalls}
                    streaming={streaming}
                    currentThinking={currentThinking}
                    defaultOpen={!streamingContent}
                  />
                )}

                {/* Streaming content bubble */}
                {streamingContent && (
                  <div className="text-foreground leading-relaxed max-w-full">
                    <MarkdownRenderer content={streamingContent} />
                  </div>
                )}
              </motion.div>
            )}
            </AnimatePresence>

            <div ref={bottomRef} />
          </div>
        </div>
      </ScrollArea>

      {/* Scroll to bottom button */}
      {showScrollButton && (
        <button
          onClick={scrollToBottom}
          className="absolute bottom-4 left-1/2 -translate-x-1/2 p-2 rounded-full bg-muted border border-border shadow-lg hover:bg-muted/80 transition-all animate-in fade-in slide-in-from-bottom-2 duration-200"
        >
          <ArrowDown size={18} className="text-foreground" />
        </button>
      )}
    </div>
  )
}

function formatTimestamp(ts) {
  if (!ts) return ''
  try {
    const date = new Date(ts)
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  } catch {
    return ''
  }
}



function ExtractFactsButton({ sessionId, message }) {
  // 'idle' | 'extracting' | 'success' | 'empty'
  const [status, setStatus] = useState('idle')

  const handleExtract = async () => {
    if (status === 'extracting' || status === 'success') return
    setStatus('extracting')
    try {
      const res = await extractMessageFacts(sessionId, message.content, message.msg_id)
      // Check if the backend actually found and saved facts
      if (res.status === 'success' && res.facts_found) {
        setStatus('success')
        toast.success('Facts extracted and saved to memory')
      } else {
        setStatus('empty')
        toast.info('No extractable facts found in this message')
        // Automatically reset 'empty' state after a few seconds so they can try again if they want
        setTimeout(() => setStatus('idle'), 3000)
      }
    } catch (err) {
      console.error(err)
      toast.error('Failed to extract facts')
      setStatus('idle')
    }
  }

  return (
    <button
      onClick={handleExtract}
      disabled={status === 'extracting' || status === 'success'}
      className={cn(
        "flex items-center gap-1.5 text-xs transition-all duration-200 px-2 py-1 rounded-md",
        status === 'extracting'
          ? "bg-primary/10 text-primary opacity-80" 
          : status === 'success'
          ? "bg-emerald-500/10 text-emerald-500 font-medium"
          : status === 'empty'
          ? "bg-muted/50 text-muted-foreground"
          : "text-muted-foreground hover:text-primary hover:bg-muted/50 active:scale-95"
      )}
      title="Extract facts from this message"
    >
      {status === 'extracting' ? (
        <Loader2 size={14} className="animate-spin text-primary" />
      ) : status === 'success' ? (
        <Check size={14} className="text-emerald-500" />
      ) : status === 'empty' ? (
        <Info size={14} />
      ) : (
        <Network size={14} />
      )}
      
      <span>
        {status === 'extracting' 
          ? 'Extracting...' 
          : status === 'success' 
          ? 'Facts Extracted' 
          : status === 'empty'
          ? 'No Facts Found'
          : 'Extract facts'
        }
      </span>
    </button>
  )
}
