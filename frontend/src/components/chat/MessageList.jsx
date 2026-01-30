import { useEffect, useRef, useState } from 'react'
import { ScrollArea } from '@/components/ui/scroll-area'
import { ArrowDown } from 'lucide-react'
import ThinkingBox from './ThinkingBox'
import ThinkingOrb from './ThinkingOrb'
import MarkdownRenderer from './MarkdownRenderer'

export default function MessageList({
  messages,
  streaming,
  streamingContent,
  currentToolCalls,
  currentThinking,
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
      scrollToBottom(shouldBeInstant)
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
      setUserScrolled(false)
    }
  }, [streaming])

  return (
    <div className="relative h-full">
      <ScrollArea ref={scrollAreaRef} className="h-full pr-4">
        <div className="max-w-3xl mx-auto">
          <div className="space-y-4 pb-4">
            {messages.map((msg, idx) => (
              <div
                key={idx}
                className={`flex flex-col gap-1 ${msg.role === 'user' ? 'items-end' : 'items-start'}`}
              >
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                  <span className="font-medium">{msg.role === 'user' ? 'You' : 'STELLA'}</span>
                  <span>{formatTimestamp(msg.timestamp)}</span>
                </div>

                {/* Collapsed ThinkingBox for historical messages */}
                {msg.role === 'assistant' && msg.toolCalls?.length > 0 && (
                  <ThinkingBox toolCalls={msg.toolCalls} streaming={false} defaultOpen={false} />
                )}

                <div
                  className={
                    msg.role === 'user'
                      ? 'bg-primary/15 text-foreground rounded-2xl rounded-tr-sm px-4 py-2.5 max-w-[85%] leading-relaxed'
                      : 'text-foreground leading-relaxed max-w-full'
                  }
                >
                  {msg.role === 'assistant' ? (
                    <MarkdownRenderer content={msg.content} />
                  ) : (
                    <span className="whitespace-pre-wrap">{msg.content}</span>
                  )}
                </div>
              </div>
            ))}

            {/* Show orb when streaming but nothing else yet */}
            {streaming &&
              !currentThinking &&
              currentToolCalls?.length === 0 &&
              !streamingContent && (
                <div className="flex flex-col gap-1 items-start">
                  <div className="flex items-center gap-2 text-xs text-muted-foreground">
                    <span className="font-medium">STELLA</span>
                  </div>
                  <div className="flex items-center gap-3 py-3">
                    <ThinkingOrb size={22} />
                    <span className="text-muted-foreground text-sm">Thinking...</span>
                  </div>
                </div>
              )}

            {/* Live streaming section */}
            {streaming && (currentToolCalls?.length > 0 || currentThinking || streamingContent) && (
              <div className="flex flex-col gap-1 items-start">
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                  <span className="font-medium">STELLA</span>
                </div>

                {/* ThinkingBox - collapses when streaming content arrives */}
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
              </div>
            )}

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
