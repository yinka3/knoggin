import { useEffect, useRef, useState } from 'react'
import { ScrollArea } from '@/components/ui/scroll-area'
import { ArrowDown } from 'lucide-react'
import ThinkingBox from './ThinkingBox'
import ThinkingOrb from './ThinkingOrb'
import MarkdownRenderer from './MarkdownContent'

export default function MessageList({ messages, streaming, currentToolCalls, currentThinking }) {
  const bottomRef = useRef(null)
  const scrollContainerRef = useRef(null)
  const [showScrollButton, setShowScrollButton] = useState(false)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, currentToolCalls, currentThinking, streaming])

  useEffect(() => {
    const container = scrollContainerRef.current?.querySelector('[data-radix-scroll-area-viewport]')
    if (!container) return

    const handleScroll = () => {
      const { scrollTop, scrollHeight, clientHeight } = container
      const isNearBottom = scrollHeight - scrollTop - clientHeight < 100
      setShowScrollButton(!isNearBottom)
    }

    container.addEventListener('scroll', handleScroll)
    return () => container.removeEventListener('scroll', handleScroll)
  }, [])

  const scrollToBottom = () => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  if (!messages || messages.length === 0) {
    return (
      <div className="text-muted-foreground text-center py-8">
        No messages yet. Start the conversation.
      </div>
    )
  }

  return (
    <div className="relative h-full" ref={scrollContainerRef}>
      <ScrollArea className="h-full">
        <div className="flex flex-col gap-6 pr-4 max-w-3xl mx-auto">
          {messages.map((msg, idx) => (
            <div 
              key={msg.id || idx} 
              className={`flex flex-col gap-1 ${msg.role === 'user' ? 'items-end' : 'items-start'}`}
            >
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                <span className="font-medium">
                  {msg.role === 'user' ? 'You' : 'STELLA'}
                </span>
                <span>{formatTimestamp(msg.timestamp)}</span>
              </div>
              
              {msg.role === 'assistant' && msg.toolCalls && (
                <ThinkingBox toolCalls={msg.toolCalls} defaultOpen={false} />
              )}
              
              <div className={
                msg.role === 'user'
                  ? "bg-primary/15 text-foreground rounded-2xl rounded-tr-sm px-4 py-2.5 max-w-[85%] leading-relaxed"
                  : "text-foreground leading-relaxed max-w-full"
              }>
                {msg.role === 'assistant' ? (
                  <MarkdownRenderer content={msg.content} />
                ) : (
                  <span className="whitespace-pre-wrap">{msg.content}</span>
                )}
              </div>
            </div>
          ))}

          {/* Show orb when streaming but no content yet */}
          {streaming && currentToolCalls?.length === 0 && !currentThinking && (
            <div className="flex flex-col gap-1 items-start">
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                <span className="font-medium">STELLA</span>
              </div>
              <div className="flex items-center gap-3 py-3">
                <ThinkingOrb size={22} />
                <span className="text-muted-foreground text-sm">
                  Thinking...
                </span>
              </div>
            </div>
          )}
          
          {/* Live ThinkingBox while streaming */}
          {streaming && (currentToolCalls?.length > 0 || currentThinking) && (
            <div className="flex flex-col gap-1 items-start">
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                <span className="font-medium">STELLA</span>
              </div>
              <ThinkingBox 
                toolCalls={currentToolCalls} 
                streaming={true} 
                currentThinking={currentThinking}
              />
            </div>
          )}
          
          <div ref={bottomRef} />
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