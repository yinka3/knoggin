import { useEffect, useRef } from 'react'
import { motion, AnimatePresence } from 'motion/react'
import { Loader2 } from 'lucide-react'

function MessageCard({ message, agentMap }) {
  const agentName = message.agent_name || agentMap?.[message.agent_id] || message.agent_id

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.3, ease: 'easeOut' }}
      className="px-4 py-3 rounded-lg bg-card/50 border border-border/30"
    >
      <span className="text-xs font-medium text-primary/80 uppercase tracking-wide">
        {agentName}
      </span>
      <p className="mt-1.5 text-sm text-foreground leading-relaxed whitespace-pre-wrap">
        {message.content}
      </p>
    </motion.div>
  )
}

export default function CommunityTheater({ messages, connected, isLive, isLoading, agentMap }) {
  const scrollRef = useRef(null)

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [messages])

  return (
    <div className="h-full flex flex-col rounded-xl border border-border/50 bg-card/30 overflow-hidden">
      {/* Message area */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto p-4 space-y-3">
        {isLoading ? (
          <div className="h-full flex items-center justify-center">
            <Loader2 size={24} className="text-muted-foreground animate-spin" />
          </div>
        ) : (
          <>
            <AnimatePresence initial={false}>
              {messages.map((msg, idx) => (
                <MessageCard
                  key={`${msg.agent_id}-${msg.timestamp}-${idx}`}
                  message={msg}
                  agentMap={agentMap}
                />
              ))}
            </AnimatePresence>

            {/* Empty / idle states */}
            {messages.length === 0 && (
              <div className="h-full flex flex-col items-center justify-center text-center py-12">
                {isLive ? (
                  <>
                    <div className="h-2 w-2 rounded-full bg-emerald-500/50 animate-pulse mb-3" />
                    <p className="text-sm text-muted-foreground">Discussion starting...</p>
                  </>
                ) : connected ? (
                  <>
                    <div className="h-2 w-2 rounded-full bg-amber-500/50 mb-3" />
                    <p className="text-sm text-muted-foreground">No active discussion</p>
                    <p className="text-xs text-muted-foreground/60 mt-1">
                      Trigger one manually or wait for the next scheduled run
                    </p>
                  </>
                ) : (
                  <>
                    <div className="h-2 w-2 rounded-full bg-neutral-600 mb-3" />
                    <p className="text-sm text-muted-foreground">Connecting...</p>
                  </>
                )}
              </div>
            )}
          </>
        )}
      </div>

      {/* Live indicator bar at bottom */}
      {isLive && (
        <div className="px-4 py-2 border-t border-border/30 bg-emerald-500/5">
          <div className="flex items-center gap-2">
            <div className="h-1.5 w-1.5 rounded-full bg-emerald-500 animate-pulse" />
            <span className="text-[10px] text-emerald-500/70 uppercase tracking-wider font-medium">
              Agents are discussing
            </span>
          </div>
        </div>
      )}
    </div>
  )
}
