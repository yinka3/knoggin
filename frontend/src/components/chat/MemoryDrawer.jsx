import { useState, useEffect, useCallback } from 'react'
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Badge } from '@/components/ui/badge'
import { BrainCircuit, ChevronDown, ChevronUp } from 'lucide-react'
import { getSessionMemory } from '@/api/agents'
import { useSocket } from '@/context/SocketContext'
import { formatDate } from '@/lib/format'
import { cn } from '@/lib/utils'

const PREVIEW_COUNT = 4

function TopicGroup({ topic, entries }) {
  const [expanded, setExpanded] = useState(false)
  const hasMore = entries.length > PREVIEW_COUNT
  const visible = expanded ? entries : entries.slice(0, PREVIEW_COUNT)

  return (
    <div>
      <div className="flex items-center justify-between mb-2">
        <span className="text-[11px] text-muted-foreground/50 uppercase tracking-wider">
          {topic}
        </span>
        <span className="text-[10px] text-muted-foreground/30">{entries.length}</span>
      </div>

      <div className="space-y-1">
        {visible.map(entry => (
          <div
            key={entry.id}
            className="px-3 py-2 rounded-lg border border-white/[0.04] bg-white/[0.01] hover:border-white/[0.08] transition-colors"
          >
            <p className="text-xs text-muted-foreground leading-relaxed">{entry.content}</p>
            <span className="text-[10px] text-muted-foreground/30 mt-1.5 block">
              {formatDate(entry.created_at)}
            </span>
          </div>
        ))}
      </div>

      {hasMore && (
        <button
          onClick={() => setExpanded(!expanded)}
          className="flex items-center gap-1 mt-1.5 text-[10px] text-muted-foreground/40 hover:text-muted-foreground transition-colors"
        >
          {expanded ? (
            <>
              <ChevronUp size={12} />
              Show less
            </>
          ) : (
            <>
              <ChevronDown size={12} />
              Show all {entries.length}
            </>
          )}
        </button>
      )}
    </div>
  )
}

export default function MemoryDrawer({ sessionId, open, onOpenChange, onCountChange }) {
  const [blocks, setBlocks] = useState({})
  const [total, setTotal] = useState(0)

  const loadMemory = useCallback(async () => {
    try {
      const data = await getSessionMemory(sessionId)
      setBlocks(data.blocks || {})
      const count = data.total || 0
      setTotal(count)
      onCountChange?.(count)
    } catch (err) {
      console.error('Failed to load memory:', err)
    }
  }, [sessionId, onCountChange])

  useEffect(() => {
    if (open && sessionId) {
      setTimeout(() => loadMemory(), 0)
    }
  }, [open, sessionId, loadMemory])

  useSocket('memory_saved', () => loadMemory())
  useSocket('memory_forgotten', () => loadMemory())

  if (!sessionId) return null

  const topicNames = Object.keys(blocks)

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="w-80 sm:w-96 p-0 flex flex-col">
        <div className="px-5 pt-5 pb-3 border-b border-border/30">
          <SheetHeader>
            <SheetTitle className="flex items-center justify-between text-base">
              <span>Memory</span>
              <Badge variant="outline" className="text-[10px] font-normal">
                {total} {total === 1 ? 'entry' : 'entries'}
              </Badge>
            </SheetTitle>
          </SheetHeader>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4">
          {total === 0 ? (
            <div className="flex flex-col items-center justify-center h-full text-center">
              <BrainCircuit size={20} className="text-muted-foreground/30 mb-2" />
              <p className="text-xs text-muted-foreground/60">No memories yet</p>
              <p className="text-[10px] text-muted-foreground/30 mt-1">
                Your agent saves context as you chat
              </p>
            </div>
          ) : (
            <div className="space-y-5">
              {topicNames.map(topic => (
                <TopicGroup key={topic} topic={topic} entries={blocks[topic]} />
              ))}
            </div>
          )}
        </div>

        <div className="px-5 py-2.5 border-t border-border/30">
          <p className="text-[10px] text-muted-foreground/30 text-center">
            Saved by your agent during conversation
          </p>
        </div>
      </SheetContent>
    </Sheet>
  )
}
