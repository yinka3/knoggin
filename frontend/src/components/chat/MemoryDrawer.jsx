import { useState, useEffect } from 'react'
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from '@/components/ui/sheet'
import { Badge } from '@/components/ui/badge'
import { BrainCircuit } from 'lucide-react'
import { getSessionMemory } from '@/api/agents'
import { cn } from '@/lib/utils'
import { useSocket } from '@/context/SocketContext'
import { formatDate } from '@/lib/format'



export default function MemoryDrawer({ sessionId }) {
  const [open, setOpen] = useState(false)
  const [blocks, setBlocks] = useState({})
  const [total, setTotal] = useState(0)

  useEffect(() => {
    if (open && sessionId) {
      loadMemory()
    }
  }, [open, sessionId])

  async function loadMemory() {
    try {
      const data = await getSessionMemory(sessionId)
      setBlocks(data.blocks || {})
      setTotal(data.total || 0)
    } catch (err) {
      console.error('Failed to load memory:', err)
    }
  }

  useSocket('memory_saved', () => {
    loadMemory()
  })

  useSocket('memory_forgotten', () => {
    loadMemory()
  })

  if (!sessionId) return null

  const topicNames = Object.keys(blocks)

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger asChild>
        <button
          className={cn(
            'relative flex items-center gap-1.5 px-2 py-1 rounded-md text-xs transition-colors duration-150',
            'text-muted-foreground hover:text-foreground hover:bg-muted/50',
            total > 0 && 'text-primary'
          )}
        >
          <BrainCircuit size={14} />
          <span className="hidden sm:inline">Memory</span>
          {total > 0 && (
            <Badge variant="secondary" className="h-4 px-1 text-[10px] min-w-[16px] justify-center">
              {total}
            </Badge>
          )}
        </button>
      </SheetTrigger>

      <SheetContent side="right" className="w-80 sm:w-96">
        <SheetHeader>
          <SheetTitle className="flex items-center justify-between">
            <span>Agent Memory</span>
            <Badge variant="outline" className="text-[10px]">
              {total} entries
            </Badge>
          </SheetTitle>
        </SheetHeader>

        <div className="mt-6 space-y-5">
          {total === 0 ? (
            <div className="text-center py-8">
              <BrainCircuit size={24} className="mx-auto text-muted-foreground/40 mb-2" />
              <p className="text-xs text-muted-foreground">No memories saved yet</p>
              <p className="text-[10px] text-muted-foreground/60 mt-1">
                Your agent will save important context as you chat
              </p>
            </div>
          ) : (
            topicNames.map(topic => (
              <div key={topic}>
                <div className="flex items-center gap-2 mb-2">
                  <span className="text-xs font-medium text-foreground">{topic}</span>
                  <Badge variant="secondary" className="text-[10px] h-4 px-1">
                    {blocks[topic].length}
                  </Badge>
                </div>
                <div className="space-y-1.5">
                  {blocks[topic].map(entry => (
                    <div
                      key={entry.id}
                      className="px-3 py-2 rounded-lg bg-muted/50 border border-border/50"
                    >
                      <p className="text-xs text-foreground leading-relaxed">{entry.content}</p>
                      <div className="flex items-center justify-between mt-1.5">
                        <span className="text-[10px] text-muted-foreground">
                          {formatDate(entry.created_at)}
                        </span>
                        <span className="text-[10px] text-muted-foreground/50 font-mono">
                          {entry.id}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ))
          )}

          <p className="text-[10px] text-muted-foreground/50 text-center">
            Memories are saved by your agent during conversation
          </p>
        </div>
      </SheetContent>
    </Sheet>
  )
}
