/* eslint-disable react-hooks/set-state-in-effect */
import { useEffect, useState } from 'react'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Skeleton } from '@/components/ui/skeleton'
import { getSessionMemory } from '@/api/sessions'
import { formatDistanceToNow } from 'date-fns'
import { StickyNote } from 'lucide-react'

function formatDate(iso) {
  if (!iso) return ''
  try {
    return formatDistanceToNow(new Date(iso), { addSuffix: true })
  } catch {
    return iso
  }
}

export default function AgentNotesDrawer({ sessionId, open, onOpenChange, onCountChange }) {
  const [memories, setMemories] = useState({})
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!open || !sessionId) return

    setLoading(true)
    getSessionMemory(sessionId)
      .then(data => {
        setMemories(data.memories || {})
        setTotal(data.total || 0)
        onCountChange?.(data.total || 0)
      })
      .catch(err => console.error('Failed to load agent notes:', err))
      .finally(() => setLoading(false))
  }, [open, sessionId, onCountChange])

  const topics = Object.entries(memories)

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md max-h-[80vh] flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-base">
            <StickyNote size={16} />
            Agent Notes
            {total > 0 && (
              <span className="text-xs text-muted-foreground font-normal">
                ({total})
              </span>
            )}
          </DialogTitle>
          <p className="text-xs text-muted-foreground">
            Notes your agent saved during conversations. Managed automatically via save_memory / forget_memory.
          </p>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto -mx-6 px-6 space-y-4">
          {loading ? (
            <div className="space-y-3">
              <Skeleton className="h-16 rounded-lg" />
              <Skeleton className="h-16 rounded-lg" />
            </div>
          ) : topics.length === 0 ? (
            <div className="text-center py-8">
              <StickyNote size={32} className="mx-auto text-muted-foreground/30 mb-2" />
              <p className="text-sm text-muted-foreground">No saved notes yet</p>
              <p className="text-xs text-muted-foreground mt-1">
                Your agent will save notes as it learns important things
              </p>
            </div>
          ) : (
            topics.map(([topic, entries]) => (
              <div key={topic}>
                <div className="flex items-center gap-2 mb-2">
                  <span className="text-xs font-medium text-foreground">{topic}</span>
                  <span className="text-[10px] text-muted-foreground bg-muted px-1.5 py-0.5 rounded">
                    {entries.length}
                  </span>
                </div>
                <div className="space-y-1.5">
                  {entries.map(entry => (
                    <div
                      key={entry.id}
                      className="px-3 py-2 rounded-lg bg-muted/50 border border-border/50"
                    >
                      <p className="text-sm text-foreground">{entry.content}</p>
                      <p className="text-[10px] text-muted-foreground mt-1">
                        {formatDate(entry.created_at)}
                      </p>
                    </div>
                  ))}
                </div>
              </div>
            ))
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}
