import { useState, useEffect } from 'react'
import { getDiscussions } from '@/api/community'
import { formatDistanceToNow } from 'date-fns'
import { MessageSquare, Clock, Radio } from 'lucide-react'
import { cn } from '@/lib/utils'

export default function HistoryTab({ onSelectDiscussion, selectedId, activeDiscussionId }) {
  const [discussions, setDiscussions] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function load() {
      try {
        const res = await getDiscussions()
        setDiscussions(res.discussions || [])
      } catch (err) {
        console.error('Failed to load discussions:', err)
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [])

  if (loading) {
    return (
      <div className="p-4 space-y-2">
        {[1, 2, 3].map(i => (
          <div key={i} className="h-16 rounded-lg bg-muted/30 animate-pulse" />
        ))}
      </div>
    )
  }

  if (discussions.length === 0) {
    return (
      <div className="h-full flex flex-col items-center justify-center text-center p-6">
        <MessageSquare size={24} className="text-muted-foreground/40 mb-2" />
        <p className="text-sm text-muted-foreground">No discussions yet</p>
        <p className="text-xs text-muted-foreground/60 mt-1">Trigger a discussion to get started</p>
      </div>
    )
  }

  return (
    <div className="p-2 space-y-1">
      {discussions.map(discussion => {
        const isSelected = selectedId === discussion.id
        const isActive = activeDiscussionId === discussion.id
        const timeAgo = discussion.created_at
          ? formatDistanceToNow(new Date(discussion.created_at), { addSuffix: true })
          : null

        return (
          <button
            key={discussion.id}
            onClick={() => onSelectDiscussion(discussion)}
            className={cn(
              'w-full text-left p-3 rounded-lg transition-colors',
              isSelected
                ? 'bg-primary/10 border border-primary/30'
                : 'hover:bg-muted/30 border border-transparent'
            )}
          >
            <div className="flex items-center gap-2">
              {isActive && <Radio size={12} className="text-emerald-500 animate-pulse shrink-0" />}
              <p className="text-sm text-foreground font-medium truncate flex-1">
                {discussion.topic || 'Untitled discussion'}
              </p>
            </div>

            <div className="flex items-center gap-3 mt-1.5 text-xs text-muted-foreground">
              {timeAgo && (
                <span className="flex items-center gap-1">
                  <Clock size={10} />
                  {timeAgo}
                </span>
              )}

              {discussion.message_count !== undefined && (
                <span className="flex items-center gap-1">
                  <MessageSquare size={10} />
                  {discussion.message_count}
                </span>
              )}

              {isActive ? (
                <span className="px-1.5 py-0.5 rounded text-[10px] uppercase tracking-wide bg-emerald-500/10 text-emerald-500">
                  Live
                </span>
              ) : (
                <span className="px-1.5 py-0.5 rounded text-[10px] uppercase tracking-wide bg-muted text-muted-foreground">
                  {discussion.status || 'closed'}
                </span>
              )}
            </div>
          </button>
        )
      })}
    </div>
  )
}
