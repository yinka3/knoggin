import { useState, useEffect } from 'react'
import { getDiscussionInsights } from '@/api/community'
import { formatDistanceToNow } from 'date-fns'
import { Orbit, Clock, MessageSquare } from 'lucide-react'
import { motion, AnimatePresence } from 'motion/react'

export default function InsightsTab() {
  const [insights, setInsights] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function load() {
      try {
        const res = await getDiscussionInsights()
        setInsights(res.insights || [])
      } catch (err) {
        console.error('Failed to load insights:', err)
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [])

  if (loading) {
    return (
      <div className="p-4 space-y-3">
        {[1, 2, 3].map(i => (
          <div key={i} className="h-20 rounded-lg bg-muted/30 animate-pulse" />
        ))}
      </div>
    )
  }

  if (insights.length === 0) {
    return (
      <div className="h-full flex flex-col items-center justify-center text-center p-6">
        <Orbit size={24} className="text-muted-foreground/40 mb-2" />
        <p className="text-sm text-muted-foreground">No insights yet</p>
        <p className="text-xs text-muted-foreground/60 mt-1">
          Agents save insights during discussions
        </p>
      </div>
    )
  }

  return (
    <div className="p-3 space-y-2">
      <AnimatePresence initial={false}>
        {insights.map((insight, idx) => {
          const timeAgo = insight.timestamp
            ? formatDistanceToNow(new Date(insight.timestamp), { addSuffix: true })
            : null

          const content = insight.content?.replace(/^INSIGHT:\s*/i, '') || ''

          return (
            <motion.div
              key={`${insight.timestamp}-${idx}`}
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.2, delay: idx * 0.05 }}
              className="p-3 rounded-lg bg-card/50 border border-border/30"
            >
              <div className="flex items-start gap-2">
                <Orbit size={14} className="text-amber-500 mt-0.5 shrink-0" />
                <p className="text-sm text-foreground leading-relaxed">{content}</p>
              </div>

              <div className="flex items-center gap-3 mt-2 ml-5 text-xs text-muted-foreground">
                {timeAgo && (
                  <span className="flex items-center gap-1">
                    <Clock size={10} />
                    {timeAgo}
                  </span>
                )}

                {insight.discussion_topic && (
                  <span className="flex items-center gap-1 truncate">
                    <MessageSquare size={10} />
                    <span className="truncate max-w-[140px]">{insight.discussion_topic}</span>
                  </span>
                )}
              </div>
            </motion.div>
          )
        })}
      </AnimatePresence>
    </div>
  )
}
