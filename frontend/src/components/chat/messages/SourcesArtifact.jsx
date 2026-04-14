import { useState } from 'react'
import { ExternalLink, Globe, ChevronDown, ChevronUp } from 'lucide-react'
import { motion, AnimatePresence } from 'motion/react'

/**
 * Displays web search sources as a collapsible artifact panel below a message.
 * Each source shows title, URL domain, and snippet for manual verification.
 */
export default function SourcesArtifact({ sources }) {
  const [expanded, setExpanded] = useState(false)

  if (!sources?.length) return null

  return (
    <motion.div
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.25, ease: 'easeOut' }}
      className="mt-3 rounded-xl border border-border/60 bg-card/50 backdrop-blur-sm overflow-hidden"
    >
      {/* Header */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between px-3.5 py-2.5 hover:bg-muted/30 transition-colors"
      >
        <div className="flex items-center gap-2">
          <div className="p-1 rounded-md bg-blue-500/10">
            <Globe size={13} className="text-blue-500" />
          </div>
          <span className="text-xs font-medium text-foreground">
            {sources.length} source{sources.length !== 1 ? 's' : ''} used
          </span>
        </div>
        {expanded ? (
          <ChevronUp size={14} className="text-muted-foreground" />
        ) : (
          <ChevronDown size={14} className="text-muted-foreground" />
        )}
      </button>

      {/* Source List */}
      <AnimatePresence>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2, ease: 'easeInOut' }}
            className="overflow-hidden"
          >
            <div className="border-t border-border/40 divide-y divide-border/30">
              {sources.map((source, i) => (
                <SourceRow key={i} source={source} index={i} />
              ))}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  )
}

function SourceRow({ source, index }) {
  const domain = (() => {
    try {
      return new URL(source.url).hostname.replace('www.', '')
    } catch {
      return source.url || 'unknown'
    }
  })()

  return (
    <div className="px-3.5 py-2.5 hover:bg-muted/20 transition-colors group">
      <div className="flex items-start gap-2.5">
        <span className="text-[10px] font-mono text-muted-foreground/60 mt-0.5 shrink-0 w-4 text-right">
          {index + 1}
        </span>
        <div className="min-w-0 flex-1 space-y-0.5">
          <div className="flex items-center gap-2">
            <span className="text-xs font-medium text-foreground truncate">
              {source.title || 'Untitled'}
            </span>
            {source.url && (
              <a
                href={source.url}
                target="_blank"
                rel="noopener noreferrer"
                className="shrink-0 opacity-0 group-hover:opacity-100 transition-opacity"
              >
                <ExternalLink size={11} className="text-muted-foreground hover:text-primary" />
              </a>
            )}
          </div>
          <div className="text-[10px] text-muted-foreground/70 font-medium">
            {domain}
          </div>
          {source.snippet && (
            <p className="text-[11px] text-muted-foreground leading-relaxed line-clamp-2">
              {source.snippet}
            </p>
          )}
        </div>
      </div>
    </div>
  )
}
