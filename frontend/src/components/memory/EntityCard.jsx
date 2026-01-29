// src/components/memory/EntityCard.jsx
import { formatDistanceToNow } from 'date-fns'

export default function EntityCard({ entity, onClick }) {
  const lastMentioned = entity.last_mentioned
    ? formatDistanceToNow(new Date(entity.last_mentioned), { addSuffix: true })
    : 'never'

  return (
    <button
      onClick={() => onClick(entity.id)}
      className="w-full text-left p-4 rounded-xl bg-card border border-border 
        hover:border-primary/50 hover:bg-card/80
        transition-all duration-200 cursor-pointer group"
    >
      <div className="flex items-start justify-between gap-3 mb-2">
        <h3 className="font-medium text-foreground group-hover:text-primary transition-colors">
          {entity.canonical_name}
        </h3>
        <span className="text-[11px] text-muted-foreground bg-muted px-2 py-0.5 rounded-full shrink-0">
          {entity.type || 'unknown'}
        </span>
      </div>

      <div className="flex items-center gap-2 text-xs text-muted-foreground">
        {entity.topic && (
          <>
            <span>{entity.topic}</span>
            <span>·</span>
          </>
        )}
        <span>{lastMentioned}</span>
      </div>
    </button>
  )
}