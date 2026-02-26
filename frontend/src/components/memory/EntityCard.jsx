import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'

export default function EntityCard({ entity, onClick }) {
  return (
    <div
      onClick={onClick}
      className={cn(
        'cursor-pointer rounded-xl px-4 py-3',
        'border border-white/[0.08] bg-card/40 backdrop-blur-xl',
        'hover:border-white/[0.15] hover:shadow-lg hover:shadow-primary/10',
        'hover:-translate-y-0.5 transition-all duration-200'
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <h3 className="text-sm font-semibold text-foreground truncate">
            {entity.canonical_name}
          </h3>
          <span className="text-[11px] text-muted-foreground/50">{entity.type}</span>
        </div>

        {entity.fact_count > 0 && (
          <span className="text-[10px] text-muted-foreground/40 tabular-nums shrink-0">
            {entity.fact_count}
          </span>
        )}
      </div>

      {entity.topic && (
        <span className="inline-block mt-2.5 text-[10px] text-muted-foreground px-2 py-0.5 rounded-md border border-white/[0.06] bg-white/[0.02]">
          {entity.topic}
        </span>
      )}
    </div>
  )
}
