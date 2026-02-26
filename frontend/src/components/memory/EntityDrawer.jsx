import { useEffect, useState } from 'react'
import { formatDistanceToNow } from 'date-fns'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Skeleton } from '@/components/ui/skeleton'
import { Separator } from '@/components/ui/separator'
import { getProfile } from '@/api/profiles'
import { ChevronDown, ChevronUp } from 'lucide-react'

const FACTS_PREVIEW = 8

function formatFactDate(iso) {
  if (!iso) return null
  try {
    return formatDistanceToNow(new Date(iso), { addSuffix: true })
  } catch {
    return null
  }
}

function Pill({ children, onClick }) {
  return (
    <button
      onClick={onClick}
      className="inline-flex items-center px-2.5 py-1 rounded-md text-xs text-muted-foreground border border-white/[0.06] bg-white/[0.02] hover:bg-white/[0.05] hover:text-foreground transition-colors"
    >
      {children}
    </button>
  )
}

export default function EntityDrawer({ entityId, open, onOpenChange, onEntityClick }) {
  const [profile, setProfile] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [showAllFacts, setShowAllFacts] = useState(false)

  useEffect(() => {
    if (!entityId || !open) {
      setProfile(null)
      setShowAllFacts(false)
      return
    }

    async function load() {
      setLoading(true)
      setError(null)
      setShowAllFacts(false)
      try {
        const data = await getProfile(entityId)
        setProfile(data)
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }

    load()
  }, [entityId, open])

  const facts = (profile?.facts || []).slice().sort((a, b) => {
    if (!a.valid_at && !b.valid_at) return 0
    if (!a.valid_at) return 1
    if (!b.valid_at) return -1
    return new Date(b.valid_at) - new Date(a.valid_at)
  })

  const visibleFacts = showAllFacts ? facts : facts.slice(0, FACTS_PREVIEW)
  const hasMore = facts.length > FACTS_PREVIEW

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-2xl w-full max-h-[80vh] overflow-y-auto p-0 bg-background/95 backdrop-blur-xl border-border/60">
        {/* Header */}
        <div className="px-6 pt-6 pb-4">
          <DialogHeader className="space-y-1">
            {loading ? (
              <div className="space-y-2">
                <Skeleton className="h-6 w-48" />
                <Skeleton className="h-4 w-24" />
              </div>
            ) : (
              <>
                <DialogTitle className="text-lg font-semibold">
                  {profile?.canonical_name || 'Unknown'}
                </DialogTitle>
                <div className="flex items-center gap-2 text-[11px]">
                  <span className="text-muted-foreground/60 uppercase tracking-wide">
                    {profile?.type}
                  </span>
                  {profile?.topic && (
                    <>
                      <span className="w-1 h-1 rounded-full bg-muted-foreground/30" />
                      <span className="text-muted-foreground/40">
                        topic: <span className="text-muted-foreground">{profile.topic}</span>
                      </span>
                    </>
                  )}
                </div>
              </>
            )}
          </DialogHeader>
        </div>

        {/* Content */}
        <div className="px-6 pb-6 space-y-4">
          {loading ? (
            <div className="space-y-3">
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-5/6" />
              <Skeleton className="h-4 w-4/6" />
            </div>
          ) : error ? (
            <div className="p-4 rounded-lg bg-destructive/10 text-destructive text-sm border border-destructive/20">
              Error loading profile: {error}
            </div>
          ) : profile ? (
            <>
              {/* Knowledge */}
              <div>
                <p className="text-xs text-muted-foreground/50 mb-2">Knowledge ({facts.length})</p>

                {facts.length > 0 ? (
                  <>
                    {visibleFacts.map((fact, idx) => {
                      const cleaned = fact.content.replace(/\s*\[MSG_\d+\]\s*/g, '')
                      const date = formatFactDate(fact.valid_at)
                      return (
                        <div
                          key={idx}
                          className="flex items-baseline justify-between gap-3 py-1.5 border-b border-border/20 last:border-0"
                        >
                          <span className="text-xs text-muted-foreground leading-snug">
                            {cleaned}
                          </span>
                          {date && (
                            <span className="text-[10px] text-muted-foreground/30 shrink-0">
                              {date}
                            </span>
                          )}
                        </div>
                      )
                    })}

                    {hasMore && (
                      <button
                        onClick={() => setShowAllFacts(!showAllFacts)}
                        className="flex items-center gap-1 mt-2 text-[11px] text-muted-foreground/50 hover:text-muted-foreground transition-colors"
                      >
                        {showAllFacts ? (
                          <>
                            <ChevronUp size={12} />
                            Show less
                          </>
                        ) : (
                          <>
                            <ChevronDown size={12} />
                            Show all {facts.length}
                          </>
                        )}
                      </button>
                    )}
                  </>
                ) : (
                  <p className="text-xs text-muted-foreground/40 italic">No facts recorded yet.</p>
                )}
              </div>

              {/* Hierarchy */}
              {(profile.hierarchy?.parent || profile.hierarchy?.children?.length > 0) && (
                <div>
                  <Separator className="mb-4 bg-border/20" />
                  <p className="text-xs text-muted-foreground/50 mb-2">Hierarchy</p>
                  <div className="space-y-2">
                    {profile.hierarchy.parent && (
                      <div className="flex items-center gap-2">
                        <span className="text-[11px] text-muted-foreground/40 w-12">Parent</span>
                        <Pill onClick={() => onEntityClick(profile.hierarchy.parent.id)}>
                          {profile.hierarchy.parent.name}
                        </Pill>
                      </div>
                    )}
                    {profile.hierarchy.children?.length > 0 && (
                      <div className="flex items-start gap-2">
                        <span className="text-[11px] text-muted-foreground/40 w-12 pt-1">
                          Children
                        </span>
                        <div className="flex flex-wrap gap-1.5">
                          {profile.hierarchy.children.map(child => (
                            <Pill key={child.id} onClick={() => onEntityClick(child.id)}>
                              {child.name}
                            </Pill>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              )}

              {/* Connections */}
              {profile.connections?.length > 0 && (
                <div>
                  <Separator className="mb-4 bg-border/20" />
                  <p className="text-xs text-muted-foreground/50 mb-2">
                    Connections ({profile.connections.length})
                  </p>
                  <div className="flex flex-wrap gap-1.5">
                    {profile.connections.map(conn => (
                      <Pill key={conn.id} onClick={() => onEntityClick(conn.id)}>
                        {conn.name}
                      </Pill>
                    ))}
                  </div>
                </div>
              )}

              {/* Timestamps — tucked at bottom */}
              {(profile.last_updated || profile.last_mentioned) && (
                <div className="flex items-center gap-4 pt-2 text-[10px] text-muted-foreground/30">
                  {profile.last_updated && (
                    <span>
                      Updated{' '}
                      {formatDistanceToNow(new Date(profile.last_updated), { addSuffix: true })}
                    </span>
                  )}
                  {profile.last_mentioned && (
                    <span>
                      Mentioned{' '}
                      {formatDistanceToNow(new Date(profile.last_mentioned), { addSuffix: true })}
                    </span>
                  )}
                </div>
              )}
            </>
          ) : null}
        </div>
      </DialogContent>
    </Dialog>
  )
}
