import { useEffect, useState } from 'react'
import { formatDistanceToNow } from 'date-fns'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { getProfile } from '@/api/profiles'

function SectionHeader({ children }) {
  return <h3 className="text-xs font-medium text-muted-foreground mb-3">{children}</h3>
}

function Pill({ children, onClick }) {
  const Component = onClick ? 'button' : 'span'
  return (
    <Component
      onClick={onClick}
      className={`inline-flex items-center px-2.5 py-1 rounded-full bg-muted text-xs text-foreground ${
        onClick ? 'hover:bg-primary/20 hover:text-primary transition-colors cursor-pointer' : ''
      }`}
    >
      {children}
    </Component>
  )
}

const FACTS_PREVIEW_COUNT = 5

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

  const lastMentioned = profile?.last_mentioned
    ? formatDistanceToNow(new Date(profile.last_mentioned), { addSuffix: true })
    : null

  const lastUpdated = profile?.last_updated
    ? formatDistanceToNow(new Date(profile.last_updated), { addSuffix: true })
    : null

  const facts = profile?.facts || []
  const visibleFacts = showAllFacts ? facts : facts.slice(0, FACTS_PREVIEW_COUNT)
  const hasMoreFacts = facts.length > FACTS_PREVIEW_COUNT

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="bg-background border-border sm:max-w-lg max-h-[85vh] overflow-hidden flex flex-col">
        <DialogHeader className="pb-4">
          {loading ? (
            <Skeleton className="h-6 w-48" />
          ) : (
            <div className="flex items-center gap-3">
              <DialogTitle className="text-lg font-medium">
                {profile?.canonical_name || 'Unknown'}
              </DialogTitle>
              {profile?.type && (
                <span className="text-xs text-muted-foreground bg-muted px-2 py-0.5 rounded-full">
                  {profile.type}
                </span>
              )}
            </div>
          )}
        </DialogHeader>

        <div className="flex-1 overflow-y-auto space-y-6">
          {loading ? (
            <div className="space-y-4">
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-3/4" />
              <Skeleton className="h-4 w-1/2" />
            </div>
          ) : error ? (
            <div className="text-destructive text-sm">{error}</div>
          ) : (
            profile && (
              <>
                {/* Metadata */}
                <div className="flex flex-wrap gap-x-6 gap-y-2 text-sm text-muted-foreground">
                  {profile.topic && (
                    <div>
                      <span className="text-foreground">{profile.topic}</span>
                    </div>
                  )}
                  {lastMentioned && <div>Mentioned {lastMentioned}</div>}
                  {lastUpdated && <div>Updated {lastUpdated}</div>}
                </div>

                {/* Facts */}
                <div>
                  <SectionHeader>Facts ({facts.length})</SectionHeader>
                  {facts.length > 0 ? (
                    <>
                      <ul className="space-y-2">
                        {visibleFacts.map((fact, idx) => (
                          <li key={fact.id || idx} className="text-sm text-foreground leading-relaxed">
                            {fact.content}
                          </li>
                        ))}
                      </ul>
                      {hasMoreFacts && (
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => setShowAllFacts(!showAllFacts)}
                          className="mt-3 text-primary hover:text-primary"
                        >
                          {showAllFacts
                            ? 'Show less'
                            : `Show ${facts.length - FACTS_PREVIEW_COUNT} more`}
                        </Button>
                      )}
                    </>
                  ) : (
                    <p className="text-sm text-muted-foreground">No facts recorded</p>
                  )}
                </div>

                {/* Connections */}
                <div>
                  <SectionHeader>Connections ({profile.connections?.length || 0})</SectionHeader>
                  {profile.connections?.length > 0 ? (
                    <div className="flex flex-wrap gap-2">
                      {profile.connections.map((conn) => (
                        <Pill key={conn.id} onClick={() => onEntityClick(conn.id)}>
                          {conn.name}
                        </Pill>
                      ))}
                    </div>
                  ) : (
                    <p className="text-sm text-muted-foreground">No connections</p>
                  )}
                </div>

                {/* Hierarchy */}
                {(profile.hierarchy?.parent || profile.hierarchy?.children?.length > 0) && (
                  <div>
                    <SectionHeader>Hierarchy</SectionHeader>
                    <div className="space-y-3">
                      {profile.hierarchy.parent && (
                        <div className="flex items-center gap-3">
                          <span className="text-xs text-muted-foreground w-16">Parent</span>
                          <Pill onClick={() => onEntityClick(profile.hierarchy.parent.id)}>
                            {profile.hierarchy.parent.name}
                          </Pill>
                        </div>
                      )}
                      {profile.hierarchy.children?.length > 0 && (
                        <div className="flex items-start gap-3">
                          <span className="text-xs text-muted-foreground w-16 pt-1">Children</span>
                          <div className="flex flex-wrap gap-2">
                            {profile.hierarchy.children.map((child) => (
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
              </>
            )
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}
