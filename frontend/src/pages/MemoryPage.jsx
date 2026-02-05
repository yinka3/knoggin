import { useState, useEffect, useCallback } from 'react'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Search } from 'lucide-react'
import { listProfiles } from '@/api/profiles'
import EntityCard from '@/components/memory/EntityCard'
import EntityDrawer from '@/components/memory/EntityDrawer'

const PAGE_SIZE = 20

export default function MemoryPage() {
  const [entities, setEntities] = useState([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [showSkeleton, setShowSkeleton] = useState(false)
  const [selectedEntityId, setSelectedEntityId] = useState(null)
  const [drawerOpen, setDrawerOpen] = useState(false)

  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearch(search)
    }, 300)
    return () => clearTimeout(timer)
  }, [search])

  useEffect(() => {
    if (loading) {
      const timer = setTimeout(() => setShowSkeleton(true), 150)
      return () => clearTimeout(timer)
    }
    setShowSkeleton(false)
  }, [loading])

  const fetchEntities = useCallback(
    async (offset = 0, append = false) => {
      if (append) {
        setLoadingMore(true)
      } else {
        setLoading(true)
      }

      try {
        const params = { limit: PAGE_SIZE, offset }
        if (debouncedSearch) params.q = debouncedSearch

        const data = await listProfiles(params)

        if (append) {
          setEntities(prev => [...prev, ...data.entities])
        } else {
          setEntities(data.entities || [])
        }
        setTotal(data.total || 0)
      } catch (err) {
        console.error('Failed to load entities:', err)
      } finally {
        setLoading(false)
        setLoadingMore(false)
      }
    },
    [debouncedSearch]
  )

  useEffect(() => {
    fetchEntities(0, false)
  }, [fetchEntities])

  function handleLoadMore() {
    fetchEntities(entities.length, true)
  }

  function handleCardClick(entityId) {
    setSelectedEntityId(entityId)
    setDrawerOpen(true)
  }

  function handleEntityClick(entityId) {
    setSelectedEntityId(entityId)
  }

  const hasMore = entities.length < total

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="border-b border-border p-4">
        <div className="flex items-baseline justify-between mb-1">
          <h1 className="text-lg font-medium text-foreground">Memory</h1>
          {!loading && (
            <span className="text-sm text-muted-foreground">
              {total} {total === 1 ? 'entity' : 'entities'}
            </span>
          )}
        </div>
        <p className="text-sm text-muted-foreground mb-4">
          People, places, and things STELLA remembers
        </p>
        <div className="relative">
          <Search
            size={16}
            className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground"
          />
          <Input
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Search entities..."
            className="pl-9 bg-muted border-border rounded-xl"
          />
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4">
        {loading && showSkeleton ? (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {[...Array(6)].map((_, i) => (
              <Skeleton key={i} className="h-24 rounded-xl" />
            ))}
          </div>
        ) : loading ? null : entities.length === 0 ? (
          <div className="text-center py-16">
            <p className="text-muted-foreground">
              {debouncedSearch ? `No entities matching "${debouncedSearch}"` : 'No entities yet'}
            </p>
            {!debouncedSearch && (
              <p className="text-sm text-muted-foreground mt-1">
                Start chatting and STELLA will remember things for you
              </p>
            )}
          </div>
        ) : (
          <>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
              {entities.map(entity => (
                <EntityCard key={entity.id} entity={entity} onClick={handleCardClick} />
              ))}
            </div>

            {hasMore && (
              <div className="mt-6 text-center">
                <Button
                  variant="outline"
                  onClick={handleLoadMore}
                  disabled={loadingMore}
                  className="rounded-xl"
                >
                  {loadingMore ? 'Loading...' : `Load more (${entities.length} of ${total})`}
                </Button>
              </div>
            )}
          </>
        )}
      </div>

      <EntityDrawer
        entityId={selectedEntityId}
        open={drawerOpen}
        onOpenChange={setDrawerOpen}
        onEntityClick={handleEntityClick}
      />
    </div>
  )
}
