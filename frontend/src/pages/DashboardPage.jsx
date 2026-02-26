import { useState, useEffect, useRef, useCallback } from 'react'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Brain,
  FileText,
  GitBranch,
  MessageSquare,
  RefreshCw,
  Users,
  Coins,
  Hash,
} from 'lucide-react'
import { getStats, getStatsBreakdown } from '@/api/stats'
import { motion } from 'motion/react'
import { useSocket } from '@/context/SocketContext'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'

function StatCard({ icon: Icon, label, value, loading, subtitle, delay = 0 }) {
  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.95, y: 10 }}
      animate={{ opacity: 1, scale: 1, y: 0 }}
      transition={{ duration: 0.4, delay, ease: [0.22, 1, 0.36, 1] }}
      className={cn(
        'rounded-xl p-4 border bg-card',
        'border-white/[0.08] bg-gradient-to-br from-white/[0.03] via-transparent to-transparent',
        'hover:border-white/[0.15] hover:shadow-lg hover:shadow-white/[0.03]',
        'transition-all duration-300'
      )}
    >
      <div className="p-2 rounded-lg bg-primary/10 text-primary w-fit mb-3">
        <Icon size={16} />
      </div>

      {loading ? (
        <Skeleton className="h-7 w-14" />
      ) : value === '0' || value === '$0.000000' ? (
        <p className="text-2xl font-semibold text-muted-foreground/40">{value}</p>
      ) : (
        <p className="text-2xl font-semibold text-foreground">{value ?? '—'}</p>
      )}
      <p className="text-xs text-muted-foreground mt-1">{label}</p>
      {subtitle && <p className="text-[10px] text-muted-foreground/60 mt-0.5">{subtitle}</p>}
    </motion.div>
  )
}

// Leaderboard card
function LeaderboardCard({ title, items, loading, className, delay = 0 }) {
  const maxValue = items?.[0]?.connections || items?.[0]?.count || 1

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay, ease: [0.22, 1, 0.36, 1] }}
      className={cn('rounded-xl p-5 border border-border bg-card', className)}
    >
      <div className="flex items-center gap-2 mb-4">
        <Users size={16} className="text-muted-foreground" />
        <h3 className="text-sm font-medium text-foreground">{title}</h3>
      </div>

      {loading ? (
        <div className="space-y-3">
          {[1, 2, 3, 4, 5].map(i => (
            <Skeleton key={i} className="h-8 w-full" />
          ))}
        </div>
      ) : items?.length > 0 ? (
        <div className="space-y-3">
          {items.slice(0, 5).map((item, idx) => (
            <div key={item.name || idx} className="group">
              <div className="flex items-center justify-between mb-1.5">
                <div className="flex items-center gap-2">
                  <span
                    className={cn(
                      'text-xs font-medium w-5 h-5 rounded-full flex items-center justify-center',
                      idx === 0 ? 'bg-primary/20 text-primary' : 'bg-muted text-muted-foreground'
                    )}
                  >
                    {idx + 1}
                  </span>
                  <span className="text-sm text-foreground truncate max-w-[120px]">
                    {item.name}
                  </span>
                </div>
                <span className="text-xs text-muted-foreground">
                  {item.connections || item.count}
                </span>
              </div>
              <div className="h-1.5 bg-muted rounded-full overflow-hidden">
                <motion.div
                  initial={{ width: 0 }}
                  animate={{
                    width: `${((item.connections || item.count) / maxValue) * 100}%`,
                  }}
                  transition={{ duration: 0.5, delay: idx * 0.1 }}
                  className={cn('h-full rounded-full', idx === 0 ? 'bg-primary' : 'bg-zinc-400/60')}
                />
              </div>
            </div>
          ))}
        </div>
      ) : (
        <p className="text-sm text-muted-foreground">No data yet</p>
      )}
    </motion.div>
  )
}

function formatTokens(num) {
  if (!num) return '0'
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(2)}M`
  if (num >= 1_000) return `${(num / 1_000).toFixed(1)}K`
  return num.toLocaleString()
}

function formatCost(num) {
  if (!num) return '$0.000000'
  return `$${num.toFixed(6)}`
}

export default function DashboardPage() {
  const [stats, setStats] = useState(null)
  const [breakdown, setBreakdown] = useState(null)
  const [loading, setLoading] = useState(true)
  const [breakdownLoading, setBreakdownLoading] = useState(true)
  const [error, setError] = useState(null)

  async function loadData() {
    try {
      const [statsData, breakdownData] = await Promise.all([getStats(), getStatsBreakdown()])
      setStats(statsData)
      setBreakdown(breakdownData)
      setError(null)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
      setBreakdownLoading(false)
    }
  }

  useEffect(() => {
    loadData()
  }, [])

  const debounceRef = useRef(null)
  const debouncedLoad = useCallback(() => {
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => loadData(), 2000)
  }, [])

  useSocket('facts_changed', () => {
    toast.info('New facts extracted', { description: 'Refreshing dashboard...' })
    debouncedLoad()
  })

  useSocket('entities_merged', data => {
    toast.success('Entities merged', {
      description: `${data.data.primary} ← ${data.data.secondary}`,
    })
    debouncedLoad()
  })

  useSocket('profiles_refined', data => {
    toast.info('Profiles refined', {
      description: `Updated ${data.data.count} entities`,
    })
    debouncedLoad()
  })

  return (
    <div className="flex flex-col h-full relative">
      {/* Header */}
      <div className="border-b border-border/60 p-6 relative">
        <div className="max-w-6xl mx-auto flex items-center justify-between">
          <motion.div
            initial={{ opacity: 0, x: -12 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
          >
            <h1 className="text-lg font-semibold text-foreground tracking-tight">Dashboard</h1>
            <p className="text-sm text-muted-foreground">Your knowledge graph at a glance</p>
          </motion.div>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-6 relative">
        <div className="max-w-6xl mx-auto">
          {error && (
            <div className="mb-4 p-4 rounded-xl bg-destructive/10 text-destructive text-sm border border-destructive/20">
              {error}
            </div>
          )}

          {/* Stat Cards — 2 rows of 3 */}
          <div className="grid grid-cols-3 gap-4 mb-4">
            <StatCard
              icon={Brain}
              label="Total Entities"
              value={stats?.entities?.toLocaleString()}
              loading={loading}
              subtitle="People, places, and things"
              delay={0.1}
            />
            <StatCard
              icon={FileText}
              label="Facts"
              value={stats?.facts?.toLocaleString()}
              loading={loading}
              delay={0.15}
            />
            <StatCard
              icon={GitBranch}
              label="Connections"
              value={stats?.relationships?.toLocaleString()}
              loading={loading}
              delay={0.2}
            />
            <StatCard
              icon={MessageSquare}
              label="Sessions"
              value={stats?.sessions?.toLocaleString()}
              loading={loading}
              delay={0.25}
            />
            <StatCard
              icon={Hash}
              label="Total Tokens"
              value={formatTokens(stats?.total_tokens)}
              loading={loading}
              subtitle="All-time usage"
              delay={0.3}
            />
            <StatCard
              icon={Coins}
              label="Total Cost"
              value={formatCost(stats?.total_cost)}
              loading={loading}
              subtitle="OpenRouter spend"
              delay={0.35}
            />
          </div>

          {/* Most Connected — full width */}
          <LeaderboardCard
            title="Most Connected"
            items={breakdown?.top_connected}
            loading={breakdownLoading}
            delay={0.4}
          />
        </div>
      </div>
    </div>
  )
}
