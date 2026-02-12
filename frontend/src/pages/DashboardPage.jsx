import { useState, useEffect, useRef, useCallback } from 'react'
import { Skeleton } from '@/components/ui/skeleton'
import { MessageSquare, Bot, Users, FileText, GitBranch, Brain, RefreshCw } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { getStats, getStatsBreakdown } from '@/api/stats'
import { PieChart, Pie, BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import { motion } from 'motion/react'
import { useSocket } from '@/context/SocketContext'
import { toast } from 'sonner'

const COLORS = [
  '#2eaa6e',
  '#34d882',
  '#25875a',
  '#1d6847',
  '#3ee898',
  '#4af4a8',
  '#19503a',
  '#0f3d2b',
]

function StatCard({ icon: Icon, label, value, loading }) {
  return (
    <div className="bg-card border border-border rounded-xl p-4 flex items-center gap-4 hover:border-primary/30 transition-colors">
      <div className="p-3 rounded-lg bg-primary/10 text-primary">
        <Icon size={24} />
      </div>
      <div>
        <p className="text-sm text-muted-foreground">{label}</p>
        {loading ? (
          <Skeleton className="h-7 w-16 mt-1" />
        ) : (
          <p className="text-2xl font-semibold text-foreground">{value?.toLocaleString() ?? '—'}</p>
        )}
      </div>
    </div>
  )
}

function ChartCard({ title, children, loading }) {
  return (
    <div className="bg-card border border-border rounded-xl p-4">
      <h3 className="text-sm font-medium text-foreground mb-4">{title}</h3>
      {loading ? (
        <div className="h-[200px] flex items-center justify-center">
          <Skeleton className="h-32 w-32 rounded-full" />
        </div>
      ) : (
        children
      )}
    </div>
  )
}

function CustomTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const data = payload[0].payload
  return (
    <div className="bg-popover border border-border rounded-lg px-3 py-2 shadow-lg">
      <p className="text-sm font-medium text-foreground">{data.name || data.type || data.topic}</p>
      <p className="text-xs text-muted-foreground">{data.count || data.connections} items</p>
    </div>
  )
}

function LeaderboardItem({ rank, name, type, value, maxValue }) {
  const percentage = maxValue > 0 ? (value / maxValue) * 100 : 0
  return (
    <div className="flex items-center gap-3 py-2">
      <span className="text-xs text-muted-foreground w-5">{rank}</span>
      <div className="flex-1 min-w-0">
        <div className="flex items-center justify-between mb-1">
          <span className="text-sm text-foreground truncate">{name}</span>
          <span className="text-xs text-muted-foreground ml-2">{value}</span>
        </div>
        <div className="h-1.5 bg-muted rounded-full overflow-hidden">
          <div
            className="h-full bg-primary rounded-full transition-all duration-500"
            style={{ width: `${percentage}%` }}
          />
        </div>
      </div>
    </div>
  )
}

export default function DashboardPage() {
  const [stats, setStats] = useState(null)
  const [breakdown, setBreakdown] = useState(null)
  const [loading, setLoading] = useState(true)
  const [breakdownLoading, setBreakdownLoading] = useState(true)
  const [error, setError] = useState(null)
  const [refreshing, setRefreshing] = useState(false)

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

  async function handleRefresh() {
    setRefreshing(true)
    await loadData()
    setRefreshing(false)
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

  useSocket('entities_merged', (data) => {
    toast.success('Entities merged', { 
      description: `${data.data.primary} ← ${data.data.secondary}` 
    })
    debouncedLoad()
  })
  
  useSocket('profiles_refined', (data) => {
     toast.info('Profiles refined', {
       description: `Updated ${data.data.count} entities`
     })
     debouncedLoad()
  })

  const maxConnections = breakdown?.top_connected?.[0]?.connections || 1

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="border-b border-border p-4">
        <div className="max-w-6xl mx-auto flex items-center justify-between">
          <div>
            <h1 className="text-lg font-medium text-foreground">Dashboard</h1>
            <p className="text-sm text-muted-foreground">Your knowledge graph at a glance</p>
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={handleRefresh}
            disabled={refreshing}
            className="gap-2"
          >
            <RefreshCw size={14} className={refreshing ? 'animate-spin' : ''} />
            Refresh
          </Button>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4">
        <div className="max-w-6xl mx-auto space-y-6">
          {error && (
            <div className="p-4 rounded-xl bg-destructive/10 text-destructive text-sm">{error}</div>
          )}

          {/* Stats Grid */}
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4">
            {[
              { icon: Users, label: 'Entities', value: stats?.entities },
              { icon: FileText, label: 'Facts', value: stats?.facts },
              { icon: GitBranch, label: 'Relationships', value: stats?.relationships },
              { icon: MessageSquare, label: 'Sessions', value: stats?.sessions },
              { icon: Bot, label: 'Agents', value: stats?.agents },
              {
                icon: Brain,
                label: 'Graph Nodes',
                value: stats ? stats.entities + stats.facts : null,
              },
            ].map((card, i) => (
              <motion.div
                key={card.label}
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.2, delay: i * 0.06 }}
              >
                <StatCard
                  icon={card.icon}
                  label={card.label}
                  value={card.value}
                  loading={loading}
                />
              </motion.div>
            ))}
          </div>

          {/* Charts Row */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
            {/* Entity Types - Donut Chart */}
            <motion.div
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.2, delay: 0.36 }}
            >
              <ChartCard title="Entities by Type" loading={breakdownLoading}>
                {breakdown?.by_type?.length > 0 ? (
                  <ResponsiveContainer width="100%" height={200}>
                    <PieChart>
                      <Pie
                        data={breakdown.by_type.map((entry, idx) => ({
                          ...entry,
                          fill: COLORS[idx % COLORS.length],
                        }))}
                        dataKey="count"
                        nameKey="type"
                        cx="50%"
                        cy="50%"
                        innerRadius={50}
                        outerRadius={80}
                        paddingAngle={2}
                      />
                      <Tooltip content={<CustomTooltip />} />
                    </PieChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="h-[200px] flex items-center justify-center text-muted-foreground text-sm">
                    No entity data yet
                  </div>
                )}
                {/* Legend */}
                {breakdown?.by_type?.length > 0 && (
                  <div className="flex flex-wrap gap-2 mt-2 justify-center">
                    {breakdown.by_type.slice(0, 5).map((item, idx) => (
                      <div key={item.type} className="flex items-center gap-1.5 text-xs">
                        <div
                          className="w-2 h-2 rounded-full"
                          style={{ backgroundColor: COLORS[idx % COLORS.length] }}
                        />
                        <span className="text-muted-foreground">{item.type}</span>
                      </div>
                    ))}
                  </div>
                )}
              </ChartCard>
            </motion.div>

            {/* Topics - Bar Chart */}
            <motion.div
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.2, delay: 0.44 }}
            >
              <ChartCard title="Entities by Topic" loading={breakdownLoading}>
                {breakdown?.by_topic?.length > 0 ? (
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart
                      data={breakdown.by_topic.slice(0, 6)}
                      layout="vertical"
                      margin={{ left: 0, right: 16 }}
                    >
                      <XAxis type="number" hide />
                      <YAxis
                        type="category"
                        dataKey="topic"
                        width={80}
                        tick={{ fontSize: 12, fill: '#737373' }}
                        axisLine={false}
                        tickLine={false}
                      />
                      <Tooltip content={<CustomTooltip />} />
                      <Bar dataKey="count" fill="#2eaa6e" radius={[0, 4, 4, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="h-[200px] flex items-center justify-center text-muted-foreground text-sm">
                    No topic data yet
                  </div>
                )}
              </ChartCard>
            </motion.div>

            {/* Top Connected - Leaderboard */}
            <motion.div
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.2, delay: 0.52 }}
            >
              <ChartCard title="Most Connected Entities" loading={breakdownLoading}>
                {breakdown?.top_connected?.length > 0 ? (
                  <div className="space-y-1">
                    {breakdown.top_connected.slice(0, 5).map((entity, idx) => (
                      <LeaderboardItem
                        key={entity.name}
                        rank={idx + 1}
                        name={entity.name}
                        type={entity.type}
                        value={entity.connections}
                        maxValue={maxConnections}
                      />
                    ))}
                  </div>
                ) : (
                  <div className="h-[200px] flex items-center justify-center text-muted-foreground text-sm">
                    No connection data yet
                  </div>
                )}
              </ChartCard>
            </motion.div>
          </div>
        </div>
      </div>
    </div>
  )
}
