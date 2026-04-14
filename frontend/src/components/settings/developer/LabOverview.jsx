import { motion } from 'motion/react'
import { Activity, Zap, Brain, Target, ShieldCheck, Microscope } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { cn } from '@/lib/utils'

export default function LabOverview({ settings }) {
  const stats = [
    { 
      label: 'Extraction Precision', 
      value: '94%', 
      detail: 'NLP Pipeline Health',
      icon: Target,
      color: 'text-emerald-500',
      bg: 'bg-emerald-500/10'
    },
    { 
      label: 'Agent Autonomy', 
      value: 'High', 
      detail: `${settings?.limits?.max_tool_calls ?? 10} steps limit`,
      icon: Zap,
      color: 'text-amber-500',
      bg: 'bg-amber-500/10'
    },
    { 
      label: 'Memory Density', 
      value: 'Optimized', 
      detail: 'Entity Resolution Active',
      icon: Brain,
      color: 'text-blue-500',
      bg: 'bg-blue-500/10'
    }
  ]

  return (
    <div className="space-y-8 animate-in fade-in slide-in-from-bottom-4 duration-500">
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {stats.map((stat, i) => {
          const Icon = stat.icon
          return (
            <motion.div
              key={stat.label}
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ delay: i * 0.1 }}
            >
              <Card className="glass-card border-white/5 hover:border-white/10 transition-colors">
                <CardHeader className="flex flex-row items-center justify-between pb-2">
                  <CardTitle className="text-sm font-medium text-muted-foreground">{stat.label}</CardTitle>
                  <div className={cn("p-1.5 rounded-lg", stat.bg, stat.color)}>
                    <Icon size={16} />
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold">{stat.value}</div>
                  <p className="text-[11px] text-muted-foreground mt-1">{stat.detail}</p>
                </CardContent>
              </Card>
            </motion.div>
          )
        })}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card className="glass-card border-white/5 bg-white/[0.01]">
          <CardHeader>
            <div className="flex items-center gap-2">
              <ShieldCheck className="text-emerald-500" size={18} />
              <CardTitle className="text-base">System Integrity</CardTitle>
            </div>
            <CardDescription className="text-xs">
              Current safety guards and operational boundaries.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-1.5">
              <div className="flex justify-between text-xs">
                <span className="text-muted-foreground">Context Stability</span>
                <span className="text-emerald-500 font-mono">98.2%</span>
              </div>
              <div className="h-1 bg-white/5 rounded-full overflow-hidden">
                <div className="h-full bg-emerald-500 w-[98.2%]" />
              </div>
            </div>
            <div className="space-y-1.5">
              <div className="flex justify-between text-xs">
                <span className="text-muted-foreground">Conflict Avoidance</span>
                <span className="text-amber-500 font-mono">82.4%</span>
              </div>
              <div className="h-1 bg-white/5 rounded-full overflow-hidden">
                <div className="h-full bg-amber-500 w-[82.4%]" />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="glass-card border-white/5 bg-primary/5 relative overflow-hidden group">
          <div className="absolute top-0 right-0 p-4 opacity-10 group-hover:scale-125 transition-transform duration-700">
            <Microscope size={80} />
          </div>
          <CardHeader>
            <CardTitle className="text-base">Laboratory Controls</CardTitle>
            <CardDescription className="text-xs">
              Quickly jump to a tuning module to optimize system behavior.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap gap-2">
              {['NLP', 'Agents', 'Memory', 'Search'].map(tag => (
                <div key={tag} className="px-2 py-1 rounded bg-white/10 text-[10px] font-medium text-white/70">
                  {tag}
                </div>
              ))}
            </div>
            <p className="text-[11px] text-muted-foreground mt-6 italic">
              "System performance is currently within optimal range for deep research."
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
