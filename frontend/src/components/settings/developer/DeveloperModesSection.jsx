import { useState, useEffect } from 'react'
import { motion, AnimatePresence } from 'motion/react'
import { cn } from '@/lib/utils'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { toast } from 'sonner'
import { CheckCircle2, Zap, Brain, MonitorPlay } from 'lucide-react'
import { getDeveloperModes } from '@/api/config'

const MODE_ICONS = {
  default: MonitorPlay,
  speed: Zap,
  deep: Brain
}

export default function DeveloperModesSection({ setSettings }) {
  const [modes, setModes] = useState([])
  const [loading, setLoading] = useState(true)
  const [activeModeId, setActiveModeId] = useState(null)

  useEffect(() => {
    getDeveloperModes()
      .then((res) => {
        if (res?.modes) {
          setModes(res.modes)
        }
      })
      .catch((err) => {
        console.error('Failed to load developer modes:', err)
        toast.error('Could not load curated modes.')
      })
      .finally(() => {
        setLoading(false)
      })
  }, [])

  function applyMode(mode) {
    // Deep merge the mode settings into the current settings
    // This allows applying curations while preserving unrelated configs (if any)
    setSettings((prev) => {
      const next = JSON.parse(JSON.stringify(prev))
      Object.keys(mode.settings).forEach((section) => {
        next[section] = JSON.parse(JSON.stringify(mode.settings[section]))
      })
      return next
    })
    setActiveModeId(mode.id)
    toast.success(`Applied ${mode.name} mode`, {
      description: 'Review other tabs to see changes, then click Save.'
    })
  }

  if (loading) {
    return (
      <div className="space-y-6 max-w-3xl">
        <div className="h-8 w-64 bg-muted animate-pulse rounded" />
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {[1, 2, 3].map((i) => (
            <div key={i} className="h-32 bg-muted animate-pulse rounded-xl" />
          ))}
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6 max-w-4xl pb-12 overflow-y-auto pr-4 custom-scrollbar">
      <div>
        <h2 className="text-xl font-semibold text-foreground tracking-tight">Curated Modes</h2>
        <p className="text-sm text-muted-foreground mt-1 text-balance">
          Apply a pre-configured set of developer settings to optimize Knoggin for specific use cases.
          Selecting a mode will overwrite your current unsaved settings. Note: changes are not applied
          until you click "Save Changes" at the top.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        <AnimatePresence>
          {modes.map((mode, idx) => {
            const Icon = MODE_ICONS[mode.id] || MonitorPlay
            const isActive = activeModeId === mode.id

            return (
              <motion.div
                key={mode.id}
                initial={{ opacity: 0, scale: 0.95 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: idx * 0.05 }}
              >
                <Card
                  onClick={() => applyMode(mode)}
                  className={cn(
                    'cursor-pointer transition-all duration-300 h-full relative overflow-hidden group border-2',
                    isActive
                      ? 'border-primary bg-primary/5 shadow-md'
                      : 'border-border/50 hover:border-primary/50 hover:bg-card/80 shadow-sm'
                  )}
                >
                  {isActive && (
                    <div className="absolute top-4 right-4 text-primary">
                      <CheckCircle2 size={24} className="animate-in zoom-in duration-300" />
                    </div>
                  )}
                  <CardHeader className="pb-3">
                    <div className="p-2 w-fit rounded-lg bg-primary/10 text-primary mb-2 group-hover:scale-110 transition-transform">
                      <Icon size={24} />
                    </div>
                    <CardTitle className="text-lg">{mode.name}</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <CardDescription className="text-sm leading-relaxed text-balance">
                      {mode.description}
                    </CardDescription>
                  </CardContent>
                </Card>
              </motion.div>
            )
          })}
        </AnimatePresence>
      </div>
    </div>
  )
}
