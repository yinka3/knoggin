import { useState } from 'react'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Switch } from '@/components/ui/switch'
import { Wrench } from 'lucide-react'
import { useTools } from '@/context/ToolsContext'

export default function ToolToggles({ enabledTools, onToggle, disabled }) {
  const [open, setOpen] = useState(false)
  const { tools } = useTools()

  const allEnabled = !enabledTools || enabledTools.length === tools.length
  const enabledSet = new Set(enabledTools || tools.map(t => t.id))
  const enabledCount = enabledSet.size

  function handleToggle(toolId) {
    const current = enabledTools || tools.map(t => t.id)
    const next = current.includes(toolId)
      ? current.filter(id => id !== toolId)
      : [...current, toolId]
    onToggle(next)
  }

  function handleToggleAll() {
    onToggle(allEnabled ? [] : tools.map(t => t.id))
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          className="relative flex items-center justify-center h-6 w-6 rounded-md text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors disabled:opacity-40"
          disabled={disabled}
        >
          <Wrench size={14} />
          {!allEnabled && (
            <span className="absolute -top-1 -right-1 bg-primary text-primary-foreground text-[8px] rounded-full h-3.5 w-3.5 flex items-center justify-center font-medium">
              {enabledCount}
            </span>
          )}
        </button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-[280px] p-3 rounded-xl shadow-lg border-border/50 bg-card/95 backdrop-blur-md">
        <div className="flex items-center justify-between pb-3 mb-2 border-b border-border/40">
          <span className="text-sm font-semibold text-foreground tracking-tight flex items-center gap-2">
            <Wrench size={14} className="text-primary" />
            Active Tools
          </span>
          <button
            onClick={handleToggleAll}
            className="text-[11px] font-medium text-muted-foreground hover:text-primary transition-colors px-2 py-1 rounded-md hover:bg-primary/10"
          >
            {allEnabled ? 'Disable All' : 'Enable All'}
          </button>
        </div>
        <div className="space-y-1">
          {tools.map(tool => (
            <div
              key={tool.id}
              className="flex items-center justify-between gap-3 p-2.5 rounded-lg hover:bg-muted/50 transition-colors group cursor-pointer"
              onClick={() => handleToggle(tool.id)}
            >
              <div className="flex flex-col">
                <span className="text-xs font-medium text-foreground group-hover:text-primary transition-colors">
                  {tool.name || tool.id}
                </span>
              </div>
              <Switch
                checked={enabledSet.has(tool.id)}
                onCheckedChange={() => handleToggle(tool.id)}
                className="data-[state=unchecked]:bg-muted-foreground/30 data-[state=checked]:bg-primary"
              />
            </div>
          ))}
        </div>
      </PopoverContent>
    </Popover>
  )
}
