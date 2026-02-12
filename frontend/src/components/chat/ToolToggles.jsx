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
      <PopoverContent align="start" className="w-52 p-2">
        <div className="flex items-center justify-between pb-1.5 mb-1.5 border-b border-border">
          <span className="text-xs font-medium text-foreground">Tools</span>
          <button
            onClick={handleToggleAll}
            className="text-[10px] text-muted-foreground hover:text-foreground transition-colors"
          >
            {allEnabled ? 'Disable All' : 'Enable All'}
          </button>
        </div>
        <div className="space-y-0.5">
          {tools.map(tool => (
            <div
              key={tool.id}
              className="flex items-center justify-between gap-2 py-1 px-1 rounded hover:bg-muted/30 transition-colors"
            >
              <span className="text-xs text-foreground">{tool.name || tool.id}</span>
              <Switch
                checked={enabledSet.has(tool.id)}
                onCheckedChange={() => handleToggle(tool.id)}
                className="scale-75"
              />
            </div>
          ))}
        </div>
      </PopoverContent>
    </Popover>
  )
}
