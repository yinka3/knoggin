import { useState } from 'react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog'
import { Switch } from '@/components/ui/switch'
import { useTools } from '@/context/ToolsContext'
import { cn } from '@/lib/utils'
import { AlertCircle } from 'lucide-react'

function ParameterRow({ name, config, required }) {
  return (
    <div className="flex items-start justify-between gap-3 py-2 border-b border-border/20 last:border-0">
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-xs font-mono text-foreground">{name}</span>
          <span className="text-[10px] text-muted-foreground/50 font-mono">{config.type}</span>
          {required && (
            <span className="text-[9px] text-primary/70 uppercase tracking-wider">required</span>
          )}
        </div>
        {config.description && (
          <p className="text-[11px] text-muted-foreground/60 mt-0.5 leading-snug">
            {config.description}
          </p>
        )}
        {config.enum && (
          <div className="flex gap-1 mt-1">
            {config.enum.map(v => (
              <span
                key={v}
                className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-white/[0.03] border border-white/[0.06] text-muted-foreground"
              >
                {v}
              </span>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function ToolDetail({ tool }) {
  if (!tool) {
    return (
      <div className="flex-1 flex items-center justify-center text-sm text-muted-foreground/40">
        Select a tool to view details
      </div>
    )
  }

  const params = tool.parameters?.properties || {}
  const required = new Set(tool.parameters?.required || [])
  const paramNames = Object.keys(params)

  return (
    <div className="flex-1 overflow-y-auto p-5">
      {/* Header */}
      <div className="mb-4">
        <h3 className="text-sm font-semibold text-foreground">{tool.name}</h3>
        <span className="text-[10px] font-mono text-muted-foreground/50">{tool.id}</span>
      </div>

      {/* Description */}
      {tool.description && (
        <div className="mb-5">
          <p className="text-xs text-muted-foreground/50 mb-1">Description</p>
          <p className="text-xs text-muted-foreground leading-relaxed">{tool.description}</p>
        </div>
      )}

      {/* Parameters */}
      {paramNames.length > 0 && (
        <div>
          <p className="text-xs text-muted-foreground/50 mb-2">Parameters ({paramNames.length})</p>
          <div className="rounded-lg border border-border/30 bg-white/[0.01] overflow-hidden">
            <div className="px-3">
              {paramNames.map(name => (
                <ParameterRow
                  key={name}
                  name={name}
                  config={params[name]}
                  required={required.has(name)}
                />
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Source */}
      <div className="mt-5 flex items-center gap-2">
        <p className="text-[10px] text-muted-foreground/30">
          Source: {tool.source} · Group: {tool.group}
        </p>
      </div>
    </div>
  )
}

export default function ToolsDrawer({ open, onOpenChange }) {
  const { availableTools, enabledTools, toggleTool, loading } = useTools()
  const [selectedToolId, setSelectedToolId] = useState(null)

  const effectiveEnabled = enabledTools?.length ? enabledTools : availableTools.map(t => t.id)

  // Group tools
  const groups = availableTools.reduce((acc, tool) => {
    const key = tool.group || 'Other'
    if (!acc[key]) acc[key] = []
    acc[key].push(tool)
    return acc
  }, {})

  const sortedGroups = Object.keys(groups).sort((a, b) => {
    const priority = { Memory: 1, Graph: 2, History: 3, RAG: 4 }
    return (priority[a] || 99) - (priority[b] || 99)
  })

  const selectedTool = availableTools.find(t => t.id === selectedToolId) || null

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-3xl w-full max-h-[80vh] p-0 bg-background/95 backdrop-blur-xl border-border/60 overflow-hidden">
        <div className="flex flex-col h-full max-h-[80vh]">
          {/* Header */}
          <div className="px-5 pt-5 pb-3 border-b border-border/30">
            <DialogHeader>
              <DialogTitle className="text-base font-semibold">Tools</DialogTitle>
              <DialogDescription className="text-xs text-muted-foreground">
                Manage agent capabilities
              </DialogDescription>
            </DialogHeader>
          </div>

          {loading ? (
            <div className="flex-1 flex items-center justify-center text-sm text-muted-foreground">
              Loading tools...
            </div>
          ) : (
            <div className="flex flex-1 overflow-hidden">
              {/* Left: Tool List */}
              <div className="w-64 shrink-0 border-r border-border/30 overflow-y-auto">
                {sortedGroups.map(groupName => {
                  const tools = groups[groupName]
                  const activeCount = tools.filter(t => effectiveEnabled.includes(t.id)).length

                  return (
                    <div key={groupName}>
                      {/* Group header */}
                      <div className="flex items-center justify-between px-4 py-2 bg-white/[0.01]">
                        <span className="text-[10px] text-muted-foreground/50 uppercase tracking-wider font-medium">
                          {groupName}
                        </span>
                        <span className="text-[10px] text-muted-foreground/30">
                          {activeCount}/{tools.length}
                        </span>
                      </div>

                      {/* Tool rows */}
                      {tools.map(tool => {
                        const isEnabled = effectiveEnabled.includes(tool.id)
                        const isSelected = selectedToolId === tool.id

                        return (
                          <div
                            key={tool.id}
                            className={cn(
                              'flex items-center justify-between gap-2 px-4 py-2 cursor-pointer transition-colors',
                              isSelected
                                ? 'bg-white/[0.05] border-l-2 border-primary'
                                : 'border-l-2 border-transparent hover:bg-white/[0.02]'
                            )}
                            onClick={() => setSelectedToolId(tool.id)}
                          >
                            <span
                              className={cn(
                                'text-xs truncate',
                                isEnabled ? 'text-foreground' : 'text-muted-foreground/50'
                              )}
                            >
                              {tool.name}
                            </span>
                            <Switch
                              checked={isEnabled}
                              onCheckedChange={() => toggleTool(tool.id)}
                              onClick={e => e.stopPropagation()}
                              className="scale-75 shrink-0"
                            />
                          </div>
                        )
                      })}
                    </div>
                  )
                })}
              </div>

              {/* Right: Detail Panel */}
              <ToolDetail tool={selectedTool} />
            </div>
          )}

          {/* Footer */}
          <div className="px-4 py-2.5 border-t border-border/30 bg-white/[0.01]">
            <div className="flex items-center gap-2 text-[11px] text-muted-foreground/40">
              <AlertCircle size={12} className="shrink-0" />
              <p>Changes apply immediately to the active session.</p>
            </div>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  )
}
