import { useState, useEffect } from 'react'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { Info, X, ChevronsUpDown, Check, Trash2, Plus } from 'lucide-react'
import { cn } from '@/lib/utils'
import { toast } from 'sonner'
import {
  createAgent,
  updateAgent,
  getAgentMemory,
  addAgentMemory,
  deleteAgentMemory,
} from '@/api/agents'
import { motion, AnimatePresence } from 'motion/react'

const CORE_TOOLS = [
  {
    id: 'search_entity',
    label: 'Concept Lookup',
    category: 'Memory',
    description: 'Quick summary of a specific person, place, or idea, and its immediate links.',
  },
  {
    id: 'search_messages',
    label: 'Search Chat History',
    category: 'History',
    description: 'Find exact quotes or keywords from past conversations when memories are unclear.',
  },
  {
    id: 'get_connections',
    label: 'Explore Network',
    category: 'Connections',
    description:
      'See everything a concept is related to, including the exact chat messages that prove it.',
  },
  {
    id: 'get_recent_activity',
    label: 'Activity Timeline',
    category: 'History',
    description: 'See the most recent events or updates involving a specific topic.',
  },
  {
    id: 'find_path',
    label: 'Find Hidden Links',
    category: 'Connections',
    description:
      'Discover how two completely different concepts or people are connected to each other.',
  },
  {
    id: 'get_hierarchy',
    label: 'Explore Categories',
    category: 'Connections',
    description:
      'Find out what broader category a concept belongs to, or what smaller items it contains.',
  },
  {
    id: 'search_files',
    label: 'Search Uploaded Files',
    category: 'Files',
    description: 'Read through your uploaded documents, PDFs, and code files to find answers.',
  },
  {
    id: 'web_search',
    label: 'Search the Web',
    category: 'Web',
    description: 'Look up real-time facts and information from the internet.',
  },
  {
    id: 'news_search',
    label: 'Search Recent News',
    category: 'Web',
    description: 'Find recent articles and current events to stay up to date.',
  },
]

export default function AgentEditorModal({
  isOpen,
  onClose,
  agent,
  defaultPersona,
  defaultInstructions,
  agentModels,
  onSave,
}) {
  const [saving, setSaving] = useState(false)
  const [name, setName] = useState('')
  const [persona, setPersona] = useState('')
  const [instructions, setInstructions] = useState('')
  const [model, setModel] = useState('')
  const [modelOpen, setModelOpen] = useState(false)
  const [temperature, setTemperature] = useState(0.7)
  const [enabledTools, setEnabledTools] = useState(null)

  // Memory fields
  const [memory, setMemory] = useState({ rules: [], preferences: [], icks: [] })
  const [newMemoryContent, setNewMemoryContent] = useState('')
  const [memoryCategory, setMemoryCategory] = useState('rules')
  const [memoryLoading, setMemoryLoading] = useState(false)

  useEffect(() => {
    if (!isOpen) return

    if (agent) {
      setName(agent.name)
      setPersona(agent.persona)
      setInstructions(agent.instructions || '')
      setModel(agent.model || '')
      setTemperature(agent.temperature ?? 0.7)
      setEnabledTools(agent.enabled_tools || null)

      setMemoryLoading(true)
      getAgentMemory(agent.id)
        .then(res => {
          const memObj = { rules: [], preferences: [], icks: [] }
          if (res && res.data) {
            ;['rules', 'preferences', 'icks'].forEach(cat => {
              if (res.data[cat]) {
                memObj[cat] = Object.entries(res.data[cat])
                  .map(([id, val]) => ({ id, ...val }))
                  .sort((a, b) => new Date(a.created_at) - new Date(b.created_at))
              }
            })
          }
          setMemory(memObj)
        })
        .catch(err => console.error('Failed to fetch memory', err))
        .finally(() => setMemoryLoading(false))
    } else {
      setName('')
      setPersona('')
      setInstructions('')
      setModel('')
      setTemperature(0.7)
      setEnabledTools(null)
      setMemory({ rules: [], preferences: [], icks: [] })
      setMemoryCategory('rules')
      setNewMemoryContent('')
    }
  }, [isOpen, agent])

  async function handleSave() {
    if (!name.trim() || !persona.trim()) {
      toast.error('Name and persona are required')
      return
    }

    setSaving(true)
    try {
      if (agent) {
        await updateAgent(agent.id, {
          name: name.trim(),
          persona: persona.trim(),
          instructions: instructions.trim() || null,
          model: model.trim() || null,
          temperature: parseFloat(temperature),
          enabled_tools: enabledTools,
        })
        toast.success('Agent updated')
      } else {
        await createAgent({
          name: name.trim(),
          persona: persona.trim(),
          instructions: instructions.trim() || null,
          model: model.trim() || null,
          temperature: parseFloat(temperature),
          enabled_tools: enabledTools,
        })
        toast.success('Agent created')
      }
      onSave()
      onClose()
    } catch (err) {
      toast.error(err.message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <Dialog open={isOpen} onOpenChange={open => !open && onClose()}>
      <DialogContent className="bg-card/95 backdrop-blur-xl border-white/[0.08] sm:max-w-2xl shadow-2xl shadow-black/40">
        <DialogHeader>
          <DialogTitle className="text-base tracking-tight">
            {agent ? 'Edit Agent' : 'New Agent'}
          </DialogTitle>
          <DialogDescription>
            {agent
              ? 'Update the agent configuration'
              : 'Create a new AI assistant with a custom personality'}
          </DialogDescription>
        </DialogHeader>

        <Tabs defaultValue="settings" className="w-full flex-1 flex flex-col min-h-0">
          <TabsList className="flex bg-transparent border-b border-white/[0.05] rounded-none px-6 h-12 p-0 gap-6 shrink-0 items-center justify-start">
            <TabsTrigger
              value="settings"
              className="data-[state=active]:bg-transparent data-[state=active]:text-primary px-0 h-full rounded-none border-b-2 border-transparent data-[state=active]:border-primary transition-all text-[11px] font-bold uppercase tracking-wider"
            >
              Settings
            </TabsTrigger>
            <TabsTrigger
              value="tools"
              className="data-[state=active]:bg-transparent data-[state=active]:text-primary px-0 h-full rounded-none border-b-2 border-transparent data-[state=active]:border-primary transition-all text-[11px] font-bold uppercase tracking-wider"
            >
              Tools
            </TabsTrigger>
            <TabsTrigger
              value="memory"
              className="data-[state=active]:bg-transparent data-[state=active]:text-primary px-0 h-full rounded-none border-b-2 border-transparent data-[state=active]:border-primary transition-all text-[11px] font-bold uppercase tracking-wider"
            >
              Working Memory
            </TabsTrigger>
          </TabsList>

          <TabsContent value="settings" className="space-y-4 mt-0">
            <div className="space-y-2">
              <Label
                htmlFor="name"
                className="text-muted-foreground text-[11px] uppercase tracking-wider font-semibold"
              >
                Name
              </Label>
              <Input
                id="name"
                value={name}
                onChange={e => setName(e.target.value)}
                placeholder="Agent"
                className="bg-muted/50 border-white/[0.06] rounded-xl focus:border-primary/50"
              />
            </div>

            <div className="space-y-4">
              <Label
                htmlFor="persona"
                className="text-muted-foreground text-[11px] uppercase tracking-wider font-semibold"
              >
                Persona
              </Label>
              <textarea
                id="persona"
                value={persona}
                onChange={e => setPersona(e.target.value)}
                placeholder={defaultPersona}
                rows={3}
                className="w-full bg-muted/50 border border-white/[0.06] rounded-xl px-3 py-2 text-sm text-foreground placeholder-muted-foreground/50 resize-none focus:outline-none focus:border-primary/50 focus:ring-1 focus:ring-primary/20 transition-colors"
              />
            </div>

            <div className="space-y-2">
              <div className="flex justify-between items-center">
                <Label
                  htmlFor="instructions"
                  className="text-muted-foreground text-[11px] uppercase tracking-wider font-semibold"
                >
                  Agent Instructions
                </Label>
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <button
                        type="button"
                        className="text-muted-foreground/40 hover:text-muted-foreground transition-colors outline-none"
                      >
                        <Info size={12} />
                      </button>
                    </TooltipTrigger>
                    <TooltipContent
                      side="top"
                      align="center"
                      className="w-[280px] p-4 text-[11px] leading-relaxed glass-card border-white/[0.08] backdrop-blur-xl shadow-2xl z-[100] space-y-3"
                    >
                      <p className="text-muted-foreground/90 font-medium">
                        Custom instructions and general guidance for your agent to follow.
                      </p>
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              </div>

              <textarea
                id="instructions"
                value={instructions}
                onChange={e => setInstructions(e.target.value)}
                placeholder={defaultInstructions || 'Add specific agent instructions...'}
                rows={4}
                className="w-full bg-muted/50 border border-white/[0.06] rounded-xl px-3 py-2 text-xs font-mono text-foreground placeholder-muted-foreground/50 resize-y focus:outline-none focus:border-primary/50 focus:ring-1 focus:ring-primary/20 transition-colors"
              />
            </div>

            <div className="space-y-6">
              <div className="space-y-2">
                <Label
                  htmlFor="model"
                  className="text-muted-foreground text-[11px] uppercase tracking-wider font-semibold"
                >
                  Model
                </Label>
                <Popover open={modelOpen} onOpenChange={setModelOpen}>
                  <PopoverTrigger asChild>
                    <button
                      type="button"
                      className={cn(
                        'flex w-full items-center justify-between rounded-xl bg-muted/50 border border-white/[0.06] px-3 py-2 text-sm transition-colors hover:border-white/[0.12] focus:outline-none focus:border-primary/50 focus:ring-1 focus:ring-primary/20',
                        !model && 'text-muted-foreground/50'
                      )}
                    >
                      <span className="truncate">
                        {model
                          ? agentModels.find(m => m.id === model)?.name || model.split('/').pop()
                          : 'Default'}
                      </span>
                      <div className="flex items-center gap-1 shrink-0 ml-1">
                        {model && (
                          <span
                            role="button"
                            className="text-muted-foreground/50 hover:text-foreground p-0.5 rounded transition-colors"
                            onClick={e => {
                              e.stopPropagation()
                              setModel('')
                            }}
                          >
                            <X size={12} />
                          </span>
                        )}
                        <ChevronsUpDown size={12} className="text-muted-foreground/50" />
                      </div>
                    </button>
                  </PopoverTrigger>
                  <PopoverContent
                    className="w-[var(--radix-popover-trigger-width)] p-0 bg-card/95 backdrop-blur-xl border-white/[0.08] shadow-2xl"
                    align="start"
                  >
                    <Command className="bg-transparent">
                      <CommandInput placeholder="Search models..." className="text-xs" />
                      <CommandList className="max-h-[200px]">
                        <CommandEmpty className="py-4 text-center text-xs text-muted-foreground">
                          No models found
                        </CommandEmpty>
                        <CommandGroup>
                          <CommandItem
                            value="__default__"
                            onSelect={() => {
                              setModel('')
                              setModelOpen(false)
                            }}
                            className="text-xs"
                          >
                            <Check
                              size={12}
                              className={cn('mr-2 shrink-0', !model ? 'opacity-100' : 'opacity-0')}
                            />
                            <span className="text-muted-foreground">Use global default</span>
                          </CommandItem>
                          {agentModels.map(m => (
                            <CommandItem
                              key={m.id}
                              value={m.name || m.id}
                              onSelect={() => {
                                setModel(m.id)
                                setModelOpen(false)
                              }}
                              className="text-xs"
                            >
                              <Check
                                size={12}
                                className={cn(
                                  'mr-2 shrink-0',
                                  model === m.id ? 'opacity-100' : 'opacity-0'
                                )}
                              />
                              <span className="truncate">{m.name || m.id}</span>
                            </CommandItem>
                          ))}
                        </CommandGroup>
                      </CommandList>
                    </Command>
                  </PopoverContent>
                </Popover>
              </div>

              <div className="space-y-4">
                <div className="flex justify-between items-center">
                  <div className="flex items-center gap-1.5">
                    <Label className="text-muted-foreground text-[11px] uppercase tracking-wider font-semibold">
                      Creativity
                    </Label>
                    <TooltipProvider>
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <button
                            type="button"
                            className="text-muted-foreground/40 hover:text-muted-foreground transition-colors outline-none"
                          >
                            <Info size={12} />
                          </button>
                        </TooltipTrigger>
                        <TooltipContent
                          side="top"
                          align="center"
                          className="w-[240px] p-3 text-[10px] leading-relaxed glass-card border-white/[0.08] backdrop-blur-xl shadow-2xl z-[100]"
                        >
                          <p className="text-muted-foreground/90">
                            Lower values (<span className="text-primary/80 font-bold">Precise</span>
                            ) are best for factual retrieval and complex logic. Higher values (
                            <span className="text-primary/80 font-bold">Creative</span>) allow for
                            more abstract reasoning and personality.
                          </p>
                        </TooltipContent>
                      </Tooltip>
                    </TooltipProvider>
                  </div>
                  <span className="text-[10px] text-muted-foreground tabular-nums font-bold">
                    {temperature.toFixed(2)}
                  </span>
                </div>

                <div className="space-y-2 px-1">
                  <input
                    type="range"
                    min="0"
                    max="1"
                    step="0.05"
                    value={temperature}
                    onChange={e => setTemperature(parseFloat(e.target.value))}
                    className="w-full accent-primary h-1 bg-white/[0.05] rounded-full appearance-none [&::-webkit-slider-thumb]:appearance-none [&::-webkit-slider-thumb]:w-3.5 [&::-webkit-slider-thumb]:h-3.5 [&::-webkit-slider-thumb]:bg-primary [&::-webkit-slider-thumb]:border-2 [&::-webkit-slider-thumb]:border-background [&::-webkit-slider-thumb]:rounded-full cursor-pointer hover:[&::-webkit-slider-thumb]:scale-110 transition-all"
                  />

                  <div className="flex justify-between items-center text-[9px] font-bold uppercase tracking-widest text-muted-foreground/30">
                    <span>Precise</span>
                    <span>Creative</span>
                  </div>
                </div>
              </div>
            </div>
          </TabsContent>

          <TabsContent
            value="tools"
            className="space-y-4 p-6 mt-0 overflow-y-auto min-h-0 custom-scrollbar"
          >
            <div className="space-y-6">
              {['Memory', 'Connections', 'History', 'Files', 'Web'].map(category => {
                const categoryTools = CORE_TOOLS.filter(t => t.category === category)
                if (categoryTools.length === 0) return null

                const activeCount = categoryTools.filter(
                  t => enabledTools === null || enabledTools.includes(t.id)
                ).length

                return (
                  <div key={category} className="space-y-3">
                    <div className="flex items-center justify-between px-1">
                      <span className="text-[10px] text-muted-foreground/50 uppercase tracking-widest font-bold">
                        {category}
                      </span>
                      <span className="text-[10px] text-muted-foreground/30 font-medium">
                        {activeCount}/{categoryTools.length}
                      </span>
                    </div>

                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                      {categoryTools.map(tool => {
                        const isEnabled = enabledTools === null || enabledTools.includes(tool.id)
                        return (
                          <label
                            key={tool.id}
                            className="flex items-start justify-between gap-4 p-3 rounded-xl border border-white/[0.04] bg-white/[0.01] hover:bg-white/[0.03] cursor-pointer transition-all group"
                          >
                            <div className="flex flex-col gap-1">
                              <span
                                className={cn(
                                  'text-[11px] font-semibold leading-none transition-colors',
                                  isEnabled ? 'text-foreground' : 'text-muted-foreground/50'
                                )}
                              >
                                {tool.label}
                              </span>
                              <p className="text-[10px] text-muted-foreground/60 leading-relaxed max-w-[400px]">
                                {tool.description}
                              </p>
                            </div>
                            <Switch
                              checked={isEnabled}
                              className="scale-75 shrink-0 mt-0.5"
                              onCheckedChange={checked => {
                                if (enabledTools === null) {
                                  if (!checked)
                                    setEnabledTools(
                                      CORE_TOOLS.map(t => t.id).filter(id => id !== tool.id)
                                    )
                                } else {
                                  if (checked) {
                                    setEnabledTools([...enabledTools, tool.id])
                                  } else {
                                    setEnabledTools(enabledTools.filter(id => id !== tool.id))
                                  }
                                }
                              }}
                            />
                          </label>
                        )
                      })}
                    </div>
                  </div>
                )
              })}
            </div>
          </TabsContent>

          <TabsContent value="memory" className="space-y-4 mt-0">
            <div className="flex gap-2">
              <Button
                type="button"
                variant={memoryCategory === 'rules' ? 'secondary' : 'ghost'}
                size="sm"
                onClick={() => setMemoryCategory('rules')}
                className={cn(
                  'h-7 px-3 text-xs',
                  memoryCategory !== 'rules' && 'opacity-50 hover:opacity-100'
                )}
              >
                Rules
              </Button>
              <Button
                type="button"
                variant={memoryCategory === 'preferences' ? 'secondary' : 'ghost'}
                size="sm"
                onClick={() => setMemoryCategory('preferences')}
                className={cn(
                  'h-7 px-3 text-xs',
                  memoryCategory !== 'preferences' && 'opacity-50 hover:opacity-100'
                )}
              >
                Preferences
              </Button>
              <Button
                type="button"
                variant={memoryCategory === 'icks' ? 'secondary' : 'ghost'}
                size="sm"
                onClick={() => setMemoryCategory('icks')}
                className={cn(
                  'h-7 px-3 text-xs',
                  memoryCategory !== 'icks' && 'opacity-50 hover:opacity-100'
                )}
              >
                Icks
              </Button>
            </div>

            <div className="bg-black/20 rounded-xl border border-white/[0.04] p-3 min-h-[160px] max-h-[240px] overflow-y-auto">
              {memoryLoading ? (
                <div className="flex justify-center py-8">
                  <span className="text-xs text-muted-foreground animate-pulse">
                    Loading memory...
                  </span>
                </div>
              ) : memory[memoryCategory]?.length === 0 ? (
                <div className="flex justify-center py-8">
                  <span className="text-xs text-muted-foreground/50">
                    No {memoryCategory} found
                  </span>
                </div>
              ) : (
                <ul className="space-y-1.5">
                  <AnimatePresence>
                    {memory[memoryCategory]?.map(item => (
                      <motion.li
                        key={item.id}
                        layout
                        initial={{ opacity: 0, height: 0, scale: 0.95 }}
                        animate={{ opacity: 1, height: 'auto', scale: 1 }}
                        exit={{
                          opacity: 0,
                          height: 0,
                          scale: 0.95,
                          transition: { duration: 0.15 },
                        }}
                        transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
                        className="group flex gap-2 items-start text-sm bg-white/[0.03] p-2 rounded-lg overflow-hidden"
                      >
                        <span className="flex-1 leading-snug">{item.content}</span>
                        <button
                          className="opacity-0 group-hover:opacity-100 p-1 text-muted-foreground hover:text-destructive transition-all"
                          onClick={async () => {
                            try {
                              await deleteAgentMemory(agent?.id, memoryCategory, item.id)
                              setMemory(prev => ({
                                ...prev,
                                [memoryCategory]: prev[memoryCategory].filter(
                                  m => m.id !== item.id
                                ),
                              }))
                            } catch {
                              toast.error('Failed to delete memory')
                            }
                          }}
                        >
                          <Trash2 size={12} />
                        </button>
                      </motion.li>
                    ))}
                  </AnimatePresence>
                </ul>
              )}
            </div>

            <div className="flex gap-2 relative">
              <Input
                value={newMemoryContent}
                onChange={e => setNewMemoryContent(e.target.value)}
                placeholder={`Add new ${memoryCategory.replace(/s$/, '')}...`}
                className="bg-muted/50 border-white/[0.06] rounded-xl text-xs h-9"
                onKeyDown={async e => {
                  if (e.key === 'Enter' && newMemoryContent.trim()) {
                    e.preventDefault()
                    try {
                      const res = await addAgentMemory(
                        agent?.id,
                        memoryCategory,
                        newMemoryContent.trim()
                      )
                      setMemory(prev => ({
                        ...prev,
                        [memoryCategory]: [
                          ...prev[memoryCategory],
                          {
                            id: res.id,
                            content: res.content,
                            created_at: new Date().toISOString(),
                          },
                        ],
                      }))
                      setNewMemoryContent('')
                    } catch {
                      toast.error('Failed to save memory')
                    }
                  }
                }}
              />
              <Button
                size="sm"
                className="rounded-xl h-9 px-3"
                disabled={!newMemoryContent.trim()}
                onClick={async () => {
                  try {
                    const res = await addAgentMemory(
                      agent?.id,
                      memoryCategory,
                      newMemoryContent.trim()
                    )
                    setMemory(prev => ({
                      ...prev,
                      [memoryCategory]: [
                        ...prev[memoryCategory],
                        {
                          id: res.id,
                          content: res.content,
                          created_at: new Date().toISOString(),
                        },
                      ],
                    }))
                    setNewMemoryContent('')
                  } catch {
                    toast.error('Failed to save memory')
                  }
                }}
              >
                <Plus size={14} />
              </Button>
            </div>
          </TabsContent>
        </Tabs>

        <div className="flex justify-end gap-2 mt-4">
          <Button variant="ghost" onClick={onClose} className="text-muted-foreground">
            Cancel
          </Button>
          <Button
            onClick={handleSave}
            disabled={saving}
            className="rounded-xl shadow-lg shadow-primary/10"
          >
            {saving ? 'Saving...' : agent ? 'Save Changes' : 'Create Agent'}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  )
}
