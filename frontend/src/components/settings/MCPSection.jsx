import { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { toast } from 'sonner'
import {
  getMCPPresets,
  getMCPServers,
  addMCPServer,
  removeMCPServer,
  toggleMCPServer,
} from '@/api/config'
import {
  Plus,
  X,
  Search,
  Plug,
  Unplug,
  Trash2,
  Terminal,
  ExternalLink,
  Loader2,
  Copy,
  Check,
  Server,
  Activity,
} from 'lucide-react'

export default function MCPSection() {
  const [mcpServers, setMcpServers] = useState([])
  const [mcpPresets, setMcpPresets] = useState([])
  const [mcpLoading, setMcpLoading] = useState(false)
  const [addingServer, setAddingServer] = useState(false)
  const [presetSearch, setPresetSearch] = useState('')
  const [selectedPreset, setSelectedPreset] = useState(null)
  const [presetEnv, setPresetEnv] = useState({})
  const [customMode, setCustomMode] = useState(false)
  const [customServer, setCustomServer] = useState({ name: '', command: 'uvx', args: '' })
  const [togglingServer, setTogglingServer] = useState(null)

  useEffect(() => {
    async function load() {
      try {
        const [serversData, presetsData] = await Promise.all([getMCPServers(), getMCPPresets()])
        setMcpServers(serversData.servers || [])
        setMcpPresets(presetsData.presets || [])
      } catch (err) {
        console.error('Failed to load MCP data:', err)
      }
    }
    load()
  }, [])

  async function handleAddPreset() {
    if (!selectedPreset) return
    setMcpLoading(true)
    try {
      const env = Object.keys(presetEnv).length > 0 ? presetEnv : undefined
      await addMCPServer({
        name: selectedPreset.id,
        command: selectedPreset.command,
        args: selectedPreset.args,
        env,
        enabled: true,
      })
      toast.success(`${selectedPreset.name} connected`)
      setSelectedPreset(null)
      setPresetEnv({})
      setAddingServer(false)
      const data = await getMCPServers()
      setMcpServers(data.servers || [])
    } catch (err) {
      toast.error(err.message)
    } finally {
      setMcpLoading(false)
    }
  }

  async function handleAddCustom() {
    const { name, command, args } = customServer
    if (!name.trim()) return
    setMcpLoading(true)
    try {
      await addMCPServer({
        name: name.trim(),
        command: command || 'uvx',
        args: args.split(' ').filter(Boolean),
        enabled: true,
      })
      toast.success(`Server '${name}' connected`)
      setCustomServer({ name: '', command: 'uvx', args: '' })
      setCustomMode(false)
      setAddingServer(false)
      const data = await getMCPServers()
      setMcpServers(data.servers || [])
    } catch (err) {
      toast.error(err.message)
    } finally {
      setMcpLoading(false)
    }
  }

  async function handleRemoveServer(name) {
    try {
      await removeMCPServer(name)
      toast.success(`Server '${name}' removed`)
      setMcpServers(prev => prev.filter(s => s.name !== name))
    } catch (err) {
      toast.error(err.message)
    }
  }

  async function handleToggleServer(name) {
    setTogglingServer(name)
    try {
      const result = await toggleMCPServer(name)
      setMcpServers(prev => prev.map(s => (s.name === name ? { ...s, ...result } : s)))
      toast.success(result.enabled ? `Server '${name}' enabled` : `Server '${name}' disabled`)
    } catch (err) {
      toast.error(err.message)
    } finally {
      setTogglingServer(null)
    }
  }

  const filteredPresets = mcpPresets.filter(p => {
    if (!presetSearch) return true
    const q = presetSearch.toLowerCase()
    return (
      p.name.toLowerCase().includes(q) ||
      p.description.toLowerCase().includes(q) ||
      p.tags.some(t => t.includes(q))
    )
  })

  // State for copy buttons
  const [copiedClaude, setCopiedClaude] = useState(false)
  const [copiedCursor, setCopiedCursor] = useState(false)

  const handleCopyClaude = () => {
    const config = `{
  "mcpServers": {
    "knoggin": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/sse-client", "http://localhost:8000/mcp"]
    }
  }
}`
    navigator.clipboard.writeText(config)
    setCopiedClaude(true)
    setTimeout(() => setCopiedClaude(false), 2000)
    toast.success('Claude config copied to clipboard')
  }

  const handleCopyCursor = () => {
    // For cursor, they support pasting a URL directly or we can provide instructions
    const config = `http://localhost:8000/mcp`
    navigator.clipboard.writeText(config)
    setCopiedCursor(true)
    setTimeout(() => setCopiedCursor(false), 2000)
    toast.success('Cursor URL copied to clipboard')
  }

  return (
    <div className="space-y-4">
      {/* Knoggin MCP Server Info Card */}
      <div className="bg-gradient-to-br from-primary/5 to-primary/10 rounded-xl border border-primary/20 overflow-hidden">
        <div className="px-4 py-3 border-b border-primary/10 flex items-center justify-between bg-primary/5">
          <div className="flex items-center gap-2">
            <Server size={16} className="text-primary" />
            <span className="text-sm font-semibold text-foreground">Knoggin MCP Server</span>
          </div>
          <div className="flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-emerald-500/10 border border-emerald-500/20">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
            <span className="text-[10px] font-medium text-emerald-600 dark:text-emerald-400">
              Running
            </span>
          </div>
        </div>

        <div className="p-4 space-y-4">
          <div className="flex items-start justify-between gap-4">
            <div className="space-y-1">
              <Label className="text-[11px] text-muted-foreground uppercase tracking-wider font-semibold">
                Connection URL
              </Label>
              <code className="block px-2 py-1 rounded bg-background border border-border text-xs font-mono text-foreground select-all">
                http://localhost:8000/mcp
              </code>
            </div>
          </div>

          <div className="flex flex-wrap gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={handleCopyClaude}
              className="h-8 text-xs bg-background hover:bg-muted"
            >
              {copiedClaude ? (
                <Check size={14} className="mr-1.5 text-emerald-500" />
              ) : (
                <Copy size={14} className="mr-1.5 text-muted-foreground" />
              )}
              Claude Desktop Config
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={handleCopyCursor}
              className="h-8 text-xs bg-background hover:bg-muted"
            >
              {copiedCursor ? (
                <Check size={14} className="mr-1.5 text-emerald-500" />
              ) : (
                <Copy size={14} className="mr-1.5 text-muted-foreground" />
              )}
              Cursor Server URL
            </Button>
          </div>

          <div className="space-y-2 pt-2 border-t border-primary/10">
            <Label className="text-[11px] text-muted-foreground uppercase tracking-wider font-semibold flex items-center gap-1.5">
              <Activity size={12} />
              Exposed Tools
            </Label>
            <div className="flex flex-wrap gap-1.5">
              {[
                'search_entity',
                'get_connections',
                'find_path',
                'get_hierarchy',
                'search_messages',
                'get_recent_activity',
                'save_fact',
                'save_relationship',
              ].map(tool => (
                <span
                  key={tool}
                  className="px-1.5 py-0.5 rounded bg-primary/10 text-primary border border-primary/20 text-[10px] font-mono"
                >
                  {tool}
                </span>
              ))}
            </div>
          </div>
        </div>
      </div>

      <div className="space-y-2">
        {mcpServers.length === 0 && !addingServer && (
          <div className="text-center py-8 text-sm text-muted-foreground bg-card rounded-xl border border-border">
            <Unplug size={24} className="mx-auto mb-2 text-muted-foreground/50" />
            No MCP servers configured
          </div>
        )}

        {mcpServers.map(server => (
          <div
            key={server.name}
            className={`bg-card rounded-xl border border-border p-4 transition-opacity ${!server.enabled ? 'opacity-60 hover:opacity-100' : ''}`}
          >
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <span
                  className={`w-2 h-2 rounded-full ${
                    server.connected
                      ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.5)]'
                      : server.enabled
                        ? 'bg-amber-500 animate-pulse'
                        : 'bg-muted-foreground/30'
                  }`}
                />
                <div>
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-semibold text-foreground">{server.name}</span>
                    <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-muted text-muted-foreground font-mono truncate max-w-[200px]">
                      {server.command} {server.args?.join(' ')}
                    </span>
                  </div>
                  <p className="text-[11px] text-muted-foreground mt-0.5">
                    {server.connected
                      ? `${server.tool_count} tool${server.tool_count !== 1 ? 's' : ''}: ${server.tools?.join(', ') || 'none'}`
                      : server.last_error
                        ? `Error: ${server.last_error.slice(0, 60)}`
                        : 'Disconnected'}
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <Button
                  variant={server.enabled ? 'outline' : 'default'}
                  size="sm"
                  onClick={() => handleToggleServer(server.name)}
                  disabled={togglingServer === server.name}
                  className={`h-8 gap-1.5 px-3 rounded-lg font-medium transition-all ${server.enabled ? 'text-muted-foreground hover:text-foreground hover:bg-muted' : ''}`}
                >
                  {togglingServer === server.name ? (
                    <Loader2 size={14} className="animate-spin" />
                  ) : server.enabled ? (
                    <>
                      <Unplug size={14} />
                      Disconnect
                    </>
                  ) : (
                    <>
                      <Plug size={14} />
                      Connect
                    </>
                  )}
                </Button>
                <button
                  onClick={() => handleRemoveServer(server.name)}
                  className="p-1.5 text-muted-foreground hover:text-destructive transition-colors ml-1"
                  title="Remove Server"
                >
                  <Trash2 size={16} />
                </button>
              </div>
            </div>
          </div>
        ))}

        {/* Add Server */}
        {addingServer ? (
          <div className="bg-card rounded-xl border border-primary/50 p-4 space-y-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                {!selectedPreset && (
                  <div className="relative flex-1">
                    <Search
                      size={14}
                      className="absolute left-2.5 top-1/2 -translate-y-1/2 text-muted-foreground"
                    />
                    <Input
                      value={presetSearch}
                      onChange={e => {
                        setPresetSearch(e.target.value)
                        setCustomMode(false)
                      }}
                      placeholder="Search servers..."
                      autoFocus
                      className="pl-8 bg-muted border-border rounded-lg text-sm h-8 w-56"
                    />
                  </div>
                )}
                {selectedPreset && (
                  <span className="text-sm font-medium text-foreground">{selectedPreset.name}</span>
                )}
              </div>
              <button
                onClick={() => {
                  setAddingServer(false)
                  setSelectedPreset(null)
                  setPresetEnv({})
                  setPresetSearch('')
                  setCustomMode(false)
                }}
                className="p-1.5 text-muted-foreground hover:text-foreground"
              >
                <X size={14} />
              </button>
            </div>

            {/* Preset Grid */}
            {!customMode && (
              <div className="space-y-3">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {filteredPresets.map(preset => {
                    const installed = mcpServers.some(s => s.name === preset.id)
                    const isSelected = selectedPreset?.id === preset.id
                    return (
                      <div
                        key={preset.id}
                        className={`p-4 rounded-xl border transition-all overflow-hidden ${
                          installed
                            ? 'border-border/50 opacity-60'
                            : isSelected
                              ? 'border-primary/50 shadow-sm bg-primary/5 md:col-span-2'
                              : 'border-border hover:border-primary/30 bg-card hover:bg-muted/30'
                        }`}
                      >
                        <div className="flex items-start justify-between gap-4">
                          <div className="space-y-1.5 flex-1">
                            <div className="flex items-center justify-between">
                              <span
                                className={`text-sm font-semibold ${
                                  installed ? 'text-muted-foreground' : 'text-foreground'
                                } transition-colors uppercase tracking-wide`}
                              >
                                {preset.name}
                              </span>
                              {/* Actions on top right of the card */}
                              {!installed && !isSelected && (
                                <Button
                                  size="sm"
                                  variant="secondary"
                                  onClick={() => {
                                    setSelectedPreset(preset)
                                    setPresetEnv({})
                                  }}
                                  className="h-7 rounded-full px-3 text-[11px] font-semibold"
                                >
                                  Install
                                </Button>
                              )}
                              {installed && (
                                <span className="text-[10px] px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-500 font-semibold uppercase tracking-wider">
                                  Installed
                                </span>
                              )}
                            </div>

                            <div className="text-xs text-muted-foreground leading-relaxed">
                              {preset.description}
                            </div>

                            {/* Risk and Tools Info softly tucked below */}
                            <div className="flex flex-col gap-1.5 pt-1.5">
                              {preset.risk && (
                                <div className="flex items-start gap-1.5">
                                  <span
                                    className={`shrink-0 inline-flex items-center px-1.5 py-0.5 rounded text-[9px] font-semibold uppercase tracking-wider ${
                                      preset.risk === 'safe'
                                        ? 'bg-emerald-500/10 text-emerald-500'
                                        : preset.risk === 'moderate'
                                          ? 'bg-amber-500/10 text-amber-500'
                                          : 'bg-destructive/10 text-destructive'
                                    }`}
                                  >
                                    {preset.risk}
                                  </span>
                                  <span className="text-[10px] text-muted-foreground/80 leading-tight line-clamp-2">
                                    {preset.risk_note}
                                  </span>
                                </div>
                              )}
                              {preset.allowed_tools && (
                                <div className="text-[10px] text-muted-foreground/70 leading-tight line-clamp-1 overflow-hidden">
                                  <span className="font-medium">Tools:</span>{' '}
                                  {preset.allowed_tools.join(', ')}
                                </div>
                              )}
                            </div>
                          </div>
                        </div>

                        {/* Inline Expansion for Configuration */}
                        {isSelected && (
                          <div className="mt-4 pt-4 border-t border-primary/20 space-y-4 animate-[fadeIn_0.2s_ease-out]">
                            {preset.env_vars.length > 0 ? (
                              <div className="space-y-3">
                                <div className="flex items-center justify-between">
                                  <p className="text-xs font-medium text-foreground">
                                    Configuration Required
                                  </p>
                                  {preset.help_url && (
                                    <a
                                      href={preset.help_url}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="inline-flex items-center gap-1.5 text-[11px] text-primary hover:underline"
                                    >
                                      <ExternalLink size={11} />
                                      {preset.help_label || 'Setup guide'}
                                    </a>
                                  )}
                                </div>
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                                  {preset.env_vars.map(v => (
                                    <div key={v.key} className="space-y-1.5">
                                      <Label className="text-xs text-muted-foreground focus-within:text-primary transition-colors">
                                        {v.label}
                                      </Label>
                                      <Input
                                        type="password"
                                        value={presetEnv[v.key] || ''}
                                        onChange={e =>
                                          setPresetEnv(prev => ({
                                            ...prev,
                                            [v.key]: e.target.value,
                                          }))
                                        }
                                        placeholder={v.placeholder}
                                        className="bg-background border-border rounded-lg text-sm font-mono h-9 transition-colors focus:border-primary"
                                      />
                                    </div>
                                  ))}
                                </div>
                              </div>
                            ) : (
                              <p className="text-xs text-muted-foreground bg-background rounded-lg p-3 border border-border">
                                No credentials required. Ready to connect.
                              </p>
                            )}

                            <div className="flex items-center gap-2 justify-end pt-2">
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() => {
                                  setSelectedPreset(null)
                                  setPresetEnv({})
                                }}
                                className="rounded-lg h-8 text-xs font-medium"
                              >
                                Cancel
                              </Button>
                              <Button
                                size="sm"
                                onClick={handleAddPreset}
                                disabled={
                                  mcpLoading || preset.env_vars.some(v => !presetEnv[v.key]?.trim())
                                }
                                className="rounded-lg h-8 text-xs font-semibold px-4 shadow-sm"
                              >
                                {mcpLoading ? (
                                  <Loader2 size={14} className="mr-1.5 animate-spin" />
                                ) : (
                                  <Plug size={14} className="mr-1.5" />
                                )}
                                Connect Integration
                              </Button>
                            </div>
                          </div>
                        )}
                      </div>
                    )
                  })}
                </div>

                {filteredPresets.length === 0 && presetSearch && (
                  <div className="text-center py-6 bg-muted/20 rounded-xl border border-dashed border-border">
                    <p className="text-xs text-muted-foreground mb-2">No matching presets found.</p>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setCustomMode(true)}
                      className="h-8 text-xs rounded-lg"
                    >
                      <Terminal size={14} className="mr-1.5" />
                      Configure custom server
                    </Button>
                  </div>
                )}

                {filteredPresets.length > 0 && (
                  <button
                    onClick={() => setCustomMode(true)}
                    className="w-full flex items-center justify-center gap-2 p-3 rounded-xl border border-dashed border-border text-xs font-medium text-muted-foreground hover:text-foreground hover:bg-muted/30 transition-all"
                  >
                    <Terminal size={14} />
                    Connect custom MCP server
                  </button>
                )}
              </div>
            )}

            {/* Custom Mode */}
            {customMode && (
              <div className="space-y-3">
                <div className="grid grid-cols-3 gap-3">
                  <div>
                    <Label className="text-[11px] text-muted-foreground">Name</Label>
                    <Input
                      value={customServer.name}
                      onChange={e => setCustomServer(s => ({ ...s, name: e.target.value }))}
                      placeholder="my-server"
                      className="mt-1 bg-muted border-border rounded-lg text-sm"
                    />
                  </div>
                  <div>
                    <Label className="text-[11px] text-muted-foreground">Command</Label>
                    <Input
                      value={customServer.command}
                      onChange={e => setCustomServer(s => ({ ...s, command: e.target.value }))}
                      placeholder="uvx"
                      className="mt-1 bg-muted border-border rounded-lg text-sm"
                    />
                  </div>
                  <div>
                    <Label className="text-[11px] text-muted-foreground">Args</Label>
                    <Input
                      value={customServer.args}
                      onChange={e => setCustomServer(s => ({ ...s, args: e.target.value }))}
                      placeholder="mcp-server-xxx"
                      className="mt-1 bg-muted border-border rounded-lg text-sm"
                    />
                  </div>
                </div>
                <div className="flex items-center gap-2 justify-end">
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setCustomMode(false)}
                    className="rounded-lg"
                  >
                    Back
                  </Button>
                  <Button
                    size="sm"
                    onClick={handleAddCustom}
                    disabled={!customServer.name.trim() || mcpLoading}
                    className="rounded-lg"
                  >
                    {mcpLoading ? (
                      <Loader2 size={14} className="mr-1 animate-spin" />
                    ) : (
                      <Plug size={14} className="mr-1" />
                    )}
                    Connect
                  </Button>
                </div>
              </div>
            )}
          </div>
        ) : (
          <button
            onClick={() => setAddingServer(true)}
            className="w-full flex items-center justify-center gap-2 px-4 py-3 rounded-xl border border-dashed border-border text-sm text-muted-foreground hover:text-foreground hover:border-primary/50 transition-colors"
          >
            <Plus size={16} />
            Add MCP Server
          </button>
        )}
      </div>
    </div>
  )
}
