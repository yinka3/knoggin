import { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
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
        const [serversData, presetsData] = await Promise.all([
          getMCPServers(),
          getMCPPresets(),
        ])
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
      setMcpServers(prev => prev.map(s => s.name === name ? { ...s, ...result } : s))
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

  return (
    <div className="space-y-2">
      {mcpServers.length === 0 && !addingServer && (
        <div className="text-center py-8 text-sm text-muted-foreground bg-card rounded-xl border border-border">
          <Unplug size={24} className="mx-auto mb-2 text-muted-foreground/50" />
          No MCP servers configured
        </div>
      )}

      {mcpServers.map(server => (
        <div key={server.name} className="bg-card rounded-xl border border-border p-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <span
                className={`w-2 h-2 rounded-full ${
                  server.connected
                    ? 'bg-emerald-500'
                    : server.enabled
                      ? 'bg-amber-500 animate-pulse'
                      : 'bg-muted-foreground/30'
                }`}
              />
              <div>
                <div className="flex items-center gap-2">
                  <span className="text-sm font-medium text-foreground">{server.name}</span>
                  <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-muted text-muted-foreground font-mono">
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
              <Switch
                checked={server.enabled}
                onCheckedChange={() => handleToggleServer(server.name)}
                disabled={togglingServer === server.name}
              />
              <button
                onClick={() => handleRemoveServer(server.name)}
                className="p-1.5 text-muted-foreground hover:text-destructive transition-colors"
              >
                <Trash2 size={14} />
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
                  <Search size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-muted-foreground" />
                  <Input
                    value={presetSearch}
                    onChange={e => { setPresetSearch(e.target.value); setCustomMode(false) }}
                    placeholder="Search servers..."
                    autoFocus
                    className="pl-8 bg-muted border-border rounded-lg text-sm h-8 w-56"
                  />
                </div>
              )}
              {selectedPreset && (
                <span className="text-sm font-medium text-foreground">
                  {selectedPreset.name}
                </span>
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
          {!selectedPreset && !customMode && (
            <div className="space-y-2">
              <div className="grid grid-cols-2 gap-2">
                {filteredPresets.map(preset => {
                  const installed = mcpServers.some(s => s.name === preset.id)
                  return (
                    <button
                      key={preset.id}
                      onClick={() => !installed && (setSelectedPreset(preset), setPresetEnv({}))}
                      disabled={installed}
                      className={`text-left p-3 rounded-lg border transition-all group ${
                        installed
                          ? 'border-border/50 opacity-50 cursor-default'
                          : 'border-border hover:border-primary/50 hover:bg-muted/50'
                      }`}
                    >
                      <div className="flex items-center justify-between">
                        <span
                          className={`text-sm font-medium ${
                            installed
                              ? 'text-muted-foreground'
                              : 'text-foreground group-hover:text-primary'
                          } transition-colors`}
                        >
                          {preset.name}
                        </span>
                        {installed && (
                          <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-emerald-500/10 text-emerald-500 font-medium">
                            Connected
                          </span>
                        )}
                      </div>
                      <div className="text-[11px] text-muted-foreground mt-0.5">
                        {preset.description}
                      </div>
                      <div className="text-[10px] text-muted-foreground/60 mt-1 font-mono">
                        {preset.command} {preset.args[0]}
                      </div>
                    </button>
                  )
                })}
              </div>
              {filteredPresets.length === 0 && presetSearch && (
                <p className="text-center text-[11px] text-muted-foreground py-2">
                  No presets match "{presetSearch}" — try{' '}
                  <button
                    onClick={() => setCustomMode(true)}
                    className="text-primary hover:underline"
                  >
                    custom server
                  </button>
                </p>
              )}
              <button
                onClick={() => setCustomMode(true)}
                className="w-full flex items-center gap-2 p-2.5 rounded-lg border border-dashed border-border text-sm text-muted-foreground hover:text-foreground hover:border-primary/50 transition-colors"
              >
                <Terminal size={14} />
                Custom server (advanced)
              </button>
            </div>
          )}

          {/* Preset Env Vars */}
          {selectedPreset && (
            <div className="space-y-3">
              {selectedPreset.env_vars.length > 0 ? (
                <>
                  <p className="text-[11px] text-muted-foreground">
                    Provide credentials for {selectedPreset.name}:
                  </p>
                  {selectedPreset.help_url && (
                    <a
                      href={selectedPreset.help_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-1.5 text-[11px] text-primary hover:underline"
                    >
                      <ExternalLink size={11} />
                      {selectedPreset.help_label || 'Setup guide'}
                    </a>
                  )}
                  {selectedPreset.env_vars.map(v => (
                    <div key={v.key}>
                      <Label className="text-[11px] text-muted-foreground">{v.label}</Label>
                      <Input
                        type="password"
                        value={presetEnv[v.key] || ''}
                        onChange={e =>
                          setPresetEnv(prev => ({ ...prev, [v.key]: e.target.value }))
                        }
                        placeholder={v.placeholder}
                        className="mt-1 bg-muted border-border rounded-lg text-sm font-mono"
                      />
                    </div>
                  ))}
                </>
              ) : (
                <p className="text-[11px] text-muted-foreground">
                  No credentials required — ready to connect.
                </p>
              )}
              <div className="flex items-center gap-2 justify-end">
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => {
                    setSelectedPreset(null)
                    setPresetEnv({})
                  }}
                  className="rounded-lg"
                >
                  Back
                </Button>
                <Button
                  size="sm"
                  onClick={handleAddPreset}
                  disabled={
                    mcpLoading ||
                    selectedPreset.env_vars.some(v => !presetEnv[v.key]?.trim())
                  }
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
  )
}
