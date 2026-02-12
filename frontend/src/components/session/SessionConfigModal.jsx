import { useState, useEffect } from 'react'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { getConfig } from '@/api/config'
import { Loader2, Sparkles } from 'lucide-react'
import { listAgents } from '@/api/agents'
import { Bot } from 'lucide-react'
import { toast } from 'sonner'
import { generateTopicsFromDescription } from '@/api/topics'

const DEFAULT_CONFIG = {
  General: {
    active: true,
    labels: [],
    hierarchy: {},
    aliases: [],
    label_aliases: {},
  },
  Identity: {
    active: true,
    labels: ['person'],
    hierarchy: {},
    aliases: [],
    label_aliases: {},
  },
}

export default function SessionConfigModal({ open, onOpenChange, sessions, onCreateSession }) {
  const [mode, setMode] = useState('defaults')
  const [selectedSessionId, setSelectedSessionId] = useState(null)
  const [description, setDescription] = useState('')
  const [defaultTopics, setDefaultTopics] = useState(null)
  const [loadingDefaults, setLoadingDefaults] = useState(false)
  const [generatedConfig, setGeneratedConfig] = useState(null)
  const [generating, setGenerating] = useState(false)
  const [error, setError] = useState(null)
  const [agents, setAgents] = useState([])
  const [attemptsRemaining, setAttemptsRemaining] = useState(3)
  const [selectedAgentId, setSelectedAgentId] = useState(null)
  const [loadingAgents, setLoadingAgents] = useState(false)

  useEffect(() => {
    if (open) {
      setLoadingDefaults(true)
      getConfig()
        .then(config => setDefaultTopics(config.default_topics || null))
        .catch(err => console.error('Failed to load default topics:', err))
        .finally(() => setLoadingDefaults(false))

      setLoadingAgents(true)
      listAgents()
        .then(data => {
          setAgents(data.agents || [])
          const defaultAgent = data.agents?.find(a => a.is_default)
          if (defaultAgent) {
            setSelectedAgentId(defaultAgent.id)
          }
        })
        .catch(err => console.error('Failed to load agents:', err))
        .finally(() => setLoadingAgents(false))
    }
  }, [open])

  function resetState() {
    setMode('defaults')
    setSelectedSessionId(null)
    setDescription('')
    setGeneratedConfig(null)
    setGenerating(false)
    setError(null)
    setDefaultTopics(null)
    setLoadingDefaults(false)
    setAgents([])
    setSelectedAgentId(null)
    setLoadingAgents(false)
  }

  function handleOpenChange(open) {
    if (!open) resetState()
    onOpenChange(open)
  }

  async function handleGenerate() {
    if (!description.trim() || attemptsRemaining <= 0) return

    setGenerating(true)
    setError(null)

    try {
      const data = await generateTopicsFromDescription(description.trim())
      setGeneratedConfig(data.topics)
      setAttemptsRemaining(data.attempts_remaining)
    } catch (err) {
      if (err.message.includes('limit reached')) {
        setAttemptsRemaining(0)
      }
      setError(err.message || 'Failed to generate config.')
    } finally {
      setGenerating(false)
    }
  }

  function getSelectedConfig() {
    if (mode === 'defaults') {
      return defaultTopics || DEFAULT_CONFIG
    }

    if (mode === 'copy' && selectedSessionId) {
      const session = sessions.find(s => s.session_id === selectedSessionId)
      const config = session?.topics_config
      if (!config) {
        toast.warning('Selected session has no topic config — using defaults')
      }
      return config || DEFAULT_CONFIG
    }

    if (mode === 'generate' && generatedConfig) {
      return generatedConfig
    }

    return null
  }

  function handleCreate() {
    const config = getSelectedConfig()
    if (config) {
      onCreateSession(config, selectedAgentId)
      handleOpenChange(false)
    }
  }

  const canCreate =
    (mode === 'defaults' && !loadingDefaults && (defaultTopics || DEFAULT_CONFIG)) ||
    (mode === 'copy' && selectedSessionId) ||
    (mode === 'generate' && generatedConfig)

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="bg-background border-border sm:max-w-lg">
        <DialogHeader>
          <DialogTitle className="text-foreground">Configure Session</DialogTitle>
          <DialogDescription className="text-muted-foreground">
            Set up topics for this chat session
          </DialogDescription>
        </DialogHeader>

        {/* Agent Selection */}
        <div className="pb-4 border-b border-border">
          <Label className="text-muted-foreground text-sm mb-2 block">Agent</Label>
          {loadingAgents ? (
            <div className="flex items-center gap-2 text-muted-foreground text-sm">
              <span className="w-3 h-3 border-2 border-muted-foreground/30 border-t-muted-foreground rounded-full animate-spin" />
              Loading agents...
            </div>
          ) : (
            <Select value={selectedAgentId || ''} onValueChange={setSelectedAgentId}>
              <SelectTrigger className="w-full bg-muted border-border">
                <SelectValue placeholder="Select an agent" />
              </SelectTrigger>
              <SelectContent className="bg-popover border-border">
                {agents.map(agent => (
                  <SelectItem key={agent.id} value={agent.id}>
                    <div className="flex items-center gap-2">
                      <Bot size={14} className="text-muted-foreground" />
                      <span>{agent.name}</span>
                      {agent.is_default && (
                        <Badge variant="secondary" className="text-[10px] ml-1">
                          default
                        </Badge>
                      )}
                    </div>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          )}
        </div>

        <RadioGroup value={mode} onValueChange={setMode} className="space-y-3 py-4">
          {/* Option 1: Defaults */}
          <div
            className={`flex items-start space-x-3 rounded-lg border p-4 cursor-pointer transition-colors ${
              mode === 'defaults'
                ? 'border-accent bg-muted/50'
                : 'border-border hover:border-muted-foreground'
            }`}
            onClick={() => setMode('defaults')}
          >
            <RadioGroupItem value="defaults" id="defaults" className="mt-1" />
            <div className="flex-1">
              <Label htmlFor="defaults" className="text-foreground cursor-pointer">
                Start with defaults
              </Label>
              <p className="text-sm text-muted-foreground mt-1">
                {loadingDefaults ? (
                  <span className="flex items-center gap-2">
                    <span className="w-3 h-3 border-2 border-muted-foreground/30 border-t-muted-foreground rounded-full animate-spin" />
                    Loading...
                  </span>
                ) : defaultTopics ? (
                  Object.keys(defaultTopics).join(' + ')
                ) : (
                  'General + Identity topics'
                )}
              </p>
            </div>
          </div>

          {/* Option 2: Copy from session */}
          <div
            className={`flex items-start space-x-3 rounded-lg border p-4 cursor-pointer transition-colors ${
              mode === 'copy'
                ? 'border-accent bg-muted/50'
                : 'border-border hover:border-muted-foreground'
            }`}
            onClick={() => setMode('copy')}
          >
            <RadioGroupItem value="copy" id="copy" className="mt-1" />
            <div className="flex-1 space-y-2">
              <Label htmlFor="copy" className="text-foreground cursor-pointer">
                Copy from existing session
              </Label>
              {mode === 'copy' && (
                <Select value={selectedSessionId || ''} onValueChange={setSelectedSessionId}>
                  <SelectTrigger className="w-full bg-muted border-border">
                    <SelectValue placeholder="Select a session" />
                  </SelectTrigger>
                  <SelectContent className="bg-popover border-border">
                    {sessions.map(session => (
                      <SelectItem key={session.session_id} value={session.session_id}>
                        {session.session_id.slice(0, 8)}...
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              )}
            </div>
          </div>

          {/* Option 3: Generate */}
          <div
            className={`flex items-start space-x-3 rounded-lg border p-4 cursor-pointer transition-colors ${
              mode === 'generate'
                ? 'border-accent bg-muted/50'
                : 'border-border hover:border-muted-foreground'
            }`}
            onClick={() => setMode('generate')}
          >
            <RadioGroupItem value="generate" id="generate" className="mt-1" />
            <div className="flex-1 space-y-3">
              <Label
                htmlFor="generate"
                className="text-foreground cursor-pointer flex items-center gap-2"
              >
                Generate from description
                <Sparkles size={14} className="text-primary" />
              </Label>

              {mode === 'generate' && (
                <>
                  <textarea
                    value={description}
                    onChange={e => setDescription(e.target.value)}
                    placeholder="Describe what you want to track... e.g. 'Track my job search - companies, interviews, contacts, offers'"
                    rows={3}
                    className="w-full bg-muted border border-border rounded-lg px-3 py-2 text-sm text-foreground placeholder-muted-foreground resize-none focus:outline-none focus:border-accent"
                  />

                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleGenerate}
                    disabled={generating || !description.trim()}
                    className="border-primary text-primary hover:bg-primary hover:text-primary-foreground"
                  >
                    {generating ? (
                      <>
                        <Loader2 size={14} className="mr-2 animate-spin" />
                        Generating...
                      </>
                    ) : (
                      'Generate Config'
                    )}
                  </Button>

                  {error && <p className="text-sm text-destructive">{error}</p>}

                  {generatedConfig && (
                    <div className="rounded-lg bg-muted/80 border border-accent/20 p-3 space-y-2">
                      <p className="text-xs text-muted-foreground">Generated topics:</p>
                      <div className="flex flex-wrap gap-2">
                        {Object.keys(generatedConfig).map(topic => (
                          <Badge key={topic} variant="secondary" className="text-xs">
                            {topic}
                          </Badge>
                        ))}
                      </div>
                    </div>
                  )}
                </>
              )}
            </div>
          </div>
        </RadioGroup>

        <DialogFooter className="gap-2 sm:gap-0">
          <Button
            variant="ghost"
            onClick={() => handleOpenChange(false)}
            className="text-muted-foreground"
          >
            Cancel
          </Button>
          <Button
            onClick={handleCreate}
            disabled={!canCreate || generating}
            className="bg-primary text-primary-foreground hover:bg-accent"
          >
            Create Session
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
