import { useState } from 'react'
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
import { Loader2, Sparkles } from 'lucide-react'

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
  const [generatedConfig, setGeneratedConfig] = useState(null)
  const [generating, setGenerating] = useState(false)
  const [error, setError] = useState(null)

  function resetState() {
    setMode('defaults')
    setSelectedSessionId(null)
    setDescription('')
    setGeneratedConfig(null)
    setGenerating(false)
    setError(null)
  }

  function handleOpenChange(open) {
    if (!open) resetState()
    onOpenChange(open)
  }

  async function handleGenerate() {
    if (!description.trim()) return

    setGenerating(true)
    setError(null)

    try {
      // TODO: Call API endpoint
      // const res = await fetch('/topics/generate', {
      //   method: 'POST',
      //   headers: { 'Content-Type': 'application/json' },
      //   body: JSON.stringify({ description })
      // })
      // const data = await res.json()
      // setGeneratedConfig(data.config)

      // Placeholder for now
      await new Promise(resolve => setTimeout(resolve, 1500))
      setGeneratedConfig({
        General: { active: true, labels: [], hierarchy: {}, aliases: [], label_aliases: {} },
        'Job Search': {
          active: true,
          labels: ['Company', 'Role', 'Contact', 'Interview'],
          hierarchy: {},
          aliases: ['jobs', 'career'],
          label_aliases: {},
        },
      })
    } catch (err) {
      setError('Failed to generate config. Please try again.')
    } finally {
      setGenerating(false)
    }
  }

  function getSelectedConfig() {
    if (mode === 'defaults') {
      return DEFAULT_CONFIG
    }

    if (mode === 'copy' && selectedSessionId) {
      const session = sessions.find(s => s.session_id === selectedSessionId)
      return session?.topics_config || DEFAULT_CONFIG
    }

    if (mode === 'generate' && generatedConfig) {
      return generatedConfig
    }

    return null
  }

  function handleCreate() {
    const config = getSelectedConfig()
    if (config) {
      onCreateSession(config)
      handleOpenChange(false)
    }
  }

  const canCreate =
    mode === 'defaults' ||
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
              <p className="text-sm text-muted-foreground mt-1">General + Identity topics</p>
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
                <Sparkles size={14} className="text-accent" />
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
