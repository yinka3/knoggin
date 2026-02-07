import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Brain, Eye, EyeOff, X, Plus, Check, ExternalLink } from 'lucide-react'
import { updateConfig, getConfigStatus } from '@/api/config'
import { cn } from '@/lib/utils'

function StepDots({ current, total }) {
  return (
    <div className="flex gap-2 justify-center">
      {Array.from({ length: total }).map((_, i) => (
        <div
          key={i}
          className={cn(
            'w-2 h-2 rounded-full transition-colors duration-300',
            i + 1 === current ? 'bg-primary' : 'bg-muted-foreground/30'
          )}
        />
      ))}
    </div>
  )
}

export default function OnboardingPage() {
  const navigate = useNavigate()
  const [step, setStep] = useState(1)
  const [userName, setUserName] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [aliases, setAliases] = useState('')
  const [facts, setFacts] = useState([])
  const [newFact, setNewFact] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState(null)
  const [showKey, setShowKey] = useState(false)

  useEffect(() => {
    getConfigStatus().then(status => {
      if (status.configured) {
        navigate('/chat', { replace: true })
      }
    })
  }, [navigate])

  function canProceed() {
    if (step === 1) return userName.trim().length > 0
    if (step === 2) return apiKey.trim().startsWith('sk-or-')
    return true
  }

  function handleNext() {
    if (step === 3) {
      handleSubmit()
    } else if (step === 4) {
      navigate('/chat')
    } else {
      setStep(s => s + 1)
    }
  }

  function handleBack() {
    setStep(s => s - 1)
    setError(null)
  }

  function handleSkip() {
    handleSubmit()
  }

  async function handleSubmit() {
    setSaving(true)
    setError(null)

    try {
      const aliasArray = aliases
        .split(',')
        .map(a => a.trim())
        .filter(Boolean)

      await updateConfig({
        user_name: userName.trim(),
        user_aliases: aliasArray,
        user_facts: facts,
        llm: { api_key: apiKey.trim() },
        configured_at: new Date().toISOString(),
      })

      setStep(4)
    } catch (err) {
      setError(err.message || 'Failed to save configuration')
    } finally {
      setSaving(false)
    }
  }

  function handleAddFact() {
    if (newFact.trim()) {
      setFacts([...facts, newFact.trim()])
      setNewFact('')
    }
  }

  function handleRemoveFact(index) {
    setFacts(facts.filter((_, i) => i !== index))
  }

  function handleFactKeyDown(e) {
    if (e.key === 'Enter') {
      e.preventDefault()
      handleAddFact()
    }
  }

  function handleKeyDown(e) {
    if (e.key === 'Enter' && canProceed()) {
      handleNext()
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center p-4 gradient-bg">
      <div className="w-full max-w-md">
        <div className="bg-card border border-border rounded-2xl p-8 shadow-xl">
          {/* Header */}
          <div className="mb-8">
            {step === 1 && (
              <div className="flex justify-center mb-6">
                <div className="p-4 rounded-2xl bg-primary/10">
                  <Brain size={40} className="text-primary" />
                </div>
              </div>
            )}
            <StepDots current={step} total={4} />
          </div>

          {/* Step 1: Welcome + Name */}
          {step === 1 && (
            <div className="space-y-6 animate-in fade-in duration-300">
              <div className="text-center">
                <h1 className="text-2xl font-semibold text-foreground mb-2">Welcome to Knoggin</h1>
                <p className="text-muted-foreground">
                  Your personal knowledge assistant. Let's get you set up.
                </p>
              </div>

              <div className="space-y-2">
                <Label htmlFor="name" className="text-muted-foreground">
                  What should I call you?
                </Label>
                <Input
                  id="name"
                  value={userName}
                  onChange={e => setUserName(e.target.value)}
                  onKeyDown={handleKeyDown}
                  placeholder="Your name"
                  autoFocus
                  className="bg-muted border-border rounded-xl h-12 text-base"
                />
              </div>
            </div>
          )}

          {/* Step 2: API Key */}
          {step === 2 && (
            <div className="space-y-6 animate-in fade-in duration-300">
              <div className="text-center">
                <h1 className="text-2xl font-semibold text-foreground mb-2">Connect your brain</h1>
                <p className="text-muted-foreground">
                  I use OpenRouter to think. Paste your API key below.
                </p>
              </div>

              <div className="space-y-2">
                <Label htmlFor="apiKey" className="text-muted-foreground">
                  OpenRouter API Key
                </Label>
                <div className="relative">
                  <Input
                    id="apiKey"
                    type={showKey ? 'text' : 'password'}
                    value={apiKey}
                    onChange={e => setApiKey(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder="sk-or-..."
                    autoFocus
                    className="bg-muted border-border rounded-xl h-12 text-base pr-12 font-mono"
                  />

                  <button
                    type="button"
                    onClick={() => setShowKey(!showKey)}
                    className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors"
                  >
                    {showKey ? <EyeOff size={18} /> : <Eye size={18} />}
                  </button>
                </div>
                {apiKey.trim().length > 0 && !apiKey.trim().startsWith('sk-or-') && (
                  <p className="text-xs text-destructive">OpenRouter keys start with sk-or-</p>
                )}
                <div className="flex items-center justify-between text-xs">
                  <a
                    href="https://openrouter.ai/keys"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-primary hover:underline flex items-center gap-1"
                  >
                    Get your key <ExternalLink size={12} />
                  </a>
                  <span className="text-muted-foreground">Your key stays local</span>
                </div>
              </div>
            </div>
          )}

          {/* Step 3: Aliases + Facts */}
          {step === 3 && (
            <div className="space-y-6 animate-in fade-in duration-300">
              <div className="text-center">
                <h1 className="text-2xl font-semibold text-foreground mb-2">
                  Help me remember you
                </h1>
                <p className="text-muted-foreground">
                  Optional — you can always add this later in Settings.
                </p>
              </div>

              {/* Aliases */}
              <div className="space-y-2">
                <Label htmlFor="aliases" className="text-muted-foreground">
                  Other names you go by
                </Label>
                <Input
                  id="aliases"
                  value={aliases}
                  onChange={e => setAliases(e.target.value)}
                  placeholder="Nicknames, handles (comma-separated)"
                  className="bg-muted border-border rounded-xl"
                />
              </div>

              {/* Facts */}
              <div className="space-y-2">
                <Label className="text-muted-foreground">Things I should know about you</Label>

                {/* Existing facts */}
                {facts.length > 0 && (
                  <div className="space-y-2 mb-3">
                    {facts.map((fact, i) => (
                      <div
                        key={i}
                        className="flex items-center gap-2 bg-muted rounded-lg px-3 py-2 text-sm group"
                      >
                        <span className="flex-1 text-foreground">{fact}</span>
                        <button
                          onClick={() => handleRemoveFact(i)}
                          className="text-muted-foreground hover:text-destructive transition-colors opacity-0 group-hover:opacity-100"
                        >
                          <X size={16} />
                        </button>
                      </div>
                    ))}
                  </div>
                )}

                {/* Add new fact */}
                <div className="flex gap-2">
                  <Input
                    value={newFact}
                    onChange={e => setNewFact(e.target.value)}
                    onKeyDown={handleFactKeyDown}
                    placeholder="I'm a software engineer..."
                    className="bg-muted border-border rounded-xl flex-1"
                  />
                  <Button
                    type="button"
                    variant="outline"
                    size="icon"
                    onClick={handleAddFact}
                    disabled={!newFact.trim()}
                    className="rounded-xl shrink-0"
                  >
                    <Plus size={18} />
                  </Button>
                </div>
              </div>
            </div>
          )}

          {/* Step 4: Success */}
          {step === 4 && (
            <div className="space-y-6 animate-in fade-in duration-300 text-center">
              <div className="flex justify-center">
                <div className="p-4 rounded-full bg-primary/20">
                  <Check size={40} className="text-primary" />
                </div>
              </div>
              <div>
                <h1 className="text-2xl font-semibold text-foreground mb-2">You're all set</h1>
                <p className="text-muted-foreground">Let's start building your second brain.</p>
              </div>
            </div>
          )}

          {/* Error */}
          {error && (
            <div className="mt-4 p-3 rounded-lg bg-destructive/10 text-destructive text-sm">
              {error}
            </div>
          )}

          {/* Footer / Navigation */}
          <div className="mt-8 flex items-center gap-3">
            {step > 1 && step < 4 && (
              <Button
                variant="ghost"
                onClick={handleBack}
                disabled={saving}
                className="text-muted-foreground"
              >
                Back
              </Button>
            )}

            <div className="flex-1" />

            {step === 3 && (
              <Button
                variant="ghost"
                onClick={handleSkip}
                disabled={saving}
                className="text-muted-foreground"
              >
                Skip
              </Button>
            )}

            <Button
              onClick={handleNext}
              disabled={!canProceed() || saving}
              className="rounded-xl px-6"
            >
              {saving ? 'Saving...' : step === 4 ? 'Start chatting' : 'Continue'}
            </Button>
          </div>

          {/* Settings hint on step 2 */}
          {step === 2 && (
            <p className="mt-6 text-xs text-center text-muted-foreground">
              You can change models and other settings later in Settings.
            </p>
          )}
        </div>
      </div>
    </div>
  )
}
