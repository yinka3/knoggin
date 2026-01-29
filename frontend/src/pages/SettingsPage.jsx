// src/pages/SettingsPage.jsx
import { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Lock, Save, ChevronDown, ChevronRight, Plus, X } from 'lucide-react'
import { getConfig, updateConfig } from '@/api/config'

const REASONING_MODELS = [
  { value: 'google/gemini-2.5-flash', label: 'gemini-2.5-flash', tier: 'default' },
  { value: 'google/gemini-2.5-flash-lite', label: 'gemini-2.5-flash-lite', tier: 'budget' },
]

const AGENT_MODELS = [
  { value: 'google/gemini-3-flash-preview', label: 'gemini-3-flash-preview', tier: 'default' },
  { value: 'deepseek/deepseek-v3.2', label: 'deepseek-v3.2', tier: 'budget' },
  { value: 'x-ai/grok-4.1-fast', label: 'grok-4.1-fast', tier: 'budget' },
  { value: 'anthropic/claude-sonnet-4.5', label: 'claude-sonnet-4.5', tier: 'premium' },
]

function TierBadge({ tier }) {
  const styles = {
    default: 'bg-muted text-muted-foreground',
    budget: 'bg-primary/20 text-primary',
    premium: 'bg-amber-500/20 text-amber-400',
  }
  return (
    <Badge variant="secondary" className={`text-[10px] ml-auto rounded-full ${styles[tier]}`}>
      {tier}
    </Badge>
  )
}

function SectionHeader({ children, description }) {
  return (
    <div className="mb-4">
      <h2 className="text-base font-medium text-foreground">{children}</h2>
      {description && <p className="text-sm text-muted-foreground mt-0.5">{description}</p>}
    </div>
  )
}

export default function SettingsPage() {
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState(null)
  const [success, setSuccess] = useState(false)

  const [userName, setUserName] = useState('')
  const [userSummary, setUserSummary] = useState('')
  const [reasoningModel, setReasoningModel] = useState('')
  const [agentModel, setAgentModel] = useState('')
  const [defaultTopics, setDefaultTopics] = useState({})
  const [expandedTopic, setExpandedTopic] = useState(null)

  const [addingTopic, setAddingTopic] = useState(false)
  const [newTopicName, setNewTopicName] = useState('')
  const [showSkeleton, setShowSkeleton] = useState(false)

  useEffect(() => {
    if (loading) {
      const timer = setTimeout(() => setShowSkeleton(true), 150)
      return () => clearTimeout(timer)
    }
    setShowSkeleton(false)
  }, [loading])

  useEffect(() => {
    async function load() {
      try {
        const config = await getConfig()
        setUserName(config.user_name || '')
        setUserSummary(config.user_summary || '')
        setReasoningModel(config.reasoning_model || '')
        setAgentModel(config.agent_model || '')
        setDefaultTopics(config.default_topics || {})
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [])

  async function handleSave() {
    setSaving(true)
    setError(null)
    setSuccess(false)

    try {
      await updateConfig({
        user_summary: userSummary || null,
        reasoning_model: reasoningModel,
        agent_model: agentModel,
        default_topics: defaultTopics,
      })
      setSuccess(true)
      setTimeout(() => setSuccess(false), 3000)
    } catch (err) {
      setError(err.message)
    } finally {
      setSaving(false)
    }
  }

  function handleAddTopic() {
    const name = newTopicName.trim()
    if (name && !defaultTopics[name]) {
      setDefaultTopics({
        ...defaultTopics,
        [name]: {
          active: true,
          labels: [],
          aliases: [],
          hierarchy: {},
          label_aliases: {},
        },
      })
      setExpandedTopic(name)
      setNewTopicName('')
      setAddingTopic(false)
    }
  }

  if (loading) {
    return showSkeleton ? (
      <div className="p-6 max-w-2xl mx-auto space-y-6">
        <Skeleton className="h-8 w-32" />
        <Skeleton className="h-32 w-full rounded-xl" />
        <Skeleton className="h-32 w-full rounded-xl" />
      </div>
    ) : null
  }

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="border-b border-border p-4">
        <div className="max-w-2xl mx-auto flex items-center justify-between">
          <div>
            <h1 className="text-lg font-medium text-foreground">Settings</h1>
            <p className="text-sm text-muted-foreground">Manage your preferences</p>
          </div>
          <Button onClick={handleSave} disabled={saving} className="rounded-xl">
            <Save size={16} className="mr-2" />
            {saving ? 'Saving...' : 'Save'}
          </Button>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4">
        <div className="max-w-2xl mx-auto space-y-8">
          {/* Alerts */}
          {error && (
            <div className="p-4 rounded-xl bg-destructive/10 text-destructive text-sm">{error}</div>
          )}

          {success && (
            <div className="p-4 rounded-xl bg-primary/10 text-primary text-sm">
              Settings saved successfully
            </div>
          )}

          {/* Profile Section */}
          <section>
            <SectionHeader description="Your identity for STELLA">Profile</SectionHeader>
            <div className="space-y-4 bg-card rounded-xl p-4 border border-border">
              <div className="space-y-2">
                <Label htmlFor="name" className="text-muted-foreground flex items-center gap-2">
                  Name
                  <Lock size={12} />
                </Label>
                <Input
                  id="name"
                  value={userName}
                  disabled
                  className="bg-muted border-border text-muted-foreground rounded-xl"
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="summary" className="text-muted-foreground">
                  Summary
                </Label>
                <textarea
                  id="summary"
                  value={userSummary}
                  onChange={e => setUserSummary(e.target.value)}
                  placeholder="A brief description about yourself for the agent..."
                  rows={3}
                  className="w-full bg-muted border border-border rounded-xl px-3 py-2 text-sm text-foreground placeholder-muted-foreground resize-none focus:outline-none focus:border-primary focus:ring-1 focus:ring-primary/30 transition-colors"
                />
              </div>
            </div>
          </section>

          {/* Models Section */}
          <section>
            <SectionHeader description="Choose which models power STELLA">Models</SectionHeader>
            <div className="space-y-4 bg-card rounded-xl p-4 border border-border">
              <div className="space-y-2">
                <Label className="text-muted-foreground">Reasoning Model</Label>
                <Select value={reasoningModel} onValueChange={setReasoningModel}>
                  <SelectTrigger className="bg-muted border-border rounded-xl">
                    <SelectValue placeholder="Select model" />
                  </SelectTrigger>
                  <SelectContent className="bg-popover border-border rounded-xl">
                    {REASONING_MODELS.map(model => (
                      <SelectItem key={model.value} value={model.value} className="rounded-lg">
                        <span className="flex items-center gap-2 w-full">
                          {model.label}
                          <TierBadge tier={model.tier} />
                        </span>
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label className="text-muted-foreground">Agent Model</Label>
                <Select value={agentModel} onValueChange={setAgentModel}>
                  <SelectTrigger className="bg-muted border-border rounded-xl">
                    <SelectValue placeholder="Select model" />
                  </SelectTrigger>
                  <SelectContent className="bg-popover border-border rounded-xl">
                    {AGENT_MODELS.map(model => (
                      <SelectItem key={model.value} value={model.value} className="rounded-lg">
                        <span className="flex items-center gap-2 w-full">
                          {model.label}
                          <TierBadge tier={model.tier} />
                        </span>
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
          </section>

          {/* Default Topics Section */}
          <section>
            <SectionHeader description="Topics applied to new sessions">
              Default Topics
            </SectionHeader>
            <div className="space-y-2">
              {Object.entries(defaultTopics).map(([name, config]) => (
                <div key={name} className="rounded-xl overflow-hidden bg-card border border-border">
                  <button
                    onClick={() => setExpandedTopic(expandedTopic === name ? null : name)}
                    className="w-full flex items-center justify-between px-4 py-3 hover:bg-muted/50 transition-colors"
                  >
                    <div className="flex items-center gap-3">
                      <span
                        className={`w-2 h-2 rounded-full ${config.active !== false ? 'bg-primary' : 'bg-muted-foreground/30'}`}
                      />
                      <div className="text-left">
                        <span className="text-foreground font-medium">{name}</span>
                        <p className="text-xs text-muted-foreground mt-0.5">
                          {config.labels?.length > 0 ? config.labels.join(', ') : 'No labels'}
                        </p>
                      </div>
                    </div>
                    {expandedTopic === name ? (
                      <ChevronDown size={16} className="text-muted-foreground" />
                    ) : (
                      <ChevronRight size={16} className="text-muted-foreground" />
                    )}
                  </button>

                  {expandedTopic === name && (
                    <div className="px-4 pb-4 pt-2 border-t border-border space-y-4">
                      {/* Topic Aliases */}
                      <div>
                        <Label className="text-xs text-muted-foreground">Topic Aliases</Label>
                        <Input
                          value={config.aliases?.join(', ') || ''}
                          onChange={e => {
                            const newAliases = e.target.value
                              .split(',')
                              .map(s => s.trim())
                              .filter(Boolean)
                            setDefaultTopics({
                              ...defaultTopics,
                              [name]: { ...config, aliases: newAliases },
                            })
                          }}
                          placeholder="work, job, office"
                          className="mt-1 bg-muted border-border rounded-lg text-sm"
                        />
                        <p className="text-[11px] text-muted-foreground mt-1">
                          Comma-separated alternate names for this topic
                        </p>
                      </div>

                      {/* Labels */}
                      <div>
                        <Label className="text-xs text-muted-foreground">Labels</Label>
                        <Input
                          value={config.labels?.join(', ') || ''}
                          onChange={e => {
                            const newLabels = e.target.value
                              .split(',')
                              .map(s => s.trim())
                              .filter(Boolean)
                            setDefaultTopics({
                              ...defaultTopics,
                              [name]: { ...config, labels: newLabels },
                            })
                          }}
                          placeholder="person, company, project"
                          className="mt-1 bg-muted border-border rounded-lg text-sm"
                        />
                        <p className="text-[11px] text-muted-foreground mt-1">
                          Entity types to extract for this topic
                        </p>
                      </div>

                      {/* Label Aliases */}
                      <div>
                        <Label className="text-xs text-muted-foreground">Label Aliases</Label>
                        <textarea
                          value={
                            Object.keys(config.label_aliases || {}).length > 0
                              ? JSON.stringify(config.label_aliases, null, 2)
                              : ''
                          }
                          onChange={e => {
                            try {
                              const parsed = e.target.value.trim() ? JSON.parse(e.target.value) : {}
                              setDefaultTopics({
                                ...defaultTopics,
                                [name]: { ...config, label_aliases: parsed },
                              })
                            } catch {
                              // Invalid JSON, don't update
                            }
                          }}
                          placeholder='{"org": "company", "firm": "company"}'
                          rows={3}
                          className="mt-1 w-full bg-muted border border-border rounded-lg px-3 py-2 text-sm font-mono text-foreground placeholder-muted-foreground resize-none focus:outline-none focus:border-primary focus:ring-1 focus:ring-primary/30 transition-colors"
                        />
                        <p className="text-[11px] text-muted-foreground mt-1">
                          JSON mapping alternate label names to canonical labels
                        </p>
                      </div>

                      {/* Hierarchy */}
                      <div>
                        <Label className="text-xs text-muted-foreground">Hierarchy</Label>
                        <textarea
                          value={
                            Object.keys(config.hierarchy || {}).length > 0
                              ? JSON.stringify(config.hierarchy, null, 2)
                              : ''
                          }
                          onChange={e => {
                            try {
                              const parsed = e.target.value.trim() ? JSON.parse(e.target.value) : {}
                              setDefaultTopics({
                                ...defaultTopics,
                                [name]: { ...config, hierarchy: parsed },
                              })
                            } catch {
                              // Invalid JSON, don't update
                            }
                          }}
                          placeholder='{"company": ["team", "project"]}'
                          rows={3}
                          className="mt-1 w-full bg-muted border border-border rounded-lg px-3 py-2 text-sm font-mono text-foreground placeholder-muted-foreground resize-none focus:outline-none focus:border-primary focus:ring-1 focus:ring-primary/30 transition-colors"
                        />
                        <p className="text-[11px] text-muted-foreground mt-1">
                          JSON defining parent-child relationships between labels
                        </p>
                      </div>
                    </div>
                  )}
                </div>
              ))}

              {/* Add Topic - Inline */}
              {addingTopic ? (
                <div className="flex items-center gap-2 p-2 rounded-xl border border-primary/50 bg-card">
                  <Input
                    value={newTopicName}
                    onChange={e => setNewTopicName(e.target.value)}
                    onKeyDown={e => {
                      if (e.key === 'Enter') handleAddTopic()
                      if (e.key === 'Escape') {
                        setAddingTopic(false)
                        setNewTopicName('')
                      }
                    }}
                    placeholder="Topic name..."
                    autoFocus
                    className="flex-1 bg-muted border-border rounded-lg text-sm"
                  />
                  <Button
                    size="sm"
                    onClick={handleAddTopic}
                    disabled={!newTopicName.trim() || defaultTopics[newTopicName.trim()]}
                    className="rounded-lg"
                  >
                    Add
                  </Button>
                  <button
                    onClick={() => {
                      setAddingTopic(false)
                      setNewTopicName('')
                    }}
                    className="p-2 text-muted-foreground hover:text-foreground transition-colors"
                  >
                    <X size={16} />
                  </button>
                </div>
              ) : (
                <button
                  onClick={() => setAddingTopic(true)}
                  className="w-full flex items-center justify-center gap-2 px-4 py-3 rounded-xl border border-dashed border-border text-sm text-muted-foreground hover:text-foreground hover:border-primary/50 transition-colors"
                >
                  <Plus size={16} />
                  Add Topic
                </button>
              )}
            </div>
          </section>
        </div>
      </div>
    </div>
  )
}
