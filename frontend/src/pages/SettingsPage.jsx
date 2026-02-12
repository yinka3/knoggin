import { useState, useEffect, useRef, useCallback } from 'react'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Skeleton } from '@/components/ui/skeleton'
import { Save, Plus, X, Lock } from 'lucide-react'
import { getConfig, getCuratedModels, updateConfig } from '@/api/config'
import { toast } from 'sonner'
import TopicEditor from '@/components/TopicEditor'
import useDelayedLoading from '@/hooks/useDelayedLoading'
import LLMSection from '@/components/settings/LLMSection'
import MCPSection from '@/components/settings/MCPSection'

function SectionHeader({ children, description }) {
  return (
    <div className="mb-3">
      <h2 className="text-sm font-medium text-foreground">{children}</h2>
      {description && (
        <p className="text-xs text-muted-foreground mt-0.5">{description}</p>
      )}
    </div>
  )
}

export default function SettingsPage() {
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState(null)

  const [userName, setUserName] = useState('')
  const [userAliases, setUserAliases] = useState('')

  const [reasoningModel, setReasoningModel] = useState('')
  const [agentModel, setAgentModel] = useState('')
  const [defaultTopics, setDefaultTopics] = useState({})

  const initialState = useRef(null)

  const [reasoningModels, setReasoningModels] = useState([])
  const [agentModels, setAgentModels] = useState([])
  const [modelsLoading, setModelsLoading] = useState(false)

  const [addingTopic, setAddingTopic] = useState(false)
  const [newTopicName, setNewTopicName] = useState('')
  const showSkeleton = useDelayedLoading(loading)

  const [openrouterKey, setOpenrouterKey] = useState('')

  const [expandedTopic, setExpandedTopic] = useState(null)

  useEffect(() => {
    async function load() {
      try {
        setModelsLoading(true)
        const [config, models] = await Promise.all([
          getConfig(),
          getCuratedModels(),
        ])

        setUserName(config.user_name || '')
        setUserAliases((config.user_aliases || []).join(', '))
        setDefaultTopics(config.default_topics || {})
        setOpenrouterKey(config.llm?.api_key || '')
        setReasoningModel(config.llm?.reasoning_model || '')
        setAgentModel(config.llm?.agent_model || '')

        const aliases = (config.user_aliases || []).join(', ')
        initialState.current = JSON.stringify({
          userAliases: aliases,
          defaultTopics: config.default_topics || {},
          openrouterKey: config.llm?.api_key || '',
          reasoningModel: config.llm?.reasoning_model || '',
          agentModel: config.llm?.agent_model || '',
        })

        setReasoningModels(models.reasoning || [])
        setAgentModels(models.agent || [])
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
        setModelsLoading(false)
      }
    }
    load()
  }, [])

  const hasChanges = initialState.current !== null && JSON.stringify({
    userAliases, defaultTopics, openrouterKey, reasoningModel, agentModel
  }) !== initialState.current
  useEffect(() => {
    const handler = (e) => {
      if (hasChanges) { e.preventDefault(); e.returnValue = '' }
    }
    window.addEventListener('beforeunload', handler)
    return () => window.removeEventListener('beforeunload', handler)
  }, [hasChanges])

  const handleSave = useCallback(async () => {
    setSaving(true)
    setError(null)
    try {
      await updateConfig({
        user_aliases: userAliases
          .split(',')
          .map(s => s.trim())
          .filter(Boolean),
        default_topics: defaultTopics,
        llm: {
          api_key: openrouterKey,
          reasoning_model: reasoningModel,
          agent_model: agentModel,
        },
      })
      initialState.current = JSON.stringify({
        userAliases, defaultTopics, openrouterKey, reasoningModel, agentModel
      })
      toast.success('Settings saved')
    } catch (err) {
      setError(err.message)
      toast.error('Failed to save settings')
    } finally {
      setSaving(false)
    }
  }, [userAliases, defaultTopics, openrouterKey, reasoningModel, agentModel])

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
          <div className="flex items-center gap-3">
            {hasChanges && (
              <span className="text-xs text-amber-500 bg-amber-500/10 px-2.5 py-1 rounded-full">
                Unsaved changes
              </span>
            )}
            <Button onClick={handleSave} disabled={saving} className="rounded-xl">
              <Save size={16} className="mr-2" />
              {saving ? 'Saving...' : 'Save'}
            </Button>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4">
        <div className="max-w-2xl mx-auto space-y-8">
          {/* Alerts */}
          {error && (
            <div className="p-4 rounded-xl bg-destructive/10 text-destructive text-sm">{error}</div>
          )}

          {/* Profile Section */}
          <section>
            <SectionHeader description="Your identity for Knoggin">Profile</SectionHeader>
            <div className="space-y-4 bg-card rounded-xl p-4 border border-border">
              {/* Name - Locked */}
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

              {/* Aliases */}
              <div className="space-y-2">
                <Label htmlFor="aliases" className="text-muted-foreground">
                  Aliases
                </Label>
                <Input
                  id="aliases"
                  value={userAliases}
                  onChange={e => setUserAliases(e.target.value)}
                  placeholder="Nicknames, handles (comma-separated)"
                  className="bg-muted border-border rounded-xl"
                />
                <p className="text-[11px] text-muted-foreground">
                  Comma-separated names your agent should recognize as you
                </p>
              </div>
            </div>
          </section>

          {/* Models + API Keys */}
          <LLMSection
            reasoningModel={reasoningModel}
            setReasoningModel={setReasoningModel}
            agentModel={agentModel}
            setAgentModel={setAgentModel}
            reasoningModels={reasoningModels}
            agentModels={agentModels}
            modelsLoading={modelsLoading}
            openrouterKey={openrouterKey}
            setOpenrouterKey={setOpenrouterKey}
          />

          {/* Default Topics Section */}
          <section>
            <SectionHeader description="Topics applied to new sessions">
              Default Topics
            </SectionHeader>
             <TopicEditor
               topics={defaultTopics}
               onChange={setDefaultTopics}
               protectedNames={['General']}
               renderExtra={(name, config, updateField) => (
                 <>
                   <div>
                     <Label className="text-[11px] text-muted-foreground">Label Aliases (JSON)</Label>
                     <textarea
                       value={Object.keys(config.label_aliases || {}).length > 0 ? JSON.stringify(config.label_aliases, null, 2) : ''}
                       onChange={e => {
                         try {
                           const parsed = e.target.value.trim() ? JSON.parse(e.target.value) : {}
                           updateField(name, 'label_aliases', parsed)
                         } catch { /* invalid JSON */ }
                       }}
                       placeholder='{"org": "company"}'
                       rows={2}
                       className="mt-1 w-full bg-muted border border-border rounded-lg px-3 py-1.5 text-xs font-mono text-foreground placeholder-muted-foreground resize-none focus:outline-none focus:border-primary focus:ring-1 focus:ring-primary/30 transition-colors"
                     />
                   </div>
                   <div>
                     <Label className="text-[11px] text-muted-foreground">Hierarchy (JSON)</Label>
                     <textarea
                       value={Object.keys(config.hierarchy || {}).length > 0 ? JSON.stringify(config.hierarchy, null, 2) : ''}
                       onChange={e => {
                         try {
                           const parsed = e.target.value.trim() ? JSON.parse(e.target.value) : {}
                           updateField(name, 'hierarchy', parsed)
                         } catch { /* invalid JSON */ }
                       }}
                       placeholder='{"company": ["team"]}'
                       rows={2}
                       className="mt-1 w-full bg-muted border border-border rounded-lg px-3 py-1.5 text-xs font-mono text-foreground placeholder-muted-foreground resize-none focus:outline-none focus:border-primary focus:ring-1 focus:ring-primary/30 transition-colors"
                     />
                   </div>
                 </>
               )}
             />

              {/* Add Topic */}
              {addingTopic ? (
                <div className="flex items-center gap-2 px-4 py-2">
                  <Input
                    value={newTopicName}
                    onChange={e => setNewTopicName(e.target.value)}
                    onKeyDown={e => {
                      if (e.key === 'Enter') handleAddTopic()
                      if (e.key === 'Escape') { setAddingTopic(false); setNewTopicName('') }
                    }}
                    placeholder="Topic name..."
                    autoFocus
                    className="flex-1 bg-muted border-border rounded-lg text-sm h-8"
                  />
                  <Button size="sm" onClick={handleAddTopic} disabled={!newTopicName.trim() || defaultTopics[newTopicName.trim()]} className="rounded-lg h-8 text-xs">Add</Button>
                  <button onClick={() => { setAddingTopic(false); setNewTopicName('') }} className="p-1.5 text-muted-foreground hover:text-foreground"><X size={14} /></button>
                </div>
              ) : (
                <button
                  onClick={() => setAddingTopic(true)}
                  className="w-full flex items-center justify-center gap-2 px-4 py-2.5 text-sm text-muted-foreground hover:text-foreground hover:bg-muted/30 transition-colors"
                >
                  <Plus size={14} />
                  Add Topic
                </button>
              )}
          </section>

          {/* MCP Servers Section */}
          <section>
            <SectionHeader description="Connect external tool servers via MCP">
              MCP Servers
            </SectionHeader>
            <MCPSection />
          </section>
        </div>
      </div>
    </div>
  )
}
