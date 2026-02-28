import { useState, useEffect, useRef, useCallback } from 'react'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Skeleton } from '@/components/ui/skeleton'
import { Save, Plus, X, Lock, Play, Pause } from 'lucide-react'
import { getConfig, getCuratedModels, updateConfig } from '@/api/config'
import { toast } from 'sonner'
import TopicEditor from '@/components/TopicEditor'
import HierarchyEditor from '@/components/HierarchyEditor'
import useDelayedLoading from '@/hooks/useDelayedLoading'
import LLMSection from '@/components/settings/LLMSection'
import MCPSection from '@/components/settings/MCPSection'

function SectionHeader({ children, description }) {
  return (
    <div className="mb-3">
      <h2 className="text-sm font-medium text-foreground">{children}</h2>
      {description && <p className="text-xs text-muted-foreground mt-0.5">{description}</p>}
    </div>
  )
}

export default function SettingsPage() {
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState(null)

  const [userName, setUserName] = useState('')
  const [userAliases, setUserAliases] = useState('')

  const [agentModel, setAgentModel] = useState('')
  const [defaultTopics, setDefaultTopics] = useState({})

  const [devJobs, setDevJobs] = useState({
    cleaner: true,
    merger: true,
    archival: true,
    topic_config: true,
  })

  const initialState = useRef(null)

  const [addingTopic, setAddingTopic] = useState(false)
  const [newTopicName, setNewTopicName] = useState('')
  const showSkeleton = useDelayedLoading(loading)

  const [openrouterKey, setOpenrouterKey] = useState('')
  const [searchConfig, setSearchConfig] = useState({ provider: 'auto', brave_api_key: '', tavily_api_key: '' })

  useEffect(() => {
    async function load() {
      try {

        const [config, models] = await Promise.all([getConfig(), getCuratedModels()])

        setUserName(config.user_name || '')
        setUserAliases((config.user_aliases || []).join(', '))
        setDefaultTopics(config.default_topics || {})
        setOpenrouterKey(config.llm?.api_key || '')
        setSearchConfig({
          provider: config.search?.provider || 'auto',
          brave_api_key: config.search?.brave_api_key || '',
          tavily_api_key: config.search?.tavily_api_key || '',
        })
        setAgentModel(config.llm?.agent_model || '')

        const dSettings = config.developer_settings || {}
        const jobs = dSettings.jobs || {}
        setDevJobs({
          cleaner: jobs.cleaner?.enabled !== false,
          merger: jobs.merger?.enabled !== false,
          archival: jobs.archival?.enabled !== false,
          topic_config: jobs.topic_config?.enabled !== false,
        })

        const aliases = (config.user_aliases || []).join(', ')
        initialState.current = JSON.stringify({
          userAliases: aliases,
          defaultTopics: config.default_topics || {},
          openrouterKey: config.llm?.api_key || '',
          searchConfig: {
            provider: config.search?.provider || 'auto',
            brave_api_key: config.search?.brave_api_key || '',
            tavily_api_key: config.search?.tavily_api_key || '',
          },
          agentModel: config.llm?.agent_model || '',
          devJobs: {
            cleaner: jobs.cleaner?.enabled !== false,
            merger: jobs.merger?.enabled !== false,
            archival: jobs.archival?.enabled !== false,
            topic_config: jobs.topic_config?.enabled !== false,
          },
        })

      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [])

  const hasChanges =
    initialState.current !== null &&
    JSON.stringify({
      userAliases,
      defaultTopics,
      openrouterKey,
      searchConfig,
      agentModel,
      devJobs,
    }) !== initialState.current
  useEffect(() => {
    const handler = e => {
      if (hasChanges) {
        e.preventDefault()
        e.returnValue = ''
      }
    }
    window.addEventListener('beforeunload', handler)
    return () => window.removeEventListener('beforeunload', handler)
  }, [hasChanges])

  const handleSave = useCallback(async () => {
    setSaving(true)
    setError(null)
    try {
      const currentConfig = await getConfig()
      const updatedDevSettings = {
        ...(currentConfig.developer_settings || {}),
        jobs: {
          ...(currentConfig.developer_settings?.jobs || {}),
          cleaner: {
            ...(currentConfig.developer_settings?.jobs?.cleaner || {}),
            enabled: devJobs.cleaner,
          },
          merger: {
            ...(currentConfig.developer_settings?.jobs?.merger || {}),
            enabled: devJobs.merger,
          },
          archival: {
            ...(currentConfig.developer_settings?.jobs?.archival || {}),
            enabled: devJobs.archival,
          },
          topic_config: {
            ...(currentConfig.developer_settings?.jobs?.topic_config || {}),
            enabled: devJobs.topic_config,
          },
        },
      }

      await updateConfig({
        user_aliases: userAliases
          .split(',')
          .map(s => s.trim())
          .filter(Boolean),
        default_topics: defaultTopics,
        llm: {
          api_key: openrouterKey,
          agent_model: agentModel,
        },
        search: searchConfig,
        developer_settings: updatedDevSettings,
      })
      initialState.current = JSON.stringify({
        userAliases,
        defaultTopics,
        openrouterKey,
        searchConfig,
        agentModel,
        devJobs,
      })
      toast.success('Settings saved')
    } catch (err) {
      setError(err.message)
      toast.error('Failed to save settings')
    } finally {
      setSaving(false)
    }
  }, [userAliases, defaultTopics, openrouterKey, searchConfig, agentModel, devJobs])

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
            openrouterKey={openrouterKey}
            setOpenrouterKey={setOpenrouterKey}
            searchConfig={searchConfig}
            setSearchConfig={setSearchConfig}
          />

          {/* Topics Section */}
          <section>
            <SectionHeader description="Customize the categories and relationships for your notes.">
              Topic Hierarchy
            </SectionHeader>
            <TopicEditor
              topics={defaultTopics}
              onChange={setDefaultTopics}
              protectedNames={[]}
              renderExtra={(name, config, updateField) => (
                <HierarchyEditor name={name} config={config} updateField={updateField} />
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
                    if (e.key === 'Escape') {
                      setAddingTopic(false)
                      setNewTopicName('')
                    }
                  }}
                  placeholder="Topic name..."
                  autoFocus
                  className="flex-1 bg-muted border-border rounded-lg text-sm h-8"
                />
                <Button
                  size="sm"
                  onClick={handleAddTopic}
                  disabled={!newTopicName.trim() || defaultTopics[newTopicName.trim()]}
                  className="rounded-lg h-8 text-xs"
                >
                  Add
                </Button>
                <button
                  onClick={() => {
                    setAddingTopic(false)
                    setNewTopicName('')
                  }}
                  className="p-1.5 text-muted-foreground hover:text-foreground"
                >
                  <X size={14} />
                </button>
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

          {/* Background Jobs Section */}
          <section>
            <SectionHeader description="Toggle optional background tasks">
              Background Jobs
            </SectionHeader>
            <div className="bg-card rounded-xl p-4 border border-border">
              <JobCard
                title="Merger / Dedup"
                description="Periodically merges duplicate entities together."
                enabled={devJobs.merger}
                onToggle={v => setDevJobs(prev => ({ ...prev, merger: v }))}
              />
              <JobCard
                title="Cleaner"
                description="Removes unused orphaned entities."
                enabled={devJobs.cleaner}
                onToggle={v => setDevJobs(prev => ({ ...prev, cleaner: v }))}
              />
              <JobCard
                title="Fact Archival"
                description="Archives old facts out of working memory."
                enabled={devJobs.archival}
                onToggle={v => setDevJobs(prev => ({ ...prev, archival: v }))}
              />
              <JobCard
                title="Topic Configs"
                description="Automatically detects when to update your Topic Hierarchy."
                enabled={devJobs.topic_config}
                onToggle={v => setDevJobs(prev => ({ ...prev, topic_config: v }))}
              />
            </div>
            <p className="text-[11px] text-muted-foreground mt-2 px-2">
              Note: The <b>Profile Refinement</b> and <b>DLQ Replay</b> jobs cannot be disabled as
              they are required for basic functionality.
            </p>
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

function JobCard({ title, description, enabled, onToggle }) {
  return (
    <div className="flex items-center justify-between py-3 border-b border-border/30 last:border-0">
      <div className="space-y-0.5">
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium text-foreground">{title}</span>
          <span
            className={`text-[10px] px-1.5 py-0.5 rounded font-medium uppercase tracking-wide ${
              enabled ? 'bg-emerald-500/10 text-emerald-500' : 'bg-muted text-muted-foreground'
            }`}
          >
            {enabled ? 'Running' : 'Paused'}
          </span>
        </div>
        <p className="text-xs text-muted-foreground">{description}</p>
      </div>

      <Button
        variant={enabled ? 'outline' : 'default'}
        size="sm"
        onClick={() => onToggle(!enabled)}
        className={`h-7 gap-1 px-2.5 rounded-lg text-xs ${
          enabled
            ? 'hover:bg-destructive/10 hover:text-destructive hover:border-destructive/30'
            : ''
        }`}
      >
        {enabled ? (
          <>
            <Pause size={12} /> Pause
          </>
        ) : (
          <>
            <Play size={12} /> Start
          </>
        )}
      </Button>
    </div>
  )
}
