import { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
import { Badge } from '@/components/ui/badge'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import {
  ChevronDown,
  Save,
  RotateCcw,
  Code2,
  Zap,
  Database,
  Search,
  GitMerge,
  Cpu,
  Clock,
  Sparkles,
} from 'lucide-react'
import { getConfig, updateConfig } from '@/api/config'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'

function Section({ title, description, icon: Icon, children, defaultOpen = false, badge }) {
  const [open, setOpen] = useState(defaultOpen)

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger
        className={cn(
          'w-full flex items-center justify-between p-4 rounded-xl transition-all duration-200',
          'bg-card border border-border',
          'hover:border-primary/30 hover:shadow-sm',
          open && 'border-primary/30 shadow-sm'
        )}
      >
        <div className="flex items-center gap-3">
          <div
            className={cn(
              'p-2 rounded-lg transition-colors duration-200',
              open ? 'bg-primary/15 text-primary' : 'bg-muted text-muted-foreground'
            )}
          >
            <Icon size={18} />
          </div>
          <div className="text-left">
            <div className="flex items-center gap-2">
              <h3 className="text-sm font-medium text-foreground">{title}</h3>
              {badge && (
                <Badge variant="secondary" className="text-[10px] px-1.5 py-0">
                  {badge}
                </Badge>
              )}
            </div>
            <p className="text-xs text-muted-foreground mt-0.5">{description}</p>
          </div>
        </div>
        <ChevronDown
          size={16}
          className={cn(
            'text-muted-foreground transition-transform duration-200',
            open && 'rotate-180'
          )}
        />
      </CollapsibleTrigger>
      <CollapsibleContent className="overflow-hidden data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:slide-up-2 data-[state=open]:slide-down-2 duration-200">
        <div className="pt-3 pb-1 px-1 space-y-1">{children}</div>
      </CollapsibleContent>
    </Collapsible>
  )
}

function SettingRow({ label, description, children }) {
  return (
    <div
      className={cn(
        'flex items-center justify-between gap-4 p-3 rounded-lg',
        'bg-muted/30 hover:bg-muted/50 transition-colors duration-150'
      )}
    >
      <div className="flex-1 min-w-0">
        <Label className="text-sm text-foreground font-normal">{label}</Label>
        {description && (
          <p className="text-[11px] text-muted-foreground mt-0.5 truncate">{description}</p>
        )}
      </div>
      <div className="w-28 shrink-0">{children}</div>
    </div>
  )
}

function NumberInput({ value, onChange, min, max, step = 1, unit, placeholder }) {
  return (
    <div className="relative">
      <Input
        type="number"
        value={value ?? ''}
        onChange={e => onChange(e.target.value ? Number(e.target.value) : null)}
        min={min}
        max={max}
        step={step}
        placeholder={placeholder ?? ''}
        className={cn(
          'bg-background border-border text-sm h-8 text-right font-mono',
          'focus:border-primary focus:ring-1 focus:ring-primary/30',
          'placeholder:text-muted-foreground/40',
          unit && 'pr-8'
        )}
      />
      {unit && (
        <span className="absolute right-2 top-1/2 -translate-y-1/2 text-[10px] text-muted-foreground">
          {unit}
        </span>
      )}
    </div>
  )
}

export default function DeveloperSettingsPage() {
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [settings, setSettings] = useState(null)
  const [original, setOriginal] = useState(null)

  useEffect(() => {
    loadSettings()
  }, [])

  async function loadSettings() {
    try {
      const config = await getConfig()
      const devSettings = config.developer_settings || {}
      setSettings(devSettings)
      setOriginal(JSON.parse(JSON.stringify(devSettings)))
    } catch (err) {
      toast.error('Failed to load settings')
    } finally {
      setLoading(false)
    }
  }

  async function handleSave() {
    setSaving(true)
    try {
      await updateConfig({ developer_settings: settings })
      setOriginal(JSON.parse(JSON.stringify(settings)))
      toast.success('Settings saved', {
        description: 'Applied to all active sessions',
      })
    } catch (err) {
      toast.error('Failed to save settings')
    } finally {
      setSaving(false)
    }
  }

  function handleReset() {
    setSettings(JSON.parse(JSON.stringify(original)))
    toast.info('Changes reverted')
  }

  function update(path, value) {
    setSettings(prev => {
      const next = JSON.parse(JSON.stringify(prev))
      const keys = path.split('.')
      let obj = next
      for (let i = 0; i < keys.length - 1; i++) {
        if (!obj[keys[i]]) obj[keys[i]] = {}
        obj = obj[keys[i]]
      }
      obj[keys[keys.length - 1]] = value
      return next
    })
  }

  const hasChanges = JSON.stringify(settings) !== JSON.stringify(original)

  if (loading) {
    return (
      <div className="p-6 max-w-2xl mx-auto space-y-4">
        <Skeleton className="h-10 w-64" />
        <Skeleton className="h-20 w-full rounded-xl" />
        <Skeleton className="h-20 w-full rounded-xl" />
        <Skeleton className="h-20 w-full rounded-xl" />
      </div>
    )
  }

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="border-b border-border p-4 bg-gradient-to-r from-background to-muted/30">
        <div className="max-w-2xl mx-auto flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2.5 rounded-xl bg-primary/10 text-primary">
              <Code2 size={22} />
            </div>
            <div>
              <h1 className="text-lg font-semibold text-foreground">Developer Settings</h1>
              <p className="text-sm text-muted-foreground">Fine-tune system behavior</p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            {hasChanges && (
              <Badge
                variant="outline"
                className="text-xs text-amber-500 border-amber-500/30 animate-in fade-in duration-200"
              >
                Unsaved changes
              </Badge>
            )}
            <Button
              variant="ghost"
              size="sm"
              onClick={handleReset}
              disabled={!hasChanges || saving}
              className="text-muted-foreground hover:text-foreground"
            >
              <RotateCcw size={14} className="mr-1.5" />
              Reset
            </Button>
            <Button
              size="sm"
              onClick={handleSave}
              disabled={!hasChanges || saving}
              className={cn(
                'transition-all duration-200',
                hasChanges && 'shadow-sm shadow-primary/25'
              )}
            >
              {saving ? (
                <>
                  <Sparkles size={14} className="mr-1.5 animate-spin" />
                  Saving...
                </>
              ) : (
                <>
                  <Save size={14} className="mr-1.5" />
                  Save
                </>
              )}
            </Button>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4">
        <div className="max-w-2xl mx-auto space-y-3">
          {/* Agent Limits */}
          <Section
            title="Agent Limits"
            description="Control reasoning depth and resource usage"
            icon={Zap}
            defaultOpen
          >
            <SettingRow label="Max Tool Calls" description="Per query limit">
              <NumberInput
                value={settings?.limits?.max_tool_calls}
                onChange={v => update('limits.max_tool_calls', v)}
                min={1}
                max={20}
              />
            </SettingRow>
            <SettingRow label="Max Attempts" description="Before fallback response">
              <NumberInput
                value={settings?.limits?.max_attempts}
                onChange={v => update('limits.max_attempts', v)}
                min={1}
                max={15}
              />
            </SettingRow>
            <SettingRow label="History Turns" description="Context for agent">
              <NumberInput
                value={settings?.limits?.agent_history_turns}
                onChange={v => update('limits.agent_history_turns', v)}
                min={1}
                max={20}
              />
            </SettingRow>
            <SettingRow label="Context Turns" description="Full conversation context">
              <NumberInput
                value={settings?.limits?.conversation_context_turns}
                onChange={v => update('limits.conversation_context_turns', v)}
                min={1}
                max={30}
              />
            </SettingRow>
            <SettingRow label="Max Accumulated" description="Messages in evidence">
              <NumberInput
                value={settings?.limits?.max_accumulated_messages}
                onChange={v => update('limits.max_accumulated_messages', v)}
                min={5}
                max={100}
              />
            </SettingRow>
          </Section>

          {/* Search */}
          <Section title="Search" description="Retrieval and ranking parameters" icon={Search}>
            <SettingRow label="Vector Limit" description="Embedding search results">
              <NumberInput
                value={settings?.search?.vector_limit}
                onChange={v => update('search.vector_limit', v)}
                min={10}
                max={200}
              />
            </SettingRow>
            <SettingRow label="FTS Limit" description="Full-text search results">
              <NumberInput
                value={settings?.search?.fts_limit}
                onChange={v => update('search.fts_limit', v)}
                min={10}
                max={200}
              />
            </SettingRow>
            <SettingRow label="Rerank Candidates" description="Before final selection">
              <NumberInput
                value={settings?.search?.rerank_candidates}
                onChange={v => update('search.rerank_candidates', v)}
                min={10}
                max={100}
              />
            </SettingRow>
            <SettingRow label="Message Limit" description="Default for search_messages">
              <NumberInput
                value={settings?.search?.default_message_limit}
                onChange={v => update('search.default_message_limit', v)}
                min={1}
                max={20}
              />
            </SettingRow>
            <SettingRow label="Entity Limit" description="Default for search_entity">
              <NumberInput
                value={settings?.search?.default_entity_limit}
                onChange={v => update('search.default_entity_limit', v)}
                min={1}
                max={20}
              />
            </SettingRow>
          </Section>

          {/* Entity Resolution */}
          <Section
            title="Entity Resolution"
            description="Deduplication and matching thresholds"
            icon={GitMerge}
          >
            <SettingRow label="Fuzzy Substring" description="Threshold for partial matches">
              <NumberInput
                value={settings?.entity_resolution?.fuzzy_substring_threshold}
                onChange={v => update('entity_resolution.fuzzy_substring_threshold', v)}
                min={50}
                max={100}
                unit="%"
              />
            </SettingRow>
            <SettingRow label="Fuzzy Non-Substring" description="Stricter full-name matching">
              <NumberInput
                value={settings?.entity_resolution?.fuzzy_non_substring_threshold}
                onChange={v => update('entity_resolution.fuzzy_non_substring_threshold', v)}
                min={50}
                max={100}
                unit="%"
              />
            </SettingRow>
            <SettingRow label="Candidate Fuzzy" description="Initial candidate filter">
              <NumberInput
                value={settings?.entity_resolution?.candidate_fuzzy_threshold}
                onChange={v => update('entity_resolution.candidate_fuzzy_threshold', v)}
                min={50}
                max={100}
                unit="%"
              />
            </SettingRow>
            <SettingRow label="Vector Threshold" description="Semantic similarity cutoff">
              <NumberInput
                value={settings?.entity_resolution?.candidate_vector_threshold}
                onChange={v => update('entity_resolution.candidate_vector_threshold', v)}
                min={0}
                max={1}
                step={0.05}
              />
            </SettingRow>
          </Section>

          {/* NLP Pipeline */}
          <Section title="NLP Pipeline" description="Extraction model confidence levels" icon={Cpu}>
            <SettingRow label="GLiNER Threshold" description="Entity extraction confidence">
              <NumberInput
                value={settings?.nlp_pipeline?.gliner_threshold}
                onChange={v => update('nlp_pipeline.gliner_threshold', v)}
                min={0.5}
                max={1}
                step={0.05}
              />
            </SettingRow>
            <SettingRow label="VP-01 Confidence" description="Reasoning layer threshold">
              <NumberInput
                value={settings?.nlp_pipeline?.vp01_min_confidence}
                onChange={v => update('nlp_pipeline.vp01_min_confidence', v)}
                min={0.5}
                max={1}
                step={0.05}
              />
            </SettingRow>
          </Section>

          {/* Ingestion */}
          <Section title="Ingestion" description="Message processing pipeline" icon={Database}>
            <SettingRow label="Batch Size" description="Messages per batch">
              <NumberInput
                value={settings?.ingestion?.batch_size}
                onChange={v => update('ingestion.batch_size', v)}
                min={1}
                max={50}
              />
            </SettingRow>
            <SettingRow label="Batch Timeout" description="Force process after">
              <NumberInput
                value={settings?.ingestion?.batch_timeout}
                onChange={v => update('ingestion.batch_timeout', v)}
                min={10}
                max={600}
                unit="s"
              />
            </SettingRow>
            <SettingRow label="Checkpoint Interval" description="Messages between checkpoints">
              <NumberInput
                value={settings?.ingestion?.checkpoint_interval}
                onChange={v => update('ingestion.checkpoint_interval', v)}
                min={1}
                max={200}
                placeholder={`${(settings?.ingestion?.batch_size || 8) * 4}`}
              />
            </SettingRow>
            <SettingRow label="Session Window" description="Messages in session context">
              <NumberInput
                value={settings?.ingestion?.session_window}
                onChange={v => update('ingestion.session_window', v)}
                min={1}
                max={100}
                placeholder={`${(settings?.ingestion?.batch_size || 8) * 3}`}
              />
            </SettingRow>
          </Section>

          {/* Background Jobs */}
          <Section title="Background Jobs" description="Scheduled maintenance tasks" icon={Clock}>
            <SettingRow label="Cleaner Interval" description="Time between cleanup runs">
              <NumberInput
                value={settings?.jobs?.cleaner?.interval_hours}
                onChange={v => update('jobs.cleaner.interval_hours', v)}
                min={1}
                max={168}
                unit="h"
              />
            </SettingRow>
            <SettingRow label="Orphan Age" description="Delete unlinked entities after">
              <NumberInput
                value={settings?.jobs?.cleaner?.orphan_age_hours}
                onChange={v => update('jobs.cleaner.orphan_age_hours', v)}
                min={1}
                max={168}
                unit="h"
              />
            </SettingRow>
            <SettingRow label="Stale Junk Days" description="Remove inactive junk after">
              <NumberInput
                value={settings?.jobs?.cleaner?.stale_junk_days}
                onChange={v => update('jobs.cleaner.stale_junk_days', v)}
                min={1}
                max={90}
                unit="d"
              />
            </SettingRow>
            <SettingRow label="Archival Retention" description="Keep old facts for">
              <NumberInput
                value={settings?.jobs?.archival?.retention_days}
                onChange={v => update('jobs.archival.retention_days', v)}
                min={1}
                max={365}
                unit="d"
              />
            </SettingRow>
          </Section>
        </div>
      </div>
    </div>
  )
}
