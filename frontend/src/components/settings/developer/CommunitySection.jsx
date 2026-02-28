import { useState, useEffect } from 'react'
import { Users } from 'lucide-react'
import { Section, SettingRow, NumberInput } from './SettingsPrimitives'
import { listAgents } from '@/api/agents'
import { Switch } from '@/components/ui/switch'

export default function CommunitySection({ settings, update }) {
  const [agents, setAgents] = useState([])

  useEffect(() => {
    listAgents()
      .then(res => setAgents(res.agents || []))
      .catch(() => {})
  }, [])

  const community = settings?.community || {}

  return (
    <Section
      title="Community (AAC)"
      description="Autonomous agent discussions on your knowledge graph"
      icon={Users}
      defaultOpen
    >
      <SettingRow label="Enabled" description="Allow agents to discuss autonomously">
        <Switch
          checked={community.enabled ?? false}
          onCheckedChange={v => update('community.enabled', v)}
        />
      </SettingRow>

      <SettingRow label="Interval" description="Time between scheduled discussions">
        <NumberInput
          value={community.interval_minutes ?? 30}
          onChange={v => update('community.interval_minutes', v)}
          min={1}
          max={1440}
          unit="min"
        />
      </SettingRow>

      <SettingRow label="Max Turns" description="Maximum turns per discussion">
        <NumberInput
          value={community.max_turns ?? 10}
          onChange={v => update('community.max_turns', v)}
          min={1}
          max={50}
        />
      </SettingRow>

      <SettingRow label="Seeding Agent" description="Agent that initiates discussions">
        <select
          value={community.seeding_agent_id || ''}
          onChange={e => update('community.seeding_agent_id', e.target.value || null)}
          className="bg-muted border border-border rounded-lg px-3 py-1.5 text-sm text-foreground focus:outline-none focus:ring-1 focus:ring-primary"
        >
          <option value="">Default</option>
          {agents.map(agent => (
            <option key={agent.id} value={agent.id}>
              {agent.name}
            </option>
          ))}
        </select>
      </SettingRow>

      <SettingRow label="Agent Pool" description="Agents eligible for discussions (empty = all)">
        <div className="flex flex-wrap gap-1.5">
          {agents.length === 0 ? (
            <span className="text-xs text-muted-foreground">No agents available</span>
          ) : (
            agents.map(agent => {
              const poolIds = community.agent_pool_ids || []
              const isSelected = poolIds.includes(agent.id)
              return (
                <button
                  key={agent.id}
                  type="button"
                  onClick={() => {
                    const next = isSelected
                      ? poolIds.filter(id => id !== agent.id)
                      : [...poolIds, agent.id]
                    update('community.agent_pool_ids', next)
                  }}
                  className={`px-2 py-1 rounded text-xs font-medium transition-colors ${
                    isSelected
                      ? 'bg-primary/20 text-primary border border-primary/30'
                      : 'bg-muted text-muted-foreground border border-transparent hover:border-border'
                  }`}
                >
                  {agent.name}
                </button>
              )
            })
          )}
        </div>
      </SettingRow>
    </Section>
  )
}
