import { Zap } from 'lucide-react'
import { Section, SubSection, SettingRow, NumberInput } from './SettingsPrimitives'

const TOOL_NAMES = [
  { id: 'search_messages', label: 'Search Messages' },
  { id: 'get_connections', label: 'Get Connections' },
  { id: 'search_entity', label: 'Search Entity' },
  { id: 'get_activity', label: 'Get Activity' },
  { id: 'find_path', label: 'Find Path' },
  { id: 'get_hierarchy', label: 'Get Hierarchy' },
  { id: 'save_memory', label: 'Save Memory' },
  { id: 'forget_memory', label: 'Forget Memory' },
  { id: 'search_files', label: 'Search Files' },
]

export default function AgentLimitsSection({ settings, update }) {
  const toolLimits = settings?.limits?.tool_limits || {}

  return (
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
      <SettingRow label="Max Consecutive Errors" description="Error tolerance per run">
        <NumberInput
          value={settings?.limits?.max_consecutive_errors}
          onChange={v => update('limits.max_consecutive_errors', v)}
          min={1}
          max={10}
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

      <SubSection title="Per-Tool Limits" icon={Zap}>
        {TOOL_NAMES.map(tool => (
          <SettingRow key={tool.id} label={tool.label} description={`Max calls for ${tool.id}`}>
            <NumberInput
              value={toolLimits[tool.id]}
              onChange={v => update(`limits.tool_limits.${tool.id}`, v)}
              min={1}
              max={20}
            />
          </SettingRow>
        ))}
      </SubSection>
    </Section>
  )
}
