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
      title="Agent Capability Limits"
      description="Control how deep the AI thinks and how much memory it uses"
      icon={Zap}
      defaultOpen
    >
      <SettingRow
        label="Max Tool Usage"
        description="The maximum number of tools the agent can use to answer a single question"
      >
        <NumberInput
          value={settings?.limits?.max_tool_calls}
          onChange={v => update('limits.max_tool_calls', v)}
          min={1}
          max={20}
        />
      </SettingRow>
      <SettingRow
        label="Max Retry Attempts"
        description="How many times the agent is allowed to try again if a tool fails before giving up"
      >
        <NumberInput
          value={settings?.limits?.max_attempts}
          onChange={v => update('limits.max_attempts', v)}
          min={1}
          max={15}
        />
      </SettingRow>
      <SettingRow
        label="Consecutive Error Tolerance"
        description="Failsafe limit to stop infinite error loops if a tool is broken"
      >
        <NumberInput
          value={settings?.limits?.max_consecutive_errors}
          onChange={v => update('limits.max_consecutive_errors', v)}
          min={1}
          max={10}
        />
      </SettingRow>
      <SettingRow
        label="Brief Context Memory"
        description="How many recent chat turns to send the agent on every request for quick context"
      >
        <NumberInput
          value={settings?.limits?.agent_history_turns}
          onChange={v => update('limits.agent_history_turns', v)}
          min={1}
          max={20}
        />
      </SettingRow>
      <SettingRow
        label="Full Conversation Depth"
        description="Maximum depth the agent can traverse backward if it needs absolute full context"
      >
        <NumberInput
          value={settings?.limits?.conversation_context_turns}
          onChange={v => update('limits.conversation_context_turns', v)}
          min={1}
          max={30}
        />
      </SettingRow>
      <SettingRow
        label="Global History Cache limit"
        description="Maximum logs kept in fast memory before forcefully offloading to the graph database"
      >
        <NumberInput
          value={settings?.limits?.max_conversation_history}
          onChange={v => update('limits.max_conversation_history', v)}
          min={100}
          max={100000}
        />
      </SettingRow>
      <SettingRow
        label="Maximum Evidence Size"
        description="The max amount of internal reasoning notes the agent can accumulate per answer"
      >
        <NumberInput
          value={settings?.limits?.max_accumulated_messages}
          onChange={v => update('limits.max_accumulated_messages', v)}
          min={5}
          max={100}
        />
      </SettingRow>

      <SubSection title="Individual Tool Allowances" icon={Zap}>
        {TOOL_NAMES.map(tool => (
          <SettingRow
            key={tool.id}
            label={tool.label}
            description={`Maximum times the agent can use ${tool.label} in one question`}
          >
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
