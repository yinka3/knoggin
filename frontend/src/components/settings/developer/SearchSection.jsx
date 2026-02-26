import { Search } from 'lucide-react'
import { Section, SettingRow, NumberInput } from './SettingsPrimitives'

export default function SearchSection({ settings, update }) {
  return (
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
      <SettingRow label="Activity Hours" description="Default lookback window">
        <NumberInput
          value={settings?.search?.default_activity_hours}
          onChange={v => update('search.default_activity_hours', v)}
          min={1}
          max={168}
          unit="h"
        />
      </SettingRow>
    </Section>
  )
}
