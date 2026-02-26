import { GitMerge } from 'lucide-react'
import { Section, SettingRow, NumberInput } from './SettingsPrimitives'

export default function EntityResolutionSection({ settings, update }) {
  return (
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
      <SettingRow label="Resolution Threshold" description="Mention resolution similarity">
        <NumberInput
          value={settings?.entity_resolution?.resolution_threshold}
          onChange={v => update('entity_resolution.resolution_threshold', v)}
          min={0}
          max={1}
          step={0.05}
        />
      </SettingRow>
      <SettingRow label="Generic Token Freq" description="Ignore high-frequency tokens">
        <NumberInput
          value={settings?.entity_resolution?.generic_token_freq}
          onChange={v => update('entity_resolution.generic_token_freq', v)}
          min={1}
          max={50}
        />
      </SettingRow>
    </Section>
  )
}
