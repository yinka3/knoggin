import { GitMerge } from 'lucide-react'
import { Section, SettingRow, NumberInput } from './SettingsPrimitives'

export default function EntityResolutionSection({ settings, update }) {
  return (
    <Section
      title="Graph Match Sensitivity"
      description="Fine-tune how strictly the system merges memories and finds related concepts"
      icon={GitMerge}
    >
      <SettingRow
        label="Partial Match Strictness"
        description="How closely a word must match to be considered the same (e.g., 'React' vs 'ReactJS')"
      >
        <NumberInput
          value={settings?.entity_resolution?.fuzzy_substring_threshold}
          onChange={v => update('entity_resolution.fuzzy_substring_threshold', v)}
          min={50}
          max={100}
          unit="%"
        />
      </SettingRow>
      <SettingRow
        label="Exact Match Strictness"
        description="How intensely complete phrases must align before merging"
      >
        <NumberInput
          value={settings?.entity_resolution?.fuzzy_non_substring_threshold}
          onChange={v => update('entity_resolution.fuzzy_non_substring_threshold', v)}
          min={50}
          max={100}
          unit="%"
        />
      </SettingRow>
      <SettingRow
        label="Initial Filter Baseline"
        description="The minimum similarity score needed before deep semantic checking begins"
      >
        <NumberInput
          value={settings?.entity_resolution?.candidate_fuzzy_threshold}
          onChange={v => update('entity_resolution.candidate_fuzzy_threshold', v)}
          min={50}
          max={100}
          unit="%"
        />
      </SettingRow>
      <SettingRow
        label="Semantic Meaning Cutoff"
        description="How similar the core meaning of two concepts must be to link them"
      >
        <NumberInput
          value={settings?.entity_resolution?.candidate_vector_threshold}
          onChange={v => update('entity_resolution.candidate_vector_threshold', v)}
          min={0}
          max={1}
          step={0.05}
        />
      </SettingRow>
      <SettingRow
        label="Final Merge Threshold"
        description="The ultimate confidence score required to officially merge two concepts"
      >
        <NumberInput
          value={settings?.entity_resolution?.resolution_threshold}
          onChange={v => update('entity_resolution.resolution_threshold', v)}
          min={0}
          max={1}
          step={0.05}
        />
      </SettingRow>
      <SettingRow
        label="Common Word Ignore List"
        description="Ignore extremely common words (like 'the', 'and') if they appear this many times"
      >
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
