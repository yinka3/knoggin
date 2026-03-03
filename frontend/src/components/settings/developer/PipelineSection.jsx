import { Cpu } from 'lucide-react'
import { Section, SettingRow, NumberInput } from './SettingsPrimitives'

export default function PipelineSection({ settings, update }) {
  return (
    <Section
      title="NLP Pipeline"
      description="Control how strictly the AI decides what information is worth saving"
      icon={Cpu}
    >
      <SettingRow
        label="Entity Extraction Confidence"
        description="Minimum score needed for the AI to recognize a concept as important"
      >
        <NumberInput
          value={settings?.nlp_pipeline?.gliner_threshold}
          onChange={v => update('nlp_pipeline.gliner_threshold', v)}
          min={0.5}
          max={1}
          step={0.05}
        />
      </SettingRow>
      <SettingRow
        label="Relationship Reasoning Threshold"
        description="Minimum certainty required before creating a permanent connection between two concepts"
      >
        <NumberInput
          value={settings?.nlp_pipeline?.vp01_min_confidence}
          onChange={v => update('nlp_pipeline.vp01_min_confidence', v)}
          min={0.5}
          max={1}
          step={0.05}
        />
      </SettingRow>
    </Section>
  )
}
