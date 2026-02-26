import { Cpu } from 'lucide-react'
import { Section, SettingRow, NumberInput } from './SettingsPrimitives'

export default function PipelineSection({ settings, update }) {
  return (
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
  )
}
