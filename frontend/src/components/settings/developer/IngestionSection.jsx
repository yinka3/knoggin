import { Database } from 'lucide-react'
import { Section, SettingRow, NumberInput } from './SettingsPrimitives'

export default function IngestionSection({ settings, update }) {
  return (
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
  )
}
