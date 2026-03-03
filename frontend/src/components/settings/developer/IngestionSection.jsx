import { Database } from 'lucide-react'
import { Section, SettingRow, NumberInput } from './SettingsPrimitives'

export default function IngestionSection({ settings, update }) {
  return (
    <Section
      title="Graph Database Ingestion"
      description="Control how messages are processed and saved into long-term memory"
      icon={Database}
    >
      <SettingRow
        label="Message Processing Batch"
        description="How many messages to hold in memory before writing to the database"
      >
        <NumberInput
          value={settings?.ingestion?.batch_size}
          onChange={v => update('ingestion.batch_size', v)}
          min={1}
          max={50}
        />
      </SettingRow>
      <SettingRow
        label="Batch Timeout"
        description="Force writing messages to the database after this many seconds, even if the batch isn't full"
      >
        <NumberInput
          value={settings?.ingestion?.batch_timeout}
          onChange={v => update('ingestion.batch_timeout', v)}
          min={10}
          max={600}
          unit="s"
        />
      </SettingRow>
      <SettingRow
        label="System Checkpoint Interval"
        description="How frequently to back up processing progress to prevent data loss on crash"
      >
        <NumberInput
          value={settings?.ingestion?.checkpoint_interval}
          onChange={v => update('ingestion.checkpoint_interval', v)}
          min={1}
          max={200}
          placeholder={`${(settings?.ingestion?.batch_size || 8) * 4}`}
        />
      </SettingRow>
      <SettingRow
        label="Context Window Size"
        description="Number of neighboring messages kept loaded for rapid relationship building"
      >
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
