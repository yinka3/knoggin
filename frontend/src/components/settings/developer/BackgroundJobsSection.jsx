import {
  Clock,
  Trash2,
  Users,
  Combine,
  AlertTriangle,
  Archive,
  MessageSquare,
} from 'lucide-react'
import { Section, SubSection, SettingRow, NumberInput } from './SettingsPrimitives'

export default function BackgroundJobsSection({ settings, update }) {
  return (
    <Section title="Background Jobs" description="Scheduled maintenance and processing tasks" icon={Clock}>

      {/* Cleaner */}
      <SubSection title="Cleaner" icon={Trash2}>
        <SettingRow label="Interval" description="Time between cleanup runs">
          <NumberInput
            value={settings?.jobs?.cleaner?.interval_hours}
            onChange={v => update('jobs.cleaner.interval_hours', v)}
            min={1}
            max={168}
            unit="h"
          />
        </SettingRow>
        <SettingRow label="Orphan Age" description="Delete unlinked entities after">
          <NumberInput
            value={settings?.jobs?.cleaner?.orphan_age_hours}
            onChange={v => update('jobs.cleaner.orphan_age_hours', v)}
            min={1}
            max={168}
            unit="h"
          />
        </SettingRow>
        <SettingRow label="Stale Junk Days" description="Remove inactive junk after">
          <NumberInput
            value={settings?.jobs?.cleaner?.stale_junk_days}
            onChange={v => update('jobs.cleaner.stale_junk_days', v)}
            min={1}
            max={90}
            unit="d"
          />
        </SettingRow>
      </SubSection>

      {/* Profile */}
      <SubSection title="Profile Refinement" icon={Users}>
        <SettingRow label="Message Context Size" description="Number of recent messages to analyze when building the user profile">
          <NumberInput
            value={settings?.jobs?.profile?.msg_window}
            onChange={v => update('jobs.profile.msg_window', v)}
            min={5}
            max={100}
          />
        </SettingRow>
        <SettingRow label="Activity Limit" description="Minimum messages required before a profile update is triggered">
          <NumberInput
            value={settings?.jobs?.profile?.volume_threshold}
            onChange={v => update('jobs.profile.volume_threshold', v)}
            min={1}
            max={100}
          />
        </SettingRow>
        <SettingRow label="Idle Time Requirement" description="Seconds of chat silence needed before profile processing begins">
          <NumberInput
            value={settings?.jobs?.profile?.idle_threshold}
            onChange={v => update('jobs.profile.idle_threshold', v)}
            min={10}
            max={600}
            unit="s"
          />
        </SettingRow>
        <SettingRow label="Profile Batch Size" description="Facts processed per batch">
          <NumberInput
            value={settings?.jobs?.profile?.profile_batch_size}
            onChange={v => update('jobs.profile.profile_batch_size', v)}
            min={1}
            max={20}
          />
        </SettingRow>
        <SettingRow label="Max Facts Context" description="Maximum number of recent facts given to the AI during profile creation">
          <NumberInput
            value={settings?.jobs?.profile?.max_facts_context}
            onChange={v => update('jobs.profile.max_facts_context', v)}
            min={10}
            max={200}
          />
        </SettingRow>
        <SettingRow label="Minimum Conflict Similarity" description="Lowest semantic similarity needed to suspect two facts contradict">
          <NumberInput
            value={settings?.jobs?.profile?.contradiction_sim_low}
            onChange={v => update('jobs.profile.contradiction_sim_low', v)}
            min={0}
            max={1}
            step={0.05}
          />
        </SettingRow>
        <SettingRow label="Maximum Conflict Similarity" description="Upper similarity bound for contradiction checks">
          <NumberInput
            value={settings?.jobs?.profile?.contradiction_sim_high}
            onChange={v => update('jobs.profile.contradiction_sim_high', v)}
            min={0}
            max={1}
            step={0.05}
          />
        </SettingRow>
        <SettingRow label="Conflict Checks per Batch" description="How many contradictions to analyze at once">
          <NumberInput
            value={settings?.jobs?.profile?.contradiction_batch_size}
            onChange={v => update('jobs.profile.contradiction_batch_size', v)}
            min={1}
            max={20}
          />
        </SettingRow>
      </SubSection>

      {/* Merger */}
      <SubSection title="Entity Merger" icon={Combine}>
        <SettingRow label="Automatic Merge Certainty" description="Safely combine facts if the AI confidence is above this score">
          <NumberInput
            value={settings?.jobs?.merger?.auto_threshold}
            onChange={v => update('jobs.merger.auto_threshold', v)}
            min={0.5}
            max={1}
            step={0.01}
          />
        </SettingRow>
        <SettingRow label="Human Review Certainty" description="Ask for your permission to merge if confidence is above this score">
          <NumberInput
            value={settings?.jobs?.merger?.hitl_threshold}
            onChange={v => update('jobs.merger.hitl_threshold', v)}
            min={0.4}
            max={1}
            step={0.01}
          />
        </SettingRow>
        <SettingRow label="Initial Similarity Filter" description="Minimum semantic similarity required to even compare two facts">
          <NumberInput
            value={settings?.jobs?.merger?.cosine_threshold}
            onChange={v => update('jobs.merger.cosine_threshold', v)}
            min={0.1}
            max={1}
            step={0.05}
          />
        </SettingRow>
      </SubSection>

      {/* DLQ */}
      <SubSection title="Dead Letter Queue" icon={AlertTriangle}>
        <SettingRow label="Interval" description="Replay check frequency">
          <NumberInput
            value={settings?.jobs?.dlq?.interval_seconds}
            onChange={v => update('jobs.dlq.interval_seconds', v)}
            min={10}
            max={600}
            unit="s"
          />
        </SettingRow>
        <SettingRow label="Batch Size" description="Messages per replay run">
          <NumberInput
            value={settings?.jobs?.dlq?.batch_size}
            onChange={v => update('jobs.dlq.batch_size', v)}
            min={1}
            max={100}
          />
        </SettingRow>
        <SettingRow label="Max Attempts" description="Before discarding permanently">
          <NumberInput
            value={settings?.jobs?.dlq?.max_attempts}
            onChange={v => update('jobs.dlq.max_attempts', v)}
            min={1}
            max={10}
          />
        </SettingRow>
      </SubSection>

      {/* Archival */}
      <SubSection title="Archival" icon={Archive}>
        <SettingRow label="Retention Days" description="Keep old facts for">
          <NumberInput
            value={settings?.jobs?.archival?.retention_days}
            onChange={v => update('jobs.archival.retention_days', v)}
            min={1}
            max={365}
            unit="d"
          />
        </SettingRow>
        <SettingRow label="Fallback Interval" description="Run archival even without profiling">
          <NumberInput
            value={settings?.jobs?.archival?.fallback_interval_hours}
            onChange={v => update('jobs.archival.fallback_interval_hours', v)}
            min={0.5}
            max={168}
            step={0.5}
            unit="h"
          />
        </SettingRow>
      </SubSection>

      {/* Topic Config */}
      <SubSection title="Topic Extraction" icon={MessageSquare}>
        <SettingRow label="Messages per Topic Check" description="How frequently (in messages) the AI scans for a new conversation topic">
          <NumberInput
            value={settings?.jobs?.topic_config?.interval_msgs}
            onChange={v => update('jobs.topic_config.interval_msgs', v)}
            min={5}
            max={200}
          />
        </SettingRow>
        <SettingRow label="Topic Context Window" description="Number of recent messages to analyze when finding a topic">
          <NumberInput
            value={settings?.jobs?.topic_config?.conversation_window}
            onChange={v => update('jobs.topic_config.conversation_window', v)}
            min={5}
            max={200}
          />
        </SettingRow>
      </SubSection>

    </Section>
  )
}
