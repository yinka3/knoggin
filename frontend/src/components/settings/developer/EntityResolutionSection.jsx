import { GitMerge } from 'lucide-react'
import { Section, SettingRow, NumberInput, TradeoffSlider } from './SettingsPrimitives'

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
        impacts={['accuracy']}
      >
        <TradeoffSlider
          value={settings?.entity_resolution?.fuzzy_substring_threshold / 100}
          onChange={v => update('entity_resolution.fuzzy_substring_threshold', Math.round(v * 100))}
          leftLabel="Permissive"
          rightLabel="Strict"
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
        impacts={['latency']}
      >
        <TradeoffSlider
          value={settings?.entity_resolution?.candidate_fuzzy_threshold / 100}
          onChange={v => update('entity_resolution.candidate_fuzzy_threshold', Math.round(v * 100))}
          leftLabel="Wide Net"
          rightLabel="Precise"
        />
      </SettingRow>
      <SettingRow
        label="Semantic Meaning Cutoff"
        description="How similar the core meaning of two concepts must be to link them"
        impacts={['accuracy', 'quality']}
      >
        <TradeoffSlider
          value={settings?.entity_resolution?.candidate_vector_threshold}
          onChange={v => update('entity_resolution.candidate_vector_threshold', v)}
          leftLabel="Conceptual"
          rightLabel="Exact"
        />
      </SettingRow>
      <SettingRow
        label="Final Merge Threshold"
        description="The ultimate confidence score required to officially merge two concepts"
        impacts={['accuracy', 'quality']}
      >
        <TradeoffSlider
          value={settings?.entity_resolution?.resolution_threshold}
          onChange={v => update('entity_resolution.resolution_threshold', v)}
          leftLabel="Experimental"
          rightLabel="Safe"
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

      <div className="mt-8 border-t border-white/5 pt-6 animate-in fade-in slide-in-from-bottom-2 duration-700">
        <h4 className="text-xs font-semibold text-foreground uppercase tracking-wider mb-4 flex items-center gap-2">
          < Microscope size={14} className="text-primary" />
          Merge Simulator
        </h4>
        <MergeSimulator settings={settings} />
      </div>
    </Section>
  )
}

function MergeSimulator({ settings }) {
  const [e1, setE1] = useState('React')
  const [e2, setE2] = useState('ReactJS')
  
  // Very basic simulation of the fuzzy matching for visual feedback
  const getFuzzyScore = (s1, s2) => {
    const l1 = s1.toLowerCase()
    const l2 = s2.toLowerCase()
    if (l1 === l2) return 100
    if (l1.includes(l2) || l2.includes(l1)) {
      const ratio = Math.min(l1.length, l2.length) / Math.max(l1.length, l2.length)
      return Math.round(ratio * 100)
    }
    return 30 // Dummy low score
  }

  const score = getFuzzyScore(e1, e2)
  const threshold = settings?.entity_resolution?.fuzzy_substring_threshold ?? 80
  const willMerge = score >= threshold

  return (
    <div className="bg-white/5 rounded-xl border border-white/10 p-4 space-y-4">
      <div className="grid grid-cols-2 gap-4">
        <div className="space-y-1.5">
          <label className="text-[10px] text-muted-foreground uppercase">Entity A</label>
          <Input 
            value={e1} 
            onChange={e => setE1(e.target.value)}
            className="h-8 bg-black/20 border-white/10 text-sm"
          />
        </div>
        <div className="space-y-1.5">
          <label className="text-[10px] text-muted-foreground uppercase">Entity B</label>
          <Input 
            value={e2} 
            onChange={e => setE2(e.target.value)}
            className="h-8 bg-black/20 border-white/10 text-sm"
          />
        </div>
      </div>

      <div className="flex items-center justify-between p-3 rounded-lg bg-black/30 border border-white/5">
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase">Simulated Match Score</span>
          <span className={cn("text-lg font-mono font-bold", willMerge ? "text-emerald-500" : "text-amber-500")}>
            {score}%
          </span>
        </div>
        <div className="flex flex-col items-end">
          <span className="text-[10px] text-muted-foreground uppercase">Outcome</span>
          <Badge variant={willMerge ? "default" : "secondary"} className={cn("mt-1", willMerge && "bg-emerald-500/20 text-emerald-500 border-emerald-500/30")}>
            {willMerge ? 'AUTO-MERGE' : 'NO MERGE'}
          </Badge>
        </div>
      </div>
      
      <p className="text-[10px] text-muted-foreground italic text-center">
        Note: This is a client-side approximation using the current "Partial Match" threshold.
      </p>
    </div>
  )
}

import { useState } from 'react'
import { Microscope } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
