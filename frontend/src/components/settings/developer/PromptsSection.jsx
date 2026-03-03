import { MessageSquareCode } from 'lucide-react'
import { Section } from './SettingsPrimitives'
import { Textarea } from '@/components/ui/textarea'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'

export default function PromptsSection({ settings, update }) {
  const nlpSettings = settings?.nlp_pipeline || {};

  return (
    <Section
      title="System Prompts"
      description="Core instructions driving the extraction algorithms. Leave '{user_name}' placeholders exactly as they are."
      icon={MessageSquareCode}
    >
      <Tabs defaultValue="ner" className="w-full">
        <TabsList className="mb-4 bg-muted/30 border border-white/[0.06] rounded-xl p-1 grid w-full grid-cols-5 h-auto">
          <TabsTrigger value="ner" className="rounded-lg data-[state=active]:bg-muted data-[state=active]:text-foreground text-xs py-2">Concept</TabsTrigger>
          <TabsTrigger value="relationship" className="rounded-lg data-[state=active]:bg-muted data-[state=active]:text-foreground text-xs py-2">Relationship</TabsTrigger>
          <TabsTrigger value="profile" className="rounded-lg data-[state=active]:bg-muted data-[state=active]:text-foreground text-xs py-2">Profile</TabsTrigger>
          <TabsTrigger value="merge" className="rounded-lg data-[state=active]:bg-muted data-[state=active]:text-foreground text-xs py-2">Merge</TabsTrigger>
          <TabsTrigger value="contradiction" className="rounded-lg data-[state=active]:bg-muted data-[state=active]:text-foreground text-xs py-2">Contradiction</TabsTrigger>
        </TabsList>

        <TabsContent value="ner" className="space-y-3 mt-2">
          <div>
            <label className="text-sm font-medium text-foreground">Concept Extraction</label>
            <p className="text-xs text-muted-foreground mt-1">Used to identify mentions of entities (e.g., people, places) in messages.</p>
          </div>
          <Textarea
            value={nlpSettings.ner_prompt || ''}
            onChange={e => update('nlp_pipeline.ner_prompt', e.target.value)}
            placeholder="Enter concept extraction prompt..."
            className="w-full min-h-[400px] bg-muted/30 border-white/[0.06] rounded-xl px-4 py-3 text-[13px] font-mono leading-relaxed text-foreground placeholder-muted-foreground/50 resize-y focus:outline-none focus:border-primary/50 focus:ring-1 focus:ring-primary/20 custom-scrollbar"
          />
        </TabsContent>

        <TabsContent value="relationship" className="space-y-3 mt-2">
          <div>
            <label className="text-sm font-medium text-foreground">Relationship Extraction</label>
            <p className="text-xs text-muted-foreground mt-1">Used to identify connections between entities.</p>
          </div>
          <Textarea
            value={nlpSettings.connection_prompt || ''}
            onChange={e => update('nlp_pipeline.connection_prompt', e.target.value)}
            placeholder="Enter relationship extraction prompt..."
            className="w-full min-h-[400px] bg-muted/30 border-white/[0.06] rounded-xl px-4 py-3 text-[13px] font-mono leading-relaxed text-foreground placeholder-muted-foreground/50 resize-y focus:outline-none focus:border-primary/50 focus:ring-1 focus:ring-primary/20 custom-scrollbar"
          />
        </TabsContent>

        <TabsContent value="profile" className="space-y-3 mt-2">
          <div>
            <label className="text-sm font-medium text-foreground">Profile Extraction</label>
            <p className="text-xs text-muted-foreground mt-1">Used to summarize and store facts about an entity over time.</p>
          </div>
          <Textarea
            value={nlpSettings.profile_prompt || ''}
            onChange={e => update('nlp_pipeline.profile_prompt', e.target.value)}
            placeholder="Enter profile extraction prompt..."
            className="w-full min-h-[400px] bg-muted/30 border-white/[0.06] rounded-xl px-4 py-3 text-[13px] font-mono leading-relaxed text-foreground placeholder-muted-foreground/50 resize-y focus:outline-none focus:border-primary/50 focus:ring-1 focus:ring-primary/20 custom-scrollbar"
          />
        </TabsContent>

        <TabsContent value="merge" className="space-y-3 mt-2">
          <div>
            <label className="text-sm font-medium text-foreground">Merge Judgment</label>
            <p className="text-xs text-muted-foreground mt-1">Used to determine if two entities represent the same object/person (entity deduplication).</p>
          </div>
          <Textarea
            value={nlpSettings.merge_prompt || ''}
            onChange={e => update('nlp_pipeline.merge_prompt', e.target.value)}
            placeholder="Enter merge judgment prompt..."
            className="w-full min-h-[400px] bg-muted/30 border-white/[0.06] rounded-xl px-4 py-3 text-[13px] font-mono leading-relaxed text-foreground placeholder-muted-foreground/50 resize-y focus:outline-none focus:border-primary/50 focus:ring-1 focus:ring-primary/20 custom-scrollbar"
          />
        </TabsContent>

        <TabsContent value="contradiction" className="space-y-3 mt-2">
          <div>
            <label className="text-sm font-medium text-foreground">Contradiction Judgment</label>
            <p className="text-xs text-muted-foreground mt-1">Used to detect if newly extracted facts contradict or supersede existing facts.</p>
          </div>
          <Textarea
            value={nlpSettings.contradiction_prompt || ''}
            onChange={e => update('nlp_pipeline.contradiction_prompt', e.target.value)}
            placeholder="Enter contradiction judgment prompt..."
            className="w-full min-h-[400px] bg-muted/30 border-white/[0.06] rounded-xl px-4 py-3 text-[13px] font-mono leading-relaxed text-foreground placeholder-muted-foreground/50 resize-y focus:outline-none focus:border-primary/50 focus:ring-1 focus:ring-primary/20 custom-scrollbar"
          />
        </TabsContent>
      </Tabs>
    </Section>
  )
}
