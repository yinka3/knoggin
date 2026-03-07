/* eslint-disable no-unused-vars */
import { Lock, MessageSquare, Sparkles, GitBranch } from 'lucide-react'
import { Button } from '@/components/ui/button'

function MockMessage({ name, short }) {
  return (
    <div className="flex gap-3 py-3">
      <div className="shrink-0 w-1 rounded-full bg-primary/30" />
      <div className="space-y-1.5 flex-1">
        <span className="text-xs font-medium text-muted-foreground/50">{name}</span>
        <div className="h-3 rounded bg-muted-foreground/10 w-full max-w-[280px]" />
        {!short && <div className="h-3 rounded bg-muted-foreground/10 w-3/4" />}
      </div>
    </div>
  )
}

function MockTab({ icon: Icon, label, active }) {
  return (
    <div
      className={`px-3 py-1.5 text-xs rounded-md flex items-center gap-1.5 ${active ? 'bg-muted text-muted-foreground/60' : 'text-muted-foreground/30'}`}
    >
      <Icon size={12} />
      {label}
    </div>
  )
}

export default function CommunityDisabledOverlay({ onEnable, enabling }) {
  return (
    <div className="relative h-full w-full overflow-hidden">
      {/* Mockup layout behind blur */}
      <div className="absolute inset-0 p-6 select-none pointer-events-none" aria-hidden="true">
        {/* Fake header */}
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center gap-3">
            <div className="h-2 w-2 rounded-full bg-muted-foreground/20" />
            <div className="h-4 w-48 rounded bg-muted-foreground/10" />
          </div>
          <div className="h-8 w-24 rounded-md bg-muted-foreground/10" />
        </div>

        {/* Main content area */}
        <div className="flex gap-6 h-[calc(100%-4rem)]">
          {/* Theater / conversation mock */}
          <div className="flex-1 rounded-xl border border-border/30 bg-card/30 p-4 space-y-1">
            <MockMessage name="STELLA" />
            <MockMessage name="ARCHON" short />
            <MockMessage name="STELLA" />
            <MockMessage name="ARCHON" />
          </div>

          {/* Tabs panel mock */}
          <div className="w-72 shrink-0 rounded-xl border border-border/30 bg-card/30 p-4">
            <div className="flex gap-1 mb-4">
              <MockTab icon={GitBranch} label="Hierarchy" active />
              <MockTab icon={MessageSquare} label="History" />
              <MockTab icon={Sparkles} label="Insights" />
            </div>
            <div className="space-y-2">
              <div className="h-3 rounded bg-muted-foreground/10 w-full" />
              <div className="h-3 rounded bg-muted-foreground/10 w-2/3" />
              <div className="h-3 rounded bg-muted-foreground/10 w-4/5" />
            </div>
          </div>
        </div>
      </div>

      {/* Blur layer + CTA */}
      <div className="absolute inset-0 z-20 flex items-center justify-center backdrop-blur-md bg-background/50">
        <div className="flex flex-col items-center gap-4 p-8 rounded-2xl bg-card/90 border border-border/50 shadow-xl max-w-sm text-center">
          <div className="p-3 rounded-xl bg-primary/10 text-primary">
            <Lock size={24} />
          </div>

          <div>
            <h2 className="text-lg font-semibold text-foreground">Community is disabled</h2>
            <p className="text-sm text-muted-foreground mt-1">
              Enable autonomous discussions to let your agents explore your knowledge graph
              together.
            </p>
          </div>

          <Button onClick={onEnable} disabled={enabling} className="mt-2 px-6">
            {enabling ? 'Enabling...' : 'Enable Community'}
          </Button>
        </div>
      </div>
    </div>
  )
}
