import { MessageSquare, Bot, Orbit } from 'lucide-react'
import { cn } from '@/lib/utils'

const TABS = [
  { id: 'history', label: 'History', icon: MessageSquare },
  { id: 'agents', label: 'Agents', icon: Bot },
  { id: 'insights', label: 'Insights', icon: Orbit },
]

export default function CommunityTabs({ activeTab, onTabChange, children }) {
  return (
    <div className="h-full flex flex-col rounded-xl border border-border/50 bg-card/30 overflow-hidden">
      {/* Tab navigation */}
      <div className="flex border-b border-border/30 px-2 pt-2">
        {TABS.map(tab => {
          const Icon = tab.icon
          const isActive = activeTab === tab.id

          return (
            <button
              key={tab.id}
              onClick={() => onTabChange(tab.id)}
              className={cn(
                'flex items-center gap-1.5 px-3 py-2 text-xs font-medium rounded-t-md transition-colors',
                isActive
                  ? 'text-primary bg-primary/10 border-b-2 border-primary -mb-px'
                  : 'text-muted-foreground hover:text-foreground hover:bg-muted/30'
              )}
            >
              <Icon size={14} />
              {tab.label}
            </button>
          )
        })}
      </div>

      {/* Tab content */}
      <div className="flex-1 overflow-y-auto">{children}</div>
    </div>
  )
}
