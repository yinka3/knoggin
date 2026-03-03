import { useState } from 'react'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { Settings, Tags, Wrench, BrainCircuit, Paperclip, GitMerge } from 'lucide-react'
import { cn } from '@/lib/utils'

function MenuRow({ icon: Icon, label, badge, onClick }) {
  return (
    <button
      onClick={onClick}
      className="w-full flex items-center justify-between gap-2 p-2 rounded-lg text-sm text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors"
    >
      <div className="flex items-center gap-2">
        <Icon size={14} />
        <span>{label}</span>
      </div>
      {badge !== undefined && badge !== null && (
        <Badge variant="secondary" className="text-[10px] h-4 px-1.5 font-normal">
          {badge}
        </Badge>
      )}
    </button>
  )
}

export default function SessionSettingsPopover({
  onOpenTopics,
  onOpenTools,
  onOpenMemory,
  onOpenFiles,
  onOpenInbox,
  memoryCount = 0,
  fileCount = 0,
  inboxCount = 0,
}) {
  const [open, setOpen] = useState(false)

  function handleAction(callback) {
    setOpen(false)
    callback?.()
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <TooltipProvider>
        <Tooltip delayDuration={300}>
          <TooltipTrigger asChild>
            <PopoverTrigger asChild>
              <button
                className={cn(
                  'p-1.5 rounded-md transition-all duration-200',
                  'text-muted-foreground hover:text-foreground hover:bg-muted/50',
                  open && 'bg-muted text-foreground'
                )}
              >
                <Settings
                  size={16}
                  className={cn(open && 'rotate-90', 'transition-transform duration-300')}
                />
              </button>
            </PopoverTrigger>
          </TooltipTrigger>
          <TooltipContent side="bottom">Session settings</TooltipContent>
        </Tooltip>
      </TooltipProvider>

      <PopoverContent align="end" className="w-56 p-0">
        <div className="p-3 border-b border-border">
          <h4 className="text-sm font-medium text-foreground">Session</h4>
        </div>

        <div className="p-1.5">
          <MenuRow icon={Wrench} label="Tools" onClick={() => handleAction(onOpenTools)} />
          <MenuRow
            icon={BrainCircuit}
            label="Memory"
            badge={memoryCount > 0 ? memoryCount : null}
            onClick={() => handleAction(onOpenMemory)}
          />
          <MenuRow
            icon={Paperclip}
            label="Files"
            badge={fileCount > 0 ? fileCount : null}
            onClick={() => handleAction(onOpenFiles)}
          />
          <MenuRow
            icon={GitMerge}
            label="Merge Inbox"
            badge={inboxCount > 0 ? inboxCount : null}
            onClick={() => handleAction(onOpenInbox)}
          />
          <MenuRow icon={Tags} label="Topics" onClick={() => handleAction(onOpenTopics)} />
        </div>
      </PopoverContent>
    </Popover>
  )
}
