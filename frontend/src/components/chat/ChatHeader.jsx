import MCPBadge from './elements/MCPBadge'
import SessionSettingsPopover from './elements/SessionSettingsPopover'
import { useSocket } from '@/context/SocketContext'
import { cn } from '@/lib/utils'
import Orb from '@/components/ui/Orb'

export default function ChatHeader({
  sessionId,
  currentAgentId,
  onAgentChange,
  disabled,
  totalTokens,
  fileCount,
  onOpenTopics,
  onOpenTools,
  onOpenFiles,
  onOpenInbox,
  onOpenNotes,
  onExport,
  inboxCount,
  notesCount,
}) {
  const { isConnected } = useSocket()
  
  if (!sessionId) return null

  return (
    <div className="flex items-center justify-between px-4 py-2 border-b border-border shrink-0">
      {/* Left Zone */}
      <div className="flex items-center gap-2">
        <div className="relative flex items-center justify-center w-6 h-6">
          <Orb 
            size={16} 
            isReady={isConnected} 
            className={cn(
              "transition-all duration-500",
              !isConnected && "grayscale opacity-50"
            )}
          />
          <div 
            className={cn(
              "absolute -bottom-0.5 -right-0.5 w-2 h-2 rounded-full border border-background transition-colors duration-500",
              isConnected ? "bg-emerald-500" : "bg-red-500"
            )} 
          />
        </div>
        <AgentSelector
          currentAgentId={currentAgentId}
          onAgentChange={onAgentChange}
          disabled={disabled}
        />
        <div className="w-px h-4 bg-border/50" />
        <SessionInfoTooltip sessionId={sessionId} />
        <div className="w-px h-4 bg-border/50" />
        <TokenCounter value={totalTokens} />
        <MCPBadge />
      </div>

      {/* Right Zone */}
      <SessionSettingsPopover
        onOpenTopics={onOpenTopics}
        onOpenTools={onOpenTools}
        onOpenFiles={onOpenFiles}
        onOpenInbox={onOpenInbox}
        onOpenNotes={onOpenNotes}
        onExport={onExport}
        fileCount={fileCount}
        inboxCount={inboxCount}
        notesCount={notesCount}
      />
    </div>
  )
}
