import React from 'react'
import AgentSelector from './AgentSelector'
import SessionInfoTooltip from './SessionInfoTooltip'
import TokenCounter from './TokenCounter'
import MCPBadge from './MCPBadge'
import SessionSettingsPopover from './SessionSettingsPopover'

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
  if (!sessionId) return null

  return (
    <div className="flex items-center justify-between px-4 py-2 border-b border-border shrink-0">
      {/* Left Zone */}
      <div className="flex items-center gap-2">
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
