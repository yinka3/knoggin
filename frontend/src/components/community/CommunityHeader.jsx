import { Settings, Play, Loader2, Radio } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'

export default function CommunityHeader({
  connected,
  isLive,
  isViewingHistory,
  topic,
  turn,
  maxTurns,
  onTrigger,
  triggering,
  onClose,
  closing,
  onReturnToLive,
}) {
  return (
    <div className="border-b border-border/60 px-6 py-4">
      <div className="flex items-center justify-between">
        {/* Left: Status + Topic */}
        <div className="flex items-center gap-4 min-w-0">
          {/* Live indicator */}
          <div className="flex items-center gap-2 shrink-0">
            <div
              className={cn(
                'h-2 w-2 rounded-full',
                isLive
                  ? 'bg-emerald-500 animate-pulse'
                  : isViewingHistory
                    ? 'bg-blue-500'
                    : connected
                      ? 'bg-amber-500'
                      : 'bg-neutral-600'
              )}
            />
            <span
              className={cn(
                'text-xs font-medium uppercase tracking-wider',
                isLive
                  ? 'text-emerald-500'
                  : isViewingHistory
                    ? 'text-blue-500'
                    : connected
                      ? 'text-amber-500'
                      : 'text-muted-foreground'
              )}
            >
              {isLive ? 'Live' : isViewingHistory ? 'History' : connected ? 'Idle' : 'Offline'}
            </span>
          </div>

          {/* Topic */}
          {topic && (
            <>
              <div className="w-px h-4 bg-border" />
              <p className="text-sm text-foreground truncate">{topic}</p>
            </>
          )}

          {/* Turn counter */}
          {(isLive || isViewingHistory) && turn > 0 && (
            <>
              <div className="w-px h-4 bg-border" />
              <span className="text-xs text-muted-foreground shrink-0">
                {isLive ? `Turn ${turn}/${maxTurns}` : `${turn} messages`}
              </span>
            </>
          )}
        </div>

        {/* Right: Actions */}
        <div className="flex items-center gap-2 shrink-0">
          {isViewingHistory && (
            <Button
              size="sm"
              variant="ghost"
              onClick={onReturnToLive}
              className="gap-2 text-muted-foreground hover:text-foreground"
            >
              <Radio size={14} />
              Return to Live
            </Button>
          )}

          {isLive && (
            <Button
              size="sm"
              variant="destructive"
              onClick={onClose}
              disabled={closing}
              className="gap-2"
            >
              {closing ? <Loader2 size={14} className="animate-spin" /> : null}
              {closing ? 'Closing...' : 'End Discussion'}
            </Button>
          )}

          <Button
            size="sm"
            variant="outline"
            onClick={onTrigger}
            disabled={triggering || isLive}
            className="gap-2"
          >
            {triggering ? <Loader2 size={14} className="animate-spin" /> : <Play size={14} />}
            {triggering ? 'Starting...' : 'Trigger'}
          </Button>
        </div>
      </div>
    </div>
  )
}
