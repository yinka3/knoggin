import { useEffect, useState, useRef } from 'react'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { Badge } from '@/components/ui/badge'
import { Separator } from '@/components/ui/separator'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { ChevronDown, ChevronRight, ArrowRight } from 'lucide-react'

function BouncingDots() {
  return (
    <span className="inline-flex gap-1 ml-2">
      <span className="w-1.5 h-1.5 bg-accent rounded-full animate-bounce [animation-delay:-0.3s]" />
      <span className="w-1.5 h-1.5 bg-accent rounded-full animate-bounce [animation-delay:-0.15s]" />
      <span className="w-1.5 h-1.5 bg-accent rounded-full animate-bounce" />
    </span>
  )
}

function ArgsDisplay({ args }) {
  if (!args || Object.keys(args).length === 0) return null

  const argsString = JSON.stringify(args, null, 2)
  const shortArgs = Object.entries(args)
    .map(([k, v]) => `${k}: ${JSON.stringify(v)}`)
    .join(', ')

  if (shortArgs.length < 40) {
    return <span className="text-muted-foreground ml-2">{shortArgs}</span>
  }

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className="text-muted-foreground ml-2 cursor-help underline decoration-dotted">
            {shortArgs.slice(0, 35)}...
          </span>
        </TooltipTrigger>
        <TooltipContent side="bottom" className="max-w-sm">
          <pre className="text-xs whitespace-pre-wrap">{argsString}</pre>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

function ToolCallItem({ tc, isLast, streaming }) {
  const isRunning = streaming && isLast && tc.status === 'running'

  return (
    <div className="py-3 animate-in fade-in slide-in-from-left-2 duration-300">
      {tc.thinking && (
        <div className="mb-2 text-muted-foreground italic text-[11px] leading-relaxed pl-3 border-l-2 border-accent/30">
          {tc.thinking}
        </div>
      )}

      <div className="flex items-center flex-wrap gap-2">
        <Badge variant="outline" className="text-accent border-accent font-mono">
          {tc.tool}
        </Badge>

        {isRunning && <BouncingDots />}

        <ArgsDisplay args={tc.args} />
      </div>

      {tc.summary && (
        <div className="mt-2 text-primary text-[11px] animate-in fade-in duration-200 pl-3 border-l-2 border-primary/70 flex items-center gap-1.5">
          <ArrowRight size={10} className="shrink-0" />
          <span>{tc.summary}</span>
          {tc.count !== undefined && (
            <Badge variant="secondary" className="text-[9px] px-1.5 py-0">
              {tc.count}
            </Badge>
          )}
        </div>
      )}
    </div>
  )
}

export default function ThinkingBox({ toolCalls, streaming, currentThinking, defaultOpen = true, totalDuration: totalDurationProp }) {
  const [isOpen, setIsOpen] = useState(defaultOpen)
  const timerRef = useRef(null)
  const startTimeRef = useRef(null)
  const timerDisplayRef = useRef(null)

  useEffect(() => {
    if (streaming && (toolCalls.length > 0 || currentThinking)) {
      setTimeout(() => setIsOpen(true), 0)
    }
  }, [streaming, toolCalls.length, currentThinking])

  // Close accordion if streaming ended
  useEffect(() => {
    if (streaming && !defaultOpen && isOpen && toolCalls.length > 0) {
      const timer = setTimeout(() => setIsOpen(false), 400)
      return () => clearTimeout(timer)
    }
  }, [defaultOpen, streaming, isOpen, toolCalls.length])

  // Timer using direct DOM updates instead of setState
  useEffect(() => {
    if (streaming && toolCalls.length > 0) {
      if (!startTimeRef.current) {
        startTimeRef.current = Date.now()
      }
      timerRef.current = setInterval(() => {
        if (timerDisplayRef.current) {
          const elapsed = Date.now() - startTimeRef.current
          timerDisplayRef.current.textContent = `${(elapsed / 1000).toFixed(1)}s`
        }
      }, 100)
      return () => clearInterval(timerRef.current)
    } else if (!streaming && startTimeRef.current) {
      // Final update on stop
      if (timerDisplayRef.current) {
        const elapsed = Date.now() - startTimeRef.current
        timerDisplayRef.current.textContent = `${(elapsed / 1000).toFixed(1)}s`
      }
      startTimeRef.current = null
    }
  }, [streaming, toolCalls.length])

  if (toolCalls.length === 0 && !currentThinking) return null

  const hasRunningTool = toolCalls.some(tc => tc.status === 'running')

  // Use total_duration from backend metadata (wall-clock), fall back to sum of per-tool durations
  const storedDuration = !streaming
    ? (totalDurationProp || toolCalls.reduce((sum, tc) => sum + (tc.duration || 0), 0))
    : 0

  return (
    <Collapsible open={isOpen} onOpenChange={setIsOpen} className="glass-card rounded-xl my-3">
      <CollapsibleTrigger className="flex items-center gap-2 w-full px-4 py-2 text-xs text-muted-foreground hover:bg-muted/60 transition-colors">
        {isOpen ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        <span className="font-medium">Reasoning Process</span>

        {toolCalls.length > 0 && (
          <Badge variant="secondary" className="text-[9px] px-1.5 h-4 ml-1">
            {toolCalls.length}
          </Badge>
        )}

        {streaming && hasRunningTool && <BouncingDots />}

        {!streaming && toolCalls.length > 0 && (
          <span className="text-primary/60 text-[10px]">✓</span>
        )}

        {/* Timer — stays visible after completion */}
        {toolCalls.length > 0 && (
          <span
            ref={timerDisplayRef}
            className="ml-auto text-[10px] text-muted-foreground/70 font-mono tabular-nums"
          >
            {!streaming && storedDuration > 0
              ? `${(storedDuration / 1000).toFixed(1)}s`
              : '0.0s'}
          </span>
        )}
      </CollapsibleTrigger>

      <CollapsibleContent className="px-4 pb-3 pt-0 font-mono text-xs">
        {currentThinking && toolCalls.length === 0 && (
          <div className="py-2 text-muted-foreground/80 italic animate-in fade-in duration-300 pl-3 border-l-2 border-accent/20">
            {currentThinking}
          </div>
        )}

        {toolCalls.map((tc, idx) => (
          <div key={`${tc.tool}-${tc.startTime}`}>
            {idx > 0 && <Separator className="my-2 bg-border/50" />}
            <ToolCallItem tc={tc} isLast={idx === toolCalls.length - 1} streaming={streaming} />
          </div>
        ))}
      </CollapsibleContent>
    </Collapsible>
  )
}
