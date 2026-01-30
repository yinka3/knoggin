import { useEffect, useState, useRef } from 'react'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { Badge } from '@/components/ui/badge'
import { Separator } from '@/components/ui/separator'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { ChevronDown, ChevronRight, ArrowRight } from 'lucide-react'

function BouncingDots() {
  return (
    <span className="inline-flex gap-1 ml-2 items-center">
      <span className="w-1 h-1 bg-accent rounded-full animate-bounce [animation-delay:-0.3s]" />
      <span className="w-1 h-1 bg-accent rounded-full animate-bounce [animation-delay:-0.15s]" />
      <span className="w-1 h-1 bg-accent rounded-full animate-bounce" />
    </span>
  )
}

function ArgsDisplay({ args }) {
  if (!args || Object.keys(args).length === 0) return null

  const argsString = JSON.stringify(args, null, 2)
  const shortArgs = Object.entries(args)
    .map(([k, v]) => `${k}: ${JSON.stringify(v)}`)
    .join(', ')

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className="text-muted-foreground ml-2 text-[10px] font-mono cursor-help underline decoration-dotted max-w-[200px] truncate inline-block align-bottom">
            {shortArgs}
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
    <div className="py-2.5 animate-in fade-in slide-in-from-left-1 duration-300">
      {tc.thinking && (
        <div className="mb-1.5 text-muted-foreground italic text-[11px] leading-relaxed pl-3 border-l-2 border-accent/20">
          {tc.thinking}
        </div>
      )}

      <div className="flex items-center flex-wrap gap-2">
        <Badge variant="outline" className="text-accent border-accent/40 font-mono text-[10px] h-5">
          {tc.tool}
        </Badge>

        {isRunning && <BouncingDots />}

        <ArgsDisplay args={tc.args} />
      </div>

      {tc.summary && (
        <div className="mt-1.5 text-primary text-[11px] animate-in fade-in duration-200 pl-3 border-l-2 border-primary/50 flex items-center gap-1.5">
          <ArrowRight size={10} className="shrink-0 opacity-70" />
          <span className="opacity-90">{tc.summary}</span>
          {tc.count !== undefined && (
            <Badge variant="secondary" className="text-[9px] px-1.5 py-0 h-4">
              {tc.count}
            </Badge>
          )}
        </div>
      )}
    </div>
  )
}

export default function ThinkingBox({ toolCalls, streaming, currentThinking, defaultOpen = true }) {
  const [isOpen, setIsOpen] = useState(defaultOpen)
  const [elapsed, setElapsed] = useState(0)
  const startTimeRef = useRef(null)

  useEffect(() => {
    if (streaming && (toolCalls.length > 0 || currentThinking)) {
      setIsOpen(true)
    }
  }, [streaming, toolCalls.length, currentThinking])

  useEffect(() => {
    if (streaming && toolCalls.length > 0) {
      if (!startTimeRef.current) {
        startTimeRef.current = Date.now()
      }
      const interval = setInterval(() => {
        setElapsed(Date.now() - startTimeRef.current)
      }, 200)
      return () => clearInterval(interval)
    } else if (!streaming) {
      startTimeRef.current = null
    }
  }, [streaming, toolCalls.length])

  if (toolCalls.length === 0 && !currentThinking) return null

  return (
    <Collapsible
      open={isOpen}
      onOpenChange={setIsOpen}
      className="border border-border/50 rounded-lg bg-muted/40 my-3 overflow-hidden transition-all duration-200"
    >
      <CollapsibleTrigger className="flex items-center gap-2 w-full px-4 py-2 text-xs text-muted-foreground hover:bg-muted/60 transition-colors">
        {isOpen ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        <span className="font-medium">Reasoning Process</span>

        {toolCalls.length > 0 && (
          <Badge variant="secondary" className="text-[9px] px-1.5 h-4 ml-1">
            {toolCalls.length}
          </Badge>
        )}

        {streaming && <BouncingDots />}

        {/* Timer */}
        {(streaming || elapsed > 0) && toolCalls.length > 0 && (
          <span className="ml-auto text-[10px] text-muted-foreground/70 font-mono tabular-nums">
            {(elapsed / 1000).toFixed(1)}s
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
          <div key={idx}>
            {idx > 0 && <Separator className="my-2 bg-border/50" />}
            <ToolCallItem tc={tc} isLast={idx === toolCalls.length - 1} streaming={streaming} />
          </div>
        ))}
      </CollapsibleContent>
    </Collapsible>
  )
}
