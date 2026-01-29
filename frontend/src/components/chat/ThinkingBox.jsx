import { useEffect, useState, useRef } from 'react'
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from '@/components/ui/collapsible'
import { Badge } from '@/components/ui/badge'
import { Separator } from '@/components/ui/separator'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
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
      }, 100)
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
      className="border border-accent/20 rounded-lg bg-muted/80 shadow-md my-3"
    >
      <CollapsibleTrigger className="flex items-center gap-2 w-full px-4 py-2.5 text-xs text-muted-foreground hover:text-accent transition-colors">
        {isOpen ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        <span className="font-medium">Agent Reasoning</span>
        {toolCalls.length > 0 && (
          <Badge variant="secondary" className="text-[10px]">
            {toolCalls.length} tool{toolCalls.length !== 1 ? 's' : ''}
          </Badge>
        )}
        {streaming && <BouncingDots />}
        
        {/* Timer */}
        {(streaming || elapsed > 0) && toolCalls.length > 0 && (
          <span className="ml-auto text-sm text-muted-foreground font-mono">
            {(elapsed / 1000).toFixed(1)}s
          </span>
        )}
      </CollapsibleTrigger>
      
      <CollapsibleContent className="px-4 pb-4 font-mono text-xs">
        {currentThinking && toolCalls.length === 0 && (
          <div className="py-2 text-muted-foreground italic animate-in fade-in duration-300 pl-3 border-l-2 border-accent/30">
            {currentThinking}
          </div>
        )}
        
        {toolCalls.map((tc, idx) => (
          <div key={idx}>
            {idx > 0 && <Separator className="my-2" />}
            <ToolCallItem 
              tc={tc} 
              isLast={idx === toolCalls.length - 1}
              streaming={streaming}
            />
          </div>
        ))}
      </CollapsibleContent>
    </Collapsible>
  )
}