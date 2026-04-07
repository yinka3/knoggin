import { useState } from 'react'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { Info, Copy, Check } from 'lucide-react'
import { toast } from 'sonner'

export default function SessionInfoTooltip({ sessionId }) {
  const [copied, setCopied] = useState(false)

  async function handleCopy() {
    await navigator.clipboard.writeText(sessionId)
    setCopied(true)
    toast.success('Session ID copied')
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <TooltipProvider>
      <Tooltip delayDuration={300}>
        <TooltipTrigger asChild>
          <button className="p-1 rounded-md text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors">
            <Info size={14} />
          </button>
        </TooltipTrigger>
        <TooltipContent side="bottom" className="flex items-center gap-2">
          <span className="font-mono text-xs">{sessionId?.slice(0, 12)}...</span>
          <button onClick={handleCopy} className="p-1 rounded hover:bg-muted transition-colors">
            {copied ? <Check size={12} className="text-primary" /> : <Copy size={12} />}
          </button>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}
