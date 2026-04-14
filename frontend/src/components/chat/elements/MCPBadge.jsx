import { useState, useEffect } from 'react'
import { Plug } from 'lucide-react'
import { getMCPServers } from '@/api/config'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { Badge } from '@/components/ui/badge'

export default function MCPBadge({ className }) {
  const [servers, setServers] = useState([])

  useEffect(() => {
    async function fetchServers() {
      try {
        const data = await getMCPServers()
        if (data.servers) {
          setServers(data.servers.filter(s => s.connected))
        }
      } catch (err) {
        console.error('Failed to load MCP servers for badge', err)
      }
    }
    fetchServers()

    // We could add polling here or socket events if we want real-time updates of connection status later.
  }, [])

  if (servers.length === 0) {
    return (
      <div className="flex items-center gap-1.5 px-2 py-1 text-xs text-muted-foreground/40">
        <Plug size={14} />
        <span className="hidden sm:inline">MCP</span>
      </div>
    )
  }

  return (
    <TooltipProvider>
      <Tooltip delayDuration={0}>
        <TooltipTrigger asChild>
          <div
            className={`flex items-center gap-1.5 px-2 py-1 rounded-md text-xs transition-colors duration-150 text-emerald-500 hover:bg-muted/50 cursor-pointer ${className || ''}`}
          >
            <Plug size={14} />
            <span className="hidden sm:inline">MCP</span>
            <Badge
              variant="secondary"
              className="h-4 px-1 text-[10px] min-w-[16px] justify-center bg-emerald-500/10 text-emerald-500 border-none hover:bg-emerald-500/20"
            >
              {servers.length}
            </Badge>
          </div>
        </TooltipTrigger>
        <TooltipContent side="bottom" align="end" className="text-xs p-2 max-w-[200px]">
          <div className="font-medium mb-1.5 text-foreground">Active MCP Servers</div>
          <ul className="space-y-1.5">
            {servers.map(s => (
              <li key={s.name} className="flex items-center gap-2 text-muted-foreground">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 shrink-0" />
                <span className="truncate" title={s.name}>
                  {s.name}
                </span>
              </li>
            ))}
          </ul>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}
