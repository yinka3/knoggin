import { useState, useEffect } from 'react'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Bot } from 'lucide-react'
import { listAgents } from '@/api/agents'

export default function AgentSelector({ currentAgentId, onAgentChange, disabled }) {
  const [agents, setAgents] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    listAgents()
      .then(data => setAgents(data.agents || []))
      .catch(err => console.error('Failed to load agents:', err))
      .finally(() => setLoading(false))
  }, [])

  const currentAgent = agents.find(a => a.id === currentAgentId)

  if (loading) {
    return (
      <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
        <Bot size={14} />
        <span>Loading...</span>
      </div>
    )
  }

  return (
    <Select value={currentAgentId || ''} onValueChange={onAgentChange} disabled={disabled}>
      <SelectTrigger className="h-7 w-auto gap-1.5 border-none bg-transparent px-2 text-xs text-muted-foreground hover:text-foreground hover:bg-muted/50 focus:ring-0 focus:ring-offset-0">
        <Bot size={14} />
        <SelectValue placeholder="Select agent">{currentAgent?.name || 'No agent'}</SelectValue>
      </SelectTrigger>
      <SelectContent align="start" className="min-w-[180px]">
        {agents.map(agent => (
          <SelectItem key={agent.id} value={agent.id} className="text-sm">
            <div className="flex items-center gap-2">
              <span>{agent.name}</span>
              {agent.is_default && (
                <span className="text-[10px] text-muted-foreground">(default)</span>
              )}
            </div>
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  )
}
