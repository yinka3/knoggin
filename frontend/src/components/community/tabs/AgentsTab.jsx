import { useState, useEffect } from 'react'
import { getAgentCommunityMemory, getAgentHierarchy } from '@/api/community'
import { listAgents } from '@/api/agents'
import { formatDistanceToNow } from 'date-fns'
import { Brain, ChevronDown, Clock, GitBranch, ArrowUpRight, ArrowDownRight } from 'lucide-react'
import { motion, AnimatePresence } from 'motion/react'
import { cn } from '@/lib/utils'

function AgentCard({ agent, hierarchy, allAgents, isOpen, onToggle }) {
  const [memories, setMemories] = useState([])
  const [loading, setLoading] = useState(false)
  const [loaded, setLoaded] = useState(false)

  const agentMap = {}
  for (const a of allAgents) {
    agentMap[a.id] = a.name
  }

  const spawnedBy = hierarchy.find(h => h.child === agent.id)
  const spawned = hierarchy.filter(h => h.parent === agent.id)

  useEffect(() => {
    if (isOpen && !loaded) {
      loadData()
    }
  }, [isOpen])

  async function loadData() {
    setLoading(true)
    try {
      const memRes = await getAgentCommunityMemory(agent.id)
      setMemories(memRes.memory || [])
      setLoaded(true)
    } catch (err) {
      console.error(`Failed to load data for ${agent.id}:`, err)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="border border-border/30 rounded-lg overflow-hidden">
      {/* Header */}
      <button
        onClick={onToggle}
        className={cn(
          'w-full flex items-center justify-between px-3 py-3 transition-colors',
          isOpen ? 'bg-primary/5' : 'hover:bg-muted/30'
        )}
      >
        <div className="flex items-center gap-2">
          <Brain size={16} className={cn(isOpen ? 'text-primary' : 'text-muted-foreground')} />
          <span
            className={cn(
              'text-sm font-medium',
              isOpen ? 'text-foreground' : 'text-muted-foreground'
            )}
          >
            {agent.name}
          </span>

          {/* Quick badges */}
          {!isOpen && (
            <div className="flex items-center gap-1.5 ml-2">
              {spawned.length > 0 && (
                <span className="text-[10px] text-muted-foreground/60 flex items-center gap-0.5">
                  <GitBranch size={10} />
                  {spawned.length}
                </span>
              )}
            </div>
          )}
        </div>

        <ChevronDown
          size={14}
          className={cn(
            'text-muted-foreground transition-transform duration-200',
            isOpen && 'rotate-180'
          )}
        />
      </button>

      {/* Expanded content */}
      <AnimatePresence>
        {isOpen && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden"
          >
            <div className="px-3 py-3 border-t border-border/30 space-y-4">
              {loading ? (
                <div className="py-6 flex justify-center">
                  <div className="h-5 w-5 rounded-full border-2 border-primary/30 border-t-primary animate-spin" />
                </div>
              ) : (
                <>
                  {/* Hierarchy Section */}
                  {(spawnedBy || spawned.length > 0) && (
                    <div>
                      <div className="flex items-center gap-1.5 mb-2">
                        <GitBranch size={12} className="text-muted-foreground" />
                        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
                          Hierarchy
                        </span>
                      </div>

                      <div className="space-y-1 pl-1">
                        {spawnedBy && (
                          <div className="flex items-center gap-2 text-xs text-muted-foreground">
                            <ArrowUpRight size={12} className="text-blue-400" />
                            <span>Spawned by</span>
                            <span className="text-foreground font-medium">
                              {agentMap[spawnedBy.parent] || spawnedBy.parent}
                            </span>
                          </div>
                        )}

                        {spawned.map(s => (
                          <div
                            key={s.child}
                            className="flex items-center gap-2 text-xs text-muted-foreground"
                          >
                            <ArrowDownRight size={12} className="text-emerald-400" />
                            <span>Spawned</span>
                            <span className="text-foreground font-medium">
                              {agentMap[s.child] || s.child}
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Memories Section */}
                  <div>
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-1.5">
                        <Brain size={12} className="text-muted-foreground" />
                        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
                          Memories
                        </span>
                      </div>
                      <span className="text-[10px] text-muted-foreground/60">
                        {memories.length}/10
                      </span>
                    </div>

                    {memories.length === 0 ? (
                      <p className="text-xs text-muted-foreground/50 pl-1">No memories saved</p>
                    ) : (
                      <div className="space-y-1.5 pl-1">
                        {memories.map((mem, idx) => (
                          <div key={mem.id || idx} className="p-2 rounded bg-muted/20">
                            <p className="text-xs text-foreground/90 leading-relaxed">
                              {mem.content}
                            </p>
                            {mem.created_at && (
                              <span className="text-[10px] text-muted-foreground/50 flex items-center gap-1 mt-1">
                                <Clock size={9} />
                                {formatDistanceToNow(new Date(mem.created_at), { addSuffix: true })}
                              </span>
                            )}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                </>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

export default function AgentsTab() {
  const [agents, setAgents] = useState([])
  const [hierarchy, setHierarchy] = useState([])
  const [loading, setLoading] = useState(true)
  const [openAgentId, setOpenAgentId] = useState(null)

  useEffect(() => {
    async function load() {
      try {
        const [agentsRes, hierarchyRes] = await Promise.all([listAgents(), getAgentHierarchy()])
        setAgents(agentsRes.agents || [])
        setHierarchy(hierarchyRes.hierarchy || [])
      } catch (err) {
        console.error('Failed to load agents:', err)
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [])

  function handleToggle(agentId) {
    setOpenAgentId(prev => (prev === agentId ? null : agentId))
  }

  if (loading) {
    return (
      <div className="p-4 space-y-2">
        {[1, 2, 3].map(i => (
          <div key={i} className="h-14 rounded-lg bg-muted/30 animate-pulse" />
        ))}
      </div>
    )
  }

  if (agents.length === 0) {
    return (
      <div className="h-full flex flex-col items-center justify-center text-center p-6">
        <Brain size={24} className="text-muted-foreground/40 mb-2" />
        <p className="text-sm text-muted-foreground">No agents found</p>
        <p className="text-xs text-muted-foreground/60 mt-1">Create agents in the Agents page</p>
      </div>
    )
  }

  return (
    <div className="p-3 space-y-2">
      {agents.map(agent => (
        <AgentCard
          key={agent.id}
          agent={agent}
          hierarchy={hierarchy}
          allAgents={agents}
          isOpen={openAgentId === agent.id}
          onToggle={() => handleToggle(agent.id)}
        />
      ))}
    </div>
  )
}
