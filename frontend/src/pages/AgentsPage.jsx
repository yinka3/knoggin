import { useState, useEffect } from 'react'
import { motion, AnimatePresence } from 'motion/react'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Plus, Star, Pencil, Trash2, Cpu, Sparkles } from 'lucide-react'
import { listAgents, deleteAgent, setDefaultAgent, getAgentDefaults } from '@/api/agents'
import { getAvailableModels } from '@/api/config'
import { toast } from 'sonner'
import AgentEditorModal from '@/components/agents/AgentEditorModal'

function AgentCard({ agent, onEdit, onDelete, onSetDefault, index }) {
  return (
    <motion.div
      layout
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.35, delay: index * 0.06, ease: [0.22, 1, 0.36, 1] }}
      className="group relative"
    >
      <div className="relative glass-card rounded-2xl p-5 transition-all duration-300">
        <div className="absolute -inset-px rounded-2xl bg-primary/0 group-hover:bg-primary/[0.03] transition-colors duration-500 pointer-events-none" />

        <div className="relative flex items-center gap-3">
          <h3 className="font-semibold text-foreground text-[15px] tracking-tight">{agent.name}</h3>
          {agent.is_default && (
            <Badge className="bg-primary/15 text-primary text-[10px] border-0 px-2 py-0.5">
              <Star size={9} className="mr-1 fill-current" />
              Default
            </Badge>
          )}
          {agent.model && (
            <Badge
              variant="secondary"
              className="text-[10px] font-mono bg-muted/50 text-muted-foreground border-0 px-2 py-0.5"
            >
              <Cpu size={10} className="mr-1 opacity-60" />
              {agent.model.split('/').pop()}
            </Badge>
          )}

          <div className="ml-auto flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity duration-300">
            {!agent.is_default && (
              <Button
                variant="ghost"
                size="sm"
                onClick={() => onSetDefault(agent.id)}
                className="text-muted-foreground/70 hover:text-primary text-xs h-7 px-2"
              >
                <Star size={12} className="mr-1" />
                Set Default
              </Button>
            )}
            <Button
              variant="ghost"
              size="sm"
              onClick={() => onEdit(agent)}
              className="text-muted-foreground/70 hover:text-foreground h-7 w-7 p-0"
            >
              <Pencil size={13} />
            </Button>
            {!agent.is_default && (
              <Button
                variant="ghost"
                size="sm"
                onClick={() => onDelete(agent)}
                className="text-muted-foreground/70 hover:text-destructive h-7 w-7 p-0"
              >
                <Trash2 size={13} />
              </Button>
            )}
          </div>
        </div>

        <p className="relative text-sm text-muted-foreground mt-2.5 leading-relaxed">
          {agent.persona}
        </p>
      </div>
    </motion.div>
  )
}

export default function AgentsPage() {
  const [agents, setAgents] = useState([])
  const [loading, setLoading] = useState(true)
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editingAgent, setEditingAgent] = useState(null)
  const [deleteTarget, setDeleteTarget] = useState(null)

  const [defaultPersona, setDefaultPersona] = useState('')
  const [defaultInstructions, setDefaultInstructions] = useState('')
  const [agentModels, setAgentModels] = useState([])

  useEffect(() => {
    loadAgents()
    getAvailableModels()
      .then(m => setAgentModels(m.agent || []))
      .catch(() => {})

    getAgentDefaults()
      .then(defaults => {
        setDefaultPersona(defaults.default_persona || '')
        setDefaultInstructions(defaults.default_instructions || '')
      })
      .catch(() => {})
  }, [])

  async function loadAgents() {
    try {
      const data = await listAgents()
      setAgents(data.agents || [])
    } catch {
      toast.error('Failed to load agents')
    } finally {
      setLoading(false)
    }
  }

  function openCreate() {
    setEditingAgent(null)
    setDialogOpen(true)
  }

  function openEdit(agent) {
    setEditingAgent(agent)
    setDialogOpen(true)
  }

  async function handleDelete(agent) {
    setDeleteTarget(agent)
  }

  async function confirmDelete() {
    if (!deleteTarget) return
    try {
      await deleteAgent(deleteTarget.id)
      toast.success('Agent deleted')
      await loadAgents()
    } catch (err) {
      toast.error(err.message)
    } finally {
      setDeleteTarget(null)
    }
  }

  async function handleSetDefault(agentId) {
    try {
      await setDefaultAgent(agentId)
      toast.success('Default agent updated')
      await loadAgents()
    } catch (err) {
      toast.error(err.message)
    }
  }

  return (
    <div className="flex flex-col h-full relative">
      <div className="border-b border-border/60 p-6 relative">
        <div className="max-w-2xl mx-auto flex items-center justify-between">
          <motion.div
            initial={{ opacity: 0, x: -12 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
          >
            <h1 className="text-lg font-semibold text-foreground tracking-tight">Agents</h1>
            <p className="text-sm text-muted-foreground">Manage your AI assistants</p>
          </motion.div>
          <motion.div
            initial={{ opacity: 0, x: 12 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
          >
            <Button
              onClick={openCreate}
              className="rounded-xl shadow-lg shadow-primary/10 hover:shadow-primary/20 transition-shadow duration-300"
            >
              <Plus size={16} className="mr-2" />
              New Agent
            </Button>
          </motion.div>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-6 relative">
        <div className="max-w-2xl mx-auto">
          {loading ? (
            <div className="space-y-4">
              {[...Array(2)].map((_, i) => (
                <Skeleton key={i} className="h-28 rounded-2xl opacity-50" />
              ))}
            </div>
          ) : agents.length === 0 ? (
            <motion.div
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ duration: 0.5, ease: [0.22, 1, 0.36, 1] }}
              className="text-center py-20"
            >
              <div className="relative inline-flex mb-6">
                <div className="absolute inset-0 bg-primary/10 rounded-full blur-2xl scale-150" />
                <div className="relative glass-container w-20 h-20 rounded-2xl flex items-center justify-center">
                  <Sparkles size={32} className="text-muted-foreground/40" />
                </div>
              </div>
              <h2 className="text-lg font-medium text-foreground mb-2">No agents yet</h2>
              <p className="text-sm text-muted-foreground mb-6 max-w-xs mx-auto">
                Create your first AI assistant with a custom personality.
              </p>
              <Button
                onClick={openCreate}
                variant="outline"
                className="rounded-xl border-border/60 hover:border-primary/40 transition-colors"
              >
                <Sparkles size={14} className="mr-2 text-primary" />
                Create your first agent
              </Button>
            </motion.div>
          ) : (
            <div className="space-y-3">
              <AnimatePresence mode="popLayout">
                {agents.map((agent, i) => (
                  <AgentCard
                    key={agent.id}
                    agent={agent}
                    index={i}
                    onEdit={openEdit}
                    onDelete={handleDelete}
                    onSetDefault={handleSetDefault}
                  />
                ))}
              </AnimatePresence>
            </div>
          )}
        </div>
      </div>

      <AgentEditorModal
        isOpen={dialogOpen}
        onClose={() => {
          setDialogOpen(false)
          setEditingAgent(null)
        }}
        agent={editingAgent}
        defaultPersona={defaultPersona}
        defaultInstructions={defaultInstructions}
        agentModels={agentModels}
        onSave={loadAgents}
      />

      <AlertDialog open={!!deleteTarget} onOpenChange={open => !open && setDeleteTarget(null)}>
        <AlertDialogContent className="bg-card/95 backdrop-blur-xl border-white/[0.08] shadow-2xl shadow-black/40">
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Agent</AlertDialogTitle>
            <AlertDialogDescription>
              Delete "{deleteTarget?.name}"? This cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel className="rounded-xl">Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmDelete}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90 rounded-xl"
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
