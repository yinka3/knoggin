import { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
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
import { Plus, Star, Pencil, Trash2, Bot } from 'lucide-react'
import { listAgents, createAgent, updateAgent, deleteAgent, setDefaultAgent } from '@/api/agents'
import { getAvailableModels } from '@/api/config'
import { toast } from 'sonner'

function AgentCard({ agent, onEdit, onDelete, onSetDefault }) {
  return (
    <div className="group relative bg-card border border-border rounded-xl p-4 hover:border-primary/30 transition-colors">
      {/* Default badge */}
      {agent.is_default && (
        <Badge className="absolute -top-2 -right-2 bg-primary text-primary-foreground text-[10px]">
          <Star size={10} className="mr-1" />
          Default
        </Badge>
      )}

      <div className="flex items-start gap-3">
        <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center shrink-0">
          <Bot size={20} className="text-primary" />
        </div>

        <div className="flex-1 min-w-0">
          <h3 className="font-medium text-foreground truncate">{agent.name}</h3>
          <p className="text-sm text-muted-foreground mt-1 line-clamp-2">{agent.persona}</p>
          {agent.model && (
            <p className="text-xs text-muted-foreground/70 mt-2 font-mono">{agent.model}</p>
          )}
        </div>
      </div>

      {/* Actions */}
      <div className="flex items-center gap-2 mt-4 pt-3 border-t border-border">
        {!agent.is_default && (
          <Button
            variant="ghost"
            size="sm"
            onClick={() => onSetDefault(agent.id)}
            className="text-muted-foreground hover:text-primary text-xs"
          >
            <Star size={14} className="mr-1" />
            Set Default
          </Button>
        )}
        <div className="flex-1" />
        <Button
          variant="ghost"
          size="sm"
          onClick={() => onEdit(agent)}
          className="text-muted-foreground hover:text-foreground"
        >
          <Pencil size={14} />
        </Button>
        {!agent.is_default && (
          <Button
            variant="ghost"
            size="sm"
            onClick={() => onDelete(agent)}
            className="text-muted-foreground hover:text-destructive"
          >
            <Trash2 size={14} />
          </Button>
        )}
      </div>
    </div>
  )
}

export default function AgentsPage() {
  const [agents, setAgents] = useState([])
  const [loading, setLoading] = useState(true)
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editingAgent, setEditingAgent] = useState(null)
  const [saving, setSaving] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState(null)

  const [name, setName] = useState('')
  const [persona, setPersona] = useState('')
  const [model, setModel] = useState('')
  const [agentModels, setAgentModels] = useState([])

  useEffect(() => {
    loadAgents()
    getAvailableModels()
      .then(m => setAgentModels(m.agent || []))
      .catch(() => {})
  }, [])

  async function loadAgents() {
    try {
      const data = await listAgents()
      setAgents(data.agents || [])
    } catch (err) {
      toast.error('Failed to load agents')
    } finally {
      setLoading(false)
    }
  }

  function openCreate() {
    setEditingAgent(null)
    setName('')
    setPersona('')
    setModel('')
    setDialogOpen(true)
  }

  function openEdit(agent) {
    setEditingAgent(agent)
    setName(agent.name)
    setPersona(agent.persona)
    setModel(agent.model || '')
    setDialogOpen(true)
  }

  async function handleSave() {
    if (!name.trim() || !persona.trim()) {
      toast.error('Name and persona are required')
      return
    }

    setSaving(true)
    try {
      if (editingAgent) {
        await updateAgent(editingAgent.id, {
          name: name.trim(),
          persona: persona.trim(),
          model: model.trim() || null,
        })
        toast.success('Agent updated')
      } else {
        await createAgent({
          name: name.trim(),
          persona: persona.trim(),
          model: model.trim() || null,
        })
        toast.success('Agent created')
      }
      setDialogOpen(false)
      await loadAgents()
    } catch (err) {
      toast.error(err.message)
    } finally {
      setSaving(false)
    }
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
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="border-b border-border p-4">
        <div className="max-w-4xl mx-auto flex items-center justify-between">
          <div>
            <h1 className="text-lg font-medium text-foreground">Agents</h1>
            <p className="text-sm text-muted-foreground">Manage your AI assistants</p>
          </div>
          <Button onClick={openCreate} className="rounded-xl">
            <Plus size={16} className="mr-2" />
            New Agent
          </Button>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4">
        <div className="max-w-4xl mx-auto">
          {loading ? (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {[...Array(2)].map((_, i) => (
                <Skeleton key={i} className="h-40 rounded-xl" />
              ))}
            </div>
          ) : agents.length === 0 ? (
            <div className="text-center py-12">
              <Bot size={48} className="mx-auto text-muted-foreground/30 mb-4" />
              <p className="text-muted-foreground">No agents yet</p>
              <Button onClick={openCreate} variant="outline" className="mt-4">
                Create your first agent
              </Button>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {agents.map(agent => (
                <AgentCard
                  key={agent.id}
                  agent={agent}
                  onEdit={openEdit}
                  onDelete={handleDelete}
                  onSetDefault={handleSetDefault}
                />
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Create/Edit Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="bg-background border-border sm:max-w-md">
          <DialogHeader>
            <DialogTitle>{editingAgent ? 'Edit Agent' : 'New Agent'}</DialogTitle>
            <DialogDescription>
              {editingAgent
                ? 'Update the agent configuration'
                : 'Create a new AI assistant with a custom personality'}
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <Label htmlFor="name" className="text-muted-foreground">
                Name
              </Label>
              <Input
                id="name"
                value={name}
                onChange={e => setName(e.target.value)}
                placeholder="Agent"
                className="bg-muted border-border rounded-xl"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="persona" className="text-muted-foreground">
                Persona
              </Label>
              <textarea
                id="persona"
                value={persona}
                onChange={e => setPersona(e.target.value)}
                placeholder="Warm and direct. Match their energy. No corporate filler."
                rows={4}
                className="w-full bg-muted border border-border rounded-xl px-3 py-2 text-sm text-foreground placeholder-muted-foreground resize-none focus:outline-none focus:border-primary focus:ring-1 focus:ring-primary/30 transition-colors"
              />
              <p className="text-[11px] text-muted-foreground">
                Describe how this agent should communicate
              </p>
            </div>

             <div className="space-y-2">
              <Label htmlFor="model" className="text-muted-foreground">
                Model Override <span className="text-muted-foreground/50">(optional)</span>
              </Label>
              <Select value={model || '__default__'} onValueChange={v => setModel(v === '__default__' ? '' : v)}>
                <SelectTrigger className="bg-muted border-border rounded-xl">
                  <SelectValue placeholder="Use global agent model" />
                </SelectTrigger>
                <SelectContent className="bg-background border-border">
                  <SelectItem value="__default__">Use global agent model</SelectItem>
                  {agentModels.map(m => (
                    <SelectItem key={m.id} value={m.id}>
                      {m.name || m.id}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-[11px] text-muted-foreground">
                Select a model or leave as default to use the global agent model
              </p>
            </div>
          </div>

          <div className="flex justify-end gap-2">
            <Button variant="ghost" onClick={() => setDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleSave} disabled={saving}>
              {saving ? 'Saving...' : editingAgent ? 'Save Changes' : 'Create Agent'}
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      <AlertDialog open={!!deleteTarget} onOpenChange={open => !open && setDeleteTarget(null)}>
        <AlertDialogContent className="bg-background border-border">
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Agent</AlertDialogTitle>
            <AlertDialogDescription>
              Delete "{deleteTarget?.name}"? This cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmDelete}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
