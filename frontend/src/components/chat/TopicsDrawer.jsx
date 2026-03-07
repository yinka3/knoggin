import { useState, useEffect, useCallback } from 'react'
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '@/components/ui/sheet'
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
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { toast } from 'sonner'
import { Plus, Trash2, Pencil, ArrowRight, X, Flame } from 'lucide-react'
import { cn } from '@/lib/utils'
import { createTopic, getTopics, updateTopic, deleteTopic } from '../../api/topics'
import HierarchyEditor from '@/components/HierarchyEditor'

export default function TopicsDrawer({ sessionId, open, onOpenChange }) {
  const [topics, setTopics] = useState({})
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const [editOpen, setEditOpen] = useState(false)
  const [editTopic, setEditTopic] = useState({ name: '', labels: '', aliases: '', active: true, hot: false, hierarchy: {} })
  const [saving, setSaving] = useState(false)

  const [deleteOpen, setDeleteOpen] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState(null)

  // Quick-add state
  const [addingNew, setAddingNew] = useState(false)
  const [newTopicName, setNewTopicName] = useState('')

  const loadTopics = useCallback(async () => {
    if (!sessionId) return
    setLoading(true)
    setError(null)
    try {
      const data = await getTopics(sessionId)
      setTopics(data.topics || {})
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }, [sessionId])

  useEffect(() => {
    if (open && sessionId) {
      loadTopics()
    }
  }, [open, sessionId, loadTopics])

  // Toggle a topic's active state inline
  async function toggleTopic(name) {
    const current = topics[name]
    const newActive = current.active === false ? true : false
    try {
      await updateTopic(sessionId, name, { active: newActive })
      setTopics(prev => ({
        ...prev,
        [name]: { ...prev[name], active: newActive }
      }))
    } catch (err) {
      toast.error(err.message)
    }
  }

  // Quick-add a new topic
  async function handleQuickAdd() {
    const trimmed = newTopicName.trim()
    if (!trimmed) return
    if (topics[trimmed]) {
      toast.error('Topic already exists')
      return
    }
    try {
      await createTopic(sessionId, { name: trimmed, labels: [], aliases: [], hierarchy: {}, active: true })
      setNewTopicName('')
      setAddingNew(false)
      await loadTopics()
      toast.success('Topic created')
    } catch (err) {
      toast.error(err.message)
    }
  }

  function openEditTopic(name) {
    const config = topics[name]
    setEditTopic({
      name,
      labels: config.labels?.join(', ') || '',
      aliases: config.aliases?.join(', ') || '',
      hierarchy: config.hierarchy || {},
      active: config.active !== false,
      hot: config.hot || false,
    })
    setEditOpen(true)
  }

  async function handleSave() {
    setSaving(true)
    try {
      const payload = {
        labels: editTopic.labels
          ? editTopic.labels
              .split(',')
              .map(s => s.trim())
              .filter(Boolean)
          : [],
        aliases: editTopic.aliases
          ? editTopic.aliases
              .split(',')
              .map(s => s.trim())
              .filter(Boolean)
          : [],
        hierarchy: editTopic.hierarchy,
        active: editTopic.active,
        hot: editTopic.hot,
      }

      await updateTopic(sessionId, editTopic.name, payload)

      setEditOpen(false)
      setError(null)
      await loadTopics()
      toast.success('Topic updated')
    } catch (err) {
      setError(err.message)
    } finally {
      setSaving(false)
    }
  }

  function confirmDelete(name) {
    setDeleteTarget(name)
    setDeleteOpen(true)
  }

  async function handleDelete() {
    if (!deleteTarget) return
    try {
      await deleteTopic(sessionId, deleteTarget)
      setDeleteOpen(false)
      setEditOpen(false)
      setDeleteTarget(null)
      await loadTopics()
      toast.success('Topic deleted')
    } catch (err) {
      setError(err.message)
    }
  }

  const topicNames = Object.keys(topics)
  const activeCount = topicNames.filter(n => topics[n].active !== false).length

  return (
    <>
      <Sheet open={open} onOpenChange={onOpenChange}>
        <SheetContent className="bg-background border-border w-80 sm:w-96 px-6">
          <SheetHeader>
            <SheetTitle className="flex items-center gap-2">
              <span>Topics</span>
              <Badge variant="outline" className="text-[10px] font-normal">
                {activeCount}/{topicNames.length}
              </Badge>
            </SheetTitle>
          </SheetHeader>

          {loading && (
            <div className="mt-6 space-y-2">
              {[1, 2, 3].map(i => (
                <div
                  key={i}
                  className="h-8 bg-muted-foreground/10 rounded-full animate-pulse"
                  style={{ width: `${50 + i * 12}%` }}
                />
              ))}
            </div>
          )}

          {error && <p className="text-destructive text-sm mt-4">{error}</p>}

          {!loading && (
            <div className="mt-8">
              {/* Topic Chips */}
              <div className="flex flex-wrap gap-x-3 gap-y-4">
                {topicNames.map(name => {
                  const isActive = topics[name].active !== false
                  const labelCount = topics[name].labels?.length || 0

                  return (
                    <div key={name} className="group relative">
                      <button
                        onClick={() => toggleTopic(name)}
                        className={cn(
                          'inline-flex items-center gap-2 px-3.5 py-2 rounded-xl text-sm font-medium transition-all duration-300 border shadow-sm',
                          isActive
                            ? 'bg-primary/10 text-primary border-primary/20 hover:bg-primary/20 hover:border-primary/40 hover:scale-[1.02] active:scale-[0.98]'
                            : 'bg-muted/10 text-muted-foreground/40 border-border/20 hover:bg-muted/20 hover:text-muted-foreground/60 hover:border-border/40 hover:scale-[1.02] active:scale-[0.98]'
                        )}
                      >
                        <span className="tracking-tight">{name}</span>
                        {topics[name].hot && (
                          <Flame size={11} className="text-orange-500" />
                        )}
                        {labelCount > 0 && (
                          <span className={cn(
                            "flex items-center justify-center min-w-[18px] h-[18px] px-1 rounded-md text-[10px] font-bold transition-colors",
                            isActive 
                              ? "bg-primary/20 text-primary/80" 
                              : "bg-muted-foreground/10 text-muted-foreground/40"
                          )}>
                            {labelCount}
                          </span>
                        )}
                      </button>
                      
                      {/* Edit icon - more subtle and positioned better */}
                      <button
                        onClick={(e) => { e.stopPropagation(); openEditTopic(name) }}
                        className="absolute -top-1.5 -right-1.5 w-6 h-6 rounded-lg bg-background/80 backdrop-blur-sm border border-border/50 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-all duration-200 hover:bg-primary/10 hover:border-primary/30 hover:scale-110 shadow-lg"
                      >
                        <Pencil size={10} className="text-muted-foreground/70" />
                      </button>
                    </div>
                  )
                })}

                {/* Quick-add chip */}
                {addingNew ? (
                  <form
                    onSubmit={e => { e.preventDefault(); handleQuickAdd() }}
                    className="inline-flex items-center"
                  >
                    <Input
                      autoFocus
                      value={newTopicName}
                      onChange={e => setNewTopicName(e.target.value)}
                      onBlur={() => { if (!newTopicName.trim()) setAddingNew(false) }}
                      placeholder="Topic name"
                      className="h-9 w-32 text-sm rounded-xl px-4 bg-muted/20 border-border/30 focus:ring-1 focus:ring-primary/20 transition-all"
                    />
                  </form>
                ) : (
                  <button
                    onClick={() => setAddingNew(true)}
                    className="inline-flex items-center gap-1.5 px-4 py-2 rounded-xl text-sm font-medium text-muted-foreground/30 border border-dashed border-muted-foreground/10 hover:border-muted-foreground/30 hover:text-muted-foreground/50 hover:bg-muted/20 transition-all duration-300 hover:scale-[1.02] active:scale-[0.98]"
                  >
                    <Plus size={14} className="opacity-70" />
                    <span>Add</span>
                  </button>
                )}
              </div>

              {/* Hint */}
              <div className="flex items-center gap-2 mt-6 px-1">
                <div className="h-px flex-1 bg-gradient-to-r from-border/5 to-transparent" />
                <p className="text-[10px] font-medium tracking-wider uppercase text-muted-foreground/30">
                  Tap to toggle · hover to edit
                </p>
              </div>
            </div>
          )}
        </SheetContent>
      </Sheet>

      {/* Edit Dialog */}
      <Dialog open={editOpen} onOpenChange={setEditOpen}>
        <DialogContent className="bg-background border-border sm:max-w-md">
          <DialogHeader>
            <DialogTitle>{editTopic.name}</DialogTitle>
            <DialogDescription className="text-xs">
              Edit topic configuration
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <Label htmlFor="labels" className="text-sm flex items-center gap-2">
                Labels
                <span className="text-[10px] text-muted-foreground font-normal bg-muted px-1.5 py-0.5 rounded">
                  comma-separated
                </span>
              </Label>
              <Input
                id="labels"
                value={editTopic.labels}
                onChange={e => setEditTopic({ ...editTopic, labels: e.target.value })}
                placeholder="Company, Project, Person"
                className="bg-muted/50 border-border/50 h-9 rounded-lg"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="aliases" className="text-sm flex items-center gap-2">
                Aliases
                <span className="text-[10px] text-muted-foreground font-normal bg-muted px-1.5 py-0.5 rounded">
                  comma-separated
                </span>
              </Label>
              <Input
                id="aliases"
                value={editTopic.aliases}
                onChange={e => setEditTopic({ ...editTopic, aliases: e.target.value })}
                placeholder="job, office"
                className="bg-muted/50 border-border/50 h-9 rounded-lg"
              />
            </div>

            {/* Visual Hierarchy Editor */}
            <HierarchyEditor
              name={editTopic.name}
              config={editTopic}
              updateField={(name, field, val) => setEditTopic(prev => ({ ...prev, [field]: val }))}
            />

            <div className="flex items-center justify-between p-3 rounded-lg border border-border/30 bg-white/[0.01]">
              <div>
                <Label htmlFor="hot" className="text-sm flex items-center gap-1.5">
                  <Flame size={13} className="text-orange-500" />
                  Hot Topic
                </Label>
                <p className="text-[10px] text-muted-foreground">Pre-fetch context for this topic every message</p>
              </div>
              <Switch
                id="hot"
                checked={editTopic.hot}
                onCheckedChange={checked => setEditTopic({ ...editTopic, hot: checked })}
              />
            </div>

            <div className="flex items-center justify-between p-3 rounded-lg border border-border/30 bg-white/[0.01]">
              <div>
                <Label htmlFor="active" className="text-sm">
                  Active
                </Label>
                <p className="text-[10px] text-muted-foreground">Enable or disable this topic</p>
              </div>
              <Switch
                id="active"
                checked={editTopic.active}
                onCheckedChange={checked => setEditTopic({ ...editTopic, active: checked })}
              />
            </div>
          </div>

          <div className="flex items-center justify-between pt-2 border-t border-border/30">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => confirmDelete(editTopic.name)}
              className="text-muted-foreground hover:text-destructive"
            >
              <Trash2 size={14} className="mr-1" />
              Delete
            </Button>
            <div className="flex-1" />
            <Button
              onClick={handleSave}
              disabled={saving}
              size="sm"
            >
              {saving ? 'Saving...' : 'Save'}
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation */}
      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete "{deleteTarget}"?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently delete the topic. Consider disabling it instead.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleDelete}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}
