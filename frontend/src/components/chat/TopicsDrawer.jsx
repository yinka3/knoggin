import { useState, useEffect } from 'react'
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from '@/components/ui/sheet'
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
import { Settings, Plus, Trash2 } from 'lucide-react'
import { createTopic, getTopics, updateTopic, deleteTopic } from '../../api/topics'

export default function TopicsDrawer({ sessionId }) {
  const [open, setOpen] = useState(false)
  const [topics, setTopics] = useState({})
  const [activeTopics, setActiveTopics] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  
  // Edit dialog state
  const [editOpen, setEditOpen] = useState(false)
  const [editTopic, setEditTopic] = useState({ name: '', labels: '', aliases: '', active: true })
  const [isNew, setIsNew] = useState(false)
  const [saving, setSaving] = useState(false)
  
  // Delete dialog state
  const [deleteOpen, setDeleteOpen] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState(null)

  async function loadTopics() {
    if (!sessionId) return
    setLoading(true)
    setError(null)
    try {
      const data = await getTopics(sessionId)
      setTopics(data.topics || {})
      setActiveTopics(data.active_topics || [])
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    if (open && sessionId) {
      loadTopics()
    }
  }, [open, sessionId])

  function openNewTopic() {
    setEditTopic({ name: '', labels: '', aliases: '', hierarchy: '', labelAliases: '', active: true })
    setIsNew(true)
    setEditOpen(true)
  }

  function openEditTopic(name) {
    const config = topics[name]
    setEditTopic({
      name,
      labels: config.labels?.join(', ') || '',
      aliases: config.aliases?.join(', ') || '',
      hierarchy: Object.keys(config.hierarchy || {}).length > 0 
        ? JSON.stringify(config.hierarchy, null, 2) 
        : '',
      labelAliases: Object.keys(config.label_aliases || {}).length > 0 
        ? JSON.stringify(config.label_aliases, null, 2) 
        : '',
      active: config.active !== false
    })
    setIsNew(false)
    setEditOpen(true)
  }

  async function handleSave() {
    setSaving(true)
    try {
      let hierarchyObj = {}
      let labelAliasesObj = {}
      
      try {
        if (editTopic.hierarchy.trim()) {
          hierarchyObj = JSON.parse(editTopic.hierarchy)
        }
      } catch {
        setError('Invalid hierarchy JSON')
        setSaving(false)
        return
      }
      
      try {
        if (editTopic.labelAliases.trim()) {
          labelAliasesObj = JSON.parse(editTopic.labelAliases)
        }
      } catch {
        setError('Invalid label aliases JSON')
        setSaving(false)
        return
      }

      if (isNew) {
        await createTopic(sessionId, {
          name: editTopic.name.trim(),
          labels: editTopic.labels ? editTopic.labels.split(',').map(s => s.trim()).filter(Boolean) : [],
          aliases: editTopic.aliases ? editTopic.aliases.split(',').map(s => s.trim()).filter(Boolean) : [],
          hierarchy: hierarchyObj,
          label_aliases: labelAliasesObj,
          active: editTopic.active
        })
      } else {
        await updateTopic(sessionId, editTopic.name, {
          labels: editTopic.labels ? editTopic.labels.split(',').map(s => s.trim()).filter(Boolean) : [],
          aliases: editTopic.aliases ? editTopic.aliases.split(',').map(s => s.trim()).filter(Boolean) : [],
          hierarchy: hierarchyObj,
          label_aliases: labelAliasesObj,
          active: editTopic.active
        })
      }
      setEditOpen(false)
      setError(null)
      await loadTopics()
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
    } catch (err) {
      setError(err.message)
    }
  }

  const topicNames = Object.keys(topics)

  return (
    <>
      <Sheet open={open} onOpenChange={setOpen} modal={false}>
        <SheetTrigger asChild>
          <Button variant="ghost" size="sm" className="text-muted-foreground hover:text-accent">
            <Settings size={18} />
          </Button>
        </SheetTrigger>
        <SheetContent className="bg-background border-border w-72">
          <SheetHeader>
            <SheetTitle className="text-foreground">Topics</SheetTitle>
            <SheetDescription className="text-xs text-muted-foreground font-mono">
              {sessionId?.slice(0, 8)}...
            </SheetDescription>
          </SheetHeader>

          {loading && <p className="text-muted-foreground text-sm mt-4">Loading...</p>}
          {error && <p className="text-destructive text-sm mt-4">{error}</p>}

          {!loading && (
            <div className="mt-6 space-y-1">
              {topicNames.map((name) => {
                const config = topics[name]
                const isActive = config.active !== false
                
                return (
                  <button
                    key={name}
                    onClick={() => openEditTopic(name)}
                    className={`w-full flex items-center gap-3 px-3 py-2 rounded-lg text-left text-sm transition-colors hover:bg-muted ${
                      isActive ? 'text-foreground' : 'text-muted-foreground'
                    }`}
                  >
                    <span className={`w-2 h-2 rounded-full ${isActive ? 'bg-accent' : 'bg-muted-foreground/30'}`} />
                    <span>{name}</span>
                  </button>
                )
              })}

              <button
                onClick={openNewTopic}
                className="w-full flex items-center gap-3 px-3 py-2 rounded-lg text-left text-sm text-muted-foreground hover:text-accent hover:bg-muted transition-colors mt-4"
              >
                <Plus size={14} />
                <span>Add Topic</span>
              </button>
            </div>
          )}
        </SheetContent>
      </Sheet>

      {/* Edit/Create Dialog */}
      <Dialog open={editOpen} onOpenChange={setEditOpen}>
        <DialogContent className="bg-background border-border sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="text-foreground">
              {isNew ? 'New Topic' : editTopic.name}
            </DialogTitle>
            <DialogDescription className="text-muted-foreground text-sm">
              {isNew ? 'Create a new topic' : 'Edit topic configuration'}
            </DialogDescription>
          </DialogHeader>
          
          <div className="space-y-4 py-4">
            {isNew && (
              <div className="space-y-2">
                <Label htmlFor="name" className="text-muted-foreground">Name</Label>
                <Input
                  id="name"
                  value={editTopic.name}
                  onChange={(e) => setEditTopic({ ...editTopic, name: e.target.value })}
                  placeholder="Work"
                  className="bg-muted border-border"
                />
              </div>
            )}
            
            <div className="space-y-2">
              <Label htmlFor="labels" className="text-muted-foreground">
                Labels <span className="text-xs">(comma-separated)</span>
              </Label>
              <Input
                id="labels"
                value={editTopic.labels}
                onChange={(e) => setEditTopic({ ...editTopic, labels: e.target.value })}
                placeholder="Company, Project, Person"
                className="bg-muted border-border"
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="aliases" className="text-muted-foreground">
                Aliases <span className="text-xs">(comma-separated)</span>
              </Label>
              <Input
                id="aliases"
                value={editTopic.aliases}
                onChange={(e) => setEditTopic({ ...editTopic, aliases: e.target.value })}
                placeholder="job, office"
                className="bg-muted border-border"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="hierarchy" className="text-muted-foreground">
                Hierarchy <span className="text-xs">(JSON)</span>
              </Label>
              <textarea
                id="hierarchy"
                value={editTopic.hierarchy}
                onChange={(e) => setEditTopic({ ...editTopic, hierarchy: e.target.value })}
                placeholder='{"parent": "child"}'
                rows={2}
                className="w-full bg-muted border border-border rounded-lg px-3 py-2 text-sm text-foreground placeholder-muted-foreground resize-none focus:outline-none focus:border-accent font-mono"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="labelAliases" className="text-muted-foreground">
                Label Aliases <span className="text-xs">(JSON)</span>
              </Label>
              <textarea
                id="labelAliases"
                value={editTopic.labelAliases}
                onChange={(e) => setEditTopic({ ...editTopic, labelAliases: e.target.value })}
                placeholder='{"OldLabel": "NewLabel"}'
                rows={2}
                className="w-full bg-muted border border-border rounded-lg px-3 py-2 text-sm text-foreground placeholder-muted-foreground resize-none focus:outline-none focus:border-accent font-mono"
              />
            </div>
            
            <div className="flex items-center justify-between py-2">
              <Label htmlFor="active" className="text-muted-foreground">Active</Label>
              <Switch
                id="active"
                checked={editTopic.active}
                onCheckedChange={(checked) => setEditTopic({ ...editTopic, active: checked })}
                disabled={editTopic.name === 'General'}
              />
            </div>
          </div>
          
          <div className="flex items-center justify-between">
            {!isNew && editTopic.name !== 'General' && (
              <Button
                variant="ghost"
                size="sm"
                onClick={() => confirmDelete(editTopic.name)}
                className="text-muted-foreground hover:text-destructive"
              >
                <Trash2 size={14} className="mr-1" />
                Delete
              </Button>
            )}
            <div className="flex-1" />
            <Button
              onClick={handleSave}
              disabled={saving || (isNew && !editTopic.name.trim())}
              className="bg-primary text-primary-foreground hover:bg-accent"
            >
              {saving ? 'Saving...' : 'Save'}
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation */}
      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent className="bg-background border-border">
          <AlertDialogHeader>
            <AlertDialogTitle className="text-foreground">Delete "{deleteTarget}"?</AlertDialogTitle>
            <AlertDialogDescription className="text-muted-foreground">
              This will remove the topic configuration. Entities tagged with this topic will become uncategorized.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel className="border-border text-muted-foreground">Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleDelete}
              className="bg-destructive hover:bg-destructive/90 text-white"
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}