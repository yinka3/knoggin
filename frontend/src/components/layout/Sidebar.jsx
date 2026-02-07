import { useSession } from '../../context/SessionContext'
import { Link, useLocation } from 'react-router-dom'
import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  Plus,
  MessageSquare,
  Brain,
  Settings,
  PanelLeftClose,
  PanelLeft,
  Bot,
  Terminal,
  Trash2,
  LayoutDashboard,
  Code2,
} from 'lucide-react'
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
import { deleteSession } from '@/api/sessions'
import { toast } from 'sonner'
import { AnimatePresence, motion } from 'motion/react'

export default function Sidebar({ isOpen, onToggle }) {
  const { sessions, currentSessionId, createSession, selectSession, loadSessions, loading } =
    useSession()
  const [deleteTarget, setDeleteTarget] = useState(null)
  const location = useLocation()

  function handleDeleteClick(e, sessionId) {
    e.stopPropagation()
    setDeleteTarget(sessionId)
  }

  async function confirmDelete() {
    if (!deleteTarget) return
    try {
      await deleteSession(deleteTarget, true)
      await loadSessions()
      if (currentSessionId === deleteTarget) {
        selectSession(null)
      }
      toast.success('Session deleted')
    } catch (err) {
      toast.error(err.message || 'Failed to delete session')
    } finally {
      setDeleteTarget(null)
    }
  }

  return (
    <div
      className={`${isOpen ? 'w-64' : 'w-14'} border-r border-border flex flex-col bg-sidebar/50 backdrop-blur-xl transition-all duration-300 ease-in-out z-20`}
    >
      {/* Header */}
      <div
        className={`flex items-center ${isOpen ? 'justify-between' : 'justify-center'} p-3 border-b border-border h-14`}
      >
        {isOpen && (
          <Link to="/" className="flex items-center gap-2 group">
            <div className="relative">
              <Brain
                size={20}
                className="text-primary transition-all duration-300 group-hover:scale-110"
              />
              <div className="absolute inset-0 bg-primary/30 blur-md rounded-full opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
            </div>
            <span className="text-sm font-bold tracking-tight bg-gradient-to-r from-foreground to-foreground/70 bg-clip-text text-transparent group-hover:from-primary group-hover:to-primary/70 transition-all duration-300">
              Knoggin
            </span>
          </Link>
        )}
        <button
          onClick={onToggle}
          className="p-1.5 rounded-md text-muted-foreground hover:text-foreground hover:bg-muted/80 transition-all duration-200 hover:scale-105 active:scale-95"
        >
          {isOpen ? <PanelLeftClose size={18} /> : <PanelLeft size={18} />}
        </button>
      </div>

      {/* Nav links */}
      <div className={`p-2 space-y-1 ${!isOpen && 'flex flex-col items-center'}`}>
        {/* CHAT BUTTON */}
        <Link to="/chat" className="w-full">
          <button
            className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-md text-sm transition-all duration-200 group relative ${
              location.pathname.startsWith('/chat')
                ? 'bg-primary/10 text-primary font-medium'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
            }`}
          >
            {/* ANIMATION: Scales up and tilts left */}
            <MessageSquare
              size={18}
              className="transition-transform duration-300 ease-out group-hover:scale-110 group-hover:-rotate-6 group-hover:text-primary"
            />
            <span
              className={`whitespace-nowrap overflow-hidden transition-all duration-200 ${isOpen ? 'opacity-100 w-auto' : 'opacity-0 w-0'}`}
            >
              Chat
            </span>
          </button>
        </Link>

        <Link to="/dashboard" className="w-full">
          <button
            className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-md text-sm transition-all duration-200 group relative ${
              location.pathname === '/dashboard'
                ? 'bg-primary/10 text-primary font-medium'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
            }`}
          >
            <LayoutDashboard
              size={18}
              className="transition-transform duration-300 ease-out group-hover:scale-110 group-hover:text-primary"
            />
            <span
              className={`whitespace-nowrap overflow-hidden transition-all duration-200 ${isOpen ? 'opacity-100 w-auto' : 'opacity-0 w-0'}`}
            >
              Dashboard
            </span>
          </button>
        </Link>

        {/* MEMORY BUTTON */}
        <Link to="/memory" className="w-full">
          <button
            className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-md text-sm transition-all duration-200 group relative ${
              location.pathname === '/memory'
                ? 'bg-primary/10 text-primary font-medium'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
            }`}
          >
            {/* ANIMATION: Scales up and tilts right */}
            <Brain
              size={18}
              className="transition-transform duration-300 ease-out group-hover:scale-110 group-hover:rotate-6 group-hover:text-primary"
            />
            <span
              className={`whitespace-nowrap overflow-hidden transition-all duration-200 ${isOpen ? 'opacity-100 w-auto' : 'opacity-0 w-0'}`}
            >
              Memory
            </span>
          </button>
        </Link>

        {/* AGENTS BUTTON */}
        <Link to="/agents" className="w-full">
          <button
            className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-md text-sm transition-all duration-200 group relative ${
              location.pathname === '/agents'
                ? 'bg-primary/10 text-primary font-medium'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
            }`}
          >
            <Bot
              size={18}
              className="transition-transform duration-300 ease-out group-hover:scale-110 group-hover:text-primary"
            />
            <span
              className={`whitespace-nowrap overflow-hidden transition-all duration-200 ${isOpen ? 'opacity-100 w-auto' : 'opacity-0 w-0'}`}
            >
              Agents
            </span>
          </button>
        </Link>

        {/* DEBUG BUTTON */}
        <Link to="/debug" className="w-full">
          <button
            className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-md text-sm transition-all duration-200 group relative ${
              location.pathname === '/debug'
                ? 'bg-primary/10 text-primary font-medium'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
            }`}
          >
            <Terminal
              size={18}
              className="transition-transform duration-300 ease-out group-hover:scale-110 group-hover:text-primary"
            />
            <span
              className={`whitespace-nowrap overflow-hidden transition-all duration-200 ${isOpen ? 'opacity-100 w-auto' : 'opacity-0 w-0'}`}
            >
              Debug
            </span>
          </button>
        </Link>
      </div>

      {/* New chat button */}
      <div className={`p-2 ${!isOpen && 'flex justify-center'}`}>
        <Button
          variant="outline"
          onClick={createSession}
          className={`${isOpen ? 'w-full justify-start' : 'w-10 justify-center px-0'} rounded-md border-primary/20 hover:border-primary/50 text-primary hover:bg-primary/5 transition-all shadow-none group`}
        >
          {/* ANIMATION: Rotates 90 degrees */}
          <Plus
            size={18}
            className={`transition-transform duration-300 ease-out group-hover:rotate-90 ${isOpen ? 'mr-2' : ''}`}
          />
          <span
            className={`whitespace-nowrap overflow-hidden transition-all duration-200 ${isOpen ? 'opacity-100 w-auto' : 'opacity-0 w-0'}`}
          >
            New Chat
          </span>
        </Button>
      </div>

      {/* Sessions list */}
      {isOpen && (
        <ScrollArea className="flex-1 px-2">
          <div className="py-2 animate-in fade-in slide-in-from-left-2 duration-300">
            <span className="px-2 text-[10px] uppercase font-bold text-muted-foreground/60 tracking-wider">
              Recent
            </span>
            <div className="mt-2 space-y-0.5">
              {loading ? (
                <div className="space-y-2 px-1">
                  {[...Array(3)].map((_, i) => (
                    <div key={i} className="h-8 rounded-md bg-muted/40 animate-pulse" />
                  ))}
                </div>
              ) : sessions.length === 0 ? (
                <div className="px-2 text-sm text-muted-foreground italic">No sessions</div>
              ) : (
                <AnimatePresence initial={false}>
                  {sessions.map(session => (
                    <motion.div
                      key={session.session_id}
                      layout
                      exit={{ opacity: 0, height: 0, marginBottom: 0 }}
                      transition={{ duration: 0.15, ease: 'easeOut' }}
                      className="group relative"
                    >
                      <button
                        onClick={() => selectSession(session.session_id)}
                        className={`w-full flex items-center gap-2 px-2 py-2 rounded-md text-sm truncate transition-all duration-200 ${
                          currentSessionId === session.session_id
                            ? 'bg-muted font-medium text-foreground'
                            : 'text-muted-foreground hover:bg-muted/50 hover:text-foreground'
                        }`}
                      >
                        <span className="truncate flex-1">
                          {session.title || `Session ${session.session_id.slice(0, 4)}`}
                        </span>
                      </button>

                      <button
                        onClick={e => handleDeleteClick(e, session.session_id)}
                        className="absolute right-1 top-1/2 -translate-y-1/2 p-1.5 rounded-md opacity-0 group-hover:opacity-100 text-muted-foreground hover:text-destructive hover:bg-destructive/10 transition-all duration-200"
                      >
                        <Trash2 size={14} />
                      </button>
                    </motion.div>
                  ))}
                </AnimatePresence>
              )}
            </div>
          </div>
        </ScrollArea>
      )}

      {/* Settings at bottom */}
      <div
        className={`border-t border-border p-2 space-y-1 ${!isOpen && 'flex flex-col items-center'}`}
      >
        <Link to="/settings" className="w-full">
          <button
            className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-md text-sm transition-all duration-200 group ${
              location.pathname === '/settings'
                ? 'bg-primary/10 text-primary'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
            }`}
          >
            <Settings
              size={18}
              className="transition-transform duration-700 ease-out group-hover:rotate-180"
            />
            <span
              className={`whitespace-nowrap overflow-hidden transition-all duration-200 ${isOpen ? 'opacity-100 w-auto' : 'opacity-0 w-0'}`}
            >
              Settings
            </span>
          </button>
        </Link>

        <Link to="/settings/developer" className="w-full">
          <button
            className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-md text-sm transition-all duration-200 group ${
              location.pathname === '/settings/developer'
                ? 'bg-primary/10 text-primary'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
            }`}
          >
            <Code2
              size={18}
              className="transition-transform duration-300 ease-out group-hover:scale-110"
            />
            <span
              className={`whitespace-nowrap overflow-hidden transition-all duration-200 ${isOpen ? 'opacity-100 w-auto' : 'opacity-0 w-0'}`}
            >
              Developer
            </span>
          </button>
        </Link>
      </div>

      <AlertDialog open={!!deleteTarget} onOpenChange={open => !open && setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete session?</AlertDialogTitle>
            <AlertDialogDescription>
              This cannot be undone. All messages in this session will be permanently deleted.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={confirmDelete}>Delete</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
