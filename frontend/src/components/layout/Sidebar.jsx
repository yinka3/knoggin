import { useSession } from '../../context/SessionContext'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import React, { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  Plus,
  Brain,
  Settings,
  PanelLeftClose,
  PanelLeft,
  Bot,
  Terminal,
  Trash2,
  LayoutDashboard,
  Code2,
  Users,
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
import { cn } from '@/lib/utils'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'

function SidebarTooltip({ isOpen, label, children }) {
  if (isOpen) return children;
  return (
    <Tooltip delayDuration={0}>
      <TooltipTrigger asChild>{children}</TooltipTrigger>
      <TooltipContent side="right" className="font-medium">{label}</TooltipContent>
    </Tooltip>
  )
}

function SidebarSessionItem({ session, currentSessionId, onSelect, onDelete, loadSessions }) {
  const isDefaultTitle = !session?.title || session.title.startsWith('Session ')
  const initialTitle = session?.title || `Session ${session.session_id?.slice(0, 4)}`

  const [isEditing, setIsEditing] = useState(false)
  const [title, setTitle] = useState(initialTitle)
  const [displayedTitle, setDisplayedTitle] = useState(initialTitle)
  
  const [isTyping, setIsTyping] = useState(false)
  
  // To avoid showing pending state for ALL historical chats that happen to keep default titles,
  // we only show the shimmer if this is the currently active session and it is relatively new.
  // Alternatively, simply checking if it's the active session and has a default title.
  const isPending = isDefaultTitle && !isTyping && session.session_id === currentSessionId

  const inputRef = React.useRef(null)
  const typingIntervalRef = React.useRef(null)

  useEffect(() => {
    const newTitle = session?.title || `Session ${session.session_id?.slice(0, 4)}`
    
    if (newTitle !== title && !newTitle.startsWith('Session ') && title.startsWith('Session ')) {
      setTitle(newTitle)
      setDisplayedTitle('') 
      setIsTyping(true)
      
      let i = 0
      if (typingIntervalRef.current) clearInterval(typingIntervalRef.current)
      
      typingIntervalRef.current = setInterval(() => {
        if (i < newTitle.length) {
          setDisplayedTitle(newTitle.slice(0, i + 1))
          i++
        } else {
          clearInterval(typingIntervalRef.current)
          setIsTyping(false)
        }
      }, 50)
    } else if (newTitle !== title && !isTyping) {
       setTitle(newTitle)
       setDisplayedTitle(newTitle)
    }
    
    return () => {
      if (typingIntervalRef.current) clearInterval(typingIntervalRef.current)
    }
  }, [session?.title, session.session_id, title, isTyping])

  useEffect(() => {
    if (isEditing && inputRef.current) {
      inputRef.current.focus()
    }
  }, [isEditing])

  async function handleSave() {
    setIsEditing(false)
    const newTitle = title.trim()
    if (!newTitle || newTitle === session?.title) {
        setTitle(initialTitle)
        setDisplayedTitle(initialTitle)
        return
    }

    setDisplayedTitle(newTitle)
    try {
      import('@/api/sessions').then(({ updateSession }) => {
        updateSession(session.session_id, { title: newTitle }).then(() => {
          loadSessions()
        })
      })
    } catch (err) {
      console.error('Failed to update session title:', err)
      toast.error('Failed to update session title')
      setTitle(session?.title || `Session ${session.session_id?.slice(0, 4)}`)
      setDisplayedTitle(session?.title || `Session ${session.session_id?.slice(0, 4)}`)
    }
  }

  function handleKeyDown(e) {
    if (e.key === 'Enter') handleSave()
    if (e.key === 'Escape') {
      setIsEditing(false)
      setTitle(initialTitle)
      setDisplayedTitle(initialTitle)
    }
  }

  return (
    <motion.div
      layout
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      exit={{ opacity: 0, scale: 0.95, filter: 'blur(4px)' }}
      transition={{ duration: 0.2, ease: 'easeOut' }}
      className="group relative mb-0.5"
    >
      <button
        onClick={() => {
          if (!isEditing) onSelect()
        }}
        className={cn(
          'w-full text-left px-3 py-2 rounded-lg text-sm transition-all duration-200 flex items-center',
          session.session_id === currentSessionId
            ? 'bg-white/[0.05] border border-primary/20 text-primary'
            : 'text-muted-foreground hover:text-foreground hover:bg-white/[0.03]'
        )}
      >
        {isEditing ? (
          <input
            ref={inputRef}
            value={title}
            onChange={e => setTitle(e.target.value)}
            onBlur={handleSave}
            onKeyDown={handleKeyDown}
            className="flex-1 min-w-0 bg-transparent border-b border-primary/50 focus:outline-none focus:border-primary px-1 -mx-1"
          />
        ) : (
          <div className="flex-1 min-w-0 flex items-center gap-1.5" onClick={(e) => {
            if (session.session_id === currentSessionId && !isPending && !isTyping) {
              e.stopPropagation()
              setIsEditing(true)
            }
          }}>
            <AnimatePresence mode="wait">
              {isPending ? (
                <motion.div
                  key="pending"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  className="flex items-center gap-2 w-full"
                >
                  <div className="h-4 w-16 bg-muted/50 animate-pulse rounded-sm" />
                  <div className="h-4 w-10 bg-muted/50 animate-pulse rounded-sm" />
                </motion.div>
              ) : (
                <motion.span 
                  key="title"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  className="truncate relative"
                >
                  {displayedTitle}
                  {isTyping && (
                    <motion.span
                      animate={{ opacity: [0, 1, 0] }}
                      transition={{ repeat: Infinity, duration: 0.8 }}
                      className="inline-block w-[2px] h-4 bg-primary ml-0.5 align-middle"
                    />
                  )}
                </motion.span>
              )}
            </AnimatePresence>
          </div>
        )}
      </button>

      {!isEditing && (
        <button
          onClick={e => onDelete(e, session.session_id)}
          className="absolute right-1 top-1/2 -translate-y-1/2 p-1.5 rounded-md opacity-0 group-hover:opacity-100 text-muted-foreground hover:text-destructive hover:bg-destructive/10 transition-all duration-200"
        >
          <Trash2 size={14} />
        </button>
      )}
    </motion.div>
  )
}

export default function Sidebar({ isOpen, onToggle }) {
  const { sessions, currentSessionId, createSession, selectSession, loadSessions, loading } =
    useSession()
  const [deleteTarget, setDeleteTarget] = useState(null)
  const location = useLocation()
  const navigate = useNavigate()

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
        navigate('/chat')
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
          <Link to="/chat" className="flex items-center gap-2 group">
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
        {/* DASHBOARD BUTTON */}
        <SidebarTooltip isOpen={isOpen} label="Dashboard">
          <Link to="/dashboard" className="w-full flex justify-center">
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
        </SidebarTooltip>

        {/* MEMORY BUTTON */}
        <SidebarTooltip isOpen={isOpen} label="Memory">
          <Link to="/memory" className="w-full flex justify-center">
            <button
              className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-md text-sm transition-all duration-200 group relative ${
                location.pathname === '/memory'
                  ? 'bg-primary/10 text-primary font-medium'
                  : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
              }`}
            >
              <Brain
                size={18}
                className="transition-transform duration-300 ease-out group-hover:scale-110 group-hover:text-primary"
              />
              <span
                className={`whitespace-nowrap overflow-hidden transition-all duration-200 ${isOpen ? 'opacity-100 w-auto' : 'opacity-0 w-0'}`}
              >
                Memory
              </span>
            </button>
          </Link>
        </SidebarTooltip>

        {/* AGENTS BUTTON */}
        <SidebarTooltip isOpen={isOpen} label="Agents">
          <Link to="/agents" className="w-full flex justify-center">
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
        </SidebarTooltip>
      </div>
      <div className={`mx-3 border-t border-border/50 ${!isOpen && 'mx-2'}`} />

      {/* COMMUNITY BUTTON */}
      <div className={`p-2 space-y-1 ${!isOpen && 'flex flex-col items-center'}`}>
        <SidebarTooltip isOpen={isOpen} label="Community">
          <Link to="/community" className="w-full flex justify-center">
            <button
              className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-md text-sm transition-all duration-200 group relative ${
                location.pathname === '/community'
                  ? 'bg-primary/10 text-primary font-medium'
                  : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
              }`}
            >
              <Users
                size={18}
                className="transition-transform duration-300 ease-out group-hover:scale-110 group-hover:text-primary"
              />
              <span
                className={`whitespace-nowrap overflow-hidden transition-all duration-200 ${isOpen ? 'opacity-100 w-auto' : 'opacity-0 w-0'}`}
              >
                Community
              </span>
            </button>
          </Link>
        </SidebarTooltip>
      </div>

      {/* New chat button */}
      <div className={`px-2 pb-2 ${!isOpen && 'flex justify-center'}`}>
        <SidebarTooltip isOpen={isOpen} label="New Chat">
          <Button
            variant="ghost"
            onClick={createSession}
            className={`${isOpen ? 'w-full justify-start' : 'w-10 justify-center px-0'} rounded-md border-primary/20 hover:border-primary/50 text-primary hover:bg-primary/5 transition-all shadow-none group relative`}
          >
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
        </SidebarTooltip>
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
                <AnimatePresence initial={false} mode="popLayout">
                  {sessions.map(session => (
                    <SidebarSessionItem
                      key={session.session_id}
                      session={session}
                      currentSessionId={currentSessionId}
                      onSelect={() => selectSession(session.session_id)}
                      onDelete={handleDeleteClick}
                      loadSessions={loadSessions}
                    />
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
        <SidebarTooltip isOpen={isOpen} label="Settings">
          <Link to="/settings" className="w-full flex justify-center">
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
        </SidebarTooltip>

        <SidebarTooltip isOpen={isOpen} label="Developer">
          <Link to="/settings/developer" className="w-full flex justify-center">
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
        </SidebarTooltip>

        <SidebarTooltip isOpen={isOpen} label="Debug">
          <Link to="/debug" className="w-full flex justify-center">
            <button
              className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-md text-sm transition-all duration-200 group ${
                location.pathname === '/debug'
                  ? 'bg-primary/10 text-primary'
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
        </SidebarTooltip>
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
