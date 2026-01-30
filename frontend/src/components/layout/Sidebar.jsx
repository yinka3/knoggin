import { useSession } from '../../context/SessionContext'
import { Link, useLocation } from 'react-router-dom'
import { Button } from '@/components/ui/button'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Plus, MessageSquare, Brain, Settings, PanelLeftClose, PanelLeft } from 'lucide-react'

export default function Sidebar({ isOpen, onToggle }) {
  const { sessions, currentSessionId, createSession, selectSession, loading } = useSession()
  const location = useLocation()

  return (
    <div
      className={`${isOpen ? 'w-64' : 'w-14'} border-r border-border flex flex-col bg-sidebar/50 backdrop-blur-xl transition-all duration-300 ease-in-out z-20`}
    >
      {/* Header */}
      <div
        className={`flex items-center ${isOpen ? 'justify-between' : 'justify-center'} p-3 border-b border-border h-14`}
      >
        {isOpen && (
          <span className="text-sm font-semibold tracking-tight text-foreground animate-in fade-in duration-200">
            Knoggin
          </span>
        )}
        <button
          onClick={onToggle}
          className="p-1.5 rounded-md text-muted-foreground hover:text-foreground hover:bg-muted/80 transition-colors"
        >
          {isOpen ? <PanelLeftClose size={18} /> : <PanelLeft size={18} />}
        </button>
      </div>

      {/* Nav links */}
      <div className={`p-2 space-y-1 ${!isOpen && 'flex flex-col items-center'}`}>
        <Link to="/chat" className="w-full">
          <button
            className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-md text-sm transition-all duration-200 group relative ${
              location.pathname.startsWith('/chat')
                ? 'bg-primary/10 text-primary font-medium'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
            }`}
          >
            <MessageSquare size={18} />
            <span
              className={`whitespace-nowrap overflow-hidden transition-all duration-200 ${isOpen ? 'opacity-100 w-auto' : 'opacity-0 w-0'}`}
            >
              Chat
            </span>
          </button>
        </Link>
        <Link to="/memory" className="w-full">
          <button
            className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-md text-sm transition-all duration-200 group relative ${
              location.pathname === '/memory'
                ? 'bg-primary/10 text-primary font-medium'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
            }`}
          >
            <Brain size={18} />
            <span
              className={`whitespace-nowrap overflow-hidden transition-all duration-200 ${isOpen ? 'opacity-100 w-auto' : 'opacity-0 w-0'}`}
            >
              Memory
            </span>
          </button>
        </Link>
      </div>

      {/* New chat button */}
      <div className={`p-2 ${!isOpen && 'flex justify-center'}`}>
        <Button
          variant="outline"
          onClick={createSession}
          className={`${isOpen ? 'w-full justify-start' : 'w-10 justify-center px-0'} rounded-md border-primary/20 hover:border-primary/50 text-primary hover:bg-primary/5 transition-all shadow-none`}
        >
          <Plus size={18} className={isOpen ? 'mr-2' : ''} />
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
                sessions.map(session => (
                  <button
                    key={session.session_id}
                    onClick={() => selectSession(session.session_id)}
                    className={`w-full flex items-center gap-2 px-2 py-2 rounded-md text-sm truncate transition-colors text-left ${
                      currentSessionId === session.session_id
                        ? 'bg-muted font-medium text-foreground'
                        : 'text-muted-foreground hover:bg-muted/50 hover:text-foreground'
                    }`}
                  >
                    <span className="truncate flex-1">
                      {/* Fallback if title is missing */}
                      {session.title || `Session ${session.session_id.slice(0, 4)}`}
                    </span>
                  </button>
                ))
              )}
            </div>
          </div>
        </ScrollArea>
      )}

      {/* Settings at bottom */}
      <div className={`border-t border-border p-2 ${!isOpen && 'flex justify-center'}`}>
        <Link to="/settings" className="w-full">
          <button
            className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-md text-sm transition-all duration-200 ${
              location.pathname === '/settings'
                ? 'bg-primary/10 text-primary'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
            }`}
          >
            <Settings size={18} />
            <span
              className={`whitespace-nowrap overflow-hidden transition-all duration-200 ${isOpen ? 'opacity-100 w-auto' : 'opacity-0 w-0'}`}
            >
              Settings
            </span>
          </button>
        </Link>
      </div>
    </div>
  )
}
