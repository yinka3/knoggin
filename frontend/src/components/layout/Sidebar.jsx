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
      className={`${isOpen ? 'w-64' : 'w-14'} border-r border-border flex flex-col bg-sidebar transition-all duration-200`}
    >
      {/* Header with toggle */}
      <div
        className={`flex items-center ${isOpen ? 'justify-between' : 'justify-center'} p-3 border-b border-border`}
      >
        {isOpen && <span className="text-sm font-medium text-foreground">Knoggin</span>}
        <button
          onClick={onToggle}
          className="p-1.5 rounded-lg text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
        >
          {isOpen ? <PanelLeftClose size={18} /> : <PanelLeft size={18} />}
        </button>
      </div>

      {/* Nav links */}
      <div className={`p-2 space-y-1 ${!isOpen && 'flex flex-col items-center'}`}>
        <Link to="/chat">
          <button
            className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-xl text-sm transition-colors ${
              location.pathname.startsWith('/chat')
                ? 'bg-primary/15 text-primary'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted'
            }`}
          >
            <MessageSquare size={18} />
            {isOpen && <span>Chat</span>}
          </button>
        </Link>
        <Link to="/memory">
          <button
            className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-xl text-sm transition-colors ${
              location.pathname === '/memory'
                ? 'bg-primary/15 text-primary'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted'
            }`}
          >
            <Brain size={18} />
            {isOpen && <span>Memory</span>}
          </button>
        </Link>
      </div>

      {/* New chat button */}
      <div className={`p-2 ${!isOpen && 'flex justify-center'}`}>
        <Button
          variant="outline"
          onClick={createSession}
          className={`${isOpen ? 'w-full' : 'w-10 p-0'} rounded-xl border-primary/50 text-primary hover:bg-primary hover:text-primary-foreground transition-colors`}
        >
          <Plus size={18} className={isOpen ? 'mr-2' : ''} />
          {isOpen && 'New Chat'}
        </Button>
      </div>

      {/* Sessions list */}
      {isOpen && (
        <ScrollArea className="flex-1 px-2">
          <div className="py-2">
            <span className="px-2 text-xs font-medium text-muted-foreground">Recent</span>
            <div className="mt-2 space-y-1">
              {loading ? (
                <div className="space-y-1">
                  {[1, 2, 3, 4].map(i => (
                    <div key={i} className="flex items-center gap-2 px-2 py-2">
                      <div className="w-4 h-4 rounded bg-muted-foreground/20 animate-pulse shrink-0" />
                      <div
                        className="h-4 bg-muted-foreground/20 rounded animate-pulse"
                        style={{ width: `${60 + i * 8}%` }}
                      />
                    </div>
                  ))}
                </div>
              ) : sessions.length === 0 ? (
                <div className="px-2 text-sm text-muted-foreground">No sessions yet</div>
              ) : (
                sessions.map(session => (
                  <button
                    key={session.session_id}
                    onClick={() => selectSession(session.session_id)}
                    className={`w-full flex items-center gap-2 px-2 py-2 rounded-lg text-sm truncate transition-colors ${
                      currentSessionId === session.session_id
                        ? 'bg-muted text-foreground'
                        : 'text-muted-foreground hover:bg-muted hover:text-foreground'
                    }`}
                  >
                    <MessageSquare size={14} className="shrink-0" />
                    <span className="truncate">{session.session_id.slice(0, 8)}...</span>
                  </button>
                ))
              )}
            </div>
          </div>
        </ScrollArea>
      )}

      {/* Settings at bottom */}
      <div className={`border-t border-border p-2 ${!isOpen && 'flex justify-center'}`}>
        <Link to="/settings">
          <button
            className={`${isOpen ? 'w-full justify-start px-3' : 'w-10 justify-center'} flex items-center gap-2 py-2 rounded-xl text-sm transition-colors ${
              location.pathname === '/settings'
                ? 'bg-primary/15 text-primary'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted'
            }`}
          >
            <Settings size={18} />
            {isOpen && <span>Settings</span>}
          </button>
        </Link>
      </div>
    </div>
  )
}
