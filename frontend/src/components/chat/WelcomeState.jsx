import { useState, useEffect, useRef } from 'react'
import { Brain, Sparkles, MessageSquare, Search, Zap, ArrowUp } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'

export default function WelcomeState({ onFirstMessage }) {
  const [isReady, setIsReady] = useState(false)
  const [inputValue, setInputValue] = useState('')
  const [isFocused, setIsFocused] = useState(false)
  const textareaRef = useRef(null)

  // Transition from "Blueprint" to "Interactive" after 2.5s
  useEffect(() => {
    const timer = setTimeout(() => setIsReady(true), 2500)
    return () => clearTimeout(timer)
  }, [])

  // Auto-resize textarea
  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto'
      textareaRef.current.style.height = `${textareaRef.current.scrollHeight}px`
    }
  }, [inputValue])

  const handleKeyDown = e => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSubmit()
    }
  }

  const handleSubmit = () => {
    if (!inputValue.trim()) return
    onFirstMessage(inputValue)
  }

  const suggestions = [
    {
      label: 'Analyze my project idea',
      icon: Zap,
      prompt: 'I have a project idea I want to vet...',
    },
    {
      label: 'Who are my key connections?',
      icon: Search,
      prompt: 'Search for my key professional connections...',
    },
    {
      label: 'Catch me up on last week',
      icon: MessageSquare,
      prompt: 'What did I work on last week?',
    },
  ]

  return (
    <div className="h-full flex flex-col items-center justify-center p-8 relative overflow-hidden">
      {/* BACKGROUND GRID (Subtle texture) */}
      <div className="absolute inset-0 bg-[url('https://grainy-gradients.vercel.app/noise.svg')] opacity-5 pointer-events-none" />

      {/* THE BRAIN CONTAINER (Original Sleek Version) */}
      <div
        className={cn(
          'relative group cursor-pointer mb-8 transition-all duration-1000',
          isReady ? 'scale-100' : 'scale-95'
        )}
      >
        {/* 1. IDLE/HOVER GLOW */}
        <div
          className={cn(
            'absolute inset-0 bg-primary/20 blur-3xl rounded-full transition-all duration-1000',
            isReady
              ? 'opacity-50 group-hover:opacity-80 scale-75 group-hover:scale-110'
              : 'opacity-0 scale-50'
          )}
        />

        {/* 2. BLUEPRINT GRID */}
        {!isReady && (
          <div className="absolute inset-0 bg-grid-slate-200/50 opacity-20 animate-pulse rounded-3xl" />
        )}

        {/* 3. THE ICON BOX (Sleek, Translucent Border) */}
        <div
          className={cn(
            'relative p-6 rounded-3xl border transition-all duration-700 ease-out bg-background/50 backdrop-blur-sm',
            isReady
              ? 'border-border/50 shadow-sm group-hover:shadow-xl group-hover:border-primary/30 group-hover:-translate-y-1'
              : 'border-primary/20 shadow-none'
          )}
        >
          <Brain
            size={48}
            strokeWidth={1.5} // Original thin lines
            className={cn(
              'transition-all duration-700',
              !isReady && 'text-primary/60 animate-blueprint',
              isReady &&
                'text-muted-foreground/80 group-hover:text-primary group-hover:scale-110 group-hover:rotate-3'
            )}
          />

          {isReady && (
            <>
              <Sparkles
                size={16}
                className="absolute -top-1 -right-1 text-amber-400 opacity-0 group-hover:opacity-100 transition-all duration-500 delay-100 scale-0 group-hover:scale-100"
              />
              <Sparkles
                size={12}
                className="absolute bottom-2 -left-2 text-primary opacity-0 group-hover:opacity-100 transition-all duration-500 delay-200 scale-0 group-hover:scale-100"
              />
            </>
          )}
        </div>
      </div>

      {/* TEXT CONTENT */}
      <div className="animate-in fade-in slide-in-from-bottom-4 duration-1000 delay-500 fill-mode-backwards flex flex-col items-center w-full max-w-2xl z-10">
        <h1 className="text-2xl font-semibold tracking-tight mb-2 text-foreground">
          Good Morning, Yinka
        </h1>
        <p className="text-muted-foreground text-center max-w-[400px] mb-8 leading-relaxed">
          {isReady ? 'System Online. Ready to recall.' : 'Initializing neural pathways...'}
        </p>

        {/* INPUT BAR: High Visibility (Solid Background + Border) */}
        <div
          className={cn(
            'w-full relative flex items-end gap-2 p-2 rounded-2xl border transition-all duration-300 mb-8',
            isReady ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-4',
            // VISIBILITY FIX: Solid background (bg-background) and clear border (border-input)
            isFocused
              ? 'bg-background border-primary/50 ring-2 ring-primary/10 shadow-md'
              : 'bg-background border-input hover:border-accent'
          )}
        >
          <textarea
            ref={textareaRef}
            value={inputValue}
            onChange={e => setInputValue(e.target.value)}
            onKeyDown={handleKeyDown}
            onFocus={() => setIsFocused(true)}
            onBlur={() => setIsFocused(false)}
            placeholder="Ask me anything..."
            className="flex-1 w-full bg-transparent border-none focus:ring-0 resize-none max-h-[200px] min-h-[44px] py-3 px-4 text-sm text-foreground placeholder:text-muted-foreground/70 leading-relaxed"
            rows={1}
          />
          <Button
            size="icon"
            onClick={handleSubmit}
            disabled={!inputValue.trim()}
            className={cn(
              'rounded-xl h-10 w-10 shrink-0 transition-all duration-300 mb-1',
              inputValue.trim()
                ? 'bg-primary text-primary-foreground hover:bg-primary/90 shadow-sm'
                : 'bg-muted text-muted-foreground hover:bg-muted opacity-50'
            )}
          >
            <ArrowUp size={18} strokeWidth={2} />
          </Button>
        </div>
      </div>

      {/* SUGGESTION CARDS: Solid & Visible */}
      <div
        className={cn(
          'grid grid-cols-1 sm:grid-cols-3 gap-3 w-full max-w-2xl transition-all duration-1000 delay-700',
          isReady ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-8'
        )}
      >
        {suggestions.map((s, i) => (
          <Button
            key={i}
            variant="outline"
            // VISIBILITY FIX: Solid background (bg-card), explicit border, hover effects
            className="h-auto py-4 px-4 flex flex-col items-center gap-3 bg-card border-border hover:border-primary/50 hover:bg-accent/50 transition-all duration-300 group shadow-sm"
            onClick={() => onFirstMessage(s.prompt)}
          >
            <s.icon
              size={20}
              className="text-muted-foreground group-hover:text-primary transition-colors duration-300"
            />
            <span className="text-xs font-medium text-muted-foreground group-hover:text-foreground transition-colors">
              {s.label}
            </span>
          </Button>
        ))}
      </div>
    </div>
  )
}
