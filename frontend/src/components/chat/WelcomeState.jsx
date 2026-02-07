import { useState, useEffect, useRef } from 'react'
import { Brain, Sparkles, MessageSquare, Search, Zap, ArrowUp } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import greetings from '@/data/greetings.json'

function getGreeting() {
  const hour = new Date().getHours()
  if (hour < 6) return "You're Up Early"
  if (hour < 12) return 'Good Morning'
  if (hour < 17) return 'Good Afternoon'
  if (hour < 21) return 'Good Evening'
  return 'Late Night Mode'
}

function getSubtext() {
  const hour = new Date().getHours()

  let key
  if (hour < 6) key = 'early_morning'
  else if (hour < 12) key = 'morning'
  else if (hour < 17) key = 'afternoon'
  else if (hour < 21) key = 'evening'
  else key = 'late_night'

  const pool = greetings[key]
  return pool[Math.floor(Math.random() * pool.length)]
}

export default function WelcomeState({ onFirstMessage, userName }) {
  const [isReady, setIsReady] = useState(false)
  const [inputValue, setInputValue] = useState('')
  const [isFocused, setIsFocused] = useState(false)
  const textareaRef = useRef(null)
  const brainRef = useRef(null)
  const [tilt, setTilt] = useState({ x: 0, y: 0 })
  const [subtext] = useState(() => getSubtext())

  useEffect(() => {
    const timer = setTimeout(() => setIsReady(true), 2500)
    return () => clearTimeout(timer)
  }, [])

  function handleMouseMove(e) {
    if (!brainRef.current) return
    const rect = brainRef.current.getBoundingClientRect()
    const centerX = rect.left + rect.width / 2
    const centerY = rect.top + rect.height / 2

    const x = (e.clientX - centerX) / (rect.width / 2)
    const y = (e.clientY - centerY) / (rect.height / 2)

    setTilt({ x: y * -15, y: x * 15 })
  }

  function handleMouseLeave() {
    setTilt({ x: 0, y: 0 })
  }

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
      {/* THE BRAIN CONTAINER */}
      <div
        className={cn(
          'relative group cursor-pointer mb-8 transition-all duration-1000',
          isReady ? 'scale-100' : 'scale-95'
        )}
      >
        {/* 1. ORBITING GLOW — wider spread, viewport-scaled blur */}
        <div
          className={cn(
            'absolute rounded-full transition-all duration-700',
            'glow-orbit',
            isReady ? 'opacity-30 group-hover:opacity-100 scale-100' : 'opacity-0 scale-50'
          )}
          style={{
            inset: 'clamp(-48px, -3vw, -24px)',
            filter: `blur(clamp(24px, 2.5vw, 48px))`,
          }}
        />

        {/* Static ambient glow — wider spread, viewport-scaled blur */}
        <div
          className={cn(
            'absolute bg-primary/20 rounded-full transition-all duration-1000',
            'animate-pulse-slow',
            isReady
              ? 'opacity-50 group-hover:opacity-70 scale-100 group-hover:scale-110'
              : 'opacity-0 scale-50'
          )}
          style={{
            inset: 'clamp(-24px, -1.5vw, -8px)',
            filter: `blur(clamp(40px, 3vw, 60px))`,
          }}
        />

        {/* 2. BLUEPRINT GRID */}
        {!isReady && (
          <div className="absolute inset-0 bg-grid-slate-200/50 opacity-20 animate-pulse rounded-3xl" />
        )}

        {/* 3. THE ICON BOX */}
        <div
          ref={brainRef}
          onMouseMove={handleMouseMove}
          onMouseLeave={handleMouseLeave}
          className={cn(
            'relative p-6 rounded-3xl border transition-all duration-700 ease-out bg-background/50 backdrop-blur-sm',
            isReady
              ? 'border-border/50 shadow-sm group-hover:shadow-xl group-hover:border-primary/30'
              : 'border-primary/20 shadow-none'
          )}
          style={{
            transform: `perspective(500px) rotateX(${tilt.x}deg) rotateY(${tilt.y}deg)`,
            transition:
              tilt.x === 0 && tilt.y === 0 ? 'transform 0.5s ease-out' : 'transform 0.1s ease-out',
          }}
        >
          <Brain
            size={48}
            strokeWidth={1.5}
            className={cn(
              'transition-all duration-700',
              !isReady && 'text-primary/60 animate-blueprint',
              isReady &&
                'text-primary/70 group-hover:text-primary brain-hover drop-shadow-[0_0_8px_rgba(46,170,110,0.3)]'
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
          {getGreeting()}
          {userName ? `, ${userName}` : ''}
        </h1>
        <p className="text-muted-foreground text-center max-w-[400px] mb-4 leading-relaxed">
          {isReady ? subtext : 'Initializing neural pathways...'}
        </p>

        {/* INPUT BAR */}
        <div
          className={cn(
            'w-full relative flex items-end gap-2 p-2 rounded-2xl border transition-all duration-300 mb-8',
            isReady ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-4',
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

      {/* SUGGESTION CARDS */}
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
