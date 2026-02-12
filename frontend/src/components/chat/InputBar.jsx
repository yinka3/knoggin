import { useState, useRef, useEffect, useCallback } from 'react'
import { cn } from '@/lib/utils'
import { ArrowUp, Slash } from 'lucide-react'
import ModelSelector from './ModelSelector'
import { getAutocomplete } from '@/api/commands'

export default function InputBar({
  onSend,
  disabled,
  currentModel,
  onModelChange,
}) {
  const [message, setMessage] = useState('')
  const [suggestions, setSuggestions] = useState([])
  const [selectedIdx, setSelectedIdx] = useState(0)
  const [showSuggestions, setShowSuggestions] = useState(false)
  const textareaRef = useRef(null)
  const suggestionsRef = useRef(null)
  const debounceRef = useRef(null)

  useEffect(() => {
    const ta = textareaRef.current
    if (ta) {
      ta.style.height = 'auto'
      ta.style.height = `${Math.min(ta.scrollHeight, 150)}px`
    }
  }, [message])

  const fetchSuggestions = useCallback(async (value) => {
    if (!value.startsWith('/') || value.includes('\n')) {
      setSuggestions([])
      setShowSuggestions(false)
      return
    }

    try {
      const data = await getAutocomplete(value)
      const items = data.suggestions || []
      setSuggestions(items)
      setSelectedIdx(0)
      setShowSuggestions(items.length > 0)
    } catch {
      setSuggestions([])
      setShowSuggestions(false)
    }
  }, [])

  function handleChange(e) {
    const value = e.target.value
    setMessage(value)

    if (debounceRef.current) clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => fetchSuggestions(value), 150)
  }

  function applySuggestion(suggestion) {
    setMessage(suggestion.command + ' ')
    setShowSuggestions(false)
    setSuggestions([])
    textareaRef.current?.focus()
  }

  function handleKeyDown(e) {

    if (showSuggestions && suggestions.length > 0) {
      if (e.key === 'ArrowUp') {
        e.preventDefault()
        setSelectedIdx(prev => (prev <= 0 ? suggestions.length - 1 : prev - 1))
        return
      }
      if (e.key === 'ArrowDown') {
        e.preventDefault()
        setSelectedIdx(prev => (prev >= suggestions.length - 1 ? 0 : prev + 1))
        return
      }
      if (e.key === 'Tab' || (e.key === 'Enter' && !e.shiftKey)) {
        e.preventDefault()
        applySuggestion(suggestions[selectedIdx])
        return
      }
      if (e.key === 'Escape') {
        e.preventDefault()
        setShowSuggestions(false)
        return
      }
    }

    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  function handleSend() {
    const trimmed = message.trim()
    if (!trimmed || disabled) return
    onSend(trimmed)
    setMessage('')
    setShowSuggestions(false)
    setSuggestions([])
    if (textareaRef.current) textareaRef.current.style.height = 'auto'
  }

  useEffect(() => {
    function handleClick(e) {
      if (suggestionsRef.current && !suggestionsRef.current.contains(e.target) &&
          textareaRef.current && !textareaRef.current.contains(e.target)) {
        setShowSuggestions(false)
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  const canSend = message.trim() && !disabled

  return (
    <div className="border-t border-border px-4 py-3 bg-background">
      <div
        className={cn(
          'rounded-xl border bg-muted overflow-hidden relative',
          'focus-within:border-accent focus-within:ring-1 focus-within:ring-accent/30',
          'transition-colors duration-200'
        )}
      >
        {/* Slash command suggestions dropdown */}
        {showSuggestions && suggestions.length > 0 && (
          <div
            ref={suggestionsRef}
            className="absolute bottom-full left-0 right-0 mb-1 mx-2 z-50"
          >
            <div className="bg-popover border border-border rounded-lg shadow-lg overflow-hidden">
              <div className="px-3 py-1.5 border-b border-border/50">
                <span className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider">Commands</span>
              </div>
              {suggestions.map((s, idx) => (
                <button
                  key={s.command}
                  onClick={() => applySuggestion(s)}
                  onMouseEnter={() => setSelectedIdx(idx)}
                  className={cn(
                    'w-full flex items-center gap-3 px-3 py-2 text-left transition-colors',
                    idx === selectedIdx
                      ? 'bg-accent/10 text-foreground'
                      : 'text-foreground/80 hover:bg-muted'
                  )}
                >
                  <div className="shrink-0 w-5 h-5 rounded flex items-center justify-center bg-primary/10">
                    <Slash size={12} className="text-primary" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <span className="text-sm font-mono font-medium">{s.command}</span>
                    {s.description && (
                      <span className="text-[11px] text-muted-foreground ml-2">{s.description}</span>
                    )}
                  </div>
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Textarea row */}
        <div className="flex items-end gap-2 px-3 py-2">
          <textarea
            ref={textareaRef}
            value={message}
            onChange={handleChange}
            onKeyDown={handleKeyDown}
            placeholder="Message your agent..."
            disabled={disabled}
            rows={1}
            className={cn(
              'flex-1 resize-none bg-transparent py-1.5',
              'text-sm text-foreground placeholder-muted-foreground',
              'focus:outline-none',
              'max-h-[150px] overflow-y-auto',
              'leading-relaxed'
            )}
            style={{ minHeight: '24px' }}
          />
          <button
            onClick={handleSend}
            disabled={!canSend}
            className={cn(
              'shrink-0 rounded-lg p-1.5 transition-all duration-200',
              canSend
                ? 'bg-primary text-primary-foreground hover:bg-primary/90'
                : 'bg-muted-foreground/20 text-muted-foreground cursor-not-allowed'
            )}
          >
            <ArrowUp size={16} />
          </button>
        </div>

        {/* Compact toolbar */}
        <div className="flex items-center gap-1 px-3 py-1.5 border-t border-border/50">
          <div className="w-px h-3.5 bg-border/50 mx-0.5" />
          <ModelSelector
            currentModel={currentModel}
            onModelChange={onModelChange}
            disabled={disabled}
          />
          <div className="flex-1" />
          <span className="text-[10px] text-muted-foreground/40">/ commands · ↵ send · ⇧↵ newline</span>
        </div>
      </div>
      <p className="text-[10px] text-muted-foreground/60 text-center mt-4 px-4">
        Responses are AI-generated and may be inaccurate. Verify important information
        independently.
      </p>
    </div>
  )
}
