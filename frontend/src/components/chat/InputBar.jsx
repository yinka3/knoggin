import { useState, useRef, useEffect, useCallback } from 'react'
import { cn } from '@/lib/utils'
import { ArrowUp, Slash, TerminalSquare } from 'lucide-react'
import ModelSelector from './ModelSelector'
import { getAutocomplete } from '@/api/commands'
import { motion, AnimatePresence } from 'motion/react'

const MAX_CHARS = 4000

export default function InputBar({ onSend, disabled, currentModel, onModelChange }) {
  const [message, setMessage] = useState('')
  const [suggestions, setSuggestions] = useState([])
  const [selectedIdx, setSelectedIdx] = useState(0)
  const [showSuggestions, setShowSuggestions] = useState(false)
  const [isFocused, setIsFocused] = useState(false)
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

  const fetchSuggestions = useCallback(async value => {
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
    if (value.length <= MAX_CHARS) {
      setMessage(value)
    }

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
      if (
        suggestionsRef.current &&
        !suggestionsRef.current.contains(e.target) &&
        textareaRef.current &&
        !textareaRef.current.contains(e.target)
      ) {
        setShowSuggestions(false)
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  const canSend = message.trim() && !disabled
  const charCount = message.length
  const showCharCount = charCount > MAX_CHARS * 0.7
  const charCountColor =
    charCount > MAX_CHARS * 0.9
      ? 'text-destructive'
      : charCount > MAX_CHARS * 0.8
        ? 'text-amber-500'
        : 'text-muted-foreground/50'

  return (
    <div className="border-t border-border px-4 py-3 bg-background">
      <div
        className={cn(
          'rounded-xl overflow-hidden relative',
          'bg-card/40 backdrop-blur-xl border border-white/[0.08]',
          isFocused && 'border-white/[0.15]',
          'transition-colors duration-200'
        )}
      >
        {/* Slash command suggestions dropdown */}
        <AnimatePresence>
          {showSuggestions && suggestions.length > 0 && (
            <motion.div
              initial={{ opacity: 0, y: 10, scale: 0.98 }}
              animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={{ opacity: 0, y: 5, scale: 0.98 }}
              transition={{ duration: 0.15, ease: 'easeOut' }}
              ref={suggestionsRef}
              className="absolute bottom-full left-0 right-0 mb-1 mx-2 z-50"
            >
              <div className="glass-card rounded-lg shadow-xl overflow-hidden">
                <div className="px-3 py-1.5 border-b border-white/[0.06]">
                  <span className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider">
                    Commands
                  </span>
                </div>
                {suggestions.map((s, idx) => (
                  <button
                    key={s.command}
                    onClick={() => applySuggestion(s)}
                    onMouseEnter={() => setSelectedIdx(idx)}
                    className={cn(
                      'w-full flex items-center gap-3 px-3 py-2 text-left transition-all duration-150',
                      idx === selectedIdx
                        ? 'bg-primary/10 text-foreground'
                        : 'text-foreground/80 hover:bg-white/[0.03]'
                    )}
                  >
                    <div className="shrink-0 w-5 h-5 rounded flex items-center justify-center glass-container">
                      <Slash size={12} className="text-primary" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <span className="text-sm font-mono font-medium">{s.command}</span>
                      {s.description && (
                        <span className="text-[11px] text-muted-foreground ml-2">
                          {s.description}
                        </span>
                      )}
                    </div>
                  </button>
                ))}
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Textarea row */}
        <div className="flex items-end gap-2 px-3 py-2 relative z-10">
          <AnimatePresence>
            {message.startsWith('/') && (
              <motion.div
                initial={{ opacity: 0, scale: 0.9, x: -10 }}
                animate={{ opacity: 1, scale: 1, x: 0 }}
                exit={{ opacity: 0, scale: 0.9, x: -10 }}
                className="shrink-0 mb-1.5 flex items-center gap-1.5 glass-container px-2 py-1 rounded text-[11px] font-mono font-medium text-primary select-none"
              >
                <TerminalSquare size={12} className="opacity-80" />
                COMMAND
              </motion.div>
            )}
          </AnimatePresence>

          <textarea
            ref={textareaRef}
            value={message}
            onChange={handleChange}
            onKeyDown={handleKeyDown}
            onFocus={() => setIsFocused(true)}
            onBlur={() => setIsFocused(false)}
            placeholder="Message your agent..."
            disabled={disabled}
            rows={1}
            className={cn(
              'flex-1 resize-none bg-transparent py-1.5',
              'text-sm text-foreground placeholder-muted-foreground/60',
              'focus:outline-none',
              'max-h-[150px] overflow-y-auto',
              'leading-relaxed'
            )}
            style={{ minHeight: '24px' }}
          />

          {/* Send button with glow effect */}
          <AnimatePresence mode="popLayout">
            {canSend && (
              <motion.button
                initial={{ opacity: 0, scale: 0.8 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.8 }}
                whileHover={{ scale: 1.05 }}
                whileTap={{ scale: 0.95 }}
                transition={{ duration: 0.15 }}
                onClick={handleSend}
                disabled={!canSend}
                className={cn(
                  'shrink-0 rounded-lg p-2 transition-colors duration-150',
                  'bg-primary text-primary-foreground',
                  'hover:bg-primary/90'
                )}
              >
                <ArrowUp size={16} strokeWidth={2.5} />
              </motion.button>
            )}
          </AnimatePresence>
        </div>

        {/* Compact toolbar */}
        <div className="flex items-center gap-2 px-3 py-1.5 border-t border-white/[0.06] bg-white/[0.01]">
          <ModelSelector
            currentModel={currentModel}
            onModelChange={onModelChange}
            disabled={disabled}
          />

          <div className="flex-1" />

          {/* Character count - appears progressively */}
          <AnimatePresence>
            {showCharCount && (
              <motion.span
                initial={{ opacity: 0, x: 10 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: 10 }}
                className={cn('text-[10px] font-mono tabular-nums', charCountColor)}
              >
                {charCount}/{MAX_CHARS}
              </motion.span>
            )}
          </AnimatePresence>

          <span className="text-[10px] text-muted-foreground/40">
            <kbd className="px-1 py-0.5 rounded bg-muted/50 font-mono">/</kbd> commands
            <span className="mx-1">·</span>
            <kbd className="px-1 py-0.5 rounded bg-muted/50 font-mono">↵</kbd> send
          </span>
        </div>
      </div>

      <p className="text-[10px] text-muted-foreground/50 text-center mt-3">
        AI responses may be inaccurate — verify important information
      </p>
    </div>
  )
}
