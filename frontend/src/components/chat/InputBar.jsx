import { useState, useRef, useEffect } from 'react'
import { cn } from '@/lib/utils'
import { ArrowUp } from 'lucide-react'
import ToolToggles from './ToolToggles'
import ModelSelector from './ModelSelector'

export default function InputBar({
  onSend,
  disabled,
  enabledTools,
  onToolsChange,
  currentModel,
  onModelChange,
}) {
  const [message, setMessage] = useState('')
  const textareaRef = useRef(null)

  useEffect(() => {
    const ta = textareaRef.current
    if (ta) {
      ta.style.height = 'auto'
      ta.style.height = `${Math.min(ta.scrollHeight, 150)}px`
    }
  }, [message])

  function handleKeyDown(e) {
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
    if (textareaRef.current) textareaRef.current.style.height = 'auto'
  }

  const canSend = message.trim() && !disabled

  return (
    <div className="border-t border-border px-4 py-3 bg-background">
      <div
        className={cn(
          'rounded-xl border bg-muted overflow-hidden',
          'focus-within:border-accent focus-within:ring-1 focus-within:ring-accent/30',
          'transition-colors duration-200'
        )}
      >
        {/* Textarea row */}
        <div className="flex items-end gap-2 px-3 py-2">
          <textarea
            ref={textareaRef}
            value={message}
            onChange={e => setMessage(e.target.value)}
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
          <ToolToggles enabledTools={enabledTools} onToggle={onToolsChange} disabled={disabled} />
          <div className="w-px h-3.5 bg-border/50 mx-0.5" />
          <ModelSelector
            currentModel={currentModel}
            onModelChange={onModelChange}
            disabled={disabled}
          />
          <div className="flex-1" />
          <span className="text-[10px] text-muted-foreground/40">↵ send · ⇧↵ newline</span>
        </div>
      </div>
      <p className="text-[10px] text-muted-foreground/120 text-center mt-4 px-4">
        Responses are AI-generated and may be inaccurate. Verify important information
        independently.
      </p>
    </div>
  )
}
