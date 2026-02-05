import { useState, useRef, useEffect } from 'react'
import { cn } from '@/lib/utils'
import { ArrowUp } from 'lucide-react'

export default function InputBar({ onSend, disabled }) {
  const [message, setMessage] = useState('')
  const textareaRef = useRef(null)

  useEffect(() => {
    const textarea = textareaRef.current
    if (textarea) {
      textarea.style.height = 'auto'
      textarea.style.height = `${Math.min(textarea.scrollHeight, 150)}px`
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
    <div className="border-t border-border p-4 bg-background">
      <div
        className={cn(
          'flex items-end gap-2 rounded-2xl border bg-muted px-4 py-2',
          'focus-within:border-accent focus-within:ring-1 focus-within:ring-accent/30',
          'transition-colors duration-200'
        )}
      >
        <textarea
          ref={textareaRef}
          value={message}
          onChange={e => setMessage(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Message STELLA..."
          disabled={disabled}
          rows={1}
          className={cn(
            'flex-1 resize-none bg-transparent py-3',
            'text-foreground placeholder-muted-foreground',
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
            'shrink-0 rounded-full p-2 mb-1 transition-all duration-200',
            canSend
              ? 'bg-primary text-primary-foreground hover:bg-primary/90 shadow-sm'
              : 'bg-muted-foreground/20 text-muted-foreground cursor-not-allowed'
          )}
        >
          <ArrowUp size={18} />
        </button>
      </div>
      <div className="text-xs text-muted-foreground mt-2 text-center opacity-50">
        Enter to send · Shift+Enter for new line
      </div>
    </div>
  )
}
