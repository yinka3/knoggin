// src/components/chat/InputBar.jsx
import { useState } from 'react'
import { cn } from '@/lib/utils'
import { ArrowUp } from 'lucide-react'

export default function InputBar({ onSend, disabled }) {
  const [message, setMessage] = useState('')

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
  }

  const canSend = message.trim() && !disabled

  return (
    <div className="border-t border-border p-4">
      <div
        className={cn(
          "flex items-end gap-2 rounded-2xl border bg-muted px-4 py-2",
          "focus-within:border-accent focus-within:ring-1 focus-within:ring-accent/30",
          "transition-colors"
        )}
      >
        <textarea
          value={message}
          onChange={(e) => setMessage(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Message AGENT..."
          disabled={disabled}
          rows={1}
          className={cn(
            "flex-1 resize-none bg-transparent py-1.5",
            "text-foreground placeholder-muted-foreground",
            "focus:outline-none",
            "max-h-32 overflow-y-auto"
          )}
          style={{ minHeight: '24px' }}
        />
        <button
          onClick={handleSend}
          disabled={!canSend}
          className={cn(
            "shrink-0 rounded-full p-2 transition-colors",
            canSend
              ? "bg-primary text-primary-foreground hover:bg-primary/90"
              : "bg-muted-foreground/20 text-muted-foreground cursor-not-allowed"
          )}
        >
          <ArrowUp size={18} />
        </button>
      </div>
      <div className="text-xs text-muted-foreground mt-1.5 text-center">
        Enter to send · Shift+Enter for new line
      </div>
    </div>
  )
}