import React, { useState, useEffect, useRef } from 'react'
import { useSession } from '../../context/SessionContext'
import { updateSession } from '../../api/sessions'
import { toast } from 'sonner'
import { Pencil } from 'lucide-react'
import { motion, AnimatePresence } from 'motion/react'

export default function SessionTitle({ sessionId, isChatEmpty }) {
  const { sessions, loadSessions } = useSession()
  const session = sessions.find(s => s.session_id === sessionId)
  const isDefaultTitle = !session?.title || session.title.startsWith('Session ')
  const initialTitle = session?.title || `Session ${sessionId?.slice(0, 4)}`

  const [isEditing, setIsEditing] = useState(false)
  const [title, setTitle] = useState(initialTitle)
  const [displayedTitle, setDisplayedTitle] = useState(initialTitle)
  
  const [isTyping, setIsTyping] = useState(false)
  const isPending = isDefaultTitle && !isTyping && !isChatEmpty

  const inputRef = useRef(null)
  const typingIntervalRef = useRef(null)

  useEffect(() => {
    const newTitle = session?.title || `Session ${sessionId?.slice(0, 4)}`
    
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
  }, [session?.title, sessionId, title, isTyping])

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
      await updateSession(sessionId, { title: newTitle })
      await loadSessions()
    } catch (err) {
      console.error('Failed to update session title:', err)
      toast.error('Failed to update session title')
      setTitle(session?.title || `Session ${sessionId?.slice(0, 4)}`)
      setDisplayedTitle(session?.title || `Session ${sessionId?.slice(0, 4)}`)
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

  if (!sessionId) return null

  if (isEditing) {
    return (
      <input
        ref={inputRef}
        value={title}
        onChange={e => setTitle(e.target.value)}
        onBlur={handleSave}
        onKeyDown={handleKeyDown}
        className="h-7 px-2 bg-transparent border-b border-primary/50 text-sm font-medium focus:outline-none focus:border-primary w-[200px]"
      />
    )
  }

  return (
    <div 
      className={`group flex items-center gap-1.5 px-2 py-1 rounded-md transition-colors max-w-[250px] ${!isPending && !isTyping ? 'hover:bg-muted/50 cursor-pointer' : ''}`}
      onClick={() => { if (!isPending && !isTyping) setIsEditing(true) }}
      title={isPending ? "Generating title..." : "Click to edit session name"}
    >
      <AnimatePresence mode="wait">
        {isPending ? (
          <motion.div
            key="pending"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="flex items-center gap-2"
          >
             <div className="h-4 w-24 bg-muted animate-pulse rounded-sm" />
             <div className="h-4 w-12 bg-muted animate-pulse rounded-sm" />
          </motion.div>
        ) : (
          <motion.span 
            key="title"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            className="text-sm font-medium truncate relative"
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
      
      {!isPending && !isTyping && (
        <Pencil className="w-3 h-3 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity shrink-0" />
      )}
    </div>
  )
}
