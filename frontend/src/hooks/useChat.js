import { useState, useCallback, useRef, useEffect } from 'react'
import { sendMessage, getHistory } from '../api/chat'
import { executeCommand } from '../api/commands'
import { toast } from 'sonner'

function formatCommandResult(command, result) {
  if (!result) return `✅ \`${command}\` completed.`

  switch (command) {
    case '/pref':
      return `✅ Saved preference: **"${result.content}"**`

    case '/ick':
      return `✅ Saved ick: **"${result.content}"**`

    default:
      return `✅ \`${command}\` completed successfully.`
  }
}

export function useChat(sessionId) {
  const [messages, setMessages] = useState([])
  const [loading, setLoading] = useState(false)
  const [streaming, setStreaming] = useState(false)
  const [streamingContent, setStreamingContent] = useState('')
  const [toolCalls, setToolCalls] = useState([])
  const [currentThinking, setCurrentThinking] = useState(null)
  const [totalTokens, setTotalTokens] = useState(0)

  const toolCallsRef = useRef([])
  const thinkingRef = useRef(null)
  const streamingContentRef = useRef('')
  const abortControllerRef = useRef(null)
  const revealIndexRef = useRef(0)
  const revealIntervalRef = useRef(null)
  const sessionIdRef = useRef(sessionId)

  // Keep sessionIdRef always up-to-date
  useEffect(() => {
    sessionIdRef.current = sessionId
  }, [sessionId])

  function startReveal() {
    if (revealIntervalRef.current) return
    revealIntervalRef.current = setInterval(() => {
      const target = streamingContentRef.current
      if (revealIndexRef.current < target.length) {
        const step = Math.min(5, target.length - revealIndexRef.current)
        revealIndexRef.current += step
        setStreamingContent(target.slice(0, revealIndexRef.current))
      }
    }, 30)
  }

  function stopReveal() {
    if (revealIntervalRef.current) {
      clearInterval(revealIntervalRef.current)
      revealIntervalRef.current = null
    }
    revealIndexRef.current = 0
  }

  useEffect(() => {
    return () => stopReveal()
  }, [])

  useEffect(() => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort()
      abortControllerRef.current = null
    }

    stopReveal()
    setMessages([])
    setToolCalls([])
    setCurrentThinking(null)
    setStreamingContent('')
    setTotalTokens(0)
    setStreaming(false)
    toolCallsRef.current = []
    thinkingRef.current = null
    streamingContentRef.current = ''
    revealIndexRef.current = 0
  }, [sessionId])

  const loadHistory = useCallback(async () => {
    if (!sessionId) return
    setLoading(true)
    try {
      const data = await getHistory(sessionId)
      setMessages(data.messages || [])

      const restoredTokens = (data.messages || []).reduce((sum, msg) => {
        if (msg.usage) {
          return sum + (msg.usage.prompt_tokens || 0) + (msg.usage.completion_tokens || 0)
        }
        return sum
      }, 0)
      setTotalTokens(restoredTokens)
    } catch (err) {
      console.error('Failed to load history:', err)
    } finally {
      setLoading(false)
    }
  }, [sessionId])

  const send = useCallback(
    async (content, hotTopics = []) => {
      const trimmed = content.trim()
      const currentSessionId = sessionIdRef.current
      if (!currentSessionId || !trimmed) return

      if (trimmed.startsWith('/')) {
        const userMsg = {
          role: 'user',
          content: trimmed,
          timestamp: new Date().toISOString(),
        }
        setMessages(prev => [...prev, userMsg])
        
        setLoading(true)
        try {
          const res = await executeCommand(currentSessionId, trimmed)
          if (res.success) {
            toast.success(`Command completed: ${res.command}`)

            // Format a human-readable response based on command + result
            const friendlyContent = formatCommandResult(res.command, res.result)

            setMessages(prev => [
              ...prev,
              {
                role: 'assistant',
                content: friendlyContent,
                timestamp: new Date().toISOString(),
              },
            ])
          } else {
            toast.error(res.error || 'Command failed')
            setMessages(prev => [
              ...prev,
              {
                role: 'assistant',
                content: `Command failed: ${res.error || 'Unknown error'}`,
                timestamp: new Date().toISOString(),
              },
            ])
          }
        } catch (err) {
          console.error('Command execution failed:', err)
          toast.error('Failed to execute command')
          setMessages(prev => [
            ...prev,
            {
              role: 'assistant',
              content: 'Error: Failed to execute command. Please try again.',
              timestamp: new Date().toISOString(),
            },
          ])
        } finally {
          setLoading(false)
        }
        return
      }

      if (abortControllerRef.current) {
        abortControllerRef.current.abort()
      }
      abortControllerRef.current = new AbortController()
      const { signal } = abortControllerRef.current

      const userMsg = {
        role: 'user',
        content,
        timestamp: new Date().toISOString(),
      }
      setMessages(prev => [...prev, userMsg])
      setStreaming(true)
      setToolCalls([])
      setCurrentThinking(null)
      setStreamingContent('')

      toolCallsRef.current = []
      thinkingRef.current = null
      streamingContentRef.current = ''
      revealIndexRef.current = 0
      startReveal()

      try {
        await sendMessage(
          currentSessionId,
          content,
          hotTopics,
          (eventType, data) => {
            switch (eventType) {
              case 'thinking':
                thinkingRef.current = data.content
                setCurrentThinking(data.content)
                break

              case 'tool_start': {
                const newTc = {
                  tool: data.tool,
                  args: data.args,
                  thinking: thinkingRef.current,
                  status: 'running',
                  startTime: Date.now(),
                }
                thinkingRef.current = null
                setCurrentThinking(null)
                toolCallsRef.current = [...toolCallsRef.current, newTc]
                setToolCalls([...toolCallsRef.current])
                break
              }

              case 'tool_result':
                toolCallsRef.current = toolCallsRef.current.map((tc, idx) =>
                  idx === toolCallsRef.current.length - 1
                    ? {
                        ...tc,
                        status: 'done',
                        summary: data.summary,
                        count: data.count,
                        duration: Date.now() - tc.startTime,
                      }
                    : tc
                )
                setToolCalls([...toolCallsRef.current])
                break

              case 'token':
                streamingContentRef.current += data.content
                break

              case 'response': {
                stopReveal()
                setStreaming(false)
                setMessages(prev => [
                  ...prev,
                  {
                    role: 'assistant',
                    content: data.content,
                    timestamp: new Date().toISOString(),
                    toolCalls: toolCallsRef.current.length > 0 ? [...toolCallsRef.current] : null,
                    usage: data.usage,
                    msg_id: data.msg_id,
                    sources: data.sources || null,
                  },
                ])
                setStreamingContent('')
                streamingContentRef.current = ''
                const tokens =
                  (data.usage?.prompt_tokens || 0) + (data.usage?.completion_tokens || 0)
                setTotalTokens(prev => prev + tokens)
                break
              }

              case 'clarification': {
                stopReveal()
                setStreaming(false)
                setMessages(prev => [
                  ...prev,
                  {
                    role: 'assistant',
                    content: data.question,
                    timestamp: new Date().toISOString(),
                    toolCalls: toolCallsRef.current.length > 0 ? [...toolCallsRef.current] : null,
                    usage: data.usage,
                    isClarification: true,
                    msg_id: data.msg_id,
                  },
                ])
                setStreamingContent('')
                streamingContentRef.current = ''
                const tokens =
                  (data.usage?.prompt_tokens || 0) + (data.usage?.completion_tokens || 0)
                setTotalTokens(prev => prev + tokens)
                break
              }

              case 'msg_id': {
                 setMessages(prev => {
                   if (prev.length === 0) return prev
                   const last = prev[prev.length - 1]
                   if (last.role !== 'user') return prev
                   return [
                     ...prev.slice(0, -1),
                     { ...last, msg_id: data.msg_id }
                   ]
                 })
                 break
              }

              case 'session_title': {
                 window.dispatchEvent(
                   new CustomEvent('session_updated', {
                     detail: { sessionId: currentSessionId, title: data.title },
                   })
                 )
                 break
              }

              case 'error':
                stopReveal()
                setStreaming(false)
                console.error('Stream error:', data.message)
                setMessages(prev => [
                  ...prev,
                  {
                    role: 'assistant',
                    content: `Error: ${data.message}`,
                    timestamp: new Date().toISOString(),
                  },
                ])
                setStreamingContent('')
                streamingContentRef.current = ''
                break

              case 'status':
                break
            }
          },
          signal
        )
      } catch (err) {
        if (err.name === 'AbortError') {
          console.log('Request aborted')
          return
        }
        console.error('Send failed:', err)
        setMessages(prev => [
          ...prev,
          {
            role: 'assistant',
            content: 'Failed to get response. Please try again.',
            timestamp: new Date().toISOString(),
          },
        ])
      } finally {
        stopReveal()
        setStreaming(false)
        setToolCalls([])
        setCurrentThinking(null)
        setStreamingContent('')
        streamingContentRef.current = ''
        revealIndexRef.current = 0
        abortControllerRef.current = null
      }
    },
    []
  )

  return {
    messages,
    loading,
    streaming,
    streamingContent,
    toolCalls,
    currentThinking,
    totalTokens,
    loadHistory,
    send,
  }
}
