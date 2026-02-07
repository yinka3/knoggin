import { useState, useCallback, useRef, useEffect } from 'react'
import { sendMessage, getHistory } from '../api/chat'

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

  function startReveal() {
    if (revealIntervalRef.current) return
    revealIntervalRef.current = setInterval(() => {
      const target = streamingContentRef.current
      if (revealIndexRef.current < target.length) {
        const step = Math.min(3, target.length - revealIndexRef.current)
        revealIndexRef.current += step
        setStreamingContent(target.slice(0, revealIndexRef.current))
      }
    }, 12)
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
    async content => {
      if (!sessionId || !content.trim()) return

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
          sessionId,
          content,
          [],
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
                setMessages(prev => [
                  ...prev,
                  {
                    role: 'assistant',
                    content: data.content,
                    timestamp: new Date().toISOString(),
                    toolCalls: toolCallsRef.current.length > 0 ? [...toolCallsRef.current] : null,
                    usage: data.usage,
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
                setMessages(prev => [
                  ...prev,
                  {
                    role: 'assistant',
                    content: data.question,
                    timestamp: new Date().toISOString(),
                    toolCalls: toolCallsRef.current.length > 0 ? [...toolCallsRef.current] : null,
                    usage: data.usage,
                    isClarification: true,
                  },
                ])
                setStreamingContent('')
                streamingContentRef.current = ''
                const tokens =
                  (data.usage?.prompt_tokens || 0) + (data.usage?.completion_tokens || 0)
                setTotalTokens(prev => prev + tokens)
                break
              }

              case 'error':
                stopReveal()
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
    [sessionId]
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
