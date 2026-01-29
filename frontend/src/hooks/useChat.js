import { useState, useCallback, useRef, useEffect } from 'react'
import { sendMessage, getHistory } from '../api/chat'

export function useChat(sessionId) {
  const [messages, setMessages] = useState([])
  const [loading, setLoading] = useState(false)
  const [streaming, setStreaming] = useState(false)
  const [toolCalls, setToolCalls] = useState([])
  const [currentThinking, setCurrentThinking] = useState(null)
  const toolCallsRef = useRef([])
  const thinkingRef = useRef(null)

  useEffect(() => {
    setMessages([])
    setToolCalls([])
    setCurrentThinking(null)
    toolCallsRef.current = []
    thinkingRef.current = null
  }, [sessionId])

  const loadHistory = useCallback(async () => {
    if (!sessionId) return
    setLoading(true)
    try {
      const data = await getHistory(sessionId)
      setMessages(data.messages || [])
    } catch (err) {
      console.error('Failed to load history:', err)
    } finally {
      setLoading(false)
    }
  }, [sessionId])

  const send = useCallback(async (content) => {
    if (!sessionId || !content.trim()) return

    const userMsg = {
      role: 'user',
      content,
      timestamp: new Date().toISOString()
    }
    setMessages((prev) => [...prev, userMsg])
    setStreaming(true)
    setToolCalls([])
    setCurrentThinking(null)
    toolCallsRef.current = []
    thinkingRef.current = null

    try {
      await sendMessage(sessionId, content, [], (eventType, data) => {
        switch (eventType) {
          case 'thinking':
            thinkingRef.current = data.content
            setCurrentThinking(data.content)
            break

          case 'tool_start':
            const newTc = { 
              tool: data.tool, 
              args: data.args,
              thinking: thinkingRef.current,
              status: 'running',
              startTime: Date.now()
            }
            thinkingRef.current = null
            setCurrentThinking(null)
            toolCallsRef.current = [...toolCallsRef.current, newTc]
            setToolCalls([...toolCallsRef.current])
            break

          case 'tool_result':
            toolCallsRef.current = toolCallsRef.current.map((tc, idx) =>
              idx === toolCallsRef.current.length - 1
                ? { 
                    ...tc, 
                    status: 'done', 
                    summary: data.summary, 
                    count: data.count,
                    duration: Date.now() - tc.startTime
                  }
                : tc
            )
            setToolCalls([...toolCallsRef.current])
            break

          case 'response':
            setMessages((prev) => [...prev, {
              role: 'assistant',
              content: data.content,
              timestamp: new Date().toISOString(),
              toolCalls: toolCallsRef.current.length > 0 ? [...toolCallsRef.current] : null
            }])
            break

          case 'clarification':
            setMessages((prev) => [...prev, {
              role: 'assistant',
              content: data.question,
              timestamp: new Date().toISOString(),
              toolCalls: toolCallsRef.current.length > 0 ? [...toolCallsRef.current] : null
            }])
            break

          case 'error':
            console.error('Stream error:', data.message)
            setMessages((prev) => [...prev, {
              role: 'assistant',
              content: `Error: ${data.message}`,
              timestamp: new Date().toISOString()
            }])
            break

          case 'status':
            break
        }
      })
    } catch (err) {
      console.error('Send failed:', err)
      setMessages((prev) => [...prev, {
        role: 'assistant',
        content: 'Failed to get response. Please try again.',
        timestamp: new Date().toISOString()
      }])
    } finally {
      setStreaming(false)
      setToolCalls([])
      setCurrentThinking(null)
    }
  }, [sessionId])

  return {
    messages,
    loading,
    streaming,
    toolCalls,
    currentThinking,
    loadHistory,
    send
  }
}