import { createContext, useContext, useEffect, useRef, useState, useCallback } from 'react'
import { useSession } from './SessionContext'
import { WS_BASE } from '@/api/config-base'


const SocketContext = createContext(null)
const MAX_RECONNECT_ATTEMPTS = 5

export function SocketProvider({ children }) {
  const { currentSessionId } = useSession()
  const [isConnected, setIsConnected] = useState(false)
  const [lastEvent, setLastEvent] = useState(null)

  const socketRef = useRef(null)
  const reconnectTimeoutRef = useRef(null)
  const attemptsRef = useRef(0)

  const subscribersRef = useRef(new Map())

  useEffect(() => {
    if (!currentSessionId) return

    function connect() {
      if (socketRef.current) {
        socketRef.current.close()
      }

      const url = `${WS_BASE}/debug/${currentSessionId}/ws`
      const ws = new WebSocket(url)
      socketRef.current = ws

      ws.onopen = () => {
        console.log('[Socket] Connected')
        setIsConnected(true)
        attemptsRef.current = 0
      }

      ws.onmessage = msg => {
        try {
          const data = JSON.parse(msg.data)
          setLastEvent(data)

          if (data.component && subscribersRef.current.has(data.component)) {
            subscribersRef.current.get(data.component).forEach(cb => cb(data))
          }

          if (data.event && subscribersRef.current.has(data.event)) {
            subscribersRef.current.get(data.event).forEach(cb => cb(data))
          }
        } catch (err) {
          console.error('[Socket] Failed to parse:', err)
        }
      }

      ws.onclose = () => {
        console.log('[Socket] Disconnected')
        setIsConnected(false)
        if (attemptsRef.current < MAX_RECONNECT_ATTEMPTS) {
          const delay = Math.min(1000 * 2 ** attemptsRef.current, 30000)
          attemptsRef.current += 1
          reconnectTimeoutRef.current = setTimeout(connect, delay)
        }
      }

      ws.onerror = err => {
        console.error('[Socket] Error:', err)
        ws.close()
      }
    }

    connect()

    return () => {
      if (socketRef.current) socketRef.current.close()
      if (reconnectTimeoutRef.current) clearTimeout(reconnectTimeoutRef.current)
    }
  }, [currentSessionId])

  const subscribe = useCallback((eventKey, callback) => {
    if (!subscribersRef.current.has(eventKey)) {
      subscribersRef.current.set(eventKey, new Set())
    }
    subscribersRef.current.get(eventKey).add(callback)

    return () => {
      if (subscribersRef.current.has(eventKey)) {
        subscribersRef.current.get(eventKey).delete(callback)
        if (subscribersRef.current.get(eventKey).size === 0) {
          subscribersRef.current.delete(eventKey)
        }
      }
    }
  }, [])

  const value = {
    isConnected,
    lastEvent,
    subscribe,
  }

  return <SocketContext.Provider value={value}>{children}</SocketContext.Provider>
}

export function useSocket(eventKey, callback) {
  const context = useContext(SocketContext)
  if (!context) throw new Error('useSocket must be used within SocketProvider')

  const { subscribe, isConnected } = context

  useEffect(() => {
    if (!callback) return
    return subscribe(eventKey, callback)
  }, [eventKey, callback, subscribe])

  return { isConnected }
}
