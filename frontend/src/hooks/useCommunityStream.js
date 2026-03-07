import { useEffect, useRef, useState, useCallback } from 'react'
import { connectCommunityWS } from '@/api/community'

const MAX_RECONNECT = 5

export default function useCommunityStream(onEvent, enabled = true) {
  const [connected, setConnected] = useState(false)
  const reconnectAttempts = useRef(0)
  const reconnectTimeout = useRef(null)
  const closeRef = useRef(null)
  const disposedRef = useRef(false)
  const onEventRef = useRef(onEvent)

  useEffect(() => {
    onEventRef.current = onEvent
  }, [onEvent])

  const connect = useCallback(function doConnect() {
    if (disposedRef.current) return

    closeRef.current = connectCommunityWS(
      data => onEventRef.current?.(data),
      () => {
        if (!disposedRef.current) {
          setConnected(true)
          reconnectAttempts.current = 0
        }
      },
      () => {
        if (disposedRef.current) return
        setConnected(false)

        if (reconnectAttempts.current < MAX_RECONNECT) {
          const delay = Math.min(1000 * 2 ** reconnectAttempts.current, 30000)
          reconnectTimeout.current = setTimeout(() => {
            if (!disposedRef.current) {
              reconnectAttempts.current += 1
              doConnect()
            }
          }, delay)
        }
      }
    )
  }, [])

  useEffect(() => {
    if (!enabled) {
      disposedRef.current = true
      if (reconnectTimeout.current) clearTimeout(reconnectTimeout.current)
      if (closeRef.current) closeRef.current()
      setConnected(false)
      return
    }

    disposedRef.current = false
    reconnectAttempts.current = 0
    connect()

    return () => {
      disposedRef.current = true
      if (reconnectTimeout.current) {
        clearTimeout(reconnectTimeout.current)
        reconnectTimeout.current = null
      }
      if (closeRef.current) {
        closeRef.current()
        closeRef.current = null
      }
      setConnected(false)
    }
  }, [enabled, connect])

  const disconnect = useCallback(() => {
    disposedRef.current = true
    if (closeRef.current) {
      closeRef.current()
      closeRef.current = null
    }
    setConnected(false)
  }, [])

  return { connected, disconnect }
}
