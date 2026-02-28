import { WS_BASE } from './config-base'

export function createDebugConnection(
  sessionId,
  onEvent,
  onError,
  onClose,
  onOpen,
  verbose = false
) {
  const url = `${WS_BASE}/debug/${sessionId}/ws${verbose ? '?verbose=true' : ''}`
  const ws = new WebSocket(url)

  ws.onopen = () => {
    console.log('Debug WebSocket connected')
    if (onOpen) onOpen()
  }

  ws.onmessage = event => {
    try {
      const data = JSON.parse(event.data)
      onEvent(data)
    } catch (err) {
      console.error('Failed to parse debug event:', err)
    }
  }

  ws.onerror = error => {
    console.error('Debug WebSocket error:', error)
    if (onError) onError(error)
  }

  ws.onclose = event => {
    console.log('Debug WebSocket closed:', event.code, event.reason)
    if (onClose) onClose(event)
  }

  return {
    close: () => ws.close(),
    send: data => ws.send(JSON.stringify(data)),
  }
}
