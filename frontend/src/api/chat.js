const API_BASE = 'http://localhost:8000'

export async function getHistory(sessionId, limit = 40) {
  const res = await fetch(`${API_BASE}/chat/${sessionId}/history?limit=${limit}`)
  if (!res.ok) throw new Error('Failed to load history')
  return res.json()
}

export async function sendMessage(sessionId, message, hotTopics = [], onEvent) {
  const res = await fetch(`${API_BASE}/chat/${sessionId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      message,
      hot_topics: hotTopics,
      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    }),
  })

  if (!res.ok) {
    throw new Error('Failed to send message')
  }

  const reader = res.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  while (true) {
    const { done, value } = await reader.read()
    if (done) break

    buffer += decoder.decode(value, { stream: true })

    const messages = buffer.split('\n\n')
    buffer = messages.pop()

    for (const message of messages) {
      if (!message.trim()) continue

      const lines = message.split('\n')
      let eventType = null
      let data = null

      for (const line of lines) {
        if (line.startsWith('event: ')) {
          eventType = line.slice(7)
        } else if (line.startsWith('data: ')) {
          try {
            data = JSON.parse(line.slice(6))
          } catch (e) {
            console.error('Failed to parse SSE data:', e)
          }
        }
      }

      if (eventType && data) {
        onEvent(eventType, data)
      }
    }
  }
}
