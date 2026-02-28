import { apiGet } from './fetch'
import { API_BASE } from './config-base'

export function getHistory(sessionId, limit = 40) {
  return apiGet(`/chat/${sessionId}/history?limit=${limit}`)
}

export async function extractMessageFacts(sessionId, content, userMsgId) {
  const res = await fetch(`${API_BASE}/chat/${sessionId}/extract`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ content, user_msg_id: userMsgId }),
  })
  if (!res.ok) throw new Error('Failed to extract facts')
  return res.json()
}

export async function sendMessage(sessionId, message, hotTopics = [], onEvent, signal = null) {
  const res = await fetch(`${API_BASE}/chat/${sessionId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      message,
      hot_topics: hotTopics,
      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    }),
    signal,
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
