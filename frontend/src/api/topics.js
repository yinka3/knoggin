const API_BASE = 'http://localhost:8000'

export async function getTopics(sessionId) {
  const res = await fetch(`${API_BASE}/topics/${sessionId}`)
  if (!res.ok) throw new Error('Failed to load topics')
  return res.json()
}

export async function createTopic(sessionId, data) {
  const res = await fetch(`${API_BASE}/topics/${sessionId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  })
  if (!res.ok) {
    const err = await res.json()
    throw new Error(err.detail || 'Failed to create topic')
  }
  return res.json()
}

export async function updateTopic(sessionId, topicName, data) {
  const res = await fetch(`${API_BASE}/topics/${sessionId}/${encodeURIComponent(topicName)}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  })
  if (!res.ok) {
    const err = await res.json()
    throw new Error(err.detail || 'Failed to update topic')
  }
  return res.json()
}

export async function deleteTopic(sessionId, topicName) {
  const res = await fetch(`${API_BASE}/topics/${sessionId}/${encodeURIComponent(topicName)}?confirm=true`, {
    method: 'DELETE'
  })
  if (!res.ok) {
    const err = await res.json()
    throw new Error(err.detail || 'Failed to delete topic')
  }
  return res.json()
}