const API_BASE = 'http://localhost:8000'

export async function listSessions() {
  const res = await fetch(`${API_BASE}/sessions/`)
  if (!res.ok) throw new Error('Failed to list sessions')
  return res.json()
}

export async function createSession(topicsConfig = null) {
  const res = await fetch(`${API_BASE}/sessions/`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ topics_config: topicsConfig }),
  })
  if (!res.ok) throw new Error('Failed to create session')
  return res.json()
}

export async function getSession(sessionId) {
  const res = await fetch(`${API_BASE}/sessions/${sessionId}`)
  if (!res.ok) throw new Error('Session not found')
  return res.json()
}
