const API_BASE = 'http://localhost:8000'

export async function listSessions() {
  const res = await fetch(`${API_BASE}/sessions/`)
  if (!res.ok) throw new Error('Failed to list sessions')
  return res.json()
}

export async function createSession(topicsConfig = null, agentId = null, enabledTools = null) {
  const res = await fetch(`${API_BASE}/sessions/`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      topics_config: topicsConfig,
      agent_id: agentId,
      enabled_tools: enabledTools,
    }),
  })
  if (!res.ok) throw new Error('Failed to create session')
  return res.json()
}

export async function getSession(sessionId) {
  const res = await fetch(`${API_BASE}/sessions/${sessionId}`)
  if (!res.ok) throw new Error('Session not found')
  return res.json()
}

export async function updateSession(sessionId, { model, agentId, enabledTools }) {
  const body = {}
  if (model !== undefined) body.model = model
  if (agentId !== undefined) body.agent_id = agentId
  if (enabledTools !== undefined) body.enabled_tools = enabledTools

  const res = await fetch(`${API_BASE}/sessions/${sessionId}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(err.detail || 'Failed to update session')
  }
  return res.json()
}

export async function deleteSession(sessionId, force = false) {
  const url = force
    ? `${API_BASE}/sessions/${sessionId}?force=true`
    : `${API_BASE}/sessions/${sessionId}`

  const res = await fetch(url, {
    method: 'DELETE',
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(err.detail || 'Failed to delete session')
  }
  return res.json()
}
