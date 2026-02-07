const API_BASE = 'http://localhost:8000'

export async function listAgents() {
  const res = await fetch(`${API_BASE}/agents/`)
  if (!res.ok) throw new Error('Failed to list agents')
  return res.json()
}

export async function getAgent(agentId) {
  const res = await fetch(`${API_BASE}/agents/${agentId}`)
  if (!res.ok) throw new Error('Agent not found')
  return res.json()
}

export async function getAgentByName(name) {
  const res = await fetch(`${API_BASE}/agents/by-name/${encodeURIComponent(name)}`)
  if (!res.ok) throw new Error('Agent not found')
  return res.json()
}

export async function createAgent({ name, persona, model = null }) {
  const res = await fetch(`${API_BASE}/agents/`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, persona, model }),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(err.detail || 'Failed to create agent')
  }
  return res.json()
}

export async function updateAgent(agentId, { name, persona, model }) {
  const res = await fetch(`${API_BASE}/agents/${agentId}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, persona, model }),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(err.detail || 'Failed to update agent')
  }
  return res.json()
}

export async function deleteAgent(agentId) {
  const res = await fetch(`${API_BASE}/agents/${agentId}`, {
    method: 'DELETE',
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(err.detail || 'Failed to delete agent')
  }
  return res.json()
}

export async function setDefaultAgent(agentId) {
  const res = await fetch(`${API_BASE}/agents/${agentId}/set-default`, {
    method: 'POST',
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(err.detail || 'Failed to set default agent')
  }
  return res.json()
}
