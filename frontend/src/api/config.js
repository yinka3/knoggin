const API_BASE = 'http://localhost:8000'

export async function getConfig() {
  const res = await fetch(`${API_BASE}/config/`)
  if (!res.ok) throw new Error('Failed to load config')
  return res.json()
}

export async function updateConfig(data) {
  const res = await fetch(`${API_BASE}/config/`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  if (!res.ok) throw new Error('Failed to save config')
  return res.json()
}

export async function getConfigStatus() {
  const res = await fetch(`${API_BASE}/config/status`)
  if (!res.ok) throw new Error('Failed to check config status')
  return res.json()
}

export async function getAvailableModels() {
  const res = await fetch(`${API_BASE}/config/models`)
  if (!res.ok) throw new Error('Failed to fetch models')
  return res.json()
}

export async function getCuratedModels() {
  const res = await fetch(`${API_BASE}/config/models/curated`)
  if (!res.ok) throw new Error('Failed to fetch curated models')
  return res.json()
}
