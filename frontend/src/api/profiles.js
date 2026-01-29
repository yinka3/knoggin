const API_BASE = 'http://localhost:8000'

export async function listProfiles({ limit = 20, offset = 0, topic, type, q } = {}) {
  const params = new URLSearchParams()
  params.set('limit', limit)
  params.set('offset', offset)
  if (topic) params.set('topic', topic)
  if (type) params.set('type', type)
  if (q) params.set('q', q)

  const res = await fetch(`${API_BASE}/profiles/?${params}`)
  if (!res.ok) throw new Error('Failed to load profiles')
  return res.json()
}

export async function getProfile(entityId) {
  const res = await fetch(`${API_BASE}/profiles/${entityId}`)
  if (!res.ok) throw new Error('Profile not found')
  return res.json()
}