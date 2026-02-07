const API_BASE = 'http://localhost:8000'

export async function getStats() {
  const res = await fetch(`${API_BASE}/stats/`)
  if (!res.ok) throw new Error('Failed to load stats')
  return res.json()
}

export async function getStatsBreakdown() {
  const res = await fetch(`${API_BASE}/stats/breakdown`)
  if (!res.ok) throw new Error('Failed to load stats breakdown')
  return res.json()
}
