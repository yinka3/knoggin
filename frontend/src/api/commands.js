const API_BASE = 'http://localhost:8000'

export async function executeCommand(command, args = {}) {
  const res = await fetch(`${API_BASE}/commands/execute`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ command, args }),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(err.detail || 'Failed to execute command')
  }
  return res.json()
}

export async function getAutocomplete(input) {
  const res = await fetch(`${API_BASE}/commands/autocomplete?input=${encodeURIComponent(input)}`)
  if (!res.ok) throw new Error('Failed to get autocomplete')
  return res.json()
}
