import { API_BASE } from './config-base'

/**
 * Shared fetch wrapper for API calls.
 * Automatically prepends API_BASE, parses JSON, and throws with server error detail.
 */
export async function apiFetch(path, options = {}) {
  const res = await fetch(`${API_BASE}${path}`, options)
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    let msg = err.detail || `Request failed: ${res.status}`
    if (typeof msg === 'object') {
      msg = JSON.stringify(msg)
    }
    throw new Error(msg)
  }
  return res.json()
}

export function apiGet(path) {
  return apiFetch(path)
}

export function apiPost(path, body) {
  return apiFetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
}

export function apiPatch(path, body) {
  return apiFetch(path, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
}

export function apiDelete(path) {
  return apiFetch(path, { method: 'DELETE' })
}
