import { apiGet, apiPost } from './fetch'

export function executeCommand(sessionId, input) {
  return apiPost('/commands/execute', { session_id: sessionId, input })
}

export function getAutocomplete(prefix) {
  return apiGet(`/commands/autocomplete?prefix=${encodeURIComponent(prefix)}`)
}
