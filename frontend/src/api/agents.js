import { apiGet, apiPost, apiPatch, apiDelete } from './fetch'

export function listAgents() {
  return apiGet('/agents/')
}

export function getAgent(agentId) {
  return apiGet(`/agents/${agentId}`)
}

export function getAgentByName(name) {
  return apiGet(`/agents/by-name/${encodeURIComponent(name)}`)
}

export function createAgent({ name, persona, model = null }) {
  return apiPost('/agents/', { name, persona, model })
}

export function updateAgent(agentId, { name, persona, model }) {
  return apiPatch(`/agents/${agentId}`, { name, persona, model })
}

export function deleteAgent(agentId) {
  return apiDelete(`/agents/${agentId}`)
}

export function setDefaultAgent(agentId) {
  return apiPost(`/agents/${agentId}/set-default`)
}

export function getSessionMemory(sessionId) {
  return apiGet(`/agents/memory/${sessionId}`)
}
