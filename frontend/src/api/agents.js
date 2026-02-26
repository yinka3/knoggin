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

export function createAgent({ name, persona, model = null, temperature = 0.7, enabled_tools = null }) {
  return apiPost('/agents/', { name, persona, model, temperature, enabled_tools })
}

export function updateAgent(agentId, { name, persona, model, temperature, enabled_tools }) {
  return apiPatch(`/agents/${agentId}`, { name, persona, model, temperature, enabled_tools })
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

export function getAgentMemory(agentId) {
  return apiGet(`/agents/${agentId}/memory`)
}

export function addAgentMemory(agentId, category, content) {
  return apiPost(`/agents/${agentId}/memory/${category}`, { content })
}

export function deleteAgentMemory(agentId, category, memoryId) {
  return apiDelete(`/agents/${agentId}/memory/${category}/${memoryId}`)
}
