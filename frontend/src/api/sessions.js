import { apiGet, apiPost, apiPatch, apiDelete } from './fetch'

export function listSessions() {
  return apiGet('/sessions/')
}

export function createSession(topicsConfig = null, agentId = null, enabledTools = null) {
  return apiPost('/sessions/', {
    topics_config: topicsConfig,
    agent_id: agentId,
    enabled_tools: enabledTools,
  })
}

export function getSession(sessionId) {
  return apiGet(`/sessions/${sessionId}`)
}

export function updateSession(sessionId, { model, agentId, enabledTools }) {
  const body = {}
  if (model !== undefined) body.model = model
  if (agentId !== undefined) body.agent_id = agentId
  if (enabledTools !== undefined) body.enabled_tools = enabledTools

  return apiPatch(`/sessions/${sessionId}`, body)
}

export function deleteSession(sessionId, force = false) {
  const query = force ? '?force=true' : ''
  return apiDelete(`/sessions/${sessionId}${query}`)
}
