import { apiGet, apiPost, apiPatch, apiDelete } from './fetch'

export function getConfig() {
  return apiGet('/config/')
}

export function updateConfig(data) {
  return apiPatch('/config/', data)
}

export function getConfigStatus() {
  return apiGet('/config/status')
}

export function getAvailableModels() {
  return apiGet('/config/models')
}

export function getCuratedModels() {
  return apiGet('/config/models/curated')
}

export function getTools() {
  return apiGet('/config/tools')
}

export function getMCPPresets() {
  return apiGet('/config/mcp/presets')
}

export function getMCPServers() {
  return apiGet('/config/mcp/servers')
}

export function addMCPServer(data) {
  return apiPost('/config/mcp/servers', data)
}

export function removeMCPServer(name) {
  return apiDelete(`/config/mcp/servers/${encodeURIComponent(name)}`)
}

export function toggleMCPServer(name) {
  return apiPost(`/config/mcp/servers/${encodeURIComponent(name)}/toggle`)
}
