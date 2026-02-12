import { apiGet, apiPost, apiPatch, apiDelete } from './fetch'

export function getTopics(sessionId) {
  return apiGet(`/topics/${sessionId}`)
}

export function createTopic(sessionId, data) {
  return apiPost(`/topics/${sessionId}`, data)
}

export function updateTopic(sessionId, topicName, data) {
  return apiPatch(`/topics/${sessionId}/${encodeURIComponent(topicName)}`, data)
}

export function deleteTopic(sessionId, topicName) {
  return apiDelete(`/topics/${sessionId}/${encodeURIComponent(topicName)}?confirm=true`)
}

export function generateTopicsFromDescription(description) {
  return apiPost('/topics/generate', { description })
}
