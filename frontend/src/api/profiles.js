import { apiGet, apiDelete } from './fetch'

export function getProfiles(params = {}) {
  const { topic, search, limit, offset } = params
  const qs = new URLSearchParams()
  
  if (topic) qs.set('topic', topic)
  if (search) qs.set('q', search)
  if (limit) qs.set('limit', limit)
  if (offset) qs.set('offset', offset)
  
  const str = qs.toString()
  return apiGet(`/profiles/${str ? `?${str}` : ''}`)
}

export function getProfile(entityId) {
  return apiGet(`/profiles/${entityId}`)
}

export function deleteProfile(entityId) {
  return apiDelete(`/profiles/${entityId}`)
}
