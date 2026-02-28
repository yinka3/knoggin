import { apiFetch, apiGet, apiDelete } from './fetch'

export function uploadFile(sessionId, file) {
  const formData = new FormData()
  formData.append('file', file)

  return apiFetch(`/files/${sessionId}/upload`, {
    method: 'POST',
    body: formData,
  })
}

export function listFiles(sessionId) {
  return apiGet(`/files/${sessionId}`)
}

export function deleteFile(sessionId, fileId) {
  return apiDelete(`/files/${sessionId}/${fileId}`)
}
