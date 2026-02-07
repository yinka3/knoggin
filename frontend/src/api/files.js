const API_BASE = 'http://localhost:8000'

export async function uploadFile(sessionId, file) {
  const formData = new FormData()
  formData.append('file', file)

  const res = await fetch(`${API_BASE}/files/${sessionId}/upload`, {
    method: 'POST',
    body: formData,
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(err.detail || 'Failed to upload file')
  }
  return res.json()
}

export async function listFiles(sessionId) {
  const res = await fetch(`${API_BASE}/files/${sessionId}`)
  if (!res.ok) throw new Error('Failed to list files')
  return res.json()
}

export async function deleteFile(sessionId, fileId) {
  const res = await fetch(`${API_BASE}/files/${sessionId}/${fileId}`, {
    method: 'DELETE',
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(err.detail || 'Failed to delete file')
  }
  return res.json()
}
