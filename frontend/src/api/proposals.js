import { apiGet, apiPost } from './fetch'

export function getMergeProposals(sessionId) {
  return apiGet(`/proposals/${sessionId}/merges`)
}

export function approveMergeProposal(sessionId, index, primaryId, secondaryId) {
  return apiPost(`/proposals/${sessionId}/merges/${index}/approve`, { primary_id: primaryId, secondary_id: secondaryId })
}

export function rejectMergeProposal(sessionId, index, primaryId, secondaryId) {
  return apiPost(`/proposals/${sessionId}/merges/${index}/reject`, { primary_id: primaryId, secondary_id: secondaryId })
}

export function undoMergeProposal(sessionId, primaryId, secondaryId) {
  return apiPost(`/proposals/${sessionId}/merges/undo`, { primary_id: primaryId, secondary_id: secondaryId })
}
