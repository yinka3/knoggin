import { apiGet, apiPost } from './fetch'

export function getMergeProposals(sessionId) {
  return apiGet(`/proposals/${sessionId}/merges`)
}

export function approveMergeProposal(sessionId, index) {
  return apiPost(`/proposals/${sessionId}/merges/${index}/approve`)
}

export function rejectMergeProposal(sessionId, index) {
  return apiPost(`/proposals/${sessionId}/merges/${index}/reject`)
}
