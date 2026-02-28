import { apiGet, apiPost } from './fetch'
import { WS_BASE } from './config-base'

/**
 * Get community status and configuration
 */
export function getCommunityStats() {
  return apiGet('/community/stats')
}

/**
 * Enable or disable the community feature
 */
export function toggleCommunity(enabled) {
  return apiPost(`/community/toggle?enabled=${enabled}`)
}

/**
 * Manually trigger a discussion
 */
export function triggerDiscussion() {
  return apiPost('/community/trigger')
}

/**
 * Manually close the active discussion
 */
export function closeDiscussion() {
  return apiPost('/community/close')
}

/**
 * Get list of past discussions
 */
export function getDiscussions() {
  return apiGet('/community/discussions')
}

/**
 * Get message history for a specific discussion
 */
export function getDiscussionHistory(discussionId) {
  return apiGet(`/community/discussions/${discussionId}`)
}

/**
 * Get agent spawn hierarchy
 */
export function getAgentHierarchy() {
  return apiGet('/community/hierarchy')
}

/**
 * Get community memory for a specific agent
 */
export function getAgentCommunityMemory(agentId) {
  return apiGet(`/community/agents/${agentId}/memory`)
}

/**
 * Get recent insights from discussions
 */
export function getDiscussionInsights(limit = 10) {
  return apiGet(`/community/insights?limit=${limit}`)
}

/**
 * Connect to community WebSocket stream
 * Returns a close function
 */
export function connectCommunityWS(onEvent, onOpen, onClose) {
  const ws = new WebSocket(`${WS_BASE}/community/ws`)

  ws.onopen = () => {
    onOpen?.()
  }

  ws.onmessage = e => {
    try {
      const data = JSON.parse(e.data)
      onEvent(data)
    } catch (err) {
      console.error('[CommunityWS] Parse error:', err)
    }
  }

  ws.onclose = () => {
    onClose?.()
  }

  ws.onerror = err => {
    console.error('[CommunityWS] Error:', err)
  }

  return () => ws.close()
}
