import { apiGet, apiPost } from './fetch'

export function getOnboardingStatus() {
  return apiGet('/onboarding/status')
}

export function getQuestions(path) {
  return apiGet(`/onboarding/questions/${path}`)
}

export function generateTopics(responses) {
  return apiPost('/onboarding/generate-topics', { responses })
}

export function saveTopics(topics) {
  return apiPost('/onboarding/save', { topics })
}

export function runExtraction(responses) {
  return apiPost('/onboarding/extract', { responses })
}
