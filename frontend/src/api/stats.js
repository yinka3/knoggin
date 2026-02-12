import { apiGet } from './fetch'

export function getStats() {
  return apiGet('/stats/')
}

export function getStatsBreakdown() {
  return apiGet('/stats/breakdown')
}
