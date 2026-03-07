import { useState, useEffect, useCallback } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import { getConfigStatus } from '@/api/config'
import { Brain, RefreshCw } from 'lucide-react'

const MAX_RETRIES = 3
const RETRY_DELAY = 2000

function LoadingScreen() {
  return (
    <div className="min-h-screen flex items-center justify-center gradient-bg">
      <div className="flex flex-col items-center gap-4">
        <div className="p-4 rounded-2xl bg-primary/10 animate-pulse">
          <Brain size={32} className="text-primary" />
        </div>
        <p className="text-muted-foreground text-sm">Loading...</p>
      </div>
    </div>
  )
}

function ConnectionError({ onRetry }) {
  return (
    <div className="min-h-screen flex items-center justify-center gradient-bg">
      <div className="flex flex-col items-center gap-4 text-center max-w-sm px-6">
        <div className="p-4 rounded-2xl bg-destructive/10">
          <Brain size={32} className="text-destructive" />
        </div>
        <div>
          <h2 className="text-base font-semibold text-foreground mb-1">
            Connection failed
          </h2>
          <p className="text-sm text-muted-foreground">
            Could not reach the backend server. Make sure it's running and try again.
          </p>
        </div>
        <button
          onClick={onRetry}
          className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium bg-primary text-primary-foreground hover:bg-primary/90 transition-colors"
        >
          <RefreshCw size={14} />
          Retry
        </button>
      </div>
    </div>
  )
}

export default function ConfigGate({ children }) {
  const [checked, setChecked] = useState(false)
  const [configured, setConfigured] = useState(false)
  const [failed, setFailed] = useState(false)
  const navigate = useNavigate()
  const location = useLocation()

  const checkConfig = useCallback(async () => {
    setFailed(false)

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        const status = await getConfigStatus()
        setConfigured(status.configured)
        setChecked(true)
        return
      } catch {
        if (attempt < MAX_RETRIES - 1) {
          await new Promise(r => setTimeout(r, RETRY_DELAY))
        }
      }
    }

    setFailed(true)
  }, [])

  useEffect(() => {
    setTimeout(() => checkConfig(), 0)
  }, [checkConfig, location.pathname])

  useEffect(() => {
    if (!checked) return

    if (!configured && location.pathname !== '/onboarding') {
      navigate('/onboarding', { replace: true })
    }
    if (configured && location.pathname === '/onboarding') {
      navigate('/chat', { replace: true })
    }
  }, [checked, configured, location.pathname, navigate])

  if (failed) return <ConnectionError onRetry={checkConfig} />

  if (!checked) return <LoadingScreen />

  // Prevent flash of content/redirects if we're about to redirect
  if (!configured && location.pathname !== '/onboarding') {
    return <LoadingScreen />
  }

  if (configured && location.pathname === '/onboarding') {
    return <LoadingScreen />
  }

  return children
}

