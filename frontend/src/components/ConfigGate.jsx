import { useState, useEffect } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import { getConfigStatus } from '@/api/config'
import { Brain } from 'lucide-react'

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

export default function ConfigGate({ children }) {
  const [checked, setChecked] = useState(false)
  const [configured, setConfigured] = useState(false)
  const navigate = useNavigate()
  const location = useLocation()

  useEffect(() => {
    console.log('ConfigGate: checking status for', location.pathname)
    getConfigStatus()
      .then(status => {
        console.log('ConfigGate: status received', status)
        setConfigured(status.configured)
        setChecked(true)
      })
      .catch((err) => {
        console.error('ConfigGate: connection failed', err)
        setConfigured(false)
        setChecked(true)
      })
  }, [location.pathname])

  useEffect(() => {
    if (!checked) return
    console.log('ConfigGate: evaluating redirect', { checked, configured, path: location.pathname })
    
    if (!configured && location.pathname !== '/onboarding') {
      console.log('ConfigGate: redirecting to onboarding')
      navigate('/onboarding', { replace: true })
    }
    if (configured && location.pathname === '/onboarding') {
      console.log('ConfigGate: redirecting to chat')
      navigate('/chat', { replace: true })
    }
  }, [checked, configured, location.pathname, navigate])

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
