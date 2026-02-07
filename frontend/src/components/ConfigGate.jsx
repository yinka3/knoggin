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
    getConfigStatus()
      .then(status => {
        setConfigured(status.configured)
        setChecked(true)
      })
      .catch(() => {
        setConfigured(false)
        setChecked(true)
      })
  }, [location.pathname])

  useEffect(() => {
    if (!checked) return
    if (!configured && location.pathname !== '/onboarding') {
      navigate('/onboarding', { replace: true })
    }
    if (configured && location.pathname === '/onboarding') {
      navigate('/chat', { replace: true })
    }
  }, [checked, configured])

  if (!checked) return <LoadingScreen />
  return children
}
