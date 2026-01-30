import { useState, useEffect } from 'react'
import { Sparkles } from 'lucide-react'
import { getConfig } from '@/api/config'
import InputBar from './InputBar'

function getGreeting() {
  const hour = new Date().getHours()
  if (hour < 12) return 'Good morning'
  if (hour < 17) return 'Good afternoon'
  return 'Good evening'
}

export default function WelcomeState({ onFirstMessage }) {
  const [userName, setUserName] = useState('')
  const [agentName, setAgentName] = useState('STELLA')
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    getConfig()
      .then(config => {
        setUserName(config.user_name || '')
        setAgentName(config.agent_name || 'STELLA')
      })
      .catch(err => console.error('Failed to load config:', err))
      .finally(() => setLoading(false))
  }, [])

  const greeting = getGreeting()

  return (
    <div className="flex flex-col items-center justify-center h-full px-4">
      <div className="max-w-2xl w-full space-y-8">
        {/* Greeting */}
        <div className="text-center space-y-2">
          <div className="flex items-center justify-center gap-2 text-3xl font-medium text-foreground">
            <Sparkles className="text-primary" size={28} />
            <span>
              {greeting}
              {userName && `, ${userName}`}
            </span>
          </div>
          <p className="text-muted-foreground">How can {agentName} help you today?</p>
        </div>

        {/* Input */}
        <InputBar
          onSend={onFirstMessage}
          disabled={loading}
          placeholder="Start a conversation..."
        />
      </div>
    </div>
  )
}
