import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { Toaster } from '@/components/ui/sonner'
import { SessionProvider } from './context/SessionContext'
import ConfigGate from './components/ConfigGate'
import Layout from './components/layout/Layout'
import OnboardingPage from './pages/OnboardingPage'
import ChatPage from './pages/ChatPage'
import MemoryPage from './pages/MemoryPage'
import SettingsPage from './pages/SettingsPage'
import AgentsPage from './pages/AgentsPage'
import DebugPage from './pages/DebugPage'
import DashboardPage from './pages/DashboardPage'
import DeveloperSettingsPage from './pages/DeveloperSettingsPage'
import './index.css'

function AppRoutes() {
  return (
    <SessionProvider>
      <Layout>
        <Routes>
          <Route path="/" element={<Navigate to="/chat" replace />} />
          <Route path="/chat" element={<ChatPage />} />
          <Route path="/chat/:sessionId" element={<ChatPage />} />
          <Route path="/dashboard" element={<DashboardPage />} />
          <Route path="/memory" element={<MemoryPage />} />
          <Route path="/agents" element={<AgentsPage />} />
          <Route path="/debug" element={<DebugPage />} />
          <Route path="/settings" element={<SettingsPage />} />
          <Route path="/settings/developer" element={<DeveloperSettingsPage />} />
        </Routes>
      </Layout>
    </SessionProvider>
  )
}

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <BrowserRouter>
      <ConfigGate>
        <Routes>
          <Route path="/onboarding" element={<OnboardingPage />} />
          <Route path="/*" element={<AppRoutes />} />
        </Routes>
      </ConfigGate>
      <Toaster />
    </BrowserRouter>
  </StrictMode>
)
