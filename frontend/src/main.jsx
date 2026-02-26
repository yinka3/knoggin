import { StrictMode, lazy, Suspense } from 'react'
/* eslint-disable react-refresh/only-export-components */
import { createRoot } from 'react-dom/client'
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { Toaster } from '@/components/ui/sonner'
import { SessionProvider } from './context/SessionContext'
import { SocketProvider } from './context/SocketContext'
import { ToolsProvider } from './context/ToolsContext'
import ConfigGate from './components/ConfigGate'
import ErrorBoundary from './components/ErrorBoundary'
import Layout from './components/layout/Layout'
import './index.css'

const OnboardingPage = lazy(() => import('./pages/OnboardingPage'))
const ChatPage = lazy(() => import('./pages/ChatPage'))
const MemoryPage = lazy(() => import('./pages/MemoryPage'))
const SettingsPage = lazy(() => import('./pages/SettingsPage'))
const AgentsPage = lazy(() => import('./pages/AgentsPage'))
const DebugPage = lazy(() => import('./pages/DebugPage'))
const DashboardPage = lazy(() => import('./pages/DashboardPage'))
const DeveloperSettingsPage = lazy(() => import('./pages/DeveloperSettingsPage'))

function LoadingFallback() {
  return (
    <div className="min-h-screen flex items-center justify-center">
      <div
        className="rounded-full animate-pulse"
        style={{
          width: 32,
          height: 32,
          background: 'radial-gradient(circle at 40% 40%, rgba(52, 216, 130, 0.9), rgba(46, 170, 110, 0.6))',
          boxShadow: '0 0 20px rgba(46, 170, 110, 0.4)',
        }}
      />
    </div>
  )
}

function AppRoutes() {
  return (
    <SessionProvider>
      <SocketProvider>
        <ToolsProvider>
          <Layout>
            <Suspense fallback={<LoadingFallback />}>
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
            </Suspense>
          </Layout>
        </ToolsProvider>
      </SocketProvider>
    </SessionProvider>
  )
}

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <BrowserRouter>
      <ErrorBoundary>
        <ConfigGate>
          <Suspense fallback={<LoadingFallback />}>
            <Routes>
              <Route path="/onboarding" element={<OnboardingPage />} />
              <Route path="/*" element={<AppRoutes />} />
            </Routes>
          </Suspense>
        </ConfigGate>
        <Toaster />
      </ErrorBoundary>
    </BrowserRouter>
  </StrictMode>
)
