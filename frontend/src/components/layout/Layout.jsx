import { useState, useCallback } from 'react'
import Sidebar from './Sidebar'
import { TooltipProvider } from '@/components/ui/tooltip'

function getInitialSidebarState() {
  try {
    const stored = localStorage.getItem('sidebar-open')
    return stored === null ? true : stored === 'true'
  } catch {
    return true
  }
}

export default function Layout({ children }) {
  const [sidebarOpen, setSidebarOpen] = useState(getInitialSidebarState)

  const handleToggle = useCallback(() => {
    setSidebarOpen(prev => {
      const next = !prev
      try { localStorage.setItem('sidebar-open', String(next)) } catch {}
      return next
    })
  }, [])
  return (
    <TooltipProvider delayDuration={0}>
      <div className="flex h-screen bg-background text-foreground overflow-hidden">
        <div className="gradient-bg" />
        <Sidebar isOpen={sidebarOpen} onToggle={handleToggle} />
        <main className="flex-1 flex flex-col overflow-hidden">{children}</main>
      </div>
    </TooltipProvider>
  )
}
