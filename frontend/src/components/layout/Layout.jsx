import { useState } from 'react'
import Sidebar from './Sidebar'
import { Toaster } from '@/components/ui/sonner'

export default function Layout({ children }) {
  const [sidebarOpen, setSidebarOpen] = useState(true)

  return (
    <div className="flex h-screen bg-background text-foreground">
      <Sidebar isOpen={sidebarOpen} onToggle={() => setSidebarOpen(!sidebarOpen)} />
      <main className="flex-1 flex flex-col overflow-hidden">{children}</main>

      <Toaster />
    </div>
  )
}
