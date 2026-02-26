import { useState, useEffect } from 'react'

export default function useDelayedLoading(loading, delay = 150) {
  const [showSkeleton, setShowSkeleton] = useState(false)

  useEffect(() => {
    if (loading) {
      const timer = setTimeout(() => setShowSkeleton(true), delay)
      return () => clearTimeout(timer)
    }
    setTimeout(() => setShowSkeleton(false), 0)
  }, [loading, delay])

  return showSkeleton
}
