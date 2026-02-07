import { useEffect, useState, useRef } from 'react'
import { motion, useSpring, useTransform } from 'framer-motion'

function formatNumber(num) {
  return Math.round(num).toLocaleString()
}

function Particle({ active, index, total }) {
  const angle = (index / total) * 360
  const radians = (angle * Math.PI) / 180
  const distance = 30

  return (
    <motion.div
      className="absolute w-1.5 h-1.5 rounded-full bg-primary"
      initial={{ opacity: 0, scale: 0, x: 0, y: 0 }}
      animate={
        active
          ? {
              opacity: [0, 1, 0],
              scale: [0, 1, 0],
              x: [0, Math.cos(radians) * distance],
              y: [0, Math.sin(radians) * distance],
            }
          : {}
      }
      transition={{ duration: 0.6, ease: 'easeOut' }}
    />
  )
}

export default function TokenCounter({ value }) {
  const [showMilestone, setShowMilestone] = useState(false)
  const prevMilestone = useRef(0)
  const springValue = useSpring(0, { stiffness: 50, damping: 20 })
  const display = useTransform(springValue, formatNumber)
  const [displayText, setDisplayText] = useState('0')

  useEffect(() => {
    springValue.set(value)
  }, [value, springValue])

  useEffect(() => {
    return display.on('change', v => setDisplayText(v))
  }, [display])

  // Milestone detection
  useEffect(() => {
    const currentMilestone = Math.floor(value / 50000)
    if (currentMilestone > prevMilestone.current && value > 0) {
      setShowMilestone(true)
      setTimeout(() => setShowMilestone(false), 700)
    }
    prevMilestone.current = currentMilestone
  }, [value])

  const particles = Array.from({ length: 8 })

  return (
    <div className="relative flex items-center gap-1.5 text-xs font-mono text-muted-foreground bg-muted/50 border border-border rounded-full px-2.5 py-1">
      <motion.span
        animate={showMilestone ? { scale: [1, 1.3, 1] } : {}}
        transition={{ duration: 0.4 }}
        className="relative"
      >
        {displayText}
        {particles.map((_, i) => (
          <Particle key={i} active={showMilestone} index={i} total={8} />
        ))}
      </motion.span>
      <span>tokens</span>
    </div>
  )
}
