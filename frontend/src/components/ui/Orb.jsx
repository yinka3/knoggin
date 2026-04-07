import { motion } from 'motion/react'
import { cn } from '@/lib/utils'

export default function Orb({ 
  size = 80, 
  className, 
  isReady = true,
  variant = 'default' 
}) {
  return (
    <motion.div
      className={cn("absolute rounded-full", className)}
      style={{
        width: size,
        height: size,
      }}
      initial={variant === 'welcome' ? { scale: 0, opacity: 0 } : false}
      animate={{ scale: 1, opacity: 1 }}
      transition={{
        scale: { duration: 0.6, ease: [0.22, 1, 0.36, 1] },
        opacity: { duration: 0.4 },
      }}
    >
      {/* Core orb */}
      <div
        className={cn(
          'w-full h-full relative overflow-hidden orb-breathe bg-[rgba(30,120,80,1)]',
          isReady ? 'opacity-100' : 'opacity-80'
        )}
        style={{
          boxShadow: `
            0 0 ${size * 0.375}px rgba(46, 170, 110, 0.5),
            inset 0 0 ${size * 0.25}px rgba(0, 0, 0, 0.3),
            inset 0 0 ${size * 0.125}px rgba(255, 255, 255, 0.1)
          `,
        }}
      >
        {/* Swirling energy layers */}
        <div
          className="absolute inset-0 orb-swirl-1"
          style={{
            background: 'radial-gradient(circle at 30% 70%, rgba(52, 216, 130, 0.9), transparent 60%)',
            filter: 'blur(5px)',
          }}
        />
        <div
          className="absolute inset-0 orb-swirl-2"
          style={{
            background: 'radial-gradient(circle at 80% 40%, rgba(132, 250, 180, 0.8), transparent 60%)',
            filter: 'blur(5px)',
          }}
        />
        <div
          className="absolute inset-0 orb-swirl-3"
          style={{
            background: 'radial-gradient(circle at 40% 20%, rgba(16, 90, 50, 0.9), transparent 60%)',
            filter: 'blur(5px)',
          }}
        />

        {/* Glassy reflection */}
        <div className="absolute inset-0 orb-glass-reflection pointer-events-none" style={{ borderRadius: 'inherit' }} />
      </div>

      {/* Outer glow */}
      <div
        className={cn(
          'absolute rounded-full orb-glow-ring transition-opacity duration-1000',
          isReady ? 'opacity-100' : 'opacity-0'
        )}
        style={{
          inset: -(size * 0.25),
          background: 'radial-gradient(circle, rgba(46, 170, 110, 0.25), transparent 70%)',
          filter: 'blur(15px)',
        }}
      />
    </motion.div>
  )
}
