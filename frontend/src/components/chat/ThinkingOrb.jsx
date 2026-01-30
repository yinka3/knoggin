import { motion } from 'motion/react'

export default function ThinkingOrb({ size = 24 }) {
  return (
    <div
      style={{
        position: 'relative',
        width: size,
        height: size,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
      }}
    >
      {/* 1. The Glow (Atmosphere)
          This extends slightly outside the bounds to make it feel glowing. 
          We use negative margins to center it perfectly. */}
      <motion.div
        style={{
          position: 'absolute',
          width: '150%',
          height: '150%',
          background: 'radial-gradient(circle, rgba(74, 222, 128, 0.4) 0%, rgba(0,0,0,0) 70%)',
          borderRadius: '50%',
          filter: 'blur(5px)', // Softens the light
          zIndex: 0,
        }}
        animate={{
          scale: [0.8, 1.2, 0.8],
          opacity: [0.3, 0.6, 0.3],
        }}
        transition={{
          duration: 2,
          repeat: Infinity,
          ease: 'easeInOut',
        }}
      />

      {/* 2. The Core (The Organic Shape) */}
      <motion.div
        style={{
          width: '100%',
          height: '100%',
          // Lighter, more vivid gradient to pop against the dark UI
          background: 'linear-gradient(135deg, #4ade80 0%, #16a34a 100%)',
          // Inner shadow adds "depth" so it looks like a gem/liquid, not a flat sticker
          boxShadow: 'inset 2px 2px 6px rgba(255,255,255,0.4), inset -2px -2px 6px rgba(0,0,0,0.2)',
          zIndex: 1,
        }}
        animate={{
          // Simplified organic morphing - less jittery than the original
          borderRadius: [
            '60% 40% 30% 70% / 60% 30% 70% 40%',
            '30% 60% 70% 40% / 50% 60% 30% 60%',
            '60% 40% 30% 70% / 60% 30% 70% 40%',
          ],
          rotate: [0, 180, 360],
        }}
        transition={{
          duration: 6, // Slower rotation feels more "intelligent"
          ease: 'linear',
          repeat: Infinity,
        }}
      />

      {/* 3. The Highlight (Reflection)
          A tiny white dot that stays fixed or moves slightly to sell the "liquid" effect */}
      <motion.div
        style={{
          position: 'absolute',
          top: '20%',
          left: '20%',
          width: '25%',
          height: '25%',
          background: 'rgba(255,255,255,0.6)',
          borderRadius: '50%',
          filter: 'blur(1px)',
          zIndex: 2,
        }}
        animate={{
          scale: [1, 1.2, 1],
          opacity: [0.6, 0.8, 0.6],
        }}
        transition={{
          duration: 2,
          repeat: Infinity,
          ease: 'easeInOut',
          delay: 0.5, // Offset from the main pulse
        }}
      />
    </div>
  )
}
