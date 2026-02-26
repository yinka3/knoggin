import { motion, AnimatePresence } from 'motion/react'

/**
 * A BMO-inspired dynamic face component.
 * Redesigned for clarity at small sizes (22–32px).
 *
 * States:
 * - 'idle' / undefined → sleeping
 * - 'searching' / 'tool_call' → darting eyes + worried mouth
 * - 'generating' / 'thinking' → blinking eyes + open mouth
 * - 'done' / 'success' → happy squint + smile
 */
export default function ThinkingFace({ state = 'idle', size = 24 }) {
  const getExpression = () => {
    switch (state) {
      case 'searching':
      case 'tool_call':
        return 'searching'
      case 'generating':
      case 'thinking':
        return 'awake'
      case 'done':
      case 'success':
        return 'happy'
      default:
        return 'sleeping'
    }
  }

  const expression = getExpression()

  // Scale proportions — tuned for 22-32px
  const eyeW = Math.max(3, Math.round(size * 0.16))
  const eyeH = Math.max(4, Math.round(size * 0.22))
  const eyeGap = Math.max(4, Math.round(size * 0.28))
  const mouthSize = Math.max(6, Math.round(size * 0.35))
  const mouthOffset = Math.max(2, Math.round(size * 0.08))

  return (
    <div
      className="relative flex flex-col items-center justify-center rounded-lg bg-[hsl(160,15%,14%)] border border-primary/25 shadow-[inset_0_1px_3px_rgba(0,0,0,0.3)]"
      style={{ width: size, height: size }}
    >
      <AnimatePresence mode="wait">

        {/* === SLEEPING === */}
        {expression === 'sleeping' && (
          <motion.div
            key="sleeping"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.15 }}
            className="flex flex-col items-center"
          >
            <div className="flex items-center" style={{ gap: eyeGap }}>
              <div
                className="bg-muted-foreground/40 rounded-full"
                style={{ width: eyeW * 1.3, height: Math.max(1.5, size * 0.07) }}
              />
              <div
                className="bg-muted-foreground/40 rounded-full"
                style={{ width: eyeW * 1.3, height: Math.max(1.5, size * 0.07) }}
              />
            </div>
            {/* Wavy sleeping mouth */}
            <svg width={mouthSize} height={mouthSize * 0.4} viewBox="0 0 12 5" fill="none" style={{ marginTop: mouthOffset }}>
              <motion.path
                d="M2 3C3.5 1.5 5 3.5 6 2.5C7 1.5 8.5 3.5 10 2"
                stroke="currentColor"
                strokeWidth="1.2"
                strokeLinecap="round"
                className="text-muted-foreground/30"
                animate={{ opacity: [0.3, 0.5, 0.3] }}
                transition={{ duration: 3, repeat: Infinity }}
              />
            </svg>
          </motion.div>
        )}

        {/* === AWAKE (Generating / Thinking) === */}
        {expression === 'awake' && (
          <motion.div
            key="awake"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.15 }}
            className="flex flex-col items-center"
          >
            <div className="flex items-center" style={{ gap: eyeGap }}>
              <motion.div
                className="bg-primary rounded-full"
                style={{ width: eyeW, height: eyeH, boxShadow: '0 0 6px rgba(46,170,110,0.4)' }}
                animate={{ scaleY: [1, 1, 0.15, 1, 1] }}
                transition={{ duration: 3.5, repeat: Infinity, times: [0, 0.93, 0.96, 0.99, 1] }}
              />
              <motion.div
                className="bg-primary rounded-full"
                style={{ width: eyeW, height: eyeH, boxShadow: '0 0 6px rgba(46,170,110,0.4)' }}
                animate={{ scaleY: [1, 1, 0.15, 1, 1] }}
                transition={{ duration: 3.5, repeat: Infinity, times: [0, 0.93, 0.96, 0.99, 1] }}
              />
            </div>
            {/* Open 'o' mouth — breathing */}
            <motion.svg
              width={mouthSize * 0.5}
              height={mouthSize * 0.5}
              viewBox="0 0 8 8"
              fill="none"
              style={{ marginTop: mouthOffset }}
              animate={{ scaleY: [0.8, 1.1, 0.8], scaleX: [1, 0.9, 1] }}
              transition={{ duration: 2.5, repeat: Infinity, ease: 'easeInOut' }}
            >
              <ellipse cx="4" cy="4" rx="2.5" ry="2.5" stroke="currentColor" strokeWidth="1.2" className="text-primary/50" />
            </motion.svg>
          </motion.div>
        )}

        {/* === SEARCHING (Tool call — darting eyes) === */}
        {expression === 'searching' && (
          <motion.div
            key="searching"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.15 }}
            className="flex flex-col items-center"
          >
            <div className="flex items-center" style={{ gap: eyeGap }}>
              <motion.div
                className="bg-accent rounded-full"
                style={{ width: eyeW, height: eyeW, boxShadow: '0 0 5px rgba(52,216,130,0.4)' }}
                animate={{ x: [-1.5, 1.5, -1.5] }}
                transition={{ duration: 1.2, repeat: Infinity, ease: 'easeInOut' }}
              />
              <motion.div
                className="bg-accent rounded-full"
                style={{ width: eyeW, height: eyeW, boxShadow: '0 0 5px rgba(52,216,130,0.4)' }}
                animate={{ x: [-1.5, 1.5, -1.5] }}
                transition={{ duration: 1.2, repeat: Infinity, ease: 'easeInOut' }}
              />
            </div>
            {/* Worried squiggly mouth */}
            <motion.svg
              width={mouthSize}
              height={mouthSize * 0.4}
              viewBox="0 0 12 5"
              fill="none"
              style={{ marginTop: mouthOffset }}
              animate={{ x: [-0.5, 0.5, -0.5] }}
              transition={{ duration: 1.2, repeat: Infinity, ease: 'easeInOut' }}
            >
              <path
                d="M2 3.5C3.5 1.5 5.5 4 7 2C8.5 0 10 3 10 3"
                stroke="currentColor"
                strokeWidth="1.3"
                strokeLinecap="round"
                className="text-accent/60"
              />
            </motion.svg>
          </motion.div>
        )}

        {/* === HAPPY (Done) === */}
        {expression === 'happy' && (
          <motion.div
            key="happy"
            initial={{ opacity: 0, scale: 0.8 }}
            animate={{ opacity: 1, scale: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="flex flex-col items-center"
          >
            <div className="flex items-center" style={{ gap: eyeGap }}>
              {/* Happy squint eyes — upward arcs */}
              <svg width={eyeW * 1.6} height={eyeH * 0.7} viewBox="0 0 10 6" fill="none">
                <path d="M1 5C1 5 3 1 5 1C7 1 9 5 9 5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" className="text-primary" />
              </svg>
              <svg width={eyeW * 1.6} height={eyeH * 0.7} viewBox="0 0 10 6" fill="none">
                <path d="M1 5C1 5 3 1 5 1C7 1 9 5 9 5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" className="text-primary" />
              </svg>
            </div>
            {/* Big smile */}
            <svg width={mouthSize} height={mouthSize * 0.45} viewBox="0 0 12 5" fill="none" style={{ marginTop: mouthOffset * 0.8 }}>
              <path d="M2 1C2 1 4.5 4.5 6 4.5C7.5 4.5 10 1 10 1" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" className="text-primary/70" />
            </svg>
          </motion.div>
        )}

      </AnimatePresence>
    </div>
  )
}
