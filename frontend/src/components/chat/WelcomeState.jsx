import Orb from '@/components/ui/Orb'
import { motion } from 'motion/react'
import { ArrowUp, Loader2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import greetings from '@/data/greetings.json'

function getGreeting() {
  const hour = new Date().getHours()
  if (hour < 6) return "You're Up Early"
  if (hour < 12) return 'Good Morning'
  if (hour < 17) return 'Good Afternoon'
  if (hour < 21) return 'Good Evening'
  return 'Late Night Mode'
}

function getSubtext() {
  const hour = new Date().getHours()
  let key
  if (hour < 6) key = 'early_morning'
  else if (hour < 12) key = 'morning'
  else if (hour < 17) key = 'afternoon'
  else if (hour < 21) key = 'evening'
  else key = 'late_night'
  const pool = greetings[key]
  return pool[Math.floor(Math.random() * pool.length)]
}

// Scattered positions (px from center) → contracted positions
const DOTS = [
  { sx: -65, sy: -50 },
  { sx: 45, sy: -70 },
  { sx: 75, sy: -15 },
  { sx: 55, sy: 55 },
  { sx: -20, sy: 70 },
  { sx: -70, sy: 25 },
  { sx: 10, sy: -42 },
  { sx: -42, sy: -25 },
  { sx: 30, sy: 38 },
  { sx: -50, sy: 50 },
]

// Which dots connect (index pairs)
const EDGES = [
  [0, 7],
  [7, 5],
  [5, 9],
  [9, 4],
  [4, 8],
  [8, 3],
  [3, 2],
  [2, 1],
  [1, 6],
  [6, 0],
  [7, 6],
  [8, 2],
  [5, 4],
]

function Dot({ x, y, phase, index, size = 5 }) {
  return (
    <motion.div
      className="absolute rounded-full"
      style={{
        width: size,
        height: size,
        left: '50%',
        top: '50%',
        marginLeft: -size / 2,
        marginTop: -size / 2,
        background: 'radial-gradient(circle, rgba(46,170,110,0.95), rgba(46,170,110,0.4))',
        boxShadow: '0 0 8px rgba(46,170,110,0.6), 0 0 16px rgba(46,170,110,0.2)',
      }}
      initial={{ x: 0, y: 0, scale: 0, opacity: 0 }}
      animate={{
        x: phase >= 3 ? 0 : x,
        y: phase >= 3 ? 0 : y,
        scale: phase >= 1 && phase < 4 ? 1 : 0,
        opacity: phase >= 1 && phase < 4 ? (phase >= 3 ? 0.4 : 0.85) : 0,
      }}
      transition={{
        x: { duration: 0.5, ease: [0.22, 1, 0.36, 1] },
        y: { duration: 0.5, ease: [0.22, 1, 0.36, 1] },
        scale: { duration: 0.2, delay: phase === 1 ? index * 0.04 : 0 },
        opacity: { duration: phase >= 3 ? 0.4 : 0.2, delay: phase === 1 ? index * 0.04 : 0 },
      }}
    />
  )
}

function ConstellationCanvas({ phase }) {
  const canvasRef = useRef(null)
  const animRef = useRef(null)
  const progressRef = useRef(0)
  const contractStartRef = useRef(null)

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    const cx = canvas.width / 2
    const cy = canvas.height / 2

    // Phase 4+: clear and stop
    if (phase >= 4) {
      ctx.clearRect(0, 0, canvas.width, canvas.height)
      return
    }

    // Track when contraction starts
    if (phase === 3 && !contractStartRef.current) {
      contractStartRef.current = performance.now()
    }

    // Reset line draw progress at phase 2
    if (phase === 2) progressRef.current = 0

    function draw(timestamp) {
      ctx.clearRect(0, 0, canvas.width, canvas.height)

      if (phase >= 2) {
        progressRef.current += (1 - progressRef.current) * 0.15 // Faster draw

        // Contract toward center in phase 3
        let contractT = 0
        if (phase >= 3 && contractStartRef.current) {
          contractT = Math.min(1, (timestamp - contractStartRef.current) / 400) // Faster contract
        }
        const eased = 1 - Math.pow(1 - contractT, 3)
        const alpha = phase >= 3 ? Math.max(0, 0.35 * (1 - contractT * 1.5)) : 0.3

        if (alpha > 0.01) {
          EDGES.forEach(([fromIdx, toIdx], i) => {
            const lineProgress = Math.max(
              0,
              Math.min(1, (progressRef.current * EDGES.length - i) / 1)
            )
            if (lineProgress <= 0) return

            const from = DOTS[fromIdx]
            const to = DOTS[toIdx]
            const fx = cx + from.sx * (1 - eased)
            const fy = cy + from.sy * (1 - eased)
            const tx = cx + to.sx * (1 - eased)
            const ty = cy + to.sy * (1 - eased)

            ctx.beginPath()
            ctx.moveTo(fx, fy)
            ctx.lineTo(fx + (tx - fx) * lineProgress, fy + (ty - fy) * lineProgress)
            ctx.strokeStyle = `rgba(46, 170, 110, ${alpha * lineProgress})`
            ctx.lineWidth = 1
            ctx.stroke()
          })
        }
      }

      animRef.current = requestAnimationFrame(draw)
    }

    animRef.current = requestAnimationFrame(draw)
    return () => {
      if (animRef.current) cancelAnimationFrame(animRef.current)
    }
  }, [phase])

  return (
    <canvas
      ref={canvasRef}
      width={200}
      height={200}
      className="absolute inset-0 pointer-events-none"
      style={{ width: 200, height: 200 }}
    />
  )
}

function PulseRing({ active }) {
  if (!active) return null
  return (
    <motion.div
      className="absolute rounded-full border border-primary/30"
      style={{ left: '50%', top: '50%', translateX: '-50%', translateY: '-50%' }}
      initial={{ width: 20, height: 20, opacity: 0.6 }}
      animate={{ width: 200, height: 200, opacity: 0 }}
      transition={{ duration: 0.9, ease: 'easeOut' }}
    />
  )
}

export default function WelcomeState({ onFirstMessage, userName }) {
  // 0=empty, 1=dots appear, 2=lines draw, 3=contract+drop, 4=pulse+settle, 5=ready
  const [phase, setPhase] = useState(0)
  const [inputValue, setInputValue] = useState('')
  const [isFocused, setIsFocused] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const textareaRef = useRef(null)
  const [subtext] = useState(() => getSubtext())

  useEffect(() => {
    const timers = [
      setTimeout(() => setPhase(1), 50), // dots fade in, staggered
      setTimeout(() => setPhase(2), 300), // lines draw between dots
      setTimeout(() => setPhase(3), 600), // network contracts + brain drops
      setTimeout(() => setPhase(4), 1000), // pulse ring, constellation gone
      setTimeout(() => setPhase(5), 1200), // fully ready
    ]
    return () => timers.forEach(clearTimeout)
  }, [])

  const isReady = phase >= 5



  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto'
      textareaRef.current.style.height = `${textareaRef.current.scrollHeight}px`
    }
  }, [inputValue])

  const handleKeyDown = e => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSubmit()
    }
  }

  const handleSubmit = () => {
    if (!inputValue.trim() || submitting) return
    setSubmitting(true)
    const msg = inputValue.trim()
    setInputValue('')
    onFirstMessage(msg)
  }

  return (
    <div className="h-full flex flex-col items-center justify-center p-8 relative overflow-hidden">
      {/* === CONSTELLATION (centered) → ORB + GREETING (horizontal) === */}
      <motion.div
        className={cn(
          'flex items-center gap-6 mb-10',
          isReady ? 'flex-row' : 'flex-col'
        )}
        layout
        transition={{ layout: { type: 'spring', stiffness: 200, damping: 25, mass: 1 } }}
      >
        {/* Constellation + Orb container */}
        <motion.div
          className="relative shrink-0 flex items-center justify-center"
          style={{ width: isReady ? 80 : 200, height: isReady ? 80 : 200 }}
          layout
          transition={{ layout: { type: 'spring', stiffness: 200, damping: 25, mass: 1 } }}
        >
          {/* Constellation dots */}
          {!isReady && DOTS.map((dot, i) => (
            <Dot key={i} x={dot.sx} y={dot.sy} phase={phase} index={i} size={4 + (i % 3) * 1.5} />
          ))}

          {/* Constellation lines */}
          {!isReady && <ConstellationCanvas phase={phase} />}

          {/* Pulse ring */}
          <PulseRing active={phase >= 4} />

          {/* Gradient orb */}
          <Orb 
            size={isReady ? 80 : 200} 
            isReady={isReady} 
            className="absolute"
            variant={phase >= 3 ? 'default' : 'welcome'}
            style={{ 
              opacity: phase >= 3 ? 1 : 0,
              scale: phase >= 3 ? 1 : 0
            }}
          />
        </motion.div>

        {/* Greeting text — appears to the right of orb */}
        <motion.div
          className="flex flex-col z-10"
          initial={{ opacity: 0, x: -20 }}
          animate={isReady ? { opacity: 1, x: 0 } : { opacity: 0, x: -20 }}
          transition={{ duration: 0.5, delay: 0.1, ease: 'easeOut' }}
        >
          <h1 className="text-2xl font-semibold tracking-tight mb-1 text-foreground">
            {getGreeting()}
            {userName ? `, ${userName}` : ''}
          </h1>
          <p className="text-muted-foreground max-w-[400px] leading-relaxed">
            {subtext}
          </p>
        </motion.div>
      </motion.div>

      {/* === INPUT BAR === */}
      <motion.div
        className={cn(
          'w-full max-w-2xl relative flex items-end gap-2 p-2 rounded-2xl border transition-all duration-300 mb-8',
          isFocused
            ? 'bg-background border-primary/50 ring-2 ring-primary/10 shadow-md'
            : 'bg-background border-input hover:border-accent'
        )}
        initial={{ opacity: 0, y: 20 }}
        animate={isReady ? { opacity: 1, y: 0 } : { opacity: 0, y: 20 }}
        transition={{ duration: 0.5, delay: 0.15, ease: 'easeOut' }}
      >
        <textarea
          ref={textareaRef}
          value={inputValue}
          onChange={e => setInputValue(e.target.value)}
          onKeyDown={handleKeyDown}
          onFocus={() => setIsFocused(true)}
          onBlur={() => setIsFocused(false)}
          placeholder="Ask me anything..."
          disabled={submitting}
          className="flex-1 w-full bg-transparent border-none focus:ring-0 focus:outline-none focus-visible:outline-none resize-none max-h-[200px] min-h-[44px] py-3 px-4 text-sm text-foreground placeholder:text-muted-foreground/70 leading-relaxed disabled:opacity-50"
          rows={1}
        />
        <Button
          size="icon"
          onClick={handleSubmit}
          disabled={!inputValue.trim() || submitting}
          className={cn(
            'rounded-xl h-10 w-10 shrink-0 transition-all duration-300 mb-1',
            inputValue.trim() && !submitting
              ? 'bg-primary text-primary-foreground hover:bg-primary/90 shadow-sm'
              : 'bg-muted text-muted-foreground hover:bg-muted opacity-50'
          )}
        >
          {submitting ? (
            <Loader2 size={18} strokeWidth={2} className="animate-spin" />
          ) : (
            <ArrowUp size={18} strokeWidth={2} />
          )}
        </Button>
      </motion.div>
    </div>
  )
}
