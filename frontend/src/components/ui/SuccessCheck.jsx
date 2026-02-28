import { motion } from 'motion/react'
import { Check } from 'lucide-react'

export function SuccessCheck({ size = 20 }) {
  return (
    <motion.div
      initial={{ scale: 0, rotate: -45 }}
      animate={{ scale: 1, rotate: 0 }}
      transition={{ type: 'spring', stiffness: 400, damping: 15 }}
      className="text-primary"
    >
      <Check size={size} strokeWidth={3} />
    </motion.div>
  )
}
