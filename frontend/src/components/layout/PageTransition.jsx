import { motion } from 'motion/react'

export default function PageTransition({ children, className = '' }) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, scale: 0.98, filter: 'blur(2px)' }}
      transition={{ duration: 0.25, ease: 'easeOut' }}
      className={`h-full w-full ${className}`}
    >
      {children}
    </motion.div>
  )
}
