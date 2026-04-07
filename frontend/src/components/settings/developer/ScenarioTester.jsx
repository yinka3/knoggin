import { useState } from 'react'
import { motion, AnimatePresence } from 'motion/react'
import { Beaker, ArrowRight, Brain, Fingerprint, Database, CheckCircle2, Search } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { cn } from '@/lib/utils'

export default function ScenarioTester() {
  const [input, setInput] = useState('')
  const [isAnalyzing, setIsAnalyzing] = useState(false)
  const [results, setResults] = useState(null)

  const runAnalysis = () => {
    if (!input.trim()) return
    setIsAnalyzing(true)
    
    // Simulate pipeline latency
    setTimeout(() => {
      setResults({
        entities: ['React', 'JSX', 'Frontend'],
        facts: ['React uses JSX for templating', 'Frontend development involves React'],
        conflicts: [],
        searchQuery: 'What is React and JSX in frontend?'
      })
      setIsAnalyzing(false)
    }, 1500)
  }

  const steps = [
    { id: 'ner', label: 'Entity Extraction', icon: Fingerprint, color: 'text-blue-500' },
    { id: 'facts', label: 'Fact Parsing', icon: Brain, color: 'text-purple-500' },
    { id: 'conflict', label: 'Conflict Check', icon: Database, color: 'text-amber-500' },
    { id: 'search', label: 'Search Synthesis', icon: Search, color: 'text-emerald-500' }
  ]

  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      <div>
        <h2 className="text-xl font-semibold text-foreground tracking-tight flex items-center gap-2">
          Scenario Tester
          <Beaker size={18} className="text-primary" />
        </h2>
        <p className="text-sm text-muted-foreground mt-1 text-balance">
          Paste a sample message below to simulate how the current NLP pipeline and Agent limits would process it.
        </p>
      </div>

      <div className="space-y-4">
        <div className="relative">
          <Textarea 
            placeholder="e.g., 'I am moving my stack from Vue to React because I prefer JSX...'"
            value={input}
            onChange={e => setInput(e.target.value)}
            className="min-h-[120px] bg-white/[0.02] border-white/10 glass-card p-4 focus:ring-primary/20 transition-all resize-none"
          />
          <Button 
            onClick={runAnalysis}
            disabled={isAnalyzing || !input.trim()}
            className="absolute bottom-3 right-3 shadow-lg shadow-primary/20"
            size="sm"
          >
            {isAnalyzing ? 'Analyzing...' : 'Run Test'}
            <ArrowRight size={14} className="ml-2" />
          </Button>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          {steps.map((step, idx) => {
            const Icon = step.icon
            const isActive = isAnalyzing || results
            const isDone = results && !isAnalyzing
            
            return (
              <div 
                key={step.id} 
                className={cn(
                  "flex flex-col items-center gap-3 p-4 rounded-xl border transition-all duration-500",
                  isDone ? "bg-white/[0.03] border-white/10" : "bg-white/5 border-transparent opacity-40"
                )}
              >
                <div className={cn(
                  "p-2 rounded-lg bg-black/20",
                   isAnalyzing && "animate-pulse",
                   isDone ? step.color : "text-muted-foreground"
                )}>
                  <Icon size={20} />
                </div>
                <span className="text-[10px] uppercase tracking-widest font-bold text-center">
                  {step.label}
                </span>
                {isDone && (
                  <motion.div initial={{ scale: 0 }} animate={{ scale: 1 }}>
                    <CheckCircle2 size={14} className="text-emerald-500" />
                  </motion.div>
                )}
              </div>
            )
          })}
        </div>
      </div>

      <AnimatePresence>
        {results && !isAnalyzing && (
          <motion.div 
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            className="grid grid-cols-1 md:grid-cols-2 gap-6"
          >
            <Card className="glass-card bg-white/[0.01] border-white/5">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm uppercase tracking-wider text-muted-foreground font-bold">Extracted Graph Nodes</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2">
                  {results.entities.map(e => (
                    <div key={e} className="px-2 py-1 rounded-md bg-blue-500/10 text-blue-400 border border-blue-500/20 text-xs font-mono">
                      {e}
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>

            <Card className="glass-card bg-white/[0.01] border-white/5">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm uppercase tracking-wider text-muted-foreground font-bold">Proposed Fact Memory</CardTitle>
              </CardHeader>
              <CardContent>
                <ul className="space-y-2">
                  {results.facts.map((f, i) => (
                    <li key={i} className="text-[13px] text-foreground/80 flex gap-2">
                      <span className="text-primary mt-1 shrink-0">•</span>
                      {f}
                    </li>
                  ))}
                </ul>
              </CardContent>
            </Card>
          </motion.div>
        )}
      </AnimatePresence>
      
      {!results && !isAnalyzing && (
        <div className="flex flex-col items-center justify-center py-12 border-2 border-dashed border-white/5 rounded-2xl bg-white/[0.01]">
          <Beaker size={48} className="text-muted-foreground/20 mb-4" />
          <p className="text-sm text-muted-foreground font-medium italic">Waiting for input to start calibration...</p>
        </div>
      )}
    </div>
  )
}
