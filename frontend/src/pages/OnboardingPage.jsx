import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Brain,
  Eye,
  EyeOff,
  X,
  Zap,
  Layers,
  Loader2,
  Check,
  ExternalLink,
  Trash2,
} from 'lucide-react'
import { updateConfig, getConfigStatus } from '@/api/config'
import { getQuestions, generateTopics, saveTopics, runExtraction } from '@/api/onboarding'
import { cn } from '@/lib/utils'
import TopicEditor from '@/components/TopicEditor'
import HierarchyEditor from '@/components/HierarchyEditor'

function StepDots({ current, total }) {
  return (
    <div className="flex gap-2 justify-center">
      {Array.from({ length: total }).map((_, i) => (
        <div
          key={i}
          className={cn(
            'w-2 h-2 rounded-full transition-colors duration-300',
            i + 1 === current ? 'bg-primary' : 'bg-muted-foreground/30'
          )}
        />
      ))}
    </div>
  )
}
export default function OnboardingPage() {
  const navigate = useNavigate()
  const [step, setStep] = useState(1)
  const [userName, setUserName] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [showKey, setShowKey] = useState(false)
  const [saving] = useState(false)
  const [error, setError] = useState(null)
  const [questions, setQuestions] = useState([])
  const [currentQuestion, setCurrentQuestion] = useState(0)
  const [answers, setAnswers] = useState({})

  const stepAnimation = `space-y-6 animate-in fade-in zoom-in-95 duration-150`
  const [generatingTopics, setGeneratingTopics] = useState(false)
  const [topics, setTopics] = useState({})

  const [extracting, setExtracting] = useState(false)
  const [extractionStage, setExtractionStage] = useState(0)
  const [extractionResult, setExtractionResult] = useState(null)
  const [extractionError, setExtractionError] = useState(null)
  const [extractionComplete, setExtractionComplete] = useState(false)

  useEffect(() => {
    getConfigStatus().then(status => {
      if (status.configured) {
        navigate('/chat', { replace: true })
      }
    })
  }, [navigate])

  function canProceed() {
    if (step === 1) return userName.trim().length > 0
    if (step === 2) return apiKey.trim().length > 0
    return true
  }

  function handleNext() {
    setStep(s => s + 1)
  }

  function handleBack() {
    setStep(s => s - 1)
    setError(null)
  }

  async function handleSelectPath(path) {
    try {
      const data = await getQuestions(path)
      setQuestions(data.questions)
      setCurrentQuestion(0)
      setStep(4)
    } catch (err) {
      setError(err.message)
    }
  }


  function handleQuestionNext() {
    if (currentQuestion < questions.length - 1) {
      setCurrentQuestion(c => c + 1)
    } else {
      handleGenerateTopics()
    }
  }

  function handleQuestionBack() {
    if (currentQuestion > 0) {
      setCurrentQuestion(c => c - 1)
    } else {
      setStep(3)
    }
  }

  async function handleGenerateTopics() {
    setStep(5)
    setGeneratingTopics(true)
    setError(null)

    try {
      await updateConfig({
        user_name: userName.trim(),
        llm: { api_key: apiKey.trim() },
      })

      const responses = questions
        .filter(q => answers[q.id]?.trim())
        .map(q => ({ question: q.question, answer: answers[q.id] }))

      const data = await generateTopics(responses)
      setTopics(data.topics)
    } catch (err) {
      setError(err.message)
      setStep(4)
    } finally {
      setGeneratingTopics(false)
    }
  }

  async function handleSaveAndExtract() {
    setStep(6)
    setExtracting(true)
    setExtractionStage(0)
    setExtractionError(null)

    const ticker = setInterval(() => {
      setExtractionStage(s => Math.min(s + 1, 4))
    }, 1200)

    try {
      await saveTopics(topics)

      const responses = questions
        .filter(q => answers[q.id]?.trim())
        .map(q => ({ question: q.question, answer: answers[q.id] }))

      const result = await runExtraction(responses)
      setExtractionResult(result)

      clearInterval(ticker)
      setExtractionStage(4)
      await new Promise(r => setTimeout(r, 400))
      setExtractionComplete(true)
      await new Promise(r => setTimeout(r, 400))
      setExtracting(false)
    } catch (err) {
      clearInterval(ticker)
      setExtractionError(err.message)
      setExtracting(false)
    }
  }

  function handleRetryExtraction() {
    setExtractionError(null)
    setExtractionComplete(false)
    handleSaveAndExtract()
  }

  function handleKeyDown(e) {
    if (e.key === 'Enter' && canProceed()) {
      handleNext()
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center p-4 gradient-bg">
      <div className="w-full max-w-md">
        <div className="bg-card border border-border rounded-2xl p-8 shadow-xl">
          {/* Header */}
          <div className="mb-8">
            {step === 1 && (
              <div className="flex justify-center mb-6">
                <div className="p-4 rounded-2xl bg-primary/10">
                  <Brain size={40} className="text-primary" />
                </div>
              </div>
            )}
            {step <= 3 && <StepDots current={step} total={3} />}
          </div>

          {/* Step 1: Welcome + Name */}
          {step === 1 && (
            <div className={stepAnimation}>
              <div className="text-center">
                <h1 className="text-2xl font-semibold text-foreground mb-2">Welcome to Knoggin</h1>
                <p className="text-muted-foreground">
                  Your personal knowledge assistant. Let's get you set up.
                </p>
              </div>

              <div className="space-y-2">
                <Label htmlFor="name" className="text-muted-foreground">
                  What should I call you?
                </Label>
                <Input
                  id="name"
                  value={userName}
                  onChange={e => setUserName(e.target.value)}
                  onKeyDown={handleKeyDown}
                  placeholder="Your name"
                  autoFocus
                  className="bg-muted border-border rounded-xl h-12 text-base"
                />
              </div>
            </div>
          )}

          {/* Step 2: API Key */}
          {step === 2 && (
            <div className={stepAnimation}>
              <div className="text-center">
                <h1 className="text-2xl font-semibold text-foreground mb-2">Connect your brain</h1>
                <p className="text-muted-foreground">
                  Paste your LLM API key below. We recommend OpenRouter for the widest model selection.
                </p>
              </div>

              <div className="space-y-2">
                <Label htmlFor="apiKey" className="text-muted-foreground">
                  API Key
                </Label>
                <div className="relative">
                  <Input
                    id="apiKey"
                    type={showKey ? 'text' : 'password'}
                    value={apiKey}
                    onChange={e => setApiKey(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder="sk-or-... or your provider's key"
                    autoFocus
                    className="bg-muted border-border rounded-xl h-12 text-base pr-12 font-mono"
                  />

                  <button
                    type="button"
                    onClick={() => setShowKey(!showKey)}
                    className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors"
                  >
                    {showKey ? <EyeOff size={18} /> : <Eye size={18} />}
                  </button>
                </div>

                <div className="flex items-center justify-between text-xs">
                  <a
                    href="https://openrouter.ai/keys"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-primary hover:underline flex items-center gap-1"
                  >
                    Get an OpenRouter key <ExternalLink size={12} />
                  </a>
                  <span className="text-muted-foreground">Your key stays local</span>
                </div>
              </div>
            </div>
          )}

          {/* Step 3: Aliases + Facts */}
          {step === 3 && (
            <div className={stepAnimation}>
              <div className="text-center">
                <h1 className="text-2xl font-semibold text-foreground mb-2">
                  How would you like to start?
                </h1>
                <p className="text-muted-foreground">
                  You can seed your knowledge graph now or start with a blank slate.
                </p>
              </div>

              <div className="space-y-3">
                {/* Seed - Guided */}
                <button
                  onClick={() => handleSelectPath('guided')}
                  className="w-full text-left p-4 rounded-xl border border-border hover:border-primary/50 hover:bg-muted/30 hover:scale-[1.02] transition-all duration-200 group"
                >
                  <div className="flex items-center gap-3">
                    <div className="p-2 rounded-lg bg-primary/10 group-hover:bg-primary/20 transition-colors">
                      <Zap size={20} className="text-primary" />
                    </div>
                    <div>
                      <span className="text-sm font-medium text-foreground group-hover:text-primary transition-colors">
                        Quick seed
                      </span>
                      <p className="text-[11px] text-muted-foreground mt-0.5">
                        3 questions to get your graph started
                      </p>
                    </div>
                  </div>
                </button>

                {/* Seed - Structured */}
                <button
                  onClick={() => handleSelectPath('structured')}
                  className="w-full text-left p-4 rounded-xl border border-border hover:border-primary/50 hover:bg-muted/30 hover:scale-[1.02] transition-all duration-200 group"
                >
                  <div className="flex items-center gap-3">
                    <div className="p-2 rounded-lg bg-primary/10 group-hover:bg-primary/20 transition-colors">
                      <Layers size={20} className="text-primary" />
                    </div>
                    <div>
                      <span className="text-sm font-medium text-foreground group-hover:text-primary transition-colors">
                        Deep seed
                      </span>
                      <p className="text-[11px] text-muted-foreground mt-0.5">
                        6 questions for a richer starting graph
                      </p>
                    </div>
                  </div>
                </button>

              </div>
              
              <div className="mt-8 flex items-center">
                <Button
                  variant="ghost"
                  onClick={handleBack}
                  className="text-muted-foreground hover:text-foreground hover:bg-muted/50"
                >
                  Back
                </Button>
              </div>
            </div>
          )}

          {/* Step 4: Questions */}
          {step === 4 && questions.length > 0 && (
            <div className={stepAnimation}>
              {/* Question counter stays outside the keyed block */}
              <div className="flex items-center justify-between">
                <span className="text-[11px] text-muted-foreground font-mono">
                  {currentQuestion + 1} / {questions.length}
                </span>
                <div className="flex-1 mx-4 h-1 bg-muted rounded-full overflow-hidden">
                  <div
                    className="h-full bg-primary rounded-full transition-all duration-500"
                    style={{ width: `${((currentQuestion + 1) / questions.length) * 100}%` }}
                  />
                </div>
              </div>

              {/* Question content — keyed for remount animation */}
              <div
                key={currentQuestion}
                className="space-y-4 animate-in fade-in zoom-in-95 duration-150"
              >
                <p className="text-base text-foreground leading-relaxed">
                  {questions[currentQuestion].question}
                </p>

                <textarea
                  value={answers[questions[currentQuestion].id] || ''}
                  onChange={e =>
                    setAnswers(prev => ({
                      ...prev,
                      [questions[currentQuestion].id]: e.target.value,
                    }))
                  }
                  placeholder="Type your answer..."
                  rows={4}
                  autoFocus
                  className="w-full bg-muted border border-border rounded-xl px-4 py-3 text-sm text-foreground placeholder-muted-foreground resize-none focus:outline-none focus:border-primary/50 focus:ring-1 focus:ring-primary/30 transition-colors"
                  onKeyDown={e => {
                    if (e.key === 'Enter' && e.metaKey) handleQuestionNext()
                  }}
                />

                <p className="text-[10px] text-muted-foreground text-right">
                  ⌘ + Enter to continue
                </p>
              </div>

              {/* Navigation */}
              <div className="flex items-center gap-3">
                <Button
                  variant="ghost"
                  onClick={handleQuestionBack}
                  disabled={currentQuestion === 0}
                  className="text-muted-foreground"
                >
                  Back
                </Button>
                <div className="flex-1" />
                <Button
                  onClick={() => handleQuestionNext()}
                  disabled={!answers[questions[currentQuestion].id]?.trim()}
                  className="rounded-xl px-6"
                >
                  {currentQuestion === questions.length - 1 ? 'Generate Topics' : 'Next'}
                </Button>
              </div>
            </div>
          )}

          {step === 5 && (
            <div className={stepAnimation}>
              {generatingTopics ? (
                <div className="text-center py-8">
                  <Loader2 size={24} className="mx-auto mb-3 text-primary animate-spin" />
                  <p className="text-sm text-muted-foreground">Analyzing your responses...</p>
                </div>
              ) : (
                <>
                  <div className="text-center">
                    <h1 className="text-2xl font-semibold text-foreground mb-2">Your topics</h1>
                    <p className="text-muted-foreground">
                      Generated from your answers. Edit, add, or remove before continuing.
                    </p>
                  </div>

                  {/* Topic editor */}
                  <TopicEditor
                    topics={topics}
                    onChange={setTopics}
                    protectedNames={['Identity']}
                    maxHeight="18rem"
                    renderExtra={(name, config, updateField) => (
                      <HierarchyEditor name={name} config={config} updateField={updateField} />
                    )}
                  />

                  <div className="flex items-center gap-3">
                    <Button
                      variant="ghost"
                      onClick={() => setStep(4)}
                      className="text-muted-foreground"
                    >
                      Back
                    </Button>
                    <div className="flex-1" />
                    <Button onClick={handleSaveAndExtract} className="rounded-xl px-6">
                      Looks good
                    </Button>
                  </div>
                </>
              )}
            </div>
          )}

          {step === 6 && (
            <div className={`${stepAnimation} text-center`}>
              {extractionError ? (
                <>
                  <div className="flex justify-center">
                    <div className="p-4 rounded-full bg-destructive/20">
                      <X size={40} className="text-destructive" />
                    </div>
                  </div>
                  <div>
                    <h1 className="text-2xl font-semibold text-foreground mb-2">
                      Something went wrong
                    </h1>
                    <p className="text-muted-foreground text-sm">{extractionError}</p>
                  </div>
                  <Button onClick={handleRetryExtraction} className="rounded-xl px-6">
                    Try again
                  </Button>
                </>
              ) : extracting ? (
                <>
                  <div className="flex justify-center">
                    <div className="p-4 rounded-full bg-primary/10">
                      <Loader2 size={40} className="text-primary animate-spin" />
                    </div>
                  </div>
                  <div>
                    <h1 className="text-2xl font-semibold text-foreground mb-2">
                      Building your graph
                    </h1>
                    <p
                      key={extractionStage}
                      className="text-muted-foreground text-sm h-5 animate-in fade-in duration-500"
                    >
                      {
                        [
                          'Analyzing your responses...',
                          'Extracting entities...',
                          'Finding connections...',
                          'Creating profiles...',
                          'Almost there...',
                        ][extractionStage]
                      }
                    </p>
                  </div>
                  {/* Fake progress bar */}
                  <div className="w-48 mx-auto h-1 bg-muted rounded-full overflow-hidden">
                    <div
                      className="h-full bg-primary rounded-full transition-all duration-1000 ease-out"
                      style={{
                        width: extractionComplete
                          ? '100%'
                          : `${Math.min(((extractionStage + 1) / 5) * 100, 90)}%`,
                      }}
                    />
                  </div>
                </>
              ) : (
                <>
                  <div className="flex justify-center">
                    <div className="p-4 rounded-full bg-primary/20 animate-in zoom-in-50 duration-500">
                      <Check size={40} className="text-primary" />
                    </div>
                  </div>
                  <div>
                    <h1 className="text-2xl font-semibold text-foreground mb-2">You're all set</h1>
                    <p className="text-muted-foreground">
                      {extractionResult
                        ? `Found ${extractionResult.entities_created} entities and ${extractionResult.connections_created} connections.`
                        : "Let's start building your second brain."}
                    </p>
                  </div>
                  <Button onClick={() => navigate('/chat')} className="rounded-xl px-6">
                    Start chatting
                  </Button>
                </>
              )}
            </div>
          )}

          {/* Error */}
          {error && (
            <div className="mt-4 p-3 rounded-lg bg-destructive/10 text-destructive text-sm">
              {error}
            </div>
          )}

          {/* Footer / Navigation */}
          {step <= 2 && (
            <div className="mt-8 flex items-center gap-3">
              {step > 1 && (
                <Button
                  variant="ghost"
                  onClick={handleBack}
                  disabled={saving}
                  className="text-muted-foreground"
                >
                  Back
                </Button>
              )}
              <div className="flex-1" />
              <Button
                onClick={handleNext}
                disabled={!canProceed() || saving}
                className="rounded-xl px-6"
              >
                Continue
              </Button>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
