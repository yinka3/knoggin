import { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Badge } from '@/components/ui/badge'
import {
  FlaskCon,
  FlaskRound,
  Beaker,
  LayoutGrid,
  Activity,
  TestTube2,
  Code2,
  Save,
  RotateCcw,
  Sparkles,
  Zap,
  Search,
  Fingerprint,
  Workflow,
  Database,
  PlaySquare,
  Users,
} from 'lucide-react'
import { getConfig, updateConfig } from '@/api/config'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'
import AgentLimitsSection from '@/components/settings/developer/AgentLimitsSection'
import SearchSection from '@/components/settings/developer/SearchSection'
import EntityResolutionSection from '@/components/settings/developer/EntityResolutionSection'
import PipelineSection from '@/components/settings/developer/PipelineSection'
import IngestionSection from '@/components/settings/developer/IngestionSection'
import BackgroundJobsSection from '@/components/settings/developer/BackgroundJobsSection'
import CommunitySection from '@/components/settings/developer/CommunitySection'
import PromptsSection from '@/components/settings/developer/PromptsSection'
import DeveloperModesSection from '@/components/settings/developer/DeveloperModesSection'
import LabOverview from '@/components/settings/developer/LabOverview'
import ScenarioTester from '@/components/settings/developer/ScenarioTester'
import { motion, AnimatePresence } from 'motion/react'
import { LayoutGrid } from 'lucide-react'

const TABS = [
  { id: 'overview', label: 'Lab Overview', icon: Activity },
  { id: 'modes', label: 'Biospheres', icon: FlaskRound },
  { id: 'analyzer', label: 'Scenario Tester', icon: TestTube2 },
  { id: 'limits', label: 'Agent Limits', icon: Zap },
  { id: 'search', label: 'Search engine', icon: Search },
  { id: 'entity', label: 'Entity Resolution', icon: Fingerprint },
  { id: 'pipeline', label: 'NLP Pipeline', icon: Workflow },
  { id: 'prompts', label: 'System Prompts', icon: Code2 },
  { id: 'jobs', label: 'Background Jobs', icon: PlaySquare },
]

export default function DeveloperSettingsPage() {
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [settings, setSettings] = useState(null)
  const [original, setOriginal] = useState(null)
  const [activeTab, setActiveTab] = useState('overview')

  useEffect(() => {
    loadSettings()
  }, [])

  async function loadSettings() {
    try {
      const config = await getConfig()
      const devSettings = config.developer_settings || {}
      setSettings(devSettings)
      setOriginal(JSON.parse(JSON.stringify(devSettings)))
    } catch {
      toast.error('Failed to load settings')
    } finally {
      setLoading(false)
    }
  }

  async function handleSave() {
    setSaving(true)
    try {
      await updateConfig({ developer_settings: settings })
      setOriginal(JSON.parse(JSON.stringify(settings)))
      toast.success('Settings saved', {
        description: 'Applied to all active sessions',
      })
    } catch {
      toast.error('Failed to save settings')
    } finally {
      setSaving(false)
    }
  }

  function handleReset() {
    setSettings(JSON.parse(JSON.stringify(original)))
    toast.info('Changes reverted')
  }

  function update(path, value) {
    setSettings(prev => {
      const next = JSON.parse(JSON.stringify(prev))
      const keys = path.split('.')
      let obj = next
      for (let i = 0; i < keys.length - 1; i++) {
        if (!obj[keys[i]]) obj[keys[i]] = {}
        obj = obj[keys[i]]
      }
      obj[keys[keys.length - 1]] = value
      return next
    })
  }

  const hasChanges = JSON.stringify(settings) !== JSON.stringify(original)

  if (loading) {
    return (
      <div className="p-6 max-w-5xl mx-auto space-y-4">
        <Skeleton className="h-10 w-64" />
        <div className="flex gap-6 mt-8">
          <Skeleton className="h-[400px] w-64 rounded-xl shrink-0" />
          <Skeleton className="h-[600px] w-full rounded-xl" />
        </div>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-full bg-background">
      {/* Header */}
      <div className="border-b border-border/50 p-6 bg-card/30 backdrop-blur-md sticky top-0 z-10">
        <div className="max-w-5xl mx-auto flex items-center justify-between">
          <div className="flex items-center gap-4">
            <div className="p-3 rounded-xl bg-primary/10 text-primary shadow-inner">
              <Code2 size={24} />
            </div>
            <div>
              <h1 className="text-xl font-semibold text-foreground tracking-tight flex items-center gap-2">
                Developer Lab
                <Badge variant="outline" className="text-[10px] uppercase tracking-widest bg-primary/5 text-primary border-primary/20">Alpha</Badge>
              </h1>
              <p className="text-sm text-muted-foreground mt-0.5">
                Outcome-driven system tuning and biospheric control
              </p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            {hasChanges && (
              <Badge
                variant="outline"
                className="text-xs text-amber-500 border-amber-500/30 bg-amber-500/10 animate-in fade-in zoom-in px-3 py-1"
              >
                Unsaved changes
              </Badge>
            )}
            <Button
              variant="ghost"
              size="sm"
              onClick={handleReset}
              disabled={!hasChanges || saving}
              className="text-muted-foreground hover:text-foreground h-9 px-4 rounded-lg"
            >
              <RotateCcw size={14} className="mr-2" />
              Revert
            </Button>
            <Button
              size="sm"
              onClick={handleSave}
              disabled={!hasChanges || saving}
              className={cn(
                'h-9 px-5 rounded-lg transition-all duration-300',
                hasChanges ? 'shadow-[0_0_15px_rgba(46,170,110,0.3)]' : 'opacity-80'
              )}
            >
              {saving ? (
                <>
                  <Sparkles size={16} className="mr-2 animate-spin" />
                  Saving...
                </>
              ) : (
                <>
                  <Save size={16} className="mr-2" />
                  Save Changes
                </>
              )}
            </Button>
          </div>
        </div>
      </div>

      {/* Main Layout */}
      <div className="flex-1 overflow-hidden">
        <div className="max-w-5xl mx-auto h-full flex flex-col md:flex-row gap-8 p-6">
          {/* Sidebar Navigation */}
          <nav className="w-full md:w-64 shrink-0 space-y-1 relative">
            {TABS.map(tab => {
              const Icon = tab.icon
              const isActive = activeTab === tab.id
              return (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={cn(
                    'w-full flex items-center gap-3 px-4 py-3 rounded-xl text-sm transition-all duration-200 text-left relative z-10',
                    isActive
                      ? 'text-primary font-medium'
                      : 'text-muted-foreground hover:bg-muted/50 hover:text-foreground'
                  )}
                >
                  {isActive && (
                    <motion.div
                      layoutId="devSettingsTabPill"
                      className="absolute inset-0 bg-primary/10 rounded-xl shadow-sm ring-1 ring-primary/20 -z-10"
                      transition={{ type: 'spring', stiffness: 400, damping: 30 }}
                    />
                  )}
                  <Icon
                    size={18}
                    className={cn(
                      'shrink-0',
                      isActive ? 'text-primary animate-pulse-slow' : 'opacity-70'
                    )}
                  />
                  {tab.label}
                </button>
              )
            })}
          </nav>

          {/* Content Pane */}
          <div className="flex-1 overflow-hidden relative pr-2 pb-12 custom-scrollbar flex">
            <AnimatePresence mode="popLayout" initial={false}>
              <motion.div
                key={activeTab}
                initial={{ opacity: 0, y: 15 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, scale: 0.98, filter: 'blur(2px)' }}
                transition={{ duration: 0.25, ease: 'easeOut' }}
                className="w-full h-full overflow-y-auto"
              >
                {activeTab === 'overview' && (
                  <LabOverview settings={settings} />
                )}
                {activeTab === 'modes' && (
                  <DeveloperModesSection settings={settings} setSettings={setSettings} />
                )}
                {activeTab === 'analyzer' && (
                  <ScenarioTester />
                )}
                {activeTab === 'limits' && (
                  <AgentLimitsSection settings={settings} update={update} />
                )}
                {activeTab === 'search' && <SearchSection settings={settings} update={update} />}
                {activeTab === 'entity' && (
                  <EntityResolutionSection settings={settings} update={update} />
                )}
                {activeTab === 'pipeline' && (
                  <PipelineSection settings={settings} update={update} />
                )}
                {activeTab === 'prompts' && (
                  <PromptsSection settings={settings} update={update} />
                )}
                {activeTab === 'ingestion' && (
                  <IngestionSection settings={settings} update={update} />
                )}
                {activeTab === 'jobs' && (
                  <BackgroundJobsSection settings={settings} update={update} />
                )}
                {activeTab === 'community' && (
                  <CommunitySection settings={settings} update={update} />
                )}
              </motion.div>
            </AnimatePresence>
          </div>
        </div>
      </div>
    </div>
  )
}
