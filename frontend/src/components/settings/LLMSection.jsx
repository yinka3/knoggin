import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { KeyRound, CheckCircle2, AlertCircle, Search, Zap, Shield, Cpu, Bot, Sparkles, GitMerge } from 'lucide-react'
import { useState, useEffect } from 'react'
import { getCuratedModels } from '@/api/config'

const PROVIDERS = [
  { 
    id: 'duckduckgo', 
    label: 'DuckDuckGo', 
    icon: Search,
    tier: 'Free',
    tierColor: 'text-emerald-500 bg-emerald-500/10',
    needsKey: false,
    description: 'Free, no API key needed',
  },
  { 
    id: 'tavily', 
    label: 'Tavily', 
    icon: Zap,
    tier: 'Free tier',
    tierColor: 'text-blue-500 bg-blue-500/10',
    needsKey: true,
    keyField: 'tavily_api_key',
    placeholder: 'tvly-...',
    helpText: 'Get 1,000 free searches/mo at',
    helpUrl: 'https://tavily.com',
    helpLabel: 'tavily.com',
    description: 'Optimized for AI agents',
  },
  { 
    id: 'brave', 
    label: 'Brave Search', 
    icon: Shield,
    tier: 'Paid',
    tierColor: 'text-amber-500 bg-amber-500/10',
    needsKey: true,
    keyField: 'brave_api_key',
    placeholder: 'BSA...',
    helpText: 'Get your key at',
    helpUrl: 'https://api.search.brave.com/app/dashboard',
    helpLabel: 'brave.com/search/api',
    description: 'Premium search quality',
  },
]

const MODEL_ROLES = [
  {
    key: 'agent',
    label: 'Agent',
    icon: Bot,
    description: 'Powers the chat agent — must support tool calling',
    configKey: 'agentModel',
  },
  {
    key: 'extraction',
    label: 'Extraction',
    icon: Sparkles,
    description: 'NER, fact extraction, and connection reasoning',
    configKey: 'extractionModel',
  },
  {
    key: 'merge',
    label: 'Merge',
    icon: GitMerge,
    description: 'Entity deduplication and profile refinement',
    configKey: 'mergeModel',
  },
]

export default function LLMSection({
  openrouterKey,
  setOpenrouterKey,
  searchConfig,
  setSearchConfig,
  models,
  setModels,
}) {
  const [activeTab, setActiveTab] = useState('provider')
  const [isEditing, setIsEditing] = useState(false)

  // Determine active search provider for display
  const activeProvider = searchConfig?.provider || 'auto'
  const resolvedProvider = activeProvider === 'auto'
    ? (searchConfig?.brave_api_key ? 'brave' : searchConfig?.tavily_api_key ? 'tavily' : 'duckduckgo')
    : activeProvider

  // OpenRouter section
  const hasOpenrouterKey = Boolean(openrouterKey?.trim())

  return (
    <>
      <section>
        <SectionHeader description="Configure LLM provider, models, and web search">API Keys & Models</SectionHeader>
        <div className="bg-card rounded-xl border border-border overflow-hidden transition-all">
          {/* Tab Switcher */}
          <div className="flex border-b border-border">
            <button
              onClick={() => { setActiveTab('provider'); setIsEditing(false) }}
              className={`flex-1 flex items-center justify-center gap-2 px-4 py-2.5 text-xs font-medium transition-all relative
                ${activeTab === 'provider' ? 'text-foreground bg-muted/30' : 'text-muted-foreground hover:text-foreground hover:bg-muted/10'}`}
            >
              <KeyRound size={14} />
              LLM Provider
              {hasOpenrouterKey && <span className="w-1.5 h-1.5 rounded-full bg-emerald-500" />}
              {activeTab === 'provider' && <span className="absolute bottom-0 left-0 right-0 h-0.5 bg-primary" />}
            </button>
            <button
              onClick={() => { setActiveTab('models'); setIsEditing(false) }}
              className={`flex-1 flex items-center justify-center gap-2 px-4 py-2.5 text-xs font-medium transition-all relative
                ${activeTab === 'models' ? 'text-foreground bg-muted/30' : 'text-muted-foreground hover:text-foreground hover:bg-muted/10'}`}
            >
              <Cpu size={14} />
              Models
              {activeTab === 'models' && <span className="absolute bottom-0 left-0 right-0 h-0.5 bg-primary" />}
            </button>
            <button
              onClick={() => { setActiveTab('search'); setIsEditing(false) }}
              className={`flex-1 flex items-center justify-center gap-2 px-4 py-2.5 text-xs font-medium transition-all relative
                ${activeTab === 'search' ? 'text-foreground bg-muted/30' : 'text-muted-foreground hover:text-foreground hover:bg-muted/10'}`}
            >
              <Search size={14} />
              Web Search
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
              {activeTab === 'search' && <span className="absolute bottom-0 left-0 right-0 h-0.5 bg-primary" />}
            </button>
          </div>

          {activeTab === 'provider' ? (
            <ProviderPanel
              apiKey={openrouterKey}
              setApiKey={setOpenrouterKey}
              isEditing={isEditing}
              setIsEditing={setIsEditing}
            />
          ) : activeTab === 'models' ? (
            <ModelsPanel models={models} setModels={setModels} />
          ) : (
            <SearchProvidersPanel
              searchConfig={searchConfig}
              setSearchConfig={setSearchConfig}
              resolvedProvider={resolvedProvider}
            />
          )}
        </div>
      </section>
    </>
  )
}

function ProviderPanel({ apiKey, setApiKey, isEditing, setIsEditing }) {
  const hasKey = Boolean(apiKey?.trim())
  return (
    <>
      <div className="flex items-center justify-between p-4 bg-muted/10">
        <div className="flex items-center gap-3.5">
          <div className={`p-2.5 rounded-lg ${hasKey ? 'bg-emerald-500/10 text-emerald-500' : 'bg-amber-500/10 text-amber-500'}`}>
            <KeyRound size={18} />
          </div>
          <div className="space-y-0.5">
            <Label className="text-sm font-semibold">LLM API Key</Label>
            <div className="flex items-center gap-1.5">
              {hasKey ? (
                <>
                  <CheckCircle2 size={12} className="text-emerald-500" />
                  <span className="text-[11px] text-muted-foreground font-medium uppercase tracking-wider">Connected</span>
                </>
              ) : (
                <>
                  <AlertCircle size={12} className="text-amber-500" />
                  <span className="text-[11px] text-muted-foreground font-medium uppercase tracking-wider">Missing Key</span>
                </>
              )}
            </div>
          </div>
        </div>
        <Button
          variant={isEditing ? "ghost" : "outline"}
          size="sm"
          onClick={() => setIsEditing(!isEditing)}
          className="h-8 text-xs font-medium rounded-lg px-3 transition-colors"
        >
          {isEditing ? 'Cancel' : (hasKey ? 'Update Key' : 'Add Key')}
        </Button>
      </div>
      {isEditing && (
        <div className="p-4 border-t border-border bg-card animate-[fadeIn_0.2s_ease-out]">
          <div className="space-y-2">
            <Input
              type="password"
              value={apiKey}
              onChange={e => setApiKey(e.target.value)}
              placeholder="sk-or-... or your provider's key"
              className="bg-background border-border rounded-lg font-mono text-sm focus:border-primary transition-colors h-9"
              autoFocus
            />
            <div className="flex items-center justify-between px-1">
              <p className="text-[11px] text-muted-foreground leading-tight">
                Default: OpenRouter.{' '}
                <a href="https://openrouter.ai/keys" target="_blank" rel="noopener noreferrer" className="text-primary hover:underline font-medium">
                  Get a key
                </a>
              </p>
              <Button size="sm" onClick={() => setIsEditing(false)} className="h-7 text-[11px] px-3 rounded-md">
                Done
              </Button>
            </div>
          </div>
        </div>
      )}
    </>
  )
}

function ModelsPanel({ models, setModels }) {
  const [curatedModels, setCuratedModels] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    getCuratedModels()
      .then(data => setCuratedModels(data.models || []))
      .catch(() => setCuratedModels([]))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="p-6 text-center text-sm text-muted-foreground">
        Loading models...
      </div>
    )
  }

  return (
    <div className="divide-y divide-border/40">
      {MODEL_ROLES.map(role => {
        const Icon = role.icon
        const currentValue = models?.[role.configKey] || ''

        return (
          <div key={role.key} className="px-4 py-3.5">
            <div className="flex items-center justify-between gap-4">
              <div className="flex items-center gap-3 min-w-0">
                <div className="p-2 rounded-lg bg-muted/50 text-muted-foreground shrink-0">
                  <Icon size={16} />
                </div>
                <div className="space-y-0.5 min-w-0">
                  <span className="text-sm font-medium text-foreground">{role.label}</span>
                  <p className="text-[11px] text-muted-foreground">{role.description}</p>
                </div>
              </div>

              <Select
                value={currentValue || '__default__'}
                onValueChange={v => {
                  const newVal = v === '__default__' ? '' : v
                  setModels({ [role.configKey]: newVal })
                }}
              >
                <SelectTrigger className="h-8 w-[200px] shrink-0 border-border bg-muted/30 text-xs rounded-lg focus:ring-1 focus:ring-primary/30">
                  <SelectValue placeholder="Default">
                    {currentValue
                      ? (curatedModels.find(m => m.id === currentValue)?.name || currentValue.split('/').pop())
                      : 'Default'}
                  </SelectValue>
                </SelectTrigger>
                <SelectContent align="end" className="min-w-[260px] max-h-[280px]">
                  <SelectItem value="__default__" className="text-xs">
                    <span className="text-muted-foreground">Default</span>
                  </SelectItem>
                  {curatedModels.map(model => (
                    <SelectItem key={model.id} value={model.id} className="text-xs">
                      <div className="flex items-center justify-between gap-3 w-full">
                        <span className="truncate">{model.name}</span>
                        <span className="text-[10px] text-muted-foreground shrink-0">
                          {model.input_price === 0 ? 'Free' : `$${model.input_price}/M`}
                        </span>
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        )
      })}

      <div className="px-4 py-2.5">
        <p className="text-[11px] text-muted-foreground">
          Custom model IDs can be typed directly into the selector.
          The Agent model must support tool calling.
        </p>
      </div>
    </div>
  )
}

function SearchProvidersPanel({ searchConfig, setSearchConfig, resolvedProvider }) {
  const [editingProvider, setEditingProvider] = useState(null)

  const updateKey = (field, value) => {
    setSearchConfig(prev => ({ ...prev, [field]: value }))
  }

  const setProvider = (providerId) => {
    setSearchConfig(prev => ({ ...prev, provider: providerId }))
  }

  return (
    <div className="divide-y divide-border/40">
      {PROVIDERS.map(p => {
        const Icon = p.icon
        const isActive = resolvedProvider === p.id
        const hasKey = p.needsKey ? Boolean(searchConfig?.[p.keyField]?.trim()) : true
        const isEditing = editingProvider === p.id

        return (
          <div key={p.id} className="px-4 py-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <div className={`p-2 rounded-lg ${isActive ? 'bg-primary/10 text-primary' : 'bg-muted/50 text-muted-foreground'}`}>
                  <Icon size={16} />
                </div>
                <div className="space-y-0.5">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-medium text-foreground">{p.label}</span>
                    <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${p.tierColor}`}>
                      {p.tier}
                    </span>
                    {isActive && (
                      <span className="text-[10px] px-1.5 py-0.5 rounded font-medium bg-primary/10 text-primary">
                        Active
                      </span>
                    )}
                  </div>
                  <p className="text-[11px] text-muted-foreground">{p.description}</p>
                </div>
              </div>

              <div className="flex items-center gap-2">
                {p.needsKey && (
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setEditingProvider(isEditing ? null : p.id)}
                    className="h-7 text-[11px] px-2.5 rounded-md"
                  >
                    {hasKey ? (isEditing ? 'Cancel' : 'Update Key') : (isEditing ? 'Cancel' : 'Add Key')}
                  </Button>
                )}
                {(!p.needsKey || hasKey) && !isActive && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setProvider(p.id)}
                    className="h-7 text-[11px] px-2.5 rounded-md"
                  >
                    Use
                  </Button>
                )}
              </div>
            </div>

            {/* Key input area */}
            {isEditing && p.needsKey && (
              <div className="mt-3 pl-11 animate-[fadeIn_0.2s_ease-out]">
                <div className="space-y-2">
                  <Input
                    type="password"
                    value={searchConfig?.[p.keyField] || ''}
                    onChange={e => updateKey(p.keyField, e.target.value)}
                    placeholder={p.placeholder}
                    className="bg-background border-border rounded-lg font-mono text-sm h-8"
                    autoFocus
                  />
                  <div className="flex items-center justify-between">
                    <p className="text-[11px] text-muted-foreground">
                      {p.helpText}{' '}
                      <a href={p.helpUrl} target="_blank" rel="noopener noreferrer" className="text-primary hover:underline font-medium">
                        {p.helpLabel}
                      </a>
                    </p>
                    <Button size="sm" onClick={() => { setEditingProvider(null); if (searchConfig?.[p.keyField]) setProvider(p.id) }} className="h-6 text-[10px] px-2 rounded">
                      Done
                    </Button>
                  </div>
                </div>
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

function SectionHeader({ children, description }) {
  return (
    <div className="mb-3">
      <h2 className="text-sm font-medium text-foreground">{children}</h2>
      {description && (
        <p className="text-xs text-muted-foreground mt-0.5">{description}</p>
      )}
    </div>
  )
}
