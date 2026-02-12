import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Eye, EyeOff } from 'lucide-react'
import { useState } from 'react'

export default function LLMSection({
  reasoningModel,
  setReasoningModel,
  agentModel,
  setAgentModel,
  reasoningModels,
  agentModels,
  modelsLoading,
  openrouterKey,
  setOpenrouterKey,
}) {
  const [showKeys, setShowKeys] = useState(false)

  return (
    <>
      {/* Models Section */}
      <section>
        <SectionHeader description="Choose which models power your agent">Models</SectionHeader>
        <div className="space-y-4 bg-card rounded-xl p-4 border border-border">
          <div className="space-y-2">
            <Label className="text-muted-foreground">Reasoning Model</Label>
            <Select value={reasoningModel} onValueChange={setReasoningModel}>
              <SelectTrigger className="bg-muted border-border rounded-xl">
                <SelectValue
                  placeholder={modelsLoading ? 'Loading models...' : 'Select model'}
                />
              </SelectTrigger>
              <SelectContent className="bg-popover border-border rounded-xl max-h-64">
                {reasoningModels.length === 0 && !modelsLoading && (
                  <div className="px-2 py-1.5 text-sm text-muted-foreground">
                    No models available
                  </div>
                )}
                {reasoningModels.map(model => (
                  <SelectItem key={model.id} value={model.id} className="rounded-lg">
                    <span className="flex items-center gap-2 w-full">
                      {model.name}
                      <span className="text-[10px] text-muted-foreground ml-auto">
                        ${model.prompt_price}/M
                      </span>
                    </span>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label className="text-muted-foreground">Agent Model</Label>
            <Select value={agentModel} onValueChange={setAgentModel}>
              <SelectTrigger className="bg-muted border-border rounded-xl">
                <SelectValue
                  placeholder={modelsLoading ? 'Loading models...' : 'Select model'}
                />
              </SelectTrigger>
              <SelectContent className="bg-popover border-border rounded-xl max-h-64">
                {agentModels.length === 0 && !modelsLoading && (
                  <div className="px-2 py-1.5 text-sm text-muted-foreground">
                    No models available
                  </div>
                )}
                {agentModels.map(model => (
                  <SelectItem key={model.id} value={model.id} className="rounded-lg">
                    <span className="flex items-center gap-2 w-full">
                      {model.name}
                      <span className="text-[10px] text-muted-foreground ml-auto">
                        ${model.prompt_price}/M
                      </span>
                    </span>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
      </section>

      {/* API Keys Section */}
      <section>
        <SectionHeader description="Configure LLM provider access">API Keys</SectionHeader>
        <div className="space-y-4 bg-card rounded-xl p-4 border border-border">
          <div className="flex items-center justify-between">
            <Label className="text-muted-foreground">Show Key</Label>
            <button
              type="button"
              onClick={() => setShowKeys(!showKeys)}
              className="text-muted-foreground hover:text-foreground transition-colors"
            >
              {showKeys ? <EyeOff size={16} /> : <Eye size={16} />}
            </button>
          </div>

          <div className="space-y-2">
            <Label htmlFor="openrouterKey" className="text-muted-foreground">
              OpenRouter API Key
            </Label>
            <Input
              id="openrouterKey"
              type={showKeys ? 'text' : 'password'}
              value={openrouterKey}
              onChange={e => setOpenrouterKey(e.target.value)}
              placeholder="sk-or-..."
              className="bg-muted border-border rounded-xl font-mono text-sm"
            />
            <p className="text-[11px] text-muted-foreground">
              Get your key at{' '}
              <a
                href="https://openrouter.ai/keys"
                target="_blank"
                rel="noopener noreferrer"
                className="text-primary hover:underline"
              >
                openrouter.ai/keys
              </a>
            </p>
          </div>
        </div>
      </section>
    </>
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
