import { useState } from 'react'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'
import { ChevronDown, ChevronRight, Trash2 } from 'lucide-react'

/**
 * Shared topic accordion editor.
 *
 * Props:
 *  - topics: { [name]: config }
 *  - onChange: (updatedTopics) => void
 *  - protectedNames: string[] — topics that cannot be deleted (default: ['General'])
 *  - renderExtra: (name, config, updateField) => ReactNode — optional extra fields per topic
 *  - maxHeight: string — max height CSS value (default: 'none')
 */
export default function TopicEditor({
  topics,
  onChange,
  protectedNames = ['General'],
  renderExtra,
  maxHeight = 'none',
}) {
  const [expandedTopic, setExpandedTopic] = useState(null)

  function updateField(name, field, value) {
    onChange({ ...topics, [name]: { ...topics[name], [field]: value } })
  }

  function removeTopic(name) {
    const updated = { ...topics }
    delete updated[name]
    onChange(updated)
    setExpandedTopic(null)
  }

  function parseCSV(value) {
    return value.split(',').map(s => s.trim()).filter(Boolean)
  }

  return (
    <div
      className="bg-card rounded-xl border border-border divide-y divide-border overflow-y-auto"
      style={{ maxHeight }}
    >
      {Object.entries(topics).map(([name, config]) => (
        <div key={name}>
          <button
            onClick={() => setExpandedTopic(expandedTopic === name ? null : name)}
            className="w-full flex items-center justify-between px-4 py-2.5 hover:bg-muted/50 transition-colors"
          >
            <div className="flex items-center gap-2">
              <span
                className={`w-1.5 h-1.5 rounded-full ${
                  config.active !== false ? 'bg-primary' : 'bg-muted-foreground/30'
                }`}
              />
              <span className="text-sm text-foreground font-medium">{name}</span>
              <span className="text-[11px] text-muted-foreground">
                {config.labels?.length > 0 ? config.labels.join(', ') : '—'}
              </span>
            </div>
            {expandedTopic === name ? (
              <ChevronDown size={14} className="text-muted-foreground" />
            ) : (
              <ChevronRight size={14} className="text-muted-foreground" />
            )}
          </button>

          {expandedTopic === name && (
            <div className="px-4 pb-3 pt-2 space-y-3 bg-muted/20">
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <Label className="text-[11px] text-muted-foreground">Labels</Label>
                  <Input
                    value={config.labels?.join(', ') || ''}
                    onChange={e => updateField(name, 'labels', parseCSV(e.target.value))}
                    placeholder="person, company"
                    className="mt-1 bg-muted border-border rounded-lg text-xs h-8"
                  />
                </div>
                <div>
                  <Label className="text-[11px] text-muted-foreground">Aliases</Label>
                  <Input
                    value={config.aliases?.join(', ') || ''}
                    onChange={e => updateField(name, 'aliases', parseCSV(e.target.value))}
                    placeholder="work, projects"
                    className="mt-1 bg-muted border-border rounded-lg text-xs h-8"
                  />
                </div>
              </div>

              {renderExtra && renderExtra(name, config, updateField)}

              {!protectedNames.includes(name) && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => removeTopic(name)}
                  className="text-xs text-muted-foreground hover:text-destructive h-7"
                >
                  <Trash2 size={12} className="mr-1" />
                  Remove
                </Button>
              )}
            </div>
          )}
        </div>
      ))}
    </div>
  )
}
