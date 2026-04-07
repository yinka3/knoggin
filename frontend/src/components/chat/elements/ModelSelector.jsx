import { useState, useEffect } from 'react'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Cpu } from 'lucide-react'
import { getCuratedModels } from '@/api/config'

export default function ModelSelector({ currentModel, onModelChange, disabled }) {
  const [models, setModels] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    getCuratedModels()
      .then(data => setModels(data.models || []))
      .catch(() => setModels([]))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center gap-1 text-[11px] text-muted-foreground px-1.5">
        <Cpu size={12} />
        <span>...</span>
      </div>
    )
  }

  return (
    <Select
      value={currentModel || '__default__'}
      onValueChange={v => onModelChange(v === '__default__' ? null : v)}
      disabled={disabled}
    >
      <SelectTrigger className="h-6 w-auto max-w-[180px] gap-1 border-none bg-transparent px-1.5 text-[11px] text-muted-foreground hover:text-foreground hover:bg-muted/50 focus:ring-0 focus:ring-offset-0 rounded-md">
        <Cpu size={12} className="shrink-0" />
        <SelectValue placeholder="Model">
          {currentModel ? models.find(m => m.id === currentModel)?.name || currentModel : 'Default'}
        </SelectValue>
      </SelectTrigger>
      <SelectContent align="start" className="min-w-[220px] max-h-[280px]">
        <SelectItem value="__default__" className="text-xs">
          <span className="text-muted-foreground">Default (from config)</span>
        </SelectItem>
        {models.map(model => (
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
  )
}
