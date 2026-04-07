import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'
import { Timer, Zap, Target, Coins, Info } from 'lucide-react'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'

// eslint-disable-next-line no-unused-vars
export function Section({ title, description, icon: Icon, children, badge }) {
  return (
    <div className="w-full glass-card rounded-2xl overflow-hidden">
      <div className="flex items-center gap-4 p-5 border-b border-white/[0.06] bg-gradient-to-r from-white/[0.02] to-transparent">
        <div className="glass-container p-2.5 rounded-xl text-primary">
          <Icon size={20} />
        </div>
        <div className="text-left">
          <div className="flex items-center gap-2">
            <h3 className="text-base font-semibold text-foreground tracking-tight">{title}</h3>
            {badge && (
              <Badge variant="secondary" className="text-[10px] px-2 py-0.5 rounded-md font-medium">
                {badge}
              </Badge>
            )}
          </div>
          <p className="text-sm text-muted-foreground mt-0.5">{description}</p>
        </div>
      </div>
      <div className="p-5 space-y-2 bg-card">{children}</div>
    </div>
  )
}

export function SubSection({ title, icon: Icon, children }) {
  return (
    <div className="rounded-lg border border-border/60 bg-muted/15 overflow-hidden">
      <div className="flex items-center gap-2 px-3 py-2 border-b border-border/40 bg-muted/30">
        {Icon && <Icon size={13} className="text-muted-foreground" />}
        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
          {title}
        </span>
      </div>
      <div className="p-1 space-y-1">{children}</div>
    </div>
  )
}

export function SettingRow({ label, description, children, impacts = [] }) {
  return (
    <div
      className={cn(
        'flex items-center justify-between gap-4 p-3 rounded-lg',
        'bg-muted/30 hover:bg-muted/50 transition-colors duration-150'
      )}
    >
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <Label className="text-sm text-foreground font-normal">{label}</Label>
          <div className="flex gap-1">
            {impacts.map(impact => (
              <ImpactBadge key={impact} type={impact} />
            ))}
          </div>
        </div>
        {description && (
          <p className="text-[11px] text-muted-foreground mt-0.5 truncate">{description}</p>
        )}
      </div>
      <div className="shrink-0">{children}</div>
    </div>
  )
}

export function NumberInput({ value, onChange, min, max, step = 1, unit, placeholder }) {
  return (
    <div className="relative w-28">
      <Input
        type="number"
        value={value ?? ''}
        onChange={e => onChange(e.target.value ? Number(e.target.value) : null)}
        onWheel={e => e.target.blur()}
        min={min}
        max={max}
        step={step}
        placeholder={placeholder ?? ''}
        className={cn(
          'bg-background border-border text-sm h-8 text-right font-mono',
          'focus:border-primary focus:ring-1 focus:ring-primary/30',
          'placeholder:text-muted-foreground/40',
          unit && 'pr-8'
        )}
      />
      {unit && (
        <span className="absolute right-2 top-1/2 -translate-y-1/2 text-[10px] text-muted-foreground">
          {unit}
        </span>
      )}
    </div>
  )
}

export function ImpactBadge({ type }) {
  const configs = {
    latency: { icon: Timer, color: 'text-amber-500', label: 'Affects Latency' },
    accuracy: { icon: Target, color: 'text-emerald-500', label: 'Affects Accuracy' },
    tokens: { icon: Coins, color: 'text-blue-500', label: 'Affects Token Usage' },
    quality: { icon: Zap, color: 'text-purple-500', label: 'Affects Output Quality' },
  }

  const config = configs[type]
  if (!config) return null
  const Icon = config.icon

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <div className={cn("flex items-center justify-center p-1 rounded-md bg-white/5 border border-white/5", config.color)}>
            <Icon size={10} />
          </div>
        </TooltipTrigger>
        <TooltipContent side="top" className="text-[10px] px-2 py-1">
          {config.label}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

export function TradeoffSlider({ value, onChange, min = 0, max = 1, step = 0.05, leftLabel, rightLabel }) {
  return (
    <div className="w-64 space-y-2">
      <div className="flex justify-between text-[10px] uppercase tracking-wider text-muted-foreground font-medium">
        <span>{leftLabel}</span>
        <span>{rightLabel}</span>
      </div>
      <div className="relative h-6 flex items-center group">
        <input
          type="range"
          min={min}
          max={max}
          step={step}
          value={value ?? (min + max) / 2}
          onChange={e => onChange(Number(e.target.value))}
          className={cn(
            "w-full h-1.5 bg-muted/50 rounded-full appearance-none cursor-pointer outline-none transition-all",
            "accent-primary hover:accent-emerald-400"
          )}
        />
        <div 
          className="absolute h-3 w-1 bg-primary rounded-full pointer-events-none transition-all group-hover:h-4"
          style={{ left: `${((value - min) / (max - min)) * 100}%` }}
        />
      </div>
    </div>
  )
}
