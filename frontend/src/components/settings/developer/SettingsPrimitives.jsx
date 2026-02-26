import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'

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

export function SettingRow({ label, description, children }) {
  return (
    <div
      className={cn(
        'flex items-center justify-between gap-4 p-3 rounded-lg',
        'bg-muted/30 hover:bg-muted/50 transition-colors duration-150'
      )}
    >
      <div className="flex-1 min-w-0">
        <Label className="text-sm text-foreground font-normal">{label}</Label>
        {description && (
          <p className="text-[11px] text-muted-foreground mt-0.5 truncate">{description}</p>
        )}
      </div>
      <div className="w-28 shrink-0">{children}</div>
    </div>
  )
}

export function NumberInput({ value, onChange, min, max, step = 1, unit, placeholder }) {
  return (
    <div className="relative">
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
