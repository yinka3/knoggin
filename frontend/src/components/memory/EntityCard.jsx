import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { User, Box, Tag, MapPin, Calendar, Globe, Hash } from 'lucide-react'

const TYPE_ICONS = {
  person: User,
  project: Box,
  organization: Globe,
  location: MapPin,
  event: Calendar,
  concept: Hash,
  topic: Tag,
  // Fallback
  default: Box,
}

export default function EntityCard({ entity, onClick }) {
  const Icon = TYPE_ICONS[entity.type] || TYPE_ICONS.default

  return (
    <Card
      onClick={onClick}
      className="
        group relative cursor-pointer overflow-hidden border-border/60 bg-card/50 backdrop-blur-sm
        transition-all duration-300 ease-out
        hover:-translate-y-1 hover:shadow-xl hover:shadow-primary/5 hover:border-primary/40
      "
    >
      {/* BACKGROUND GRADIENT BLOB (Visible on Hover) */}
      <div
        className="
          absolute -right-10 -top-10 h-32 w-32 rounded-full bg-primary/10 blur-3xl 
          transition-opacity duration-500 opacity-0 group-hover:opacity-100
        "
      />

      <CardHeader className="flex flex-row items-start justify-between space-y-0 pb-2 relative z-10">
        <div className="space-y-1">
          <CardTitle className="text-base font-semibold leading-tight tracking-tight text-foreground/90 group-hover:text-primary transition-colors duration-300">
            {entity.canonical_name}
          </CardTitle>
          <div className="text-xs text-muted-foreground font-mono">{entity.type}</div>
        </div>

        {/* ICON ANIMATION */}
        <div className="p-2 rounded-md bg-muted/50 text-muted-foreground group-hover:bg-primary/10 group-hover:text-primary transition-colors duration-300">
          <Icon
            size={18}
            className="transition-transform duration-500 group-hover:scale-110 group-hover:rotate-6"
          />
        </div>
      </CardHeader>

      <CardContent className="relative z-10">
        <div className="text-sm text-muted-foreground line-clamp-2 min-h-[2.5rem] mb-3 leading-relaxed">
          {entity.summary || 'No summary available.'}
        </div>

        {/* TOPIC BADGE */}
        {entity.topic && (
          <Badge
            variant="secondary"
            className="
              text-[10px] bg-secondary/50 text-secondary-foreground/80 hover:bg-secondary 
              transition-colors duration-300
            "
          >
            {entity.topic}
          </Badge>
        )}
      </CardContent>
    </Card>
  )
}
