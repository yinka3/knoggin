import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet'
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion"
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import { ScrollArea } from '@/components/ui/scroll-area'
import { useTools } from '@/context/ToolsContext'
import { Server, Wrench, AlertCircle } from 'lucide-react'

export default function ToolsDrawer({ open, onOpenChange }) {
  const { availableTools, enabledTools, toggleTool, loading } = useTools()


  const groups = availableTools.reduce((acc, tool) => {
    const key = tool.group || 'Other'
    if (!acc[key]) acc[key] = []
    acc[key].push(tool)
    return acc
  }, {})

  const sortedGroups = Object.keys(groups).sort((a, b) => {

    const priority = { 'Memory': 1, 'Graph': 2, 'History': 3, 'RAG': 4 }
    const pa = priority[a] || 99
    const pb = priority[b] || 99
    return pa - pb
  })

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="left" className="w-[400px] sm:w-[540px] flex flex-col p-6">
        <SheetHeader className="mb-4">
          <SheetTitle className="flex items-center gap-2">
            <Wrench className="w-5 h-5 text-primary" />
            Active Tools
          </SheetTitle>
          <SheetDescription>
            Manage which capabilities the agent can access.
          </SheetDescription>
        </SheetHeader>

        {loading ? (
            <div className="flex-1 flex items-center justify-center text-muted-foreground">
                Loading tools...
            </div>
        ) : (
             <ScrollArea className="flex-1 pr-4 -mr-4">
          <Accordion type="multiple" defaultValue={sortedGroups} className="w-full">
            {sortedGroups.map(groupName => {
              const tools = groups[groupName]
              const activeCount = tools.filter(t => (enabledTools || []).includes(t.id)).length
              const allActive = activeCount === tools.length
              
              const isMCP = groupName.startsWith('MCP')

              return (
                <AccordionItem value={groupName} key={groupName} className="border-border">
                  <AccordionTrigger className="hover:no-underline py-3">
                    <div className="flex items-center gap-3 w-full">
                        {isMCP ? <Server size={16} className="text-orange-500" /> : <Wrench size={16} className="text-muted-foreground" />}
                        <span className="text-sm font-medium">{groupName}</span>
                        <div className="ml-auto flex items-center gap-2 mr-2">
                            <Badge variant="secondary" className="text-[10px] h-5 px-1.5 font-normal text-muted-foreground">
                                {activeCount}/{tools.length}
                            </Badge>
                        </div>
                    </div>
                  </AccordionTrigger>
                  <AccordionContent className="pb-4 pt-1">
                    <div className="space-y-1">
                      {tools.map(tool => {
                        const isEnabled = (enabledTools || []).includes(tool.id)
                        return (
                          <div
                            key={tool.id}
                            className="flex items-start justify-between gap-4 p-2 rounded-md hover:bg-muted/40 transition-colors group"
                          >
                            <div className="flex items-start gap-3">
                                <div className={`mt-1 h-1.5 w-1.5 rounded-full ${isEnabled ? 'bg-primary' : 'bg-muted-foreground/30'}`} />
                                <div className="space-y-0.5">
                                    <p className={`text-sm font-medium leading-none ${isEnabled ? 'text-foreground' : 'text-muted-foreground'}`}>
                                        {tool.name}
                                    </p>
                                    {tool.description && (
                                        <p className="text-xs text-muted-foreground/70 line-clamp-2">
                                            {tool.description}
                                        </p>
                                    )}
                                </div>
                            </div>
                            <Switch
                              checked={isEnabled}
                              onCheckedChange={() => toggleTool(tool.id)}
                              className="data-[state=checked]:bg-primary relative"
                            />
                          </div>
                        )
                      })}
                    </div>
                  </AccordionContent>
                </AccordionItem>
              )
            })}
          </Accordion>
        </ScrollArea>
        )}
       
        <div className="mt-auto pt-4 border-t border-border">
             <div className="flex items-center gap-2 rounded-lg bg-muted/50 p-3 text-xs text-muted-foreground">
                <AlertCircle size={14} className="shrink-0" />
                <p>Changes apply immediately to the active session.</p>
             </div>
        </div>
      </SheetContent>
    </Sheet>
  )
}
