import { useState, useEffect } from 'react'
import { motion, AnimatePresence } from 'motion/react'
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from '@/components/ui/sheet'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Check, X, GitMerge } from 'lucide-react'
import { toast } from 'sonner'
import { getMergeProposals, approveMergeProposal, rejectMergeProposal, undoMergeProposal } from '@/api/proposals'

export default function MergeInboxDrawer({ sessionId, open, onOpenChange, onCountChange }) {
  const [proposals, setProposals] = useState([])
  const [loading, setLoading] = useState(false)
  const [processingId, setProcessingId] = useState(null)

  useEffect(() => {
    if (sessionId && open) {
      loadProposals()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionId, open])

  // Initial load specifically for the badge count
  useEffect(() => {
    if (sessionId) {
      loadProposals()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionId])

  async function loadProposals() {
    setLoading(true)
    try {
      const data = await getMergeProposals(sessionId)
      const list = data.proposals || []
      setProposals(list)
      if (onCountChange) {
        onCountChange(list.length)
      }
    } catch (err) {
      console.error('Failed to load merge proposals:', err)
    } finally {
      setLoading(false)
    }
  }

  async function handleApprove(index) {
    setProcessingId(index)
    const proposal = proposals.find((p) => p.index === index)
    try {
      await approveMergeProposal(sessionId, index, proposal.primary_id, proposal.secondary_id)
      toast.success('Merge approved', {
        action: {
          label: 'Undo',
          onClick: async () => {
            try {
              await undoMergeProposal(sessionId, proposal.primary_id, proposal.secondary_id)
              toast.success('Merge undone')
              loadProposals()
            } catch {
              toast.error('Failed to undo merge')
            }
          }
        }
      })
      // Remove from list
      const updated = proposals.filter((p) => p.index !== index)
      setProposals(updated)
      if (onCountChange) onCountChange(updated.length)
    } catch (err) {
      console.error('Failed to approve merge:', err)
      toast.error('Failed to approve merge')
    } finally {
      setProcessingId(null)
    }
  }

  async function handleReject(index) {
    setProcessingId(index)
    const proposal = proposals.find((p) => p.index === index)
    try {
      await rejectMergeProposal(sessionId, index, proposal.primary_id, proposal.secondary_id)
      toast.info('Merge rejected')
      // Remove from list
      const updated = proposals.filter((p) => p.index !== index)
      setProposals(updated)
      if (onCountChange) onCountChange(updated.length)
    } catch (err) {
      console.error('Failed to reject merge:', err)
      toast.error('Failed to reject merge')
    } finally {
      setProcessingId(null)
    }
  }

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="w-80 sm:w-96 p-0 flex flex-col">
        <SheetHeader className="px-5 pt-5 pb-3 border-b border-border/30 text-left">
          <SheetTitle className="flex items-center gap-2 text-base">
            <GitMerge className="h-4 w-4 text-primary" />
            Merge Proposals
          </SheetTitle>
          <SheetDescription className="text-xs">
            Entities that seem similar but need human review. Approving will merge their facts and
            relationships.
          </SheetDescription>
        </SheetHeader>

        <div className="flex-1 overflow-y-auto mt-4 px-1 custom-scrollbar">
          {loading && proposals.length === 0 ? (
            <div className="flex justify-center p-8">
              <div className="h-8 w-8 animate-spin rounded-full border-b-2 border-primary"></div>
            </div>
          ) : proposals.length === 0 ? (
            <div className="text-center py-12 text-muted-foreground bg-muted/30 rounded-lg border border-dashed">
              <GitMerge className="h-8 w-8 mx-auto mb-3 opacity-20" />
              <p>No pending merges.</p>
              <p className="text-sm mt-1">Your knowledge graph is clean!</p>
            </div>
          ) : (
            <div className="space-y-4">
              <AnimatePresence>
                {proposals.map((proposal) => (
                  <motion.div
                    key={proposal.index}
                    initial={{ opacity: 0, scale: 0.95 }}
                    animate={{ opacity: 1, scale: 1 }}
                    exit={{ opacity: 0, scale: 0.95, height: 0, marginBottom: 0 }}
                  >
                    <Card className="border shadow-sm">
                      <CardContent className="p-4">
                        <div className="flex items-start justify-between">
                          <div className="flex-1 pr-4">
                            <div className="flex items-center gap-2 mb-2">
                              <span className="font-semibold text-foreground">
                                {proposal.primary_name}
                              </span>
                              <span className="text-muted-foreground text-sm">←</span>
                              <span className="font-semibold text-foreground">
                                {proposal.secondary_name}
                              </span>
                            </div>

                            <div className="flex items-center gap-2 text-sm text-muted-foreground">
                              <span>Confidence:</span>
                              <div className="h-1.5 w-24 bg-muted rounded-full overflow-hidden">
                                <div
                                  className="h-full bg-primary"
                                  style={{ width: `${Math.round((proposal.score || 0) * 100)}%` }}
                                />
                              </div>
                              <span className="font-mono text-xs">
                                {Math.round((proposal.score || 0) * 100)}%
                              </span>
                            </div>
                          </div>

                          <div className="flex flex-col gap-2">
                            <Button
                              size="sm"
                              variant="default"
                              className="h-8"
                              onClick={() => handleApprove(proposal.index)}
                              disabled={processingId === proposal.index}
                            >
                              <Check className="h-4 w-4 mr-1" />
                              Approve
                            </Button>
                            <Button
                              size="sm"
                              variant="outline"
                              className="h-8"
                              onClick={() => handleReject(proposal.index)}
                              disabled={processingId === proposal.index}
                            >
                              <X className="h-4 w-4 mr-1" />
                              Reject
                            </Button>
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  </motion.div>
                ))}
              </AnimatePresence>
            </div>
          )}
        </div>
      </SheetContent>
    </Sheet>
  )
}
