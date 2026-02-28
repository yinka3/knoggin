import { useState, useEffect, useCallback } from 'react'
import {
  getCommunityStats,
  toggleCommunity,
  triggerDiscussion,
  getDiscussionHistory,
  closeDiscussion,
} from '@/api/community'
import { listAgents } from '@/api/agents'
import useCommunityStream from '@/hooks/useCommunityStream'
import CommunityDisabledOverlay from '@/components/community/CommunityDisabledOverlay'
import CommunityHeader from '@/components/community/CommunityHeader'
import CommunityTheater from '@/components/community/CommunityTheater'
import CommunityTabs from '@/components/community/CommunityTabs'
import HistoryTab from '@/components/community/tabs/HistoryTab'
import AgentsTab from '@/components/community/tabs/AgentsTab'
import InsightsTab from '@/components/community/tabs/InsightsTab'
import { toast } from 'sonner'

export default function CommunityPage() {
  const [loading, setLoading] = useState(true)
  const [enabled, setEnabled] = useState(false)
  const [enabling, setEnabling] = useState(false)
  const [triggering, setTriggering] = useState(false)
  const [closing, setClosing] = useState(false)

  const [activeDiscussionId, setActiveDiscussionId] = useState(null)
  const [topic, setTopic] = useState(null)
  const [messages, setMessages] = useState([])
  const [turn, setTurn] = useState(0)
  const [maxTurns, setMaxTurns] = useState(10)

  const [selectedDiscussion, setSelectedDiscussion] = useState(null)
  const [historyLoading, setHistoryLoading] = useState(false)

  const [agentMap, setAgentMap] = useState({})

  const [activeTab, setActiveTab] = useState('history')

  const isViewingLive = !selectedDiscussion || selectedDiscussion.id === activeDiscussionId

  const handleEvent = useCallback(
    event => {
      const { event: eventType, data } = event

      switch (eventType) {
        case 'discussion_started':
          setActiveDiscussionId(data.id)
          setTopic(data.topic)
          setSelectedDiscussion(null)
          setMessages([])
          setTurn(0)
          break

        case 'discussion_seeded':
          setTopic(data.topic)
          break

        case 'message_added':
          if (!selectedDiscussion || selectedDiscussion.id === activeDiscussionId) {
            setMessages(prev => [
              ...prev,
              {
                agent_id: data.agent_id,
                agent_name: data.agent_name,
                content: data.content,
                timestamp: event.ts,
              },
            ])
            setTurn(prev => prev + 1)
          }
          break

        case 'discussion_ended':
          setActiveDiscussionId(null)
          break

        default:
          break
      }
    },
    [selectedDiscussion, activeDiscussionId]
  )

  const { connected } = useCommunityStream(handleEvent, enabled)

  useEffect(() => {
    async function load() {
      try {
        const [stats, agentsRes] = await Promise.all([getCommunityStats(), listAgents()])

        setEnabled(stats.enabled)
        setActiveDiscussionId(stats.active_discussion_id)
        setMaxTurns(stats.max_turns)

        const map = {}
        for (const agent of agentsRes.agents || []) {
          map[agent.id] = agent.name
        }
        setAgentMap(map)

        if (stats.active_discussion_id) {
          try {
            const res = await getDiscussionHistory(stats.active_discussion_id)
            setMessages(
              (res.messages || []).map(msg => ({
                agent_id: msg.agent_id,
                agent_name: map[msg.agent_id] || msg.agent_id,
                content: msg.content,
                timestamp: msg.timestamp,
              }))
            )
            setTurn(res.messages?.length || 0)
          } catch (e) {
            console.error('Failed to load active discussion history', e)
          }
        }
      } catch (err) {
        console.error('Failed to load community stats:', err)
        toast.error('Failed to load community')
      } finally {
        setLoading(false)
      }
    }

    load()
  }, [])

  async function handleSelectDiscussion(discussion) {
    if (discussion.id === activeDiscussionId) {
      setSelectedDiscussion(null)
    } else {
      setSelectedDiscussion(discussion)
    }
    
    setHistoryLoading(true)

    try {
      const res = await getDiscussionHistory(discussion.id)
      setMessages(
        (res.messages || []).map(msg => ({
          agent_id: msg.agent_id,
          agent_name: agentMap[msg.agent_id] || msg.agent_id,
          content: msg.content,
          timestamp: msg.timestamp,
        }))
      )
      setTopic(discussion.topic)
      setTurn(res.messages?.length || 0)
    } catch (err) {
      console.error('Failed to load discussion history:', err)
      toast.error('Failed to load discussion')
    } finally {
      setHistoryLoading(false)
    }
  }

  async function handleReturnToLive() {
    setSelectedDiscussion(null)
    if (!activeDiscussionId) {
      setMessages([])
      setTopic(null)
      setTurn(0)
      return
    }
    
    setHistoryLoading(true)
    try {
      const res = await getDiscussionHistory(activeDiscussionId)
      setMessages(
        (res.messages || []).map(msg => ({
          agent_id: msg.agent_id,
          agent_name: agentMap[msg.agent_id] || msg.agent_id,
          content: msg.content,
          timestamp: msg.timestamp,
        }))
      )
      setTurn(res.messages?.length || 0)
    } catch (e) {
      console.error('Failed to return to live discussion:', e)
    } finally {
      setHistoryLoading(false)
    }
  }

  async function handleEnable() {
    setEnabling(true)
    try {
      await toggleCommunity(true)
      setEnabled(true)
      toast.success('Community enabled')
    } catch (err) {
      toast.error('Failed to enable community')
    } finally {
      setEnabling(false)
    }
  }

  async function handleTrigger() {
    setTriggering(true)
    try {
      const res = await triggerDiscussion()
      if (res.discussion_id) {
        toast.success('Discussion started')
      } else {
        toast.info(res.message || 'Discussion triggered')
      }
    } catch (err) {
      toast.error(err.message || 'Failed to trigger discussion')
    } finally {
      setTriggering(false)
    }
  }

  async function handleClose() {
    setClosing(true)
    try {
      const res = await closeDiscussion()
      toast.success(res.message || 'Discussion closed')
    } catch (err) {
      toast.error(err.message || 'Failed to close discussion')
    } finally {
      setClosing(false)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div
          className="w-8 h-8 rounded-full animate-pulse"
          style={{
            background:
              'radial-gradient(circle at 40% 40%, rgba(52, 216, 130, 0.9), rgba(46, 170, 110, 0.6))',
            boxShadow: '0 0 20px rgba(46, 170, 110, 0.4)',
          }}
        />
      </div>
    )
  }

  if (!enabled) {
    return <CommunityDisabledOverlay onEnable={handleEnable} enabling={enabling} />
  }

  return (
    <div className="flex flex-col h-full">
      <CommunityHeader
        connected={connected}
        isLive={!!activeDiscussionId && isViewingLive}
        isViewingHistory={!!selectedDiscussion}
        topic={topic}
        turn={turn}
        maxTurns={maxTurns}
        onTrigger={handleTrigger}
        triggering={triggering}
        onClose={handleClose}
        closing={closing}
        onReturnToLive={handleReturnToLive}
      />

      <div className="flex-1 flex gap-4 p-4 overflow-hidden">
        {/* Theater - left/main */}
        <div className="flex-1 min-w-0">
          <CommunityTheater
            messages={messages}
            connected={connected}
            isLive={!!activeDiscussionId && isViewingLive}
            isLoading={historyLoading}
            agentMap={agentMap}
          />
        </div>

        {/* Tabs - right panel */}
        <div className="w-80 shrink-0">
          <CommunityTabs activeTab={activeTab} onTabChange={setActiveTab}>
            {activeTab === 'history' && (
              <HistoryTab
                onSelectDiscussion={handleSelectDiscussion}
                selectedId={selectedDiscussion?.id}
                activeDiscussionId={activeDiscussionId}
              />
            )}
            {activeTab === 'agents' && <AgentsTab />}
            {activeTab === 'insights' && <InsightsTab />}
          </CommunityTabs>
        </div>
      </div>
    </div>
  )
}
