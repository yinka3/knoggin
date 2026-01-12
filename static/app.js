let history = []

const topicsList = document.getElementById('topics-list')
const profilesList = document.getElementById('profiles-list')
const chatMessages = document.getElementById('chat-messages')
const statusBar = document.getElementById('status-bar')
const messageInput = document.getElementById('message-input')
const sendBtn = document.getElementById('send-btn')


// SSE Parser
function parse(chunk, buffer) {
  buffer += chunk
  const events = []
  const blocks = buffer.split("\n\n")
  const remaining = blocks.pop()

  for (const block of blocks) {
    if (!block.trim()) continue
    
    const report = { event: 'message', data: {} }
    const lines = block.split("\n")
    
    for (const line of lines) {
      const colon = line.indexOf(":")
      if (colon === -1) continue
      
      const key = line.substring(0, colon)
      const value = line.substring(colon + 2)
      
      if (key === 'data') {
        try {
          report.data = JSON.parse(value)
        } catch {
          report.data = value
        }
      } else {
        report[key] = value
      }
    }
    events.push(report)
  }

  return { events, buffer: remaining }
}


// Topics
function renderTopics(data) {
  topicsList.innerHTML = ''
  
  const all = [
    ...data.hot.map(name => ({ name, status: 'hot' })),
    ...data.active.map(name => ({ name, status: 'active' })),
    ...data.inactive.map(name => ({ name, status: 'inactive' }))
  ]

  for (const topic of all) {
    const li = document.createElement('li')
    li.className = 'topic-item'
    li.dataset.name = topic.name
    li.dataset.status = topic.status
    
    const icon = topic.status === 'hot' ? '🔥' : topic.status === 'active' ? '●' : '○'
    
    li.innerHTML = `
      <span class="topic-name">${topic.name}</span>
      <button class="topic-toggle">${icon}</button>
    `
    
    topicsList.appendChild(li)
  }
}

async function loadTopics() {
  const response = await fetch('/topics')
  const data = await response.json()
  renderTopics(data)
}

async function toggleTopic(name, newStatus) {
  statusBar.textContent = 'Updating topic...'
  statusBar.className = 'loading'
  
  const response = await fetch(`/topics/${encodeURIComponent(name)}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status: newStatus })
  })
  
  if (response.ok) {
    await loadTopics()
  } else {
    const err = await response.json()
    statusBar.textContent = err.detail || 'Failed to update'
  }
  
  statusBar.textContent = ''
  statusBar.className = ''
}


// Profiles
function renderProfiles(profiles) {
  profilesList.innerHTML = ''
  
  for (const profile of profiles) {
    const li = document.createElement('li')
    li.className = 'profile-item'
    li.dataset.name = profile.name
    
    li.innerHTML = `
      <span class="profile-name">${profile.name}</span>
      <span class="profile-type">${profile.type || ''}</span>
    `
    
    profilesList.appendChild(li)
  }
}

async function loadProfiles() {
  const response = await fetch('/entities?limit=50')
  const data = await response.json()
  renderProfiles(data)
}


// Chat
function appendMessage(content, role) {
  const div = document.createElement('div')
  div.className = `message ${role}`
  div.innerHTML = `<span class="message-content">${content}</span>`
  chatMessages.appendChild(div)
  chatMessages.scrollTop = chatMessages.scrollHeight
}

async function streamChat(query) {
  const response = await fetch('/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message: query, history: history })
  })

  const stream = response.body.pipeThrough(new TextDecoderStream())
  let buffer = ''

  for await (const chunk of stream) {
    const result = parse(chunk, buffer)
    buffer = result.buffer

    for (const event of result.events) {
      handleSSEEvent(event, query)
    }
  }
}

function handleSSEEvent(event, query) {
  const type = event.event
  const data = event.data

  if (type === 'message') {
      appendMessage(data.response, 'assistant')
      history.push({ role: 'user', content: query, timestamp: new Date().toISOString() })
      history.push({ role: 'assistant', content: data.response, timestamp: new Date().toISOString() })
  } 
  else if (type === 'clarification') {
      appendMessage(data.question, 'assistant')
      history.push({ role: 'user', content: query, timestamp: new Date().toISOString() })
      history.push({ role: 'assistant', content: data.question, timestamp: new Date().toISOString() })
  }
  else if (type === 'done') {
    loadProfiles()
  } 
  else if (type.startsWith('error')) {
    statusBar.textContent = data.message || 'Something went wrong'
  }
}

async function sendMessage() {
  const text = messageInput.value.trim()
  if (!text) return
  
  messageInput.disabled = true
  sendBtn.disabled = true
  
  appendMessage(text, 'user')
  messageInput.value = ''
  
  statusBar.textContent = 'Thinking...'
  statusBar.className = 'loading'
  
  await streamChat(text)
  
  messageInput.disabled = false
  sendBtn.disabled = false
  messageInput.focus()
  statusBar.textContent = ''
  statusBar.className = ''
}


// Initialize
document.addEventListener('DOMContentLoaded', () => {
  topicsList.addEventListener('click', async (e) => {
    if (!e.target.classList.contains('topic-toggle')) return
    
    const li = e.target.closest('.topic-item')
    const name = li.dataset.name
    const current = li.dataset.status
    
    const next = current === 'inactive' ? 'active' 
               : current === 'active' ? 'hot' 
               : 'inactive'
    
    await toggleTopic(name, next)
  })

  profilesList.addEventListener('click', (e) => {
    const li = e.target.closest('.profile-item')
    if (!li) return
    
    const name = li.dataset.name
    messageInput.value = `Who is ${name}?`
    messageInput.focus()
  })

  sendBtn.addEventListener('click', sendMessage)
  
  messageInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      sendMessage()
    }
  })

  loadTopics()
  loadProfiles()
})