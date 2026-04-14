import { useState } from 'react'

export function useDrawers() {
  const [topicsOpen, setTopicsOpen] = useState(false)
  const [toolsOpen, setToolsOpen] = useState(false)
  const [filesOpen, setFilesOpen] = useState(false)
  const [inboxOpen, setInboxOpen] = useState(false)
  const [notesOpen, setNotesOpen] = useState(false)

  const [notesCount, setNotesCount] = useState(0)
  const [fileCount, setFileCount] = useState(0)
  const [inboxCount, setInboxCount] = useState(0)

  return {
    drawers: {
      topics: { open: topicsOpen, setOpen: setTopicsOpen },
      tools: { open: toolsOpen, setOpen: setToolsOpen },
      files: { open: filesOpen, setOpen: setFilesOpen, count: fileCount, setCount: setFileCount },
      inbox: { open: inboxOpen, setOpen: setInboxOpen, count: inboxCount, setCount: setInboxCount },
      notes: { open: notesOpen, setOpen: setNotesOpen, count: notesCount, setCount: setNotesCount },
    }
  }
}
