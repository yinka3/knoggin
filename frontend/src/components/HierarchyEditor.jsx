/* eslint-disable react-hooks/set-state-in-effect */
import { useState, useEffect } from 'react'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { X, Plus } from 'lucide-react'

export default function HierarchyEditor({ name, config, updateField }) {
  // Convert { parent: ['child1', 'child2'] } to array of { key: 'parent', val: 'child1, child2' }
  const [rows, setRows] = useState(() => {
    const h = config.hierarchy || {}
    const entries = Object.entries(h)
    if (entries.length === 0) return [{ key: '', val: '' }]
    return entries.map(([k, v]) => ({ key: k, val: (v || []).join(', ') }))
  })

  // Sync from props if parent changes externally
  useEffect(() => {
    const h = config.hierarchy || {}
    const entries = Object.entries(h)
    if (entries.length === 0) {
      setRows([{ key: '', val: '' }])
    } else {
      setRows(entries.map(([k, v]) => ({ key: k, val: (v || []).join(', ') })))
    }
  }, [config.hierarchy])

  const commit = newRows => {
    const h = {}
    newRows.forEach(r => {
      const k = r.key.trim()
      if (k) {
        h[k] = r.val
          .split(',')
          .map(s => s.trim())
          .filter(Boolean)
      }
    })
    updateField(name, 'hierarchy', h)
  }

  const updateRow = (index, field, value) => {
    const newRows = [...rows]
    newRows[index][field] = value
    setRows(newRows)
    commit(newRows)
  }

  const addRow = () => {
    setRows([...rows, { key: '', val: '' }])
  }

  const removeRow = index => {
    const newRows = rows.filter((_, i) => i !== index)
    if (newRows.length === 0) newRows.push({ key: '', val: '' })
    setRows(newRows)
    commit(newRows)
  }

  return (
    <div className="space-y-2">
      <Label className="text-[11px] text-muted-foreground">Hierarchy Relations (Parent → Children)</Label>
      <div className="space-y-2">
        {rows.map((row, i) => (
          <div key={i} className="flex items-center gap-2">
            <Input
              value={row.key}
              onChange={e => updateRow(i, 'key', e.target.value)}
              placeholder="company"
              className="w-1/3 bg-muted border-border rounded-lg text-xs h-8"
            />
            <span className="text-muted-foreground/50 text-xs">→</span>
            <Input
              value={row.val}
              onChange={e => updateRow(i, 'val', e.target.value)}
              placeholder="team, product"
              className="flex-1 bg-muted border-border rounded-lg text-xs h-8"
            />
            <Button
              variant="ghost"
              size="icon"
              onClick={() => removeRow(i)}
              className="h-8 w-8 text-muted-foreground hover:text-destructive shrink-0"
              disabled={rows.length === 1 && !row.key && !row.val}
            >
              <X size={14} />
            </Button>
          </div>
        ))}
      </div>
      <Button
        variant="ghost"
        size="sm"
        onClick={addRow}
        className="h-7 text-xs text-muted-foreground hover:text-foreground px-2 -ml-2"
      >
        <Plus size={12} className="mr-1" /> Add relation
      </Button>
    </div>
  )
}
