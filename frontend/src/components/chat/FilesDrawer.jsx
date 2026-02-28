import { useState, useRef, useEffect, useCallback } from 'react'
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Badge } from '@/components/ui/badge'
import { Upload, Trash2, FileCode, FileText, File, Loader2 } from 'lucide-react'
import { uploadFile, listFiles, deleteFile } from '@/api/files'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'
import { formatDate, formatSize } from '@/lib/format'

const CODE_EXT = new Set([
  '.py',
  '.js',
  '.ts',
  '.jsx',
  '.tsx',
  '.java',
  '.go',
  '.rs',
  '.c',
  '.cpp',
  '.h',
  '.css',
  '.html',
])

const ALLOWED_EXT = [
  '.txt',
  '.md',
  '.py',
  '.js',
  '.ts',
  '.jsx',
  '.tsx',
  '.java',
  '.go',
  '.rs',
  '.c',
  '.cpp',
  '.h',
  '.css',
  '.html',
  '.pdf',
  '.docx',
  '.csv',
  '.json',
]

function getFileIcon(ext) {
  if (CODE_EXT.has(ext)) return FileCode
  return FileText
}

function FileRow({ file, onDelete, deleting }) {
  const Icon = getFileIcon(file.extension)
  const isDeleting = deleting === file.file_id

  return (
    <div
      className={cn(
        'flex items-center gap-3 px-3 py-2.5 rounded-lg group transition-all duration-150',
        'border border-white/[0.04] bg-white/[0.01]',
        'hover:border-white/[0.08]',
        isDeleting && 'opacity-40'
      )}
    >
      <div className="p-1.5 rounded-md bg-white/[0.03] shrink-0">
        <Icon size={14} className="text-muted-foreground/60" />
      </div>

      <div className="min-w-0 flex-1">
        <p className="text-xs text-foreground truncate">{file.original_name}</p>
        <div className="flex items-center gap-1.5 text-[10px] text-muted-foreground/40 mt-0.5">
          <span>{formatSize(file.size_bytes)}</span>
          <span>·</span>
          <span>
            {file.chunk_count} {file.chunk_count === 1 ? 'chunk' : 'chunks'}
          </span>
          <span>·</span>
          <span>{formatDate(file.uploaded_at)}</span>
        </div>
      </div>

      <button
        onClick={() => onDelete(file.file_id, file.original_name)}
        disabled={isDeleting}
        className={cn(
          'p-1.5 rounded-md shrink-0 transition-all duration-150',
          'opacity-0 group-hover:opacity-100',
          'text-muted-foreground/40 hover:text-destructive hover:bg-destructive/10'
        )}
      >
        {isDeleting ? <Loader2 size={12} className="animate-spin" /> : <Trash2 size={12} />}
      </button>
    </div>
  )
}

export default function FilesDrawer({ sessionId, open, onOpenChange, onCountChange }) {
  const [files, setFiles] = useState([])
  const [uploading, setUploading] = useState(false)
  const [deleting, setDeleting] = useState(null)
  const fileInputRef = useRef(null)

  const loadFiles = useCallback(async () => {
    try {
      const data = await listFiles(sessionId)
      const fileList = data.files || []
      setFiles(fileList)
      onCountChange?.(fileList.length)
    } catch (err) {
      console.error('Failed to load files:', err)
    }
  }, [sessionId, onCountChange])

  useEffect(() => {
    if (open && sessionId) {
      loadFiles()
    }
  }, [open, sessionId, loadFiles])

  async function handleFileSelect(e) {
    const selected = Array.from(e.target.files || [])
    if (!selected.length) return

    setUploading(true)
    let successCount = 0

    for (const file of selected) {
      try {
        await uploadFile(sessionId, file)
        successCount++
      } catch (err) {
        toast.error(`${file.name}: ${err.message}`)
      }
    }

    if (successCount > 0) {
      toast.success(`Uploaded ${successCount} file${successCount > 1 ? 's' : ''}`)
      await loadFiles()
    }

    setUploading(false)
    if (fileInputRef.current) fileInputRef.current.value = ''
  }

  async function handleDelete(fileId, fileName) {
    setDeleting(fileId)
    try {
      await deleteFile(sessionId, fileId)
      setFiles(prev => {
        const next = prev.filter(f => f.file_id !== fileId)
        onCountChange?.(next.length)
        return next
      })
      toast.success(`Removed ${fileName}`)
    } catch (err) {
      toast.error(err.message || 'Failed to delete')
    } finally {
      setDeleting(null)
    }
  }

  if (!sessionId) return null

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="w-80 sm:w-96 p-0 flex flex-col">
        <div className="px-5 pt-5 pb-3 border-b border-border/30">
          <SheetHeader>
            <SheetTitle className="flex items-center justify-between text-base">
              <span>Files</span>
              <Badge variant="outline" className="text-[10px] font-normal">
                {files.length}/20
              </Badge>
            </SheetTitle>
          </SheetHeader>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4">
          {/* Upload */}
          <input
            ref={fileInputRef}
            type="file"
            accept={ALLOWED_EXT.join(',')}
            onChange={handleFileSelect}
            multiple
            className="hidden"
          />
          <button
            onClick={() => fileInputRef.current?.click()}
            disabled={uploading || files.length >= 20}
            className={cn(
              'w-full flex items-center justify-center gap-2 px-3 py-3 rounded-lg mb-4',
              'border border-dashed border-white/[0.08] text-sm text-muted-foreground',
              'hover:border-white/[0.15] hover:text-foreground hover:bg-white/[0.02]',
              'transition-all duration-150',
              (uploading || files.length >= 20) && 'opacity-40 pointer-events-none'
            )}
          >
            {uploading ? (
              <>
                <Loader2 size={14} className="animate-spin" />
                <span className="text-xs">Uploading...</span>
              </>
            ) : (
              <>
                <Upload size={14} />
                <span className="text-xs">Upload files</span>
              </>
            )}
          </button>

          {/* File list */}
          {files.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-center">
              <File size={20} className="text-muted-foreground/30 mb-2" />
              <p className="text-xs text-muted-foreground/60">No files yet</p>
              <p className="text-[10px] text-muted-foreground/30 mt-1">
                Upload files for your agent to reference
              </p>
            </div>
          ) : (
            <div className="space-y-1.5">
              {files.map(f => (
                <FileRow key={f.file_id} file={f} onDelete={handleDelete} deleting={deleting} />
              ))}
            </div>
          )}
        </div>

        <div className="px-5 py-2.5 border-t border-border/30">
          <p className="text-[10px] text-muted-foreground/30 text-center">
            Supports code, text, PDF, DOCX, CSV, JSON — up to 50MB
          </p>
        </div>
      </SheetContent>
    </Sheet>
  )
}
