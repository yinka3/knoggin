import { useState, useRef, useEffect } from 'react'
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from '@/components/ui/sheet'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Paperclip, Upload, Trash2, File, FileCode, FileText, Loader2 } from 'lucide-react'
import { uploadFile, listFiles, deleteFile } from '@/api/files'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'
import { formatDate, formatSize } from '@/lib/format'

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

function getFileIcon(ext) {
  if (CODE_EXT.has(ext)) return FileCode
  return FileText
}


export default function FilesDrawer({ sessionId }) {
  const [open, setOpen] = useState(false)
  const [files, setFiles] = useState([])
  const [uploading, setUploading] = useState(false)
  const [deleting, setDeleting] = useState(null)
  const fileInputRef = useRef(null)

  useEffect(() => {
    if (open && sessionId) {
      loadFiles()
    }
  }, [open, sessionId])

  async function loadFiles() {
    try {
      const data = await listFiles(sessionId)
      setFiles(data.files || [])
    } catch (err) {
      console.error('Failed to load files:', err)
    }
  }

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
      setFiles(prev => prev.filter(f => f.file_id !== fileId))
      toast.success(`Removed ${fileName}`)
    } catch (err) {
      toast.error(err.message || 'Failed to delete')
    } finally {
      setDeleting(null)
    }
  }

  if (!sessionId) return null

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger asChild>
        <button
          className={cn(
            'relative flex items-center gap-1.5 px-2 py-1 rounded-md text-xs transition-colors duration-150',
            'text-muted-foreground hover:text-foreground hover:bg-muted/50',
            files.length > 0 && 'text-primary'
          )}
        >
          <Paperclip size={14} />
          <span className="hidden sm:inline">Files</span>
          {files.length > 0 && (
            <Badge variant="secondary" className="h-4 px-1 text-[10px] min-w-[16px] justify-center">
              {files.length}
            </Badge>
          )}
        </button>
      </SheetTrigger>

      <SheetContent side="right" className="w-80 sm:w-96">
        <SheetHeader>
          <SheetTitle className="flex items-center justify-between">
            <span>Session Files</span>
            <Badge variant="outline" className="text-[10px]">
              {files.length}/20
            </Badge>
          </SheetTitle>
        </SheetHeader>

        <div className="mt-6 space-y-4">
          {/* Upload area */}
          <input
            ref={fileInputRef}
            type="file"
            accept={ALLOWED_EXT.join(',')}
            onChange={handleFileSelect}
            multiple
            className="hidden"
          />
          <Button
            variant="outline"
            className="w-full gap-2 border-dashed"
            onClick={() => fileInputRef.current?.click()}
            disabled={uploading || files.length >= 20}
          >
            {uploading ? (
              <>
                <Loader2 size={14} className="animate-spin" />
                Uploading...
              </>
            ) : (
              <>
                <Upload size={14} />
                Upload Files
              </>
            )}
          </Button>

          {/* File list */}
          {files.length === 0 ? (
            <div className="text-center py-8">
              <File size={24} className="mx-auto text-muted-foreground/40 mb-2" />
              <p className="text-xs text-muted-foreground">No files uploaded yet</p>
              <p className="text-[10px] text-muted-foreground/60 mt-1">
                Upload files for your agent to reference
              </p>
            </div>
          ) : (
            <div className="space-y-1">
              {files.map(f => {
                const Icon = getFileIcon(f.extension)
                const isDeleting = deleting === f.file_id

                return (
                  <div
                    key={f.file_id}
                    className={cn(
                      'flex items-center gap-3 p-2.5 rounded-lg group transition-colors duration-150',
                      'hover:bg-muted/50',
                      isDeleting && 'opacity-50'
                    )}
                  >
                    <div className="p-1.5 rounded-md bg-muted shrink-0">
                      <Icon size={14} className="text-muted-foreground" />
                    </div>

                    <div className="min-w-0 flex-1">
                      <p className="text-sm truncate">{f.original_name}</p>
                      <div className="flex items-center gap-2 text-[10px] text-muted-foreground">
                        <span>{formatSize(f.size_bytes)}</span>
                        <span>·</span>
                        <span>{f.chunk_count} chunks</span>
                        <span>·</span>
                        <span>{formatDate(f.uploaded_at)}</span>
                      </div>
                    </div>

                    <button
                      onClick={() => handleDelete(f.file_id, f.original_name)}
                      disabled={isDeleting}
                      className={cn(
                        'p-1.5 rounded-md transition-all duration-150 shrink-0',
                        'opacity-0 group-hover:opacity-100',
                        'hover:bg-destructive/10 hover:text-destructive',
                        'text-muted-foreground'
                      )}
                    >
                      {isDeleting ? (
                        <Loader2 size={12} className="animate-spin" />
                      ) : (
                        <Trash2 size={12} />
                      )}
                    </button>
                  </div>
                )
              })}
            </div>
          )}

          {/* Allowed types hint */}
          <p className="text-[10px] text-muted-foreground/50 text-center">
            Supports code, text, PDF, DOCX, CSV, JSON — up to 50MB
          </p>
        </div>
      </SheetContent>
    </Sheet>
  )
}
