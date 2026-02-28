import asyncio
import os
from functools import partial
from pathlib import Path
from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from loguru import logger

from api.deps import get_app_state
from api.state import AppState
from shared.file_rag import ALLOWED_EXTENSIONS, MAX_FILE_SIZE

router = APIRouter()



@router.post("/{session_id}/upload")
async def upload_file(
    session_id: str,
    file: UploadFile = File(...),
    state: AppState = Depends(get_app_state)
):
    context = await state.get_or_resume_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")
    
    if not context.file_rag:
        raise HTTPException(status_code=500, detail="File service not available")
    
    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400, 
            detail=f"Unsupported file type: {ext}. Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}"
        )
    
    if file.size and file.size > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail=f"File too large. Max: {MAX_FILE_SIZE // (1024*1024)}MB")
    
    upload_dir = os.path.join(os.getenv("CONFIG_DIR", "./config"), "uploads", "tmp")
    os.makedirs(upload_dir, exist_ok=True)
    
    tmp_path = os.path.join(upload_dir, f"{session_id}_{file.filename}")
    
    try:
        size = 0
        with open(tmp_path, "wb") as f:
            while chunk := await file.read(1024 * 64): 
                size += len(chunk)
                if size > MAX_FILE_SIZE:
                    raise HTTPException(status_code=400, detail=f"File too large. Max: {MAX_FILE_SIZE // (1024*1024)}MB")
                f.write(chunk)
        
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            partial(context.file_rag.ingest_file, tmp_path, file.filename)
        )
        
        return result
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"File upload failed: {e}")
        raise HTTPException(status_code=500, detail="File processing failed")
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


@router.get("/{session_id}")
async def list_files(
    session_id: str,
    state: AppState = Depends(get_app_state)
):
    context = await state.get_or_resume_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")
    
    if not context.file_rag:
        return {"files": [], "total": 0}
    
    files = context.file_rag.list_files()
    return {"files": files, "total": len(files)}


@router.delete("/{session_id}/{file_id}")
async def delete_file(
    session_id: str,
    file_id: str,
    state: AppState = Depends(get_app_state)
):
    context = await state.get_or_resume_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")
    
    if not context.file_rag:
        raise HTTPException(status_code=500, detail="File service not available")
    
    deleted = context.file_rag.delete_file(file_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="File not found")
    
    return {"deleted": True, "file_id": file_id}