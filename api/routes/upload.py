"""
File upload endpoints.

Handles multipart file uploads, stores files in a temp directory,
and returns file IDs that can be referenced in processing requests.
"""

import os
import re
import shutil
import tempfile
from typing import List, Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from api.models import FileInfo, UploadResponse
from api.session_store import store

router = APIRouter(prefix="/api", tags=["upload"])

# Maximum upload file size: 50 MB (Excel files for salary data should never exceed this)
MAX_UPLOAD_SIZE_BYTES = 50 * 1024 * 1024

# Maximum number of files per upload request
MAX_FILES_PER_UPLOAD = 10


def _sanitize_filename(filename: str) -> str:
    """Sanitize filename to prevent path traversal and special character injection.

    Strips directory components and replaces non-alphanumeric characters
    (except dots, hyphens, underscores) to prevent path traversal attacks
    when the filename is used in temp file prefixes.
    """
    # Take only the basename, stripping any directory traversal
    name = os.path.basename(filename)
    # Remove any remaining path separators and null bytes
    name = name.replace("\x00", "").replace("/", "").replace("\\", "")
    # Keep only safe characters: alphanumeric, dots, hyphens, underscores, spaces
    name = re.sub(r"[^\w.\-\s]", "_", name)
    # Truncate to reasonable length for temp file prefix
    return name[:100] if name else "unnamed"


@router.post("/upload", response_model=UploadResponse)
async def upload_files(
    files: List[UploadFile] = File(..., description="One or more Excel/CSV files to upload"),
    session_id: Optional[str] = Form(None, description="Existing session ID to add files to"),
):
    """
    Upload one or more files for processing.

    Returns file IDs that can be used in subsequent processing requests.
    Accepts .xlsx, .xls, and .csv files. Maximum 50 MB per file.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    if len(files) > MAX_FILES_PER_UPLOAD:
        raise HTTPException(
            status_code=400,
            detail=f"Too many files. Maximum {MAX_FILES_PER_UPLOAD} files per request.",
        )

    # Get or create session
    session = store.get_or_create_session(session_id)

    allowed_extensions = {".xlsx", ".xls", ".csv"}
    file_infos: List[FileInfo] = []

    for upload_file in files:
        if not upload_file.filename:
            raise HTTPException(status_code=400, detail="File has no name")

        # Sanitize filename to prevent path traversal in temp prefix
        safe_name = _sanitize_filename(upload_file.filename)

        # Validate extension (use sanitized name)
        ext = os.path.splitext(safe_name)[1].lower()
        if ext not in allowed_extensions:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type '{ext}' for '{safe_name}'. "
                       f"Allowed: {', '.join(sorted(allowed_extensions))}",
            )

        # Save to temp directory with sanitized prefix
        suffix = ext
        fd, temp_path = tempfile.mkstemp(
            suffix=suffix,
            prefix=f"remupro_{safe_name}_",
            dir=str(store.upload_dir),
        )
        try:
            with os.fdopen(fd, "wb") as tmp:
                # Read in chunks to enforce size limit without loading everything at once
                size_bytes = 0
                chunk_size = 1024 * 1024  # 1 MB chunks
                while True:
                    chunk = await upload_file.read(chunk_size)
                    if not chunk:
                        break
                    size_bytes += len(chunk)
                    if size_bytes > MAX_UPLOAD_SIZE_BYTES:
                        tmp.close()
                        os.unlink(temp_path)
                        raise HTTPException(
                            status_code=413,
                            detail=f"File '{safe_name}' exceeds maximum size of "
                                   f"{MAX_UPLOAD_SIZE_BYTES // (1024 * 1024)} MB.",
                        )
                    tmp.write(chunk)
        except HTTPException:
            raise
        except Exception as exc:
            os.unlink(temp_path)
            raise HTTPException(
                status_code=500,
                detail=f"Failed to save file: {type(exc).__name__}",
            )

        # Register in session
        file_id = store.register_file(
            session_id=session.session_id,
            original_name=upload_file.filename,
            temp_path=temp_path,
            size_bytes=size_bytes,
        )

        file_infos.append(
            FileInfo(
                file_id=file_id,
                original_name=upload_file.filename,
                size_bytes=size_bytes,
                uploaded_at=session.files[file_id]["uploaded_at"],
            )
        )

    return UploadResponse(
        files=file_infos,
        session_id=session.session_id,
    )
