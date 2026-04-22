from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Dict, List, Optional


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class JobRecord:
    job_id: str
    original_filename: str
    stored_input_path: str
    safe_stem: str
    workdir: str
    status: str = "queued"
    current_step: str = "업로드 대기"
    progress: int = 0
    logs: List[str] = field(default_factory=list)
    pdf_path: Optional[str] = None
    excel_path: Optional[str] = None
    error_user: Optional[str] = None
    error_debug: Optional[str] = None
    created_at: str = field(default_factory=utcnow_iso)
    updated_at: str = field(default_factory=utcnow_iso)

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "original_filename": self.original_filename,
            "status": self.status,
            "current_step": self.current_step,
            "progress": self.progress,
            "logs": self.logs[-500:],
            "has_pdf": bool(self.pdf_path),
            "has_excel": bool(self.excel_path),
            "error_user": self.error_user,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class JobStore:
    def __init__(self) -> None:
        self._lock = Lock()
        self._jobs: Dict[str, JobRecord] = {}

    def add(self, job: JobRecord) -> None:
        with self._lock:
            self._jobs[job.job_id] = job

    def get(self, job_id: str) -> Optional[JobRecord]:
        with self._lock:
            return self._jobs.get(job_id)

    def require(self, job_id: str) -> JobRecord:
        job = self.get(job_id)
        if job is None:
            raise KeyError(job_id)
        return job

    def append_log(self, job_id: str, message: str) -> None:
        cleaned = (message or "").rstrip("\n")
        if not cleaned:
            return
        with self._lock:
            job = self._jobs[job_id]
            for line in cleaned.splitlines():
                job.logs.append(line)
            job.updated_at = utcnow_iso()

    def update_step(self, job_id: str, *, status: str, current_step: str, progress: int) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.status = status
            job.current_step = current_step
            job.progress = progress
            job.updated_at = utcnow_iso()

    def mark_completed(self, job_id: str, *, pdf_path: str, excel_path: str) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.status = "completed"
            job.current_step = "완료"
            job.progress = 100
            job.pdf_path = pdf_path
            job.excel_path = excel_path
            job.updated_at = utcnow_iso()

    def mark_failed(self, job_id: str, *, error_user: str, error_debug: str) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.status = "failed"
            job.current_step = "실패"
            job.error_user = error_user
            job.error_debug = error_debug
            job.updated_at = utcnow_iso()
