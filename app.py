from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

from pathlib import Path
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from job_store import JobStore
from pipeline_service import PipelineOrchestrator

app = FastAPI(title="Excel to PDF Pipeline")
job_store = JobStore()
orchestrator = PipelineOrchestrator(job_store)


@app.post("/api/pipeline")
async def create_pipeline(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="파일명이 없습니다.")
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail=".xlsx 파일만 업로드할 수 있습니다.")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="업로드한 파일이 비어 있습니다.")

    job = orchestrator.create_job(file.filename, content)
    background_tasks.add_task(orchestrator.run, job.job_id)
    return {"job_id": job.job_id, "status": job.status}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    job = job_store.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="작업을 찾을 수 없습니다.")
    return job.to_dict()


@app.get("/api/jobs/{job_id}/download/pdf")
def download_pdf(job_id: str):
    job = job_store.get(job_id)
    if job is None or not job.pdf_path:
        raise HTTPException(status_code=404, detail="PDF 결과가 없습니다.")
    pdf_path = Path(job.pdf_path)
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="PDF 파일이 존재하지 않습니다.")
    return FileResponse(pdf_path, media_type="application/pdf", filename=pdf_path.name)


@app.get("/api/jobs/{job_id}/download/excel")
def download_excel(job_id: str):
    job = job_store.get(job_id)
    if job is None or not job.excel_path:
        raise HTTPException(status_code=404, detail="Excel 결과가 없습니다.")
    excel_path = Path(job.excel_path)
    if not excel_path.exists():
        raise HTTPException(status_code=404, detail="Excel 파일이 존재하지 않습니다.")
    return FileResponse(
        excel_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=excel_path.name,
    )


STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
