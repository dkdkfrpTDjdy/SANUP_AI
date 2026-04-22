from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()
import re
import uuid
import tempfile
import contextlib
import importlib.util
import io
import os
import shutil
import subprocess
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from job_store import JobRecord, JobStore


ROOT_DIR = Path(__file__).resolve().parent
LEGACY_DIR = ROOT_DIR / "legacy"
LEGACY_SCRIPT_A = LEGACY_DIR / "output_debug_dashboard_all_raw_dynamic.py"
LEGACY_SCRIPT_B = LEGACY_DIR / "generate_dashboard_pdf_from_output_excel.py"
DEFAULT_REF_SHEET = "기준정보"


@dataclass
class AppSettings:
    aws_region: str = os.getenv("AWS_REGION", "ap-northeast-2")
    bedrock_model_id: str = os.getenv("BEDROCK_MODEL_ID", "")
    aws_bearer_token_bedrock: str = os.getenv("AWS_BEARER_TOKEN_BEDROCK", "")
    weasyprint_dll_directories: str = os.getenv("WEASYPRINT_DLL_DIRECTORIES", "")
    ref_sheet: str = os.getenv("REF_SHEET", DEFAULT_REF_SHEET)


class LineBufferedLogger(io.TextIOBase):
    def __init__(self, job_store: JobStore, job_id: str, log_file_path: Path) -> None:
        self.job_store = job_store
        self.job_id = job_id
        self.log_file = log_file_path.open("a", encoding="utf-8")
        self._buffer = ""

    def write(self, text: str) -> int:
        if not text:
            return 0
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._emit(line)
        return len(text)

    def flush(self) -> None:
        if self._buffer:
            self._emit(self._buffer)
            self._buffer = ""
        self.log_file.flush()

    def close(self) -> None:
        self.flush()
        self.log_file.close()
        super().close()

    def _emit(self, line: str) -> None:
        cleaned = line.rstrip("\r")
        if cleaned:
            self.job_store.append_log(self.job_id, cleaned)
            self.log_file.write(cleaned + "\n")


def sanitize_filename(filename: str) -> str:
    filename = Path(filename).name
    stem = Path(filename).stem
    stem = re.sub(r"[^A-Za-z0-9._-가-힣]+", "_", stem).strip("._-") or "input"
    suffix = Path(filename).suffix.lower() or ".xlsx"
    return stem + suffix


def make_output_stem(filename: str) -> str:
    stem = Path(filename).stem
    stem = re.sub(r"[^A-Za-z0-9._-가-힣]+", "_", stem).strip("._-") or "dashboard"
    return stem


def load_module_from_path(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"모듈을 불러올 수 없습니다: {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PipelineOrchestrator:
    def __init__(self, job_store: JobStore, settings: Optional[AppSettings] = None) -> None:
        self.job_store = job_store
        self.settings = settings or AppSettings()

    def create_job(self, original_filename: str, file_bytes: bytes) -> JobRecord:
        safe_filename = sanitize_filename(original_filename)
        safe_stem = make_output_stem(safe_filename)
        job_id = uuid.uuid4().hex
        workdir = Path(tempfile.mkdtemp(prefix=f"dashboard-run-{job_id[:8]}-"))
        input_dir = workdir / "input"
        output_dir = workdir / "output"
        logs_dir = workdir / "logs"
        input_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        logs_dir.mkdir(parents=True, exist_ok=True)

        input_path = input_dir / safe_filename
        input_path.write_bytes(file_bytes)

        job = JobRecord(
            job_id=job_id,
            original_filename=original_filename,
            stored_input_path=str(input_path),
            safe_stem=safe_stem,
            workdir=str(workdir),
            status="queued",
            current_step="업로드 완료",
            progress=5,
        )
        self.job_store.add(job)
        self.job_store.append_log(job_id, f"[INFO] 업로드 저장 완료: {input_path}")
        return job

    def run(self, job_id: str) -> None:
        job = self.job_store.require(job_id)
        workdir = Path(job.workdir)
        log_path = workdir / "logs" / "pipeline.log"
        output_excel = workdir / "output" / f"{job.safe_stem}_output.xlsx"
        output_pdf = workdir / "output" / f"{job.safe_stem}_report.pdf"

        logger = LineBufferedLogger(self.job_store, job_id, log_path)
        try:
            self._validate_legacy_scripts()
            self.job_store.update_step(job_id, status="running", current_step="Python 파일 A 실행 중", progress=20)
            self.job_store.append_log(job_id, f"[INFO] legacy A 호출: {LEGACY_SCRIPT_A}")
            self._run_legacy_a(job_id, Path(job.stored_input_path), output_excel, logger)

            if not output_excel.exists():
                raise FileNotFoundError(f"파일 A 실행 후 output Excel이 생성되지 않았습니다: {output_excel}")

            self.job_store.update_step(job_id, status="running", current_step="Python 파일 B 실행 중", progress=65)
            self.job_store.append_log(job_id, f"[INFO] legacy B 호출: {LEGACY_SCRIPT_B}")
            self._run_legacy_b(job_id, output_excel, output_pdf)

            if not output_pdf.exists():
                raise FileNotFoundError(f"파일 B 실행 후 PDF가 생성되지 않았습니다: {output_pdf}")

            self.job_store.mark_completed(job_id, pdf_path=str(output_pdf), excel_path=str(output_excel))
            self.job_store.append_log(job_id, f"[INFO] 완료: PDF={output_pdf}")
        except Exception as exc:
            debug_text = traceback.format_exc()
            self.job_store.append_log(job_id, "[ERROR] 파이프라인 실행 실패")
            self.job_store.append_log(job_id, debug_text)
            self.job_store.mark_failed(
                job_id,
                error_user=self._to_user_error(exc),
                error_debug=debug_text,
            )
        finally:
            logger.close()

    def _validate_legacy_scripts(self) -> None:
        if not LEGACY_SCRIPT_A.exists():
            raise FileNotFoundError(f"legacy script A 없음: {LEGACY_SCRIPT_A}")
        if not LEGACY_SCRIPT_B.exists():
            raise FileNotFoundError(f"legacy script B 없음: {LEGACY_SCRIPT_B}")

    def _run_legacy_a(self, job_id: str, input_excel: Path, output_excel: Path, logger: LineBufferedLogger) -> None:
        module_name = f"legacy_a_{job_id}"
        legacy_a = load_module_from_path(module_name, LEGACY_SCRIPT_A)
        if not hasattr(legacy_a, "run_pipeline_all_raw"):
            raise AttributeError("legacy script A에 run_pipeline_all_raw 함수가 없습니다.")

        with contextlib.redirect_stdout(logger), contextlib.redirect_stderr(logger):
            legacy_a.run_pipeline_all_raw(
                in_path=str(input_excel),
                out_path=str(output_excel),
                ref_sheet=self.settings.ref_sheet,
            )
        logger.flush()

    def _run_legacy_b(self, job_id: str, output_excel: Path, output_pdf: Path) -> None:
        env = os.environ.copy()
        env["AWS_REGION"] = self.settings.aws_region
        if self.settings.bedrock_model_id:
            env["BEDROCK_MODEL_ID"] = self.settings.bedrock_model_id
        if self.settings.aws_bearer_token_bedrock:
            env["AWS_BEARER_TOKEN_BEDROCK"] = self.settings.aws_bearer_token_bedrock
        if self.settings.weasyprint_dll_directories:
            env["WEASYPRINT_DLL_DIRECTORIES"] = self.settings.weasyprint_dll_directories

        cmd = [
            sys.executable,
            str(LEGACY_SCRIPT_B),
            "--output-excel",
            str(output_excel),
            "--output-pdf",
            str(output_pdf),
            "--aws-region",
            self.settings.aws_region,
            "--bedrock-model-id",
            self.settings.bedrock_model_id,
            "--bedrock-api-key",
            self.settings.aws_bearer_token_bedrock,
            "--weasyprint-dll-dir",
            self.settings.weasyprint_dll_directories,
        ]

        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT_DIR),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        assert proc.stdout is not None
        for line in proc.stdout:
            self.job_store.append_log(job_id, line.rstrip("\n"))
        return_code = proc.wait()
        if return_code != 0:
            raise RuntimeError(f"legacy script B 실행 실패 (exit code={return_code})")

    def _to_user_error(self, exc: Exception) -> str:
        message = str(exc).strip()
        if not message:
            return "파이프라인 실행에 실패했습니다. 로그를 확인해 주세요."

        if "필수 컬럼 누락" in message:
            return f"업로드한 Excel 형식이 기존 로직 기대치와 다릅니다. {message}"
        if "정제RAW 시트가 최소 2개" in message:
            return "파일 A 결과에서 정제RAW 시트가 부족하여 PDF를 생성할 수 없습니다."
        if "BEDROCK_MODEL_ID" in message:
            return "Bedrock/Nova 설정값이 없어서 LLM 단계가 실패했습니다. 기존 fallback이 동작하도록 로그를 확인해 주세요."
        return message
