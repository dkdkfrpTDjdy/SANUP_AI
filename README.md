# Excel 1개 업로드 → legacy Python 2개 순차 실행 → PDF 다운로드

이 프로젝트는 **기존 Python 비즈니스 로직 2개를 수정하지 않고**, 그 앞뒤에 최소한의 웹 UI와 백엔드 실행 흐름만 붙인 FastAPI 앱이다.

- 입력: 원본 Excel 1개
- 내부 처리:
  1. `legacy/output_debug_dashboard_all_raw_dynamic.py` 실행
  2. output Excel 생성
  3. `legacy/generate_dashboard_pdf_from_output_excel.py` 실행
  4. 최종 PDF 생성
- 출력: PDF 다운로드, 선택적으로 intermediate Excel 다운로드

## 중요한 배포 원칙

이 프로젝트는 **GitHub Pages로는 배포할 수 없다.**
이유는 Python 백엔드 실행, 파일 업로드 저장, legacy 스크립트 실행, WeasyPrint PDF 생성, AWS Bedrock 호출이 모두 서버 런타임을 필요로 하기 때문이다.

따라서 권장 방식은 아래 둘 중 하나다.

1. **GitHub 저장소에 올린 뒤 로컬에서 Docker Compose로 실행**
2. GitHub 저장소에 올린 뒤 서버/VM에서 Docker 또는 Uvicorn으로 실행

가장 간단한 방식은 **로컬 Docker Compose 실행**이다.

---

## 프로젝트 구조

```text
.
├─ app.py
├─ pipeline_service.py
├─ job_store.py
├─ requirements.txt
├─ Dockerfile
├─ docker-compose.yml
├─ .env.example
├─ legacy/
│  ├─ output_debug_dashboard_all_raw_dynamic.py
│  └─ generate_dashboard_pdf_from_output_excel.py
└─ static/
   ├─ index.html
   ├─ styles.css
   └─ app.js
```

## legacy 호출 방식

### A 호출
- `legacy/output_debug_dashboard_all_raw_dynamic.py`
- 기존 하드코딩 `IN_PATH`, `OUT_PATH`는 사용하지 않음
- wrapper가 `run_pipeline_all_raw(in_path, out_path, ref_sheet)`를 **직접 호출**
- 즉, 계산 로직은 그대로 두고 입출력 경로만 서버에서 주입

### B 호출
- `legacy/generate_dashboard_pdf_from_output_excel.py`
- 기존 CLI를 그대로 사용
- `--output-excel`, `--output-pdf`, `--aws-region`, `--bedrock-model-id`, `--bedrock-api-key`, `--weasyprint-dll-dir`를 전달
- Bedrock/Nova 정상 호출 시 기존 생성 흐름 사용
- 실패 시 기존 fallback 흐름 사용

---

## GitHub에 올리는 방법

### 1. 새 저장소 생성
예시:
- 저장소명: `excel-pdf-dashboard-pipeline`

### 2. 프로젝트 업로드
```bash
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin <YOUR_GITHUB_REPO_URL>
git push -u origin main
```

---

## 로컬에서 가장 쉽게 실행하는 방법: Docker Compose

### 선행 조건
- Docker Desktop 설치
- Git 설치

### 실행 절차

```bash
git clone <YOUR_GITHUB_REPO_URL>
cd excel-pdf-dashboard-pipeline
cp .env.example .env
```

`.env`에 필요한 환경변수를 채운 뒤:

```bash
docker compose up --build
```

브라우저에서 접속:

```text
http://localhost:8000
```

앱이 뜨면 원본 Excel 1개 업로드 후 실행하면 된다.

### 종료
```bash
docker compose down
```

---

## 환경변수

`.env.example`

```env
AWS_REGION=ap-northeast-2
BEDROCK_MODEL_ID=
AWS_BEARER_TOKEN_BEDROCK=
WEASYPRINT_DLL_DIRECTORIES=
REF_SHEET=기준정보
```

### 설명
- `AWS_REGION`: Bedrock 리전
- `BEDROCK_MODEL_ID`: Nova 모델 ID
- `AWS_BEARER_TOKEN_BEDROCK`: Bedrock 인증 토큰
- `WEASYPRINT_DLL_DIRECTORIES`: Windows 네이티브 DLL 경로가 필요한 경우만 사용. Docker에서는 일반적으로 비워둠
- `REF_SHEET`: 기준정보 시트명

---

## 로컬 Python으로 직접 실행하는 방법

Docker를 쓰지 않으려면 아래처럼 실행할 수 있다.

### macOS / Linux
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

### Windows
```bat
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

브라우저에서 `http://localhost:8000` 접속.

---

## 실행 흐름

1. 사용자가 `.xlsx` 원본 파일 1개 업로드
2. 서버가 임시 작업 디렉터리 생성
3. legacy A 실행
4. output Excel 생성 확인
5. legacy B 실행
6. PDF 생성 확인
7. UI에서 PDF 다운로드 제공
8. 필요 시 Excel 다운로드 제공

---

## 에러 처리

- 사용자용 오류: `error_user`
- 내부 디버깅용 오류: `error_debug`
- 작업별 로그: 화면 로그 패널 + 임시 작업 폴더의 `logs/pipeline.log`

대표적인 실패 원인:
- 업로드한 Excel에 필수 컬럼 누락
- `기준정보` 또는 `YYMM RAW` 시트 형식 불일치
- Bedrock 설정 누락
- WeasyPrint 시스템 라이브러리 누락

Docker 방식은 WeasyPrint 의존성 문제를 줄이는 데 유리하다.

SANUP_AI indexing test 2026-04-28

---

## 운영 메모

- GitHub는 **코드 저장소** 용도로 사용
- 실제 실행은 로컬 Docker 또는 서버 Docker에서 수행
- Bedrock/Nova 키는 절대 코드에 하드코딩하지 말고 `.env` 또는 서버 환경변수로 주입
- legacy 계산 로직은 수정하지 말고 wrapper/backend에서만 호출
