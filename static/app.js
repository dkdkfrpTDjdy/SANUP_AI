const fileInput = document.getElementById('fileInput');
const runButton = document.getElementById('runButton');
const statusText = document.getElementById('statusText');
const stepText = document.getElementById('stepText');
const progressBar = document.getElementById('progressBar');
const logBox = document.getElementById('logBox');
const pdfLink = document.getElementById('pdfLink');
const excelLink = document.getElementById('excelLink');

let pollTimer = null;
let currentJobId = null;

function setState({ status, step, progress, logs, errorUser }) {
  statusText.textContent = status || '-';
  stepText.textContent = step || '-';
  progressBar.style.width = `${Math.max(0, Math.min(100, progress || 0))}%`;
  const body = Array.isArray(logs) && logs.length ? logs.join('\n') : '로그가 여기에 표시됩니다.';
  logBox.textContent = errorUser ? `${body}\n\n[사용자용 오류]\n${errorUser}` : body;
  logBox.scrollTop = logBox.scrollHeight;
}

function setDownloadLinks(jobId, show) {
  if (!show) {
    pdfLink.classList.add('hidden');
    excelLink.classList.add('hidden');
    pdfLink.removeAttribute('href');
    excelLink.removeAttribute('href');
    return;
  }
  pdfLink.href = `/api/jobs/${jobId}/download/pdf`;
  excelLink.href = `/api/jobs/${jobId}/download/excel`;
  pdfLink.classList.remove('hidden');
  excelLink.classList.remove('hidden');
}

async function pollJob(jobId) {
  try {
    const response = await fetch(`/api/jobs/${jobId}`);
    if (!response.ok) throw new Error('작업 상태 조회 실패');
    const job = await response.json();
    setState({
      status: job.status,
      step: job.current_step,
      progress: job.progress,
      logs: job.logs,
      errorUser: job.error_user,
    });

    if (job.status === 'completed') {
      clearInterval(pollTimer);
      pollTimer = null;
      runButton.disabled = false;
      setDownloadLinks(jobId, true);
    } else if (job.status === 'failed') {
      clearInterval(pollTimer);
      pollTimer = null;
      runButton.disabled = false;
      setDownloadLinks(jobId, false);
    }
  } catch (error) {
    clearInterval(pollTimer);
    pollTimer = null;
    runButton.disabled = false;
    statusText.textContent = 'failed';
    stepText.textContent = '상태 조회 실패';
    logBox.textContent += `\n\n[CLIENT ERROR] ${error.message}`;
  }
}

runButton.addEventListener('click', async () => {
  const file = fileInput.files[0];
  if (!file) {
    alert('Excel 파일을 선택하세요.');
    return;
  }

  runButton.disabled = true;
  setDownloadLinks('', false);
  setState({ status: 'uploading', step: '업로드 중', progress: 5, logs: ['[INFO] 업로드 시작'] });

  const formData = new FormData();
  formData.append('file', file);

  try {
    const response = await fetch('/api/pipeline', {
      method: 'POST',
      body: formData,
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || '업로드 실패');

    currentJobId = payload.job_id;
    setState({ status: 'queued', step: '작업 대기', progress: 10, logs: ['[INFO] 업로드 완료'] });
    pollTimer = setInterval(() => pollJob(currentJobId), 1500);
    await pollJob(currentJobId);
  } catch (error) {
    runButton.disabled = false;
    setState({ status: 'failed', step: '업로드 실패', progress: 0, logs: [`[ERROR] ${error.message}`] });
  }
});
