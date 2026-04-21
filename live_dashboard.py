"""Tiny live-status HTTP dashboard for the autopilot run.

Launch:
    python live_dashboard.py --workdir out/autopilot_2axis --port 8765

Then open http://localhost:8765/ in a browser. The page auto-refreshes the
status block, per-iteration metrics table, and the last N scoring rows
streamed into `run.log` every 2 seconds.

Pure stdlib, no external deps, no build step.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

WORKDIR = Path(".")  # set at startup
RUN_LOG_TAIL_BYTES = 120_000
ROW_REGEX = re.compile(r"^\|\s*\d")

def count_csv_rows(path: Path) -> int | None:
    if not path.exists():
        return None
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        return max(0, sum(1 for _ in csv.reader(f)) - 1)

def read_jsonl_rows(path: Path, limit: int = 10) -> list[dict]:
    if not path.exists():
        return []
    out: list[dict] = []
    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out[-limit:]

def format_recent_score_rows(limit: int = 10) -> list[str]:
    progress_path = WORKDIR / "02_score" / "scores_progress.jsonl"
    rows = read_jsonl_rows(progress_path, limit=limit)
    if not rows:
        return []
    out: list[str] = []
    for i, row in enumerate(rows, start=max(1, len(rows) - limit + 1)):
        name = row.get("Full Name", "")
        company = row.get("Current Company", "")
        band = row.get("score_band", "")
        company_fit = row.get("company_fit", "")
        role_fit = row.get("role_fit", "")
        ft_total = row.get("ft_total", row.get("lead_score", ""))
        out.append(
            f"{i:>2}. {name} | {company} | total={ft_total} | company_fit={company_fit} | role_fit={role_fit} | {band}"
        )
    return out

def read_status() -> dict:
    p = WORKDIR / "autopilot_status.json"
    if not p.exists():
        prepared_rows = count_csv_rows(WORKDIR / "01_prepare" / "prepared_scoring_input.csv")
        scored_rows = count_csv_rows(WORKDIR / "02_score" / "scores_raw.csv")
        progress_rows = count_csv_rows(WORKDIR / "02_score" / "scores_progress.jsonl")
        processed_rows = scored_rows if scored_rows is not None else progress_rows
        phase = "running"
        if prepared_rows is None:
            phase = "waiting_for_input"
        elif processed_rows == prepared_rows:
            phase = "complete"
        elif processed_rows in (None, 0):
            phase = "starting"
        return {
            "mode": "single_pass_scoring",
            "phase": phase,
            "processed_rows": processed_rows or 0,
            "total_rows": prepared_rows or 0,
            "scoring_model": "n/a",
            "rubric_model": "n/a",
        }
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        return {"error": repr(e)}

def read_iter_metrics() -> list[dict]:
    rows: list[dict] = []
    for d in sorted(WORKDIR.glob("autopilot_iter_*")):
        m = d / "autopilot_metrics.csv"
        if not m.exists():
            continue
        try:
            import csv
            with m.open() as f:
                reader = csv.DictReader(f)
                for item in reader:
                    item = {k: (float(v) if v and v.replace(".", "", 1).replace("-", "", 1).isdigit() else v) for k, v in item.items()}
                    item["iteration_dir"] = d.name
                    rows.append(item)
                    break
        except Exception as e:
            rows.append({"iteration_dir": d.name, "error": repr(e)})
    return rows

def read_recent_rows(limit: int = 30) -> list[str]:
    if (WORKDIR / "02_score" / "scores_progress.jsonl").exists():
        return format_recent_score_rows(limit=min(limit, 10))
    p = WORKDIR / "run.log"
    if not p.exists():
        return []
    size = p.stat().st_size
    with p.open("rb") as f:
        if size > RUN_LOG_TAIL_BYTES:
            f.seek(-RUN_LOG_TAIL_BYTES, 2)
            f.readline()  # skip partial line
        blob = f.read().decode("utf-8", errors="replace")
    out: list[str] = []
    for line in blob.splitlines():
        if ROW_REGEX.match(line):
            out.append(line)
    return out[-limit:]

def latest_errors(limit: int = 10) -> list[str]:
    p = WORKDIR / "run.log"
    if not p.exists():
        return []
    size = p.stat().st_size
    with p.open("rb") as f:
        if size > RUN_LOG_TAIL_BYTES:
            f.seek(-RUN_LOG_TAIL_BYTES, 2)
            f.readline()
        blob = f.read().decode("utf-8", errors="replace")
    keywords = ("Traceback", "HTTPError", "rubric-model HTTP", "Failed batch", "Retrying", "Resume:", "STARTING NEW SESSION")
    out: list[str] = []
    for line in blob.splitlines():
        if any(k in line for k in keywords):
            out.append(line)
    return out[-limit:]

HTML = """<!doctype html>
<html lang="en" class="dark">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SCORie Live Dashboard</title>
<script src="https://cdn.tailwindcss.com"></script>
<script>
  tailwind.config = {
    darkMode: 'class',
    theme: {
      extend: {
        colors: {
          gray: {
            800: '#1c2128',
            900: '#0e1116',
          }
        }
      }
    }
  }
</script>
<style>
  body { font-family: ui-monospace, Menlo, Consolas, monospace; background: #0e1116; color: #cdd6dc; }
  .table-row-best { background-color: #16251a !important; }
  .over-target { color: #ff6b6b !important; }
  .under-target { color: #7ee787 !important; }
  .text-muted { color: #8b949e; }
  .bg-card { background: #1c2128; border: 1px solid #30363d; }
  .border-b-subtle { border-bottom: 1px solid #30363d; }
</style>
</head>
<body class="p-6 max-w-7xl mx-auto">
  <div class="flex justify-between items-center mb-6">
    <h1 class="text-2xl font-bold text-gray-100">SCORie Live Dashboard</h1>
    <div class="text-xs text-muted" id="updated">loading…</div>
  </div>

  <div class="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
    <!-- Main KPIs -->
    <div class="lg:col-span-2">
      <h2 class="text-sm font-semibold text-blue-400 uppercase tracking-wider mb-3">KPIs</h2>
      <div id="kpis" class="grid grid-cols-2 sm:grid-cols-4 gap-3"></div>
    </div>

    <!-- Upload Section -->
    <div class="bg-card rounded-lg p-4">
      <h2 class="text-sm font-semibold text-blue-400 uppercase tracking-wider mb-3">LinkedHelper Backfill</h2>
      <p class="text-xs text-muted mb-3">Provide a Google Drive link or upload a CSV file to add company sources.</p>

      <form id="uploadForm" class="space-y-3" onsubmit="submitLinkedHelper(event)">
        <div>
            <label class="block text-xs font-medium text-gray-300 mb-1">Google Drive Link</label>
            <input type="text" id="gdriveLink" name="gdrive_link" class="w-full bg-gray-900 border border-gray-700 rounded p-2 text-xs text-gray-200 focus:outline-none focus:border-blue-500" placeholder="https://drive.google.com/file/d/...">
        </div>
        <div class="text-xs text-center text-muted">- OR -</div>
        <div>
            <label class="block text-xs font-medium text-gray-300 mb-1">CSV File</label>
            <input type="file" id="csvFile" name="csv_file" accept=".csv" class="w-full text-xs text-gray-300 file:mr-4 file:py-1 file:px-2 file:rounded file:border-0 file:text-xs file:bg-gray-700 file:text-gray-200 hover:file:bg-gray-600 cursor-pointer">
        </div>
        <button type="submit" id="submitBtn" class="w-full bg-blue-600 hover:bg-blue-700 text-white text-xs font-medium py-2 px-4 rounded transition">Submit</button>
        <div id="uploadStatus" class="text-xs mt-2 hidden"></div>
      </form>
    </div>
  </div>

  <div class="mb-8">
    <h2 class="text-sm font-semibold text-blue-400 uppercase tracking-wider mb-3">Status</h2>
    <div class="bg-card rounded-lg p-4 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-x-4 gap-y-2 text-sm" id="status"></div>
  </div>

  <div class="mb-8 overflow-x-auto">
    <h2 class="text-sm font-semibold text-blue-400 uppercase tracking-wider mb-3">Per-iteration Metrics</h2>
    <div class="bg-card rounded-lg overflow-hidden border border-gray-700">
      <table class="w-full text-left text-sm" id="iters">
        <thead class="bg-gray-900 text-muted">
          <tr>
            <th class="px-4 py-3 font-medium border-b-subtle">Iter</th>
            <th class="px-4 py-3 font-medium border-b-subtle">Match</th>
            <th class="px-4 py-3 font-medium border-b-subtle">FP Share</th>
            <th class="px-4 py-3 font-medium border-b-subtle">FN Share</th>
            <th class="px-4 py-3 font-medium border-b-subtle">Sum</th>
            <th class="px-4 py-3 font-medium border-b-subtle">Separation</th>
            <th class="px-4 py-3 font-medium border-b-subtle">Combined Error</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-gray-800"></tbody>
      </table>
    </div>
  </div>

  <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
    <div>
      <h2 class="text-sm font-semibold text-blue-400 uppercase tracking-wider mb-3">Last 10 Rows</h2>
      <pre id="rows" class="bg-card rounded-lg p-4 text-xs text-blue-300 overflow-x-auto h-64 border border-gray-700 whitespace-pre"></pre>
    </div>
    <div>
      <h2 class="text-sm font-semibold text-blue-400 uppercase tracking-wider mb-3">Recent Events</h2>
      <pre id="events" class="bg-card rounded-lg p-4 text-xs text-blue-300 overflow-x-auto h-64 border border-gray-700 whitespace-pre"></pre>
    </div>
  </div>

<script>
const fmtPct = (v) => (v == null || isNaN(v)) ? '—' : (100*v).toFixed(1) + '%';
const cls = (val, thresh, over) => (val == null || thresh == null) ? '' : ((over ? val > thresh : val < thresh) ? 'over-target' : 'under-target');
const fmtCount = (a, b) => `${a ?? '—'} of ${b ?? '—'}`;

async function refresh() {
  try {
    const r = await fetch('/api/all');
    const d = await r.json();
    const s = d.status || {};
    document.getElementById('updated').textContent = 'Updated ' + new Date().toLocaleTimeString();

    // KPIs
    const bm = s.best_metrics || {};
    const cm = s.current_metrics || {};
    const bam = s.baseline_metrics || {};
    const tfp = s.target_fp, tfn = s.target_fn;

    const kpiData = [
      { label: 'Phase', value: s.phase || '—' },
      { label: 'Progress', value: fmtCount(s.processed_rows ?? s.processed, s.total_rows) },
      { label: 'Mode', value: s.mode || ((d.iters || []).length ? 'autopilot' : 'single-pass') },
      { label: 'Best Match', value: fmtPct(bm.match_rate), color: 'under-target' },
      { label: 'Best FP', value: fmtPct(bm.fp_share), color: cls(bm.fp_share, tfp, true) },
      { label: 'Best FN', value: fmtPct(bm.fn_share), color: cls(bm.fn_share, tfn, true) },
      { label: 'Target FP', value: fmtPct(tfp) },
      { label: 'Target FN', value: fmtPct(tfn) }
    ];

    document.getElementById('kpis').innerHTML = kpiData.map(k => `
      <div class="bg-card rounded-lg p-3 flex flex-col justify-center border border-gray-700">
        <div class="text-[10px] text-muted uppercase tracking-wider mb-1">${k.label}</div>
        <div class="text-lg font-bold text-gray-200 ${k.color || ''}">${k.value}</div>
      </div>
    `).join('');

    // Status
    const statusData = [
      { k: 'Mode', v: s.mode || 'autopilot' },
      { k: 'Scoring Model', v: s.scoring_model },
      { k: 'Rubric Model', v: s.rubric_model },
      { k: 'Best Version', v: s.best_version },
      { k: 'Current Version', v: s.rubric_version },
      { k: 'Processed', v: fmtCount(s.processed_rows ?? s.processed, s.total_rows) },
      { k: 'Baseline Match', v: fmtPct(bam.match_rate) },
      { k: 'Baseline FP', v: fmtPct(bam.fp_share) },
      { k: 'Baseline FN', v: fmtPct(bam.fn_share) },
      { k: 'Current Match', v: fmtPct(cm.match_rate) },
      { k: 'Current FP', v: fmtPct(cm.fp_share) },
      { k: 'Current FN', v: fmtPct(cm.fn_share) },
    ];

    document.getElementById('status').innerHTML = statusData.map(r => `
      <div class="flex flex-col py-1 border-b border-gray-800 last:border-0 sm:border-0">
        <span class="text-muted text-xs">${r.k}</span>
        <span class="text-gray-200">${r.v ?? '—'}</span>
      </div>
    `).join('');

    // Iter metrics
    const bestIter = (d.iters || []).reduce((b, r) => (b == null || (r.combined_error != null && r.combined_error < b.combined_error) ? r : b), null);

    let tbodyHtml = '';
    if (!d.iters || d.iters.length === 0) {
      tbodyHtml = '<tr><td colspan="7" class="px-4 py-3 text-center text-muted italic">Not an autopilot run</td></tr>';
    } else {
      tbodyHtml = d.iters.map(r => {
        const isBest = bestIter && r.iteration_dir === bestIter.iteration_dir;
        const rowClass = isBest ? 'table-row-best' : 'hover:bg-gray-800 transition-colors';
        return `
          <tr class="${rowClass}">
            <td class="px-4 py-2 border-b border-gray-800 text-gray-300">${r.iteration_dir || ''}</td>
            <td class="px-4 py-2 border-b border-gray-800 text-gray-300">${fmtPct(r.match_rate)}</td>
            <td class="px-4 py-2 border-b border-gray-800 ${cls(r.fp_share, tfp, true)}">${fmtPct(r.fp_share)}</td>
            <td class="px-4 py-2 border-b border-gray-800 ${cls(r.fn_share, tfn, true)}">${fmtPct(r.fn_share)}</td>
            <td class="px-4 py-2 border-b border-gray-800 text-gray-300">${fmtPct((r.match_rate||0)+(r.fp_share||0)+(r.fn_share||0))}</td>
            <td class="px-4 py-2 border-b border-gray-800 text-gray-300">${r.separation != null ? Number(r.separation).toFixed(2) : '—'}</td>
            <td class="px-4 py-2 border-b border-gray-800 text-gray-300">${r.combined_error != null ? Number(r.combined_error).toFixed(4) : '—'}</td>
          </tr>
        `;
      }).join('');
    }
    document.querySelector('#iters tbody').innerHTML = tbodyHtml;

    document.getElementById('rows').textContent = (d.rows || []).join('\\n') || '(no rows yet)';
    document.getElementById('events').textContent = (d.events || []).join('\\n') || '(no events)';
  } catch (e) {
    document.getElementById('updated').textContent = 'Refresh failed: ' + e;
  }
}

async function submitLinkedHelper(e) {
  e.preventDefault();
  const form = e.target;
  const statusEl = document.getElementById('uploadStatus');
  const btn = document.getElementById('submitBtn');

  const gdriveLink = document.getElementById('gdriveLink').value.trim();
  const fileInput = document.getElementById('csvFile');
  const file = fileInput.files[0];

  if (!gdriveLink && !file) {
    statusEl.textContent = 'Please provide a Google Drive link or select a CSV file.';
    statusEl.className = 'text-xs mt-2 text-red-400 block';
    return;
  }

  statusEl.textContent = 'Uploading...';
  statusEl.className = 'text-xs mt-2 text-blue-400 block';
  btn.disabled = true;
  btn.classList.add('opacity-50', 'cursor-not-allowed');

  try {
    const formData = new FormData();
    if (gdriveLink) {
      formData.append('gdrive_link', gdriveLink);
    }
    if (file) {
      formData.append('csv_file', file);
    }

    const response = await fetch('/api/upload_linkedhelper', {
      method: 'POST',
      body: formData
    });

    const result = await response.json();

    if (response.ok) {
      statusEl.textContent = result.message || 'Upload successful!';
      statusEl.className = 'text-xs mt-2 text-green-400 block';
      form.reset();
    } else {
      statusEl.textContent = 'Error: ' + (result.error || 'Upload failed');
      statusEl.className = 'text-xs mt-2 text-red-400 block';
    }
  } catch (err) {
    statusEl.textContent = 'Error: ' + err.message;
    statusEl.className = 'text-xs mt-2 text-red-400 block';
  } finally {
    btn.disabled = false;
    btn.classList.remove('opacity-50', 'cursor-not-allowed');
    setTimeout(() => {
      if(statusEl.className.includes('green')) statusEl.classList.add('hidden');
    }, 5000);
  }
}

refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>"""

def extract_gdrive_id(url: str) -> str | None:
    match = re.search(r'/d/([a-zA-Z0-9_-]+)', url)
    if not match:
        match = re.search(r'id=([a-zA-Z0-9_-]+)', url)
    return match.group(1) if match else None

def download_gdrive_csv(file_id: str, dest_path: Path) -> None:
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    req = urllib.request.Request(download_url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req) as response:
        with dest_path.open('wb') as out_file:
            out_file.write(response.read())


def parse_multipart_data(content_type: str, body: bytes) -> tuple[dict, dict]:
    match = re.search(r'boundary=([^;]+)', content_type)
    if not match:
        raise ValueError("No boundary found in content type")

    boundary = match.group(1).encode()
    parts = body.split(b'--' + boundary)

    files = {}
    fields = {}

    for part in parts:
        if not part.strip() or part == b'--\r\n':
            continue

        try:
            headers_part, content = part.split(b'\r\n\r\n', 1)
            content = content[:-2] # Remove trailing \r\n

            headers = headers_part.decode('utf-8')
            disposition_match = re.search(r'Content-Disposition: form-data; (.*?)\r\n', headers + '\r\n')

            if disposition_match:
                disposition = disposition_match.group(1)
                name_match = re.search(r'name="([^"]+)"', disposition)
                filename_match = re.search(r'filename="([^"]+)"', disposition)

                if name_match:
                    name = name_match.group(1)
                    if filename_match:
                        filename = filename_match.group(1)
                        if filename: # Don't save empty file inputs
                            files[name] = {"filename": filename, "content": content}
                    else:
                        fields[name] = content.decode('utf-8').strip()
        except Exception:
            pass

    return fields, files

class Handler(BaseHTTPRequestHandler):

    def log_message(self, *a, **kw):
        return

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/index"):
            body = HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == "/api/all":
            payload = {
                "status": read_status(),
                "iters": read_iter_metrics(),
                "rows": read_recent_rows(),
                "events": latest_errors(),
            }
            body = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        if self.path == "/api/upload_linkedhelper":
            content_type = self.headers.get('Content-Type', '')
            try:
                # Ensure the company sources directory exists
                sources_dir = Path("data/company_sources")
                sources_dir.mkdir(parents=True, exist_ok=True)

                if content_type.startswith('multipart/form-data'):
                    content_length = int(self.headers.get('Content-Length', '0'))
                    body = self.rfile.read(content_length)
                    fields, files = parse_multipart_data(content_type, body)

                    saved_files = []

                    # Handle File Upload
                    if 'csv_file' in files:
                        file_item = files['csv_file']
                        filename = file_item['filename']
                        if not filename.endswith('.csv'):
                            filename += '.csv'

                        # Generate unique filename
                        import time
                        ts = int(time.time())
                        safe_name = f"upload_{ts}_{re.sub(r'[^a-zA-Z0-9_.-]', '_', filename)}"
                        dest_path = sources_dir / safe_name

                        with dest_path.open('wb') as f:
                            f.write(file_item['content'])

                        saved_files.append(safe_name)

                    # Handle GDrive Link
                    if 'gdrive_link' in fields and fields['gdrive_link']:
                        gdrive_url = fields['gdrive_link'].strip()
                        file_id = extract_gdrive_id(gdrive_url)

                        if not file_id:
                            self._send_json_response(400, {"error": "Invalid Google Drive link"})
                            return

                        import time
                        ts = int(time.time())
                        dest_path = sources_dir / f"gdrive_{ts}.csv"

                        try:
                            download_gdrive_csv(file_id, dest_path)
                            saved_files.append(dest_path.name)
                        except Exception as e:
                            self._send_json_response(500, {"error": f"Failed to download from Google Drive: {str(e)}"})
                            return

                    if not saved_files:
                        self._send_json_response(400, {"error": "No file or valid link provided"})
                        return

                    self._send_json_response(200, {
                        "message": f"Successfully processed {len(saved_files)} file(s)",
                        "files": saved_files
                    })
                    return

                # Handle JSON Payload (Webhook)
                elif content_type == 'application/json':
                    content_length = int(self.headers.get('Content-Length', 0))
                    post_data = self.rfile.read(content_length)
                    data = json.loads(post_data.decode('utf-8'))

                    gdrive_url = data.get('gdrive_link')
                    if gdrive_url:
                        file_id = extract_gdrive_id(gdrive_url)
                        if not file_id:
                            self._send_json_response(400, {"error": "Invalid Google Drive link"})
                            return

                        import time
                        ts = int(time.time())
                        dest_path = sources_dir / f"webhook_gdrive_{ts}.csv"

                        try:
                            download_gdrive_csv(file_id, dest_path)
                            self._send_json_response(200, {"message": "Successfully downloaded CSV from Google Drive link"})
                        except Exception as e:
                            self._send_json_response(500, {"error": f"Failed to download: {str(e)}"})
                        return

                    self._send_json_response(400, {"error": "JSON payload must contain 'gdrive_link'"})
                    return

                else:
                    self._send_json_response(415, {"error": "Unsupported Media Type"})
                    return

            except Exception as e:
                import traceback
                traceback.print_exc()
                self._send_json_response(500, {"error": str(e)})
                return

        self.send_response(404)
        self.end_headers()

    def _send_json_response(self, status_code: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

def main() -> None:
    global WORKDIR
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--port", type=int, default=8765)
    args = ap.parse_args()
    WORKDIR = Path(args.workdir)
    if not WORKDIR.exists():
        raise SystemExit(f"workdir not found: {WORKDIR}")
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"serving http://127.0.0.1:{args.port}/ (workdir={WORKDIR})", flush=True)
    srv.serve_forever()

if __name__ == "__main__":
    main()
