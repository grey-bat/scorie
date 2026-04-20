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
<html>
<head>
<meta charset="utf-8">
<title>SCORie live status</title>
<style>
  body { font-family: ui-monospace, Menlo, Consolas, monospace; margin: 24px; background: #0e1116; color: #cdd6dc; }
  h1 { color: #f0f0f0; margin: 0 0 8px 0; }
  h2 { color: #82aaff; margin: 20px 0 8px 0; font-size: 14px; text-transform: uppercase; letter-spacing: 1px; }
  .status { display: grid; grid-template-columns: 180px auto; gap: 4px 16px; font-size: 13px; }
  .status .k { color: #8b949e; }
  .status .v { color: #e8ecf0; }
  .kpi { display: inline-block; margin: 0 12px 8px 0; padding: 6px 12px; background: #1c2128; border-radius: 6px; border: 1px solid #30363d; }
  .kpi .label { color: #8b949e; font-size: 10px; text-transform: uppercase; }
  .kpi .value { color: #e8ecf0; font-size: 18px; font-weight: bold; }
  .over-target { color: #ff6b6b !important; }
  .under-target { color: #7ee787 !important; }
  table { border-collapse: collapse; font-size: 12px; width: 100%; }
  th { text-align: left; color: #8b949e; padding: 4px 8px; border-bottom: 1px solid #30363d; position: sticky; top: 0; background: #0e1116; }
  td { padding: 3px 8px; border-bottom: 1px solid #1c2128; }
  tr.GOOD td.manual { color: #7ee787; }
  tr.SKIP td.manual { color: #ffa657; }
  tr.best { background: #16251a; }
  pre { background: #1c2128; padding: 12px; border-radius: 6px; overflow-x: auto; font-size: 11px; color: #a5d6ff; }
  .muted { color: #6e7681; font-size: 11px; }
</style>
</head>
<body>
  <h1>SCORie — live status</h1>
  <div class="muted" id="updated">loading…</div>

  <h2>kpis</h2>
  <div id="kpis"></div>

  <h2>status</h2>
  <div class="status" id="status"></div>

  <h2>per-iteration metrics</h2>
  <table id="iters"><thead><tr>
    <th>iter</th><th>match</th><th>fp_share</th><th>fn_share</th><th>sum</th><th>separation</th><th>combined_error</th>
  </tr></thead><tbody></tbody></table>

  <h2>last 10 rows</h2>
  <pre id="rows"></pre>

  <h2>recent events</h2>
  <pre id="events"></pre>

<script>
const fmtPct = (v) => (v == null || isNaN(v)) ? '—' : (100*v).toFixed(1) + '%';
const cls = (val, thresh, over) => (val == null || thresh == null) ? '' : ((over ? val > thresh : val < thresh) ? 'over-target' : 'under-target');
const fmtCount = (a, b) => `${a ?? '—'} of ${b ?? '—'}`;

async function refresh() {
  try {
    const r = await fetch('/api/all');
    const d = await r.json();
    const s = d.status || {};
    document.getElementById('updated').textContent = 'updated ' + new Date().toLocaleTimeString();

    // KPIs
    const bm = s.best_metrics || {};
    const cm = s.current_metrics || {};
    const bam = s.baseline_metrics || {};
    const tfp = s.target_fp, tfn = s.target_fn;
    const kpis = [
      ['phase', s.phase || '—'],
      ['progress', fmtCount(s.processed_rows ?? s.processed, s.total_rows)],
      ['mode', s.mode || (d.iters || []).length ? 'autopilot' : 'single-pass'],
      ['best match', fmtPct(bm.match_rate), 'under-target'],
      ['best fp', fmtPct(bm.fp_share), cls(bm.fp_share, tfp, true)],
      ['best fn', fmtPct(bm.fn_share), cls(bm.fn_share, tfn, true)],
      ['target fp', fmtPct(tfp)],
      ['target fn', fmtPct(tfn)],
    ];
    document.getElementById('kpis').innerHTML = kpis.map(k =>
      `<span class="kpi"><div class="label">${k[0]}</div><div class="value ${k[2]||''}">${k[1]}</div></span>`
    ).join('');

    // Status
    const rows = [
      ['mode', s.mode || 'autopilot'],
      ['scoring model', s.scoring_model],
      ['rubric model', s.rubric_model],
      ['best version', s.best_version],
      ['current version', s.rubric_version],
      ['processed', fmtCount(s.processed_rows ?? s.processed, s.total_rows)],
      ['baseline match', fmtPct(bam.match_rate)],
      ['baseline fp', fmtPct(bam.fp_share)],
      ['baseline fn', fmtPct(bam.fn_share)],
      ['current match', fmtPct(cm.match_rate)],
      ['current fp', fmtPct(cm.fp_share)],
      ['current fn', fmtPct(cm.fn_share)],
    ];
    document.getElementById('status').innerHTML = rows.map(r =>
      `<div class="k">${r[0]}</div><div class="v">${r[1] ?? '—'}</div>`
    ).join('');

    // Iter metrics
    const bestIter = (d.iters || []).reduce((b, r) => (b == null || (r.combined_error != null && r.combined_error < b.combined_error) ? r : b), null);
    const body = (d.iters || []).map(r => {
      const isBest = bestIter && r.iteration_dir === bestIter.iteration_dir;
      return `<tr class="${isBest ? 'best' : ''}"><td>${r.iteration_dir || ''}</td>
        <td>${fmtPct(r.match_rate)}</td>
        <td class="${cls(r.fp_share, tfp, true)}">${fmtPct(r.fp_share)}</td>
        <td class="${cls(r.fn_share, tfn, true)}">${fmtPct(r.fn_share)}</td>
        <td>${fmtPct((r.match_rate||0)+(r.fp_share||0)+(r.fn_share||0))}</td>
        <td>${r.separation != null ? Number(r.separation).toFixed(2) : '—'}</td>
        <td>${r.combined_error != null ? Number(r.combined_error).toFixed(4) : '—'}</td>
      </tr>`;
    }).join('');
    document.querySelector('#iters tbody').innerHTML = body || '<tr><td colspan=7 class="muted">not an autopilot run</td></tr>';

    document.getElementById('rows').textContent = (d.rows || []).join('\\n') || '(no rows yet)';
    document.getElementById('events').textContent = (d.events || []).join('\\n') || '(no events)';
  } catch (e) {
    document.getElementById('updated').textContent = 'refresh failed: ' + e;
  }
}

refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>"""


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
