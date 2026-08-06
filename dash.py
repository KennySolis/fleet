#!/usr/bin/env python3
"""Live dashboard over the fleet's JSONL job journal.

Reads jobs.jsonl (written live by fleet.py) and serves a self-refreshing HTML
page showing each job's status, elapsed time, a big "current activity" line,
parsed pass/warn/error tallies, and a large color-coded log tail -- all from
what the workers actually printed. Because every producer writes the same
event schema, this dashboard also reads journals written by other tooling on
the machine with zero changes.

Usage:
  python dash.py [port]   (default 8791)

Then open http://localhost:8791
"""
import sys
import os
import json
import time
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
JOBS_LOG = os.path.join(HERE, "jobs.jsonl")
TIMELINE = os.path.join(HERE, "timeline.jsonl")

# An objective focused on longer than this (seconds) gets a "too long" flag.
LONG_SECONDS = 1200  # 20 minutes

PAGE = r"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fleet -- Live Progress</title>
<style>
  * { box-sizing:border-box; }
  body { font-family:-apple-system,Segoe UI,Roboto,sans-serif; background:#0a0c0f;
         color:#e9edf2; margin:0; padding:20px 22px 40px; }
  header { display:flex; align-items:center; gap:12px; margin:0 0 20px; }
  h1 { font-size:22px; font-weight:700; color:#cdd8e4; margin:0; letter-spacing:.3px; }
  .pulse { width:11px; height:11px; border-radius:50%; background:#5fd884;
           box-shadow:0 0 0 0 rgba(95,216,132,.6); animation:beat 2s infinite; }
  @keyframes beat { 0%{box-shadow:0 0 0 0 rgba(95,216,132,.5);} 70%{box-shadow:0 0 0 9px rgba(95,216,132,0);} 100%{box-shadow:0 0 0 0 rgba(95,216,132,0);} }
  .heartbeat { color:#7d8794; font-size:13px; margin-left:auto; }

  .job { background:#14181d; border:1px solid #262c34; border-radius:14px;
         padding:18px 22px; margin-bottom:18px; }
  .job.error { border-color:#5a2626; }
  .job.check { border-color:#5a4626; }
  .job.done  { border-color:#245c39; }
  .top { display:flex; align-items:center; gap:14px; flex-wrap:wrap; }
  .name { font-weight:700; font-size:20px; color:#fff; }
  .badge { font-size:13px; padding:5px 14px; border-radius:999px; font-weight:700; letter-spacing:.5px; }
  .running { background:#3a3410; color:#f5cc55; }
  .done    { background:#123a1f; color:#6fe294; }
  .error   { background:#3a1414; color:#ff7b7b; }
  .check   { background:#3a2f10; color:#ffd35e; }
  .stuck   { background:#3a2410; color:#ffab5e; }
  .script { color:#7d8794; font-size:14px; margin-left:auto; font-family:ui-monospace,Consolas,monospace; }

  .now { margin:14px 0 4px; font-size:17px; color:#dfe6ee; line-height:1.4;
         padding:12px 16px; background:#0d1116; border-left:3px solid #4a90d9; border-radius:0 8px 8px 0; }
  .now .label { color:#6f7b88; font-size:12px; text-transform:uppercase; letter-spacing:1px; display:block; margin-bottom:4px; }

  .stats { display:flex; gap:10px; flex-wrap:wrap; margin:14px 0 6px; }
  .chip { font-size:14px; padding:7px 14px; border-radius:9px; background:#1a1f26; font-weight:600; }
  .chip b { font-size:16px; }
  .chip.pass b { color:#6fe294; } .chip.warn b { color:#ffab5e; }
  .chip.err b { color:#ff7b7b; } .chip.line b { color:#9fb4c7; }
  .chip .cap { color:#7d8794; font-weight:500; margin-left:5px; }

  details { margin-top:10px; }
  summary { cursor:pointer; color:#8a97a6; font-size:14px; padding:6px 0; user-select:none; }
  .log { background:#0a0c0f; border:1px solid #20262d; border-radius:9px; padding:14px 16px;
         margin-top:8px; max-height:340px; overflow-y:auto; font-family:ui-monospace,Consolas,monospace;
         font-size:14px; line-height:1.7; }
  .log div { white-space:pre-wrap; word-break:break-word; }
  .l-pass { color:#8fe4a8; }
  .l-warn { color:#ffbf7a; }
  .l-err  { color:#ff8f8f; font-weight:600; }
  .l-plain { color:#c4ccd6; }
  .empty { color:#5a6470; text-align:center; padding:80px 0; font-size:16px; }

  .panel { background:#12161b; border:1px solid #242a32; border-radius:14px; padding:16px 20px; margin-bottom:22px; }
  .panel h2 { font-size:17px; color:#cdd8e4; margin:0 0 4px; font-weight:700; }
  .panel .sub { color:#6f7b88; font-size:13px; margin:0 0 12px; }
  .tl-row { display:flex; align-items:center; gap:12px; padding:9px 10px; border-radius:8px; font-size:15px; }
  .tl-row:nth-child(even){ background:#0d1116; }
  .tl-time { font-family:ui-monospace,Consolas,monospace; color:#8a97a6; font-size:14px; min-width:96px; }
  .tl-icon { width:20px; text-align:center; font-size:15px; }
  .tl-obj { flex:1; color:#e4eaf2; font-weight:600; }
  .tl-detail { color:#7d8794; font-weight:400; font-size:13px; margin-left:8px; font-family:ui-monospace,Consolas,monospace; }
  .tl-dur { font-family:ui-monospace,Consolas,monospace; font-size:14px; color:#a9b4c0; white-space:nowrap; }
  .tl-run  .tl-icon{ color:#f5cc55; }
  .tl-done .tl-icon{ color:#6fe294; }
  .tl-note .tl-icon{ color:#7d8794; }
  .tl-long { color:#ff9d5e; font-weight:700; }
</style></head><body>
<header>
  <span class="pulse"></span>
  <h1>Fleet &mdash; Live Progress</h1>
  <span class="heartbeat" id="hb">connecting...</span>
</header>
<div class="panel">
  <h2>&#9201; Timeline &mdash; time per objective</h2>
  <p class="sub">every objective stamped with clock time and duration; &#9888; = focused longer than 20 min</p>
  <div id="timeline">no objectives logged yet</div>
</div>
<div id="jobs" class="empty">waiting for a job to start...</div>
<script>
function fmtDur(s){
  s = Math.round(s);
  if (s < 60) return s + 's';
  if (s < 3600) return Math.floor(s/60) + 'm ' + (s%60) + 's';
  return Math.floor(s/3600) + 'h ' + Math.floor((s%3600)/60) + 'm';
}
async function tickTimeline(){
  let rows;
  try { rows = await (await fetch('/api/timeline')).json(); } catch(e){ return; }
  const el = document.getElementById('timeline');
  if (!rows.length){ el.textContent = 'no objectives logged yet'; return; }
  el.innerHTML = rows.map(r => {
    const t = new Date(r.start_ts*1000).toLocaleTimeString();
    const icon = r.status==='running' ? '&#9654;' : (r.status==='note' ? '&bull;' : '&#10003;');
    let dur = '';
    if (r.status==='running')
      dur = '<span class="'+(r.too_long?'tl-long':'')+'">running '+fmtDur(r.duration)+(r.too_long?' &#9888;':'')+'</span>';
    else if (r.status==='done')
      dur = '<span class="'+(r.too_long?'tl-long':'')+'">took '+fmtDur(r.duration)+(r.too_long?' &#9888;':'')+'</span>';
    return '<div class="tl-row tl-'+r.status+'">'+
      '<span class="tl-time">'+t+'</span>'+
      '<span class="tl-icon">'+icon+'</span>'+
      '<span class="tl-obj">'+esc(r.objective)+
        (r.detail ? '<span class="tl-detail">'+esc(r.detail)+'</span>' : '')+'</span>'+
      '<span class="tl-dur">'+dur+'</span></div>';
  }).join('');
}
function esc(s){ return s.replace(/&/g,'&amp;').replace(/</g,'&lt;'); }
function lineClass(text, isErr){
  if (isErr) return 'l-err';
  const s = text.toLowerCase();
  if (/\bpass\b|\bok\b|\bdone\b|\bsaved\b|✓/.test(s)) return 'l-pass';
  if (/\bwarn\b|\bcheck\b|\bissue\b|verdict/.test(s)) return 'l-warn';
  return 'l-plain';
}
async function tick(){
  let jobs;
  try { jobs = await (await fetch('/api/jobs')).json(); }
  catch(e){ document.getElementById('hb').textContent='offline'; return; }
  const now = new Date();
  document.getElementById('hb').textContent = 'live · updated ' +
      now.toLocaleTimeString();
  const el = document.getElementById('jobs');
  if (!jobs.length){ el.className='empty'; el.textContent='no jobs yet'; return; }
  el.className='';
  el.innerHTML = jobs.map(j => {
    const badge = j.status;
    const el2 = j.elapsed < 90 ? j.elapsed.toFixed(0)+'s'
              : (j.elapsed/60).toFixed(1)+'m';
    const now = j.latest ? esc(j.latest) : '(no output yet)';
    const rows = j.tail.map(l =>
      '<div class="'+lineClass(l.text, l.type==='error')+'">'+esc(l.text)+'</div>'
    ).join('');
    return '<div class="job '+badge+'"><div class="top">'+
      '<span class="name">'+esc(j.job_id)+'</span>'+
      '<span class="badge '+badge+'">'+badge.toUpperCase()+'</span>'+
      '<span class="script">'+esc(j.script)+' · '+el2+
        (j.exit_code!==null?' · exit '+j.exit_code:'')+'</span>'+
      '</div>'+
      '<div class="now"><span class="label">Current activity</span>'+now+'</div>'+
      '<div class="stats">'+
        '<span class="chip pass"><b>'+j.pass_count+'</b><span class="cap">passed</span></span>'+
        '<span class="chip warn"><b>'+j.warn_count+'</b><span class="cap">checks</span></span>'+
        '<span class="chip err"><b>'+j.error_count+'</b><span class="cap">errors</span></span>'+
        '<span class="chip line"><b>'+j.line_count+'</b><span class="cap">lines</span></span>'+
      '</div>'+
      '<details'+(badge==='error'||badge==='check'?' open':'')+'><summary>show live log ('+j.tail.length+' recent lines)</summary>'+
      '<div class="log" id="log_'+esc(j.job_id)+'">'+rows+'</div></details>'+
      '</div>';
  }).join('');
  document.querySelectorAll('.log').forEach(l => l.scrollTop = l.scrollHeight);
}
tick(); tickTimeline();
setInterval(tick, 1500);
setInterval(tickTimeline, 1500);
</script>
</body></html>"""


def _demojibake(s):
    """Repair UTF-8-decoded-as-cp1252 mojibake (e.g. 'â€"' -> '—') in stored lines."""
    if "â€" not in s and "Ã" not in s:
        return s
    try:
        return s.encode("cp1252", "strict").decode("utf-8", "strict")
    except Exception:
        return s


def _linekind(text, is_err):
    low = text.lower()
    if is_err:
        return "err"
    if re.search(r"\bpass\b|\bok\b|\bsaved\b|✓", low):
        return "pass"
    if re.search(r"\bwarn\b|\bcheck\b|\bissue\b|verdict", low):
        return "warn"
    return "line"


def load_jobs():
    jobs = {}
    if not os.path.exists(JOBS_LOG):
        return []
    with open(JOBS_LOG, "r", encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except Exception:
                continue
            jid = ev.get("job_id")
            if jid is None:
                continue
            j = jobs.setdefault(jid, {
                "job_id": jid, "script": "", "start_ts": ev["ts"], "last_ts": ev["ts"],
                "exit_code": None, "tail": [], "latest": "",
                "pass_count": 0, "warn_count": 0, "error_count": 0, "line_count": 0,
            })
            j["last_ts"] = ev["ts"]
            t = ev["type"]
            if t == "start":
                j["script"] = ev.get("script", "")
                j["start_ts"] = ev["ts"]
            elif t == "done":
                j["exit_code"] = ev.get("exit_code")
            elif t in ("line", "error"):
                text = _demojibake(ev.get("text", ""))
                j["line_count"] += 1
                kind = _linekind(text, t == "error")
                if kind == "pass":
                    j["pass_count"] += 1
                elif kind == "warn":
                    j["warn_count"] += 1
                elif kind == "err":
                    j["error_count"] += 1
                j["tail"].append({"type": t, "text": text})
                j["tail"] = j["tail"][-60:]
                # "current activity" = last non-trivial line
                if text.strip():
                    j["latest"] = text.strip()

    now = time.time()
    out = []
    for j in jobs.values():
        elapsed = j["last_ts"] - j["start_ts"]
        silent = now - j["last_ts"]
        if j["exit_code"] is None:
            status = "stuck" if silent > 120 else "running"
        elif j["exit_code"] == 0:
            status = "done"
        elif j["error_count"] > 0:
            status = "error"          # real error lines present -> red
        else:
            status = "check"          # nonzero exit but no error output -> amber
        out.append({
            "job_id": j["job_id"], "script": j["script"], "status": status,
            "elapsed": elapsed, "exit_code": j["exit_code"], "tail": j["tail"],
            "latest": j["latest"], "pass_count": j["pass_count"],
            "warn_count": j["warn_count"], "error_count": j["error_count"],
            "line_count": j["line_count"],
        })
    # longest-running first
    out.sort(key=lambda j: -j["elapsed"])
    return out


def load_timeline():
    """Read timeline.jsonl, pair start/done by objective, compute durations,
    flag anything focused on longer than LONG_SECONDS."""
    if not os.path.exists(TIMELINE):
        return []
    events = []
    with open(TIMELINE, "r", encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                events.append(json.loads(raw))
            except Exception:
                continue

    now = time.time()
    rows = []
    open_starts = {}  # objective -> queue of pending start events
    for ev in events:
        kind = ev.get("kind")
        obj = ev.get("objective", "")
        if kind == "note":
            rows.append({"start_ts": ev["ts"], "objective": obj,
                         "detail": _demojibake(ev.get("detail", "")),
                         "status": "note", "duration": 0.0, "too_long": False})
        elif kind == "start":
            open_starts.setdefault(obj, []).append(ev)
        elif kind == "done":
            q = open_starts.get(obj)
            if q:
                st = q.pop(0)
                dur = ev["ts"] - st["ts"]
                rows.append({"start_ts": st["ts"], "objective": obj,
                             "detail": _demojibake(ev.get("detail") or st.get("detail", "")),
                             "status": "done", "duration": dur,
                             "too_long": dur > LONG_SECONDS})
            else:
                rows.append({"start_ts": ev["ts"], "objective": obj,
                             "detail": _demojibake(ev.get("detail", "")),
                             "status": "done", "duration": 0.0, "too_long": False})
    # any unmatched starts are still running
    for obj, q in open_starts.items():
        for st in q:
            dur = now - st["ts"]
            rows.append({"start_ts": st["ts"], "objective": obj,
                         "detail": _demojibake(st.get("detail", "")),
                         "status": "running", "duration": dur,
                         "too_long": dur > LONG_SECONDS})

    rows.sort(key=lambda r: -r["start_ts"])
    return rows


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/", ""):
            self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
        elif self.path == "/api/jobs":
            self._send(200, json.dumps(load_jobs()).encode("utf-8"), "application/json")
        elif self.path == "/api/timeline":
            self._send(200, json.dumps(load_timeline()).encode("utf-8"), "application/json")
        else:
            self.send_response(404)
            self.end_headers()


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8791
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print("Fleet progress dashboard: http://localhost:%d" % port)
    server.serve_forever()


if __name__ == "__main__":
    main()
