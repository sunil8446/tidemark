"""Human-in-the-loop review page for escalated cases (standard library only).

    uv run python -m investigator.review            # http://localhost:8765

Reads the escalation queue (TIDEMARK_CASES, default ~/.tidemark-data/cases/queue.jsonl)
and appends each analyst decision to decisions.jsonl next to it. Decisions on the
evaluation stream double as hand labels for the agent eval set.
"""

from __future__ import annotations

import html
import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs

QUEUE = Path(os.environ.get("TIDEMARK_CASES", os.path.expanduser("~/.tidemark-data/cases/queue.jsonl")))
DECISIONS = QUEUE.with_name("decisions.jsonl")

CSS = """
:root { --bg:#fff; --fg:#1d2433; --muted:#5b6475; --line:#e3e6ec; --card:#f7f8fa; --esc:#b3261e; --ok:#1e7d3a; }
@media (prefers-color-scheme: dark) { :root { --bg:#14171c; --fg:#e7e9ee; --muted:#9aa3b2; --line:#2a2f38; --card:#1b1f26; --esc:#ff8a80; --ok:#7bd88f; } }
body { margin:0; background:var(--bg); color:var(--fg); font:15px/1.5 system-ui, sans-serif; }
main { max-width:900px; margin:0 auto; padding:24px 16px; }
h1 { font-size:20px; margin:0 0 4px; } .muted { color:var(--muted); }
.case { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:16px; margin:16px 0; }
.case h2 { font-size:16px; margin:0 0 6px; font-family:ui-monospace, monospace; word-break:break-all; }
.tag { display:inline-block; padding:1px 8px; border-radius:999px; font-size:12px; border:1px solid var(--line); margin-right:6px; }
.tag.esc { color:var(--esc); border-color:var(--esc); } .tag.ok { color:var(--ok); border-color:var(--ok); }
ul { margin:6px 0 10px 18px; padding:0; } li { margin:2px 0; }
form { display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin-top:10px; }
input[type=text] { flex:1; min-width:200px; padding:6px 8px; border:1px solid var(--line); border-radius:6px; background:var(--bg); color:var(--fg); }
button { padding:6px 14px; border-radius:6px; border:1px solid var(--line); background:var(--bg); color:var(--fg); cursor:pointer; }
button.approve { border-color:var(--esc); color:var(--esc); } button.reject { border-color:var(--ok); color:var(--ok); }
"""


def load(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def decided() -> dict[str, dict]:
    return {d["alert_id"]: d for d in load(DECISIONS)}


def render() -> str:
    done = decided()
    pending = [c for c in load(QUEUE) if c["alert_id"] not in done]
    parts = [f"<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>"
             f"<title>Case Review</title><style>{CSS}</style><main><h1>Escalated cases</h1>"
             f"<p class=muted>{len(pending)} pending · {len(done)} decided</p>"]
    for c in pending:
        v = c.get("verdict") or {}
        g = c.get("grounding") or {}
        cls = v.get("classification", "error")
        claims = "".join(f"<li>{html.escape(x['statement'])} <span class=muted>({html.escape(x['value'])}, "
                         f"{html.escape(x['tool_call_id'])})</span></li>" for x in v.get("claims", []))
        alts = "".join(f"<li>{html.escape(a)}</li>" for a in v.get("benign_explanations_considered", []))
        ungrounded = ", ".join(g.get("ungrounded_numbers", [])) or "none"
        parts.append(f"""
<section class=case>
  <h2>{html.escape(c['alert_id'])}</h2>
  <span class="tag {'esc' if cls == 'escalate' else 'ok'}">{html.escape(cls)}</span>
  <span class=tag>confidence {html.escape(v.get('confidence', '?'))}</span>
  <span class=tag>stream {html.escape(c['stream'])}</span>
  <span class=tag>grounded {g.get('groundedness', '?')}</span>
  <span class=tag>${c.get('usage', {}).get('cost_usd', '?')} · {c.get('seconds', '?')} s</span>
  <p><b>{html.escape(v.get('hypothesis', c.get('error') or ''))}</b></p>
  <p>{html.escape(v.get('narrative', ''))}</p>
  <div class=muted>Claims</div><ul>{claims or '<li>none</li>'}</ul>
  <div class=muted>Ordinary explanations considered</div><ul>{alts or '<li>none</li>'}</ul>
  <div class=muted>Numbers not traced to a tool result: {html.escape(ungrounded)}</div>
  <form method=post action=/decide>
    <input type=hidden name=alert_id value="{html.escape(c['alert_id'])}">
    <input type=hidden name=stream value="{html.escape(c['stream'])}">
    <input type=text name=note placeholder="Note (optional)">
    <button class=approve name=decision value=escalate>Confirm escalation</button>
    <button class=reject name=decision value=likely_benign>Reject as benign</button>
  </form>
</section>""")
    if not pending:
        parts.append("<p>Nothing waiting for review.</p>")
    return "".join(parts) + "</main>"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = render().encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path != "/decide":
            self.send_error(404)
            return
        form = parse_qs(self.rfile.read(int(self.headers.get("Content-Length", 0))).decode())
        decision = form.get("decision", [""])[0]
        if decision not in ("escalate", "likely_benign"):
            self.send_error(400)
            return
        rec = {"alert_id": form["alert_id"][0], "stream": form.get("stream", ["live"])[0], "decision": decision,
               "note": form.get("note", [""])[0], "decided_at": datetime.now(timezone.utc).isoformat()}
        DECISIONS.parent.mkdir(parents=True, exist_ok=True)
        with DECISIONS.open("a") as f:
            f.write(json.dumps(rec) + "\n")
        self.send_response(303)
        self.send_header("Location", "/")
        self.end_headers()

    def log_message(self, *args):
        pass


def main() -> None:
    port = int(os.environ.get("TIDEMARK_REVIEW_PORT", "8765"))
    print(f"review page on http://localhost:{port} (queue: {QUEUE})")
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
