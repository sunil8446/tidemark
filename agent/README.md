# Investigator agent

A LangGraph state machine that investigates one alert with Claude and the read-only MCP tools.

```
fetch_alert ──► investigate ──► conclude ──► check_grounding ──► route
(tools only)    (Claude +       (structured   (deterministic:     escalate → review queue
                 tool loop)      Verdict)      numbers vs tools)  benign   → log
```

- **Model:** `claude-opus-5-5` at explicit `medium` effort (override with `TIDEMARK_AGENT_MODEL` / `TIDEMARK_AGENT_EFFORT`).
- **Refusal fallbacks:** on (`fallbacks: "default"`); a safety decline is re-served by Anthropic's recommended fallback model. A refusal that survives is routed to human review, never dropped.
- **Prompt caching:** the frozen system prompt plus tool list is one cache breakpoint; the growing conversation is auto-cached.
- **Grounding:** the verdict lists every number with the tool call it came from; `check_grounding` verifies both the narrative's numbers and each claim against the recorded tool outputs.
- **Cost:** exact per-investigation token usage and dollars (`investigator/cost.py`).

```bash
export UV_PROJECT_ENVIRONMENT=~/.tidemark-data/venvs/agent
uv sync && uv run pytest -q                       # scripted model, no API calls
uv run tidemark-investigate --alert-id <id>       # needs ANTHROPIC_API_KEY (or `ant auth login`)
uv run tidemark-investigate --alert-id <id> --backend cli   # on a Claude plan via `claude -p` (`claude auth login`)
uv run python -m investigator.review              # approve/reject page on :8765
```

Evaluation: [`evals/run_eval.py`](../evals/run_eval.py) (triage accuracy, groundedness, judge pass rate, cost, latency); CI replays recorded tool results so only the model is live.

**Two backends, one investigator.** `--backend api` calls the Claude API (prompt caching, refusal fallbacks, exact dollar cost). `--backend cli` runs the same prompts, MCP tools and `Verdict` schema through the Claude Code CLI ([`investigator/claude_cli.py`](investigator/claude_cli.py)), which draws on a Claude subscription's usage limits instead of an API key. Tool calls and results are read back from the CLI's `stream-json` events, so grounding is checked against exactly what the model saw; dollar figures on this backend are API-equivalent estimates from token counts.
