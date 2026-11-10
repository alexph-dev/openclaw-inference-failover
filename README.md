# openclaw-inference-failover

```
I hated that openclaw broke it on 2026.10.10 completely, so i had to workaround it myself
```

A single Python 3 script (standard library only) that keeps an OpenClaw install working when an AI account hits its
provider usage limit. An OpenClaw command job runs it once a minute.

It is a temporary workaround. Each part exists because of an open upstream issue; delete that part when the issue is
fixed, and delete the whole script when all of them are.

## What it does

- **Cron jobs.** A job bound to an account that is out of limit is rebound to the same model on a live account, or,
  if no account of that model is live, to the first live entry of `agents.defaults.model.fallbacks`. A move to
  another model is temporary: the job goes back to its own model once an account of it answers again. A job that lost
  a run to the limit before it did anything, and is not due again within 30 minutes, is run once after the move.
- **Stalled runs.** A run that sleeps out a long same-model rate-limit wait is aborted. A chat is then continued; a
  recurring job is rerun as above. One-shot (`at`) jobs are never aborted.
- **Chats.** A chat turn that failed because every account of its pinned model was out is moved the same way and
  continued, unless the user already wrote again. The script sends nothing to a chat itself: the continuation prompt
  asks the agent to open its reply with one line, in the chat's own language, saying the chat moved to another model.
- **Problems** it cannot fix (no live account anywhere, a run that may have acted before it died, a missing OpenClaw
  surface) fail the job run with the reason on stderr on two passes in a row, then stay quiet while open. It never
  fails every minute, so the scheduler does not disable it.

| Part of the script | Upstream issue it works around |
|---|---|
| Rebinding cron jobs to a live account (`jobs_pass`, `rerun_lost`) | [openclaw/openclaw#192009](https://github.com/openclaw/openclaw/issues/192009): a cron job stays bound to one exhausted account |
| Moving and continuing failed chats (`recover_chat`, `chats_home`) | [openclaw/openclaw#151980](https://github.com/openclaw/openclaw/issues/151980): a chat with a user-selected model does not follow fallbacks |
| Aborting stalled runs (`stalls_and_chats`) | [openclaw/openclaw#168101](https://github.com/openclaw/openclaw/issues/168101), [openclaw/openclaw#179962](https://github.com/openclaw/openclaw/issues/179962): a run waits for hours for the rate limit to reset |

## What it reads

| What | Source |
|---|---|
| Job list, bindings, last-run state | documented CLI: `openclaw cron list --all --json` |
| Failed runs and their errors | documented CLI: `openclaw cron runs --all --status error --limit 200 --json` (only when a job is in a fresh error streak) |
| Rebinding and rerunning a job | documented CLI: `openclaw cron edit <id> --model`, `openclaw cron run <id>` |
| Continuing a chat | documented CLI: `openclaw agent --session-key ... --deliver` |
| Account status | Gateway RPC `models.probe` |
| Session state; moving and aborting a chat run | Gateway RPC `sessions.describe`, `sessions.patch`, `sessions.abort` |
| Model, fallbacks, account order | config file (`~/.openclaw/openclaw.json`) |
| A chat's pinned model/account and queued input | internal: tables `session_nodes` and `session_pending_inputs` in each agent database (read-only) |
| Stalls and failed turns | internal: Gateway log lines (`/tmp/openclaw*/openclaw-*.log`) |

Everything that depends on the OpenClaw version (CLI arguments, JSON field names, RPC names, table and column names,
log line anchors, error wording) is in one marked section at the top of `failover.py`.

**After an OpenClaw update** the next pass sees the new version and runs a self-check: each item of that section is
looked up in the installed OpenClaw (CLI answers, one read-only `sessions.describe`, database schemas, a text search of
the installed package). If anything is missing, the pass changes nothing and fails with the exact missing items (two passes in a row, then
once a day while it stays the same, and again when the missing set changes). The same happens when a read returns an
unexpected shape. Fix the section, and the next pass works again.

When OpenClaw does not answer (a slow or restarting Gateway), the pass changes nothing and stays quiet; it becomes a
problem only after ten minutes without an answer.

The script discovers the OpenClaw state directory (`~/.openclaw`, or `OPENCLAW_STATE_DIR`), the config file, the
agents (one database per directory under `agents/`), the log directory (`logging.file` in the config, else
`/tmp/openclaw` or `/tmp/openclaw-<uid>`, whichever holds the newest log) and the package (`openclaw` on `PATH`,
its wrapper text, or `npm root -g`). It needs no settings file. `OCF_*` environment variables override these for tests
and unusual installs.

Its own state and journal live in `~/.local/state/openclaw-inference-failover` (`OCF_STATE` overrides).

## Install

```sh
git clone https://github.com/alexph-dev/openclaw-inference-failover.git ~/openclaw-inference-failover
python3 ~/openclaw-inference-failover/failover.py --check
openclaw cron add --name "inference failover" --cron "* * * * *" --exact \
  --command-argv '["python3", "'"$HOME"'/openclaw-inference-failover/failover.py", "--apply"]' \
  --timeout-seconds 300 --no-deliver
```

Jobs that the script should move must be operator jobs (`scheduledToolPolicy.mode` `trusted`); a legacy
account-mode job on an exhausted account is reported, not moved.

Requirements and limits:

- Python 3.8+ and the `openclaw` CLI on the job's `PATH`.
- Continuing a chat starts `openclaw agent` as a transient user unit (`systemd-run --user`), so that part needs
  Linux with a user systemd session. Without it the chat is still moved, and the failed start is reported.
- `openclaw cron list` returns one page of 200 jobs and has no paging option. With more jobs the script reports it
  and watches only the first page.
- Agent-to-agent (`a2a`) sessions are never continued.
- Times in the journal and in `--status` are the host's local time; set `TZ` on the job (`--command-env`) if needed.

## Use

- `failover.py`: dry run. Prints what a pass would do as JSON; takes no lock and writes no state.
- `failover.py --apply`: one real pass (what the job runs).
- `failover.py --check`: self-check plus a check of the last 24 hours; prints `{"openclaw": version, "missing": [...]}`
  and exits 1 with problems.
- `failover.py --status`: open problems, held accounts, and jobs and chats parked on a fallback model, as JSON.

Tests: `python3 test_failover.py`. They use a fake `openclaw` and fixture files and never touch a Gateway.

## Removal

Remove the job (`openclaw cron list` shows its id, then `openclaw cron rm <id>`), then delete the clone and
`~/.local/state/openclaw-inference-failover`. Jobs and chats keep the binding they have at that moment; check
`failover.py --status` first for work still parked on a fallback model.

## Status

Tested with OpenClaw 2026.10.10. A temporary workaround: remove each part as its upstream issue is fixed.

License: MIT.
