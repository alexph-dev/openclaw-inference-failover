# openclaw-inference-failover

```
I hated that openclaw broke it on 2026.10.10 completely, so i had to workaround it myself
```

A single Python 3 script (standard library only) that keeps an OpenClaw install working when an AI account hits its
provider usage limit. An OpenClaw command job runs it once a minute.

It is a temporary workaround. Each part exists because of an open upstream issue; delete that part when the issue is
fixed, and delete the whole script when all of them are.

## What it does

A limit is always one model on one account (model@account), never a whole account: a probe that answers ok for one
model says nothing about another model on that account. The targets of a provider are the accounts `auth.order` lists
for it, in that order; a provider without an `auth.order` key has the accounts OpenClaw's
automatic selection uses (see "Targets" under Requirements and limits). After those come the entries of
`agents.defaults.model.fallbacks`.

- **Cron jobs.** A job whose model@account is held or probed out is rebound to the same model on the next live account
  of `auth.order`, or, if no account of that model is live, to the first live fallback entry. A move to another model
  is temporary: the job goes back to its own model once an account of it is live again and not held for it. The script
  never runs a job: the next scheduled run uses the new binding.
- **Pinned chats.** A chat whose model or account was chosen by hand (OpenClaw applies no fallbacks to it), or that the
  script parked on a fallback model, is moved the same way, on the same passes, whenever its model@account is held or
  probed out. What its runs did is not consulted. A chat with a run in progress is moved once that run is over.
- **Strict chats.** Session keys listed in `OCF_STRICT_CHATS` (comma-separated, set in the job's environment) never
  change model: they move only to another account of their own model. While every account of that model is out, the
  chat is left alone (a sleeping run is not aborted, no problem is raised for the pin), and a turn that died on a
  limit waits as any other does: it is continued only on an account of its own model, and becomes a problem if none
  is live in time. Use it for conversations that must only ever run on one model.
- **Stalled runs.** A run that sleeps out a long same-model rate-limit wait is aborted, and that model@account is held
  until the time the provider named. One-shot (`at`) jobs are never aborted. An aborted cron run is not replayed. A
  subagent's stalled run is aborted too, so its parent sees it end as killed now instead of days later; it is not
  moved or continued, because the parent chose its model.
- **Turns that died on a limit.** A human chat turn whose run died on a limit is continued on a live model@account
  (the chat is moved first by the same rule, also when OpenClaw chose its account), unless the user already wrote
  again or a person stopped the run (a Control UI stop or `chat.abort`). One log test decides a limit death: the limit
  was the run's last word in the Gateway log; the session's status word is not read. A turn that merely timed out is
  not continued. Human main/dashboard sessions are included even without an external delivery route. The script sends
  nothing to a chat itself: the continuation prompt asks the agent to open its reply with one line, in the chat's own
  language, saying the chat moved to another model, and to reconcile completed actions and uncertain side effects
  before doing more work, rather than replaying the request. Every limit event the script drops is journaled
  (`event_drop`) with the reason.
- **Problems** it cannot fix (no live account anywhere, a missing OpenClaw surface) fail the job run with the reason on
  stderr on two passes in a row, then stay quiet while open. It never fails every minute, so the scheduler does not
  disable it.

| Part of the script | Upstream issue it works around |
|---|---|
| Rebinding cron jobs to a live account (`jobs_pass`) | [openclaw/openclaw#192009](https://github.com/openclaw/openclaw/issues/192009): a cron job stays bound to one exhausted account |
| Moving pinned chats and continuing turns that died on a limit (`chats_pass`, `recover_chat`, `chats_home`) | [openclaw/openclaw#151980](https://github.com/openclaw/openclaw/issues/151980): a chat with a user-selected model does not follow fallbacks |
| Aborting stalled runs (`stalls_and_chats`, `cron_stall`) | [openclaw/openclaw#168101](https://github.com/openclaw/openclaw/issues/168101), [openclaw/openclaw#179962](https://github.com/openclaw/openclaw/issues/179962): a run waits for hours for the rate limit to reset |

## What it reads

| What | Source |
|---|---|
| Job list, bindings, last-run state | documented CLI: `openclaw cron list --all --json` |
| Failed runs and their errors | documented CLI: `openclaw cron runs --all --status error --limit 200 --json` (only when a job is in a fresh error streak) |
| Rebinding a job | documented CLI: `openclaw cron edit <id> --model` |
| Continuing a chat | documented CLI: `openclaw agent --session-key ...` (`--deliver` only for external chats) |
| Account status | Gateway RPC `models.probe` |
| Session state; moving and aborting a chat run | Gateway RPC `sessions.describe`, `sessions.patch`, `sessions.abort` |
| Model, fallbacks, account order and declared accounts (the targets) | config file (`~/.openclaw/openclaw.json`) |
| Chats' pinned model/account and queued input | internal: tables `session_nodes` and `session_pending_inputs` in each agent database (read-only) |
| The recorded account of a job whose `payload.model` names no account | internal: table `cron_jobs` (`inferenceBinding.profileId`) in `state/openclaw.sqlite` (read-only); `cron list` does not show it |
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
- Targets: with an `auth.order` entry for a provider, an account is chosen only when that entry lists it, whatever
  the probe reports about it and however it is billed; an empty list means no account, as in OpenClaw. Without the
  key the targets are the set OpenClaw's automatic selection rotates through: the provider's `oauth` and `token`
  profiles in `auth.profiles`, `oauth` first, then config order. A profile only in the agent's saved store is not a
  target: `models.probe` does not report its type. OpenClaw then breaks ties by last use; this script prefers no account, so config order stands. An
  `api_key` profile is billed per token and is a target only when `auth.order` lists it. Work that can go nowhere
  else is reported as having no live account.
- Holds: a model@account is held until the time its provider named (the delay of a stalled run), or for an hour after
  a failure that named no time. A probe that answers ok (it tests another model) does not end a hold, and nothing
  moves back onto a model while it is held there. `--status` lists the holds.
- Cron, agent-to-agent (`a2a`), subagent and worker sessions are not continued as human chats. Dashboard keys also
  require a worker-metadata check; a remotely placed or spawned worker is not a browser conversation.
- Before mutation, recovery refreshes the exact run, queued/newer input and model selection after account probes.
  A live stall records exact session/run abort intent durably before aborting; retries reconcile only its matching
  terminal receipt. Model patches pass fresh session/lifecycle preconditions when available. A deterministic retained
  systemd unit reconciles uncertain continuation launches. The final read and CLI continuation admission are separate:
  these checks are not an atomic cross-RPC lock, so a newer user turn can still arrive in that interval.
- A temporarily selected fallback model returns to its original model only while the session is idle, its input
  queue is empty, the watchdog still owns the selection, and the original model is not held on that account. Selecting
  the configured primary clears its model override in OpenClaw, including when the original session inherited that
  default.
- Times in the journal and in `--status` are the host's local time; set `TZ` on the job (`--command-env`) if needed.

## Use

- `failover.py`: dry run. Prints what a pass would do as JSON; takes no lock and writes no state.
- `failover.py --apply`: one real pass (what the job runs).
- `failover.py --check`: self-check plus a check of the last 24 hours; prints `{"openclaw": version, "missing": [...]}`
  and exits 1 with problems.
- `failover.py --status`: open problems, held model@account pairs, and jobs and chats parked on a fallback model, as
  JSON.

Tests: `python3 test_failover.py`. They use a fake `openclaw` and fixture files and never touch a Gateway.

## Removal

Remove the job (`openclaw cron list` shows its id, then `openclaw cron rm <id>`), then delete the clone and
`~/.local/state/openclaw-inference-failover`. Jobs and chats keep the binding they have at that moment; check
`failover.py --status` first for work still parked on a fallback model.

## Status

Original surface: OpenClaw 2026.10.10; exact-run recovery receipts were checked against 2026.11.3. Run the self-check
on the executing install. A temporary workaround: remove each part as its upstream issue is fixed.

License: MIT.
