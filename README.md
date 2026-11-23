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
model says nothing about another model on that account. The targets of a provider are the accounts a non-empty
`auth.order` list names for it, in that order; a provider without one (no key, or an empty list) has all its
subscription accounts, declared in config or saved in the agent's auth store (see "Targets" under Requirements and
limits). After those come the entries of
`agents.defaults.model.fallbacks`.

- **Cron jobs.** A job whose model@account is held or probed out is rebound to the same model on the next live account
  of `auth.order` (the most preferred one when a preference is set, see below), or, if no account of that model is live, to the first live fallback entry. A move to another model
  is temporary: the job goes back to its own model once an account of it is live again and not held for it. The script
  never runs a job: the next scheduled run uses the new binding. Disabled jobs move too, after all other work of a
  pass, and stay disabled, so a run started by hand (`openclaw cron run`) finds a live account. On probing passes
  (at least every 5 min) they discover missing source and fallback providers, even when no enabled work uses them;
  on other passes they use only answers already obtained that pass. Answers are shared within a pass and refreshed
  on the next probing pass. Missing providers and the saved-account read share the time remaining before the
  200 s soft stop or the hard deadline's safety margin, reserving the existing 10 s disabled edit allowance.
  Slow providers cannot consume later candidates' shares on every pass. Unasked providers remain unknown;
  disabled discovery uncertainty cannot suppress an enabled job's confirmed no-target alert. Enabled probe
  timeouts and priority are unchanged. Disabled edits get at most 10 s per pass, excluding discovery;
  each edit's timeout fits the remaining edit allowance, and no disabled probe or edit starts past the soft stop.
  An uncertain edit is reconciled from the next job-list read before another edit. Deferred disabled work forces
  no early probe. One with nowhere to go is not reported, a failed edit is.
  Disabled one-shot (`at`) and legacy account-mode jobs are left alone.
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
- **Account preference (optional).** A settings file can name, per model, the accounts in the order they should be
  spent (example below). For such a model a move off a held account goes to the most preferred live account, and work
  returns to a more preferred account once it is live again: its hold ran out (the reset came on time) or a re-test
  cleared it (the reset came early). Before the first return to that model@account in an hour, one real request of the
  model there must answer (`return_check` in the journal; a limit holds it instead), so work does not bounce back onto
  a model still at its limit. What returns: operator cron jobs (disabled ones only on a proof the pass already has),
  and chats whose account this script set: chats it moved or parked, and chats the operator moved by hand and
  recorded with `--adopt`, while they still sit where they were put in the same session. A chat whose account or model
  a person chose is never moved back, nor one whose account OpenClaw picked itself; once a person picks something
  else for a chat the script moved, that choice stands (a person picking exactly the same model and account is not
  visible). The model never changes on a return. Moves off held accounts come first in a pass, returns and their
  checks after them. Each job or chat is moved this way at most once an hour, at most 10 jobs and 10 chats per pass,
  journaled as `job_move` / `chat_move` with `why: preference`. The first account of a provider's
  `auth.order` (where unpinned new chats and subagents start) follows the same preference: each preferred model votes
  for its first live account with its weight (enabled jobs and pinned chats on it, the default model one more), and the
  winner goes first when it beats the current first account. It is written with `openclaw config set` as a
  compare-and-set against the order just read, at most once per 15 min per provider (`order_set`); the settings file
  is then the one place to change the order, since a hand edit that the preference disagrees with is undone. The order
  is one per provider, so a model whose preferred account differs from the winner's still starts there (and is moved
  once held). Without a preference for any model of a provider nothing returns between its accounts and its
  `auth.order` is left alone.
- **Order from the accounts' limits (optional, Claude subscriptions).** The owner's rule: get the most out of what is
  paid for, so fill the account whose limit resets earliest first. With `usage` logins in the settings (below),
  `claude_usage.py` reads each account's limits at most every 5 minutes, before a pass decides anything: weekly use
  and reset, each model's own weekly limit (Fable) and reset, the 5-hour window, and the number of free limit
  resets the account holds (it never claims one). While every account the settings name for a model has a reading
  of the right account that is under 20 minutes old, the preference for that model is computed from the readings:
  accounts with room for that model (weekly under 100 %, and for Fable its own weekly limit under 100 %) first,
  earliest reset first, then the full ones, the one back soonest first. Otherwise the settings' order stands. All the
  rules of the preference above (moves, returns after a real request, at most once an hour, `auth.order`) apply to
  the computed order unchanged; each change of it is journaled as `prefer_order`. The order a model follows now is
  kept in the state as `prefer_eff`, next to `last_pass`, for other readers; a pass whose config check fails (wrong shape or
  not readable) drops it (journal `prefer_order_dropped` with the error), so they fall back to the settings' order rather than an old one.
  `--status` and `python3 claude_usage.py` show the readings.
- **Native Codex failures after tools.** An OpenAI account-limit bookkeeping event and an errored terminal
  delivery receipt for the same run are joined, including across polling passes. Only accounts whose identifiers
  match the log's SHA-256 profile labels are held. The session is rebound for its next turn; the interrupted turn
  is never automatically continued because its tools may already have changed external state. A completed run
  after account rotation and an incomplete turn without its own quota evidence are excluded.
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
| Putting the preferred live account first in `auth.order` | documented CLI: `openclaw config set auth.order.<provider> <json> --strict-json --expect-current-json <json>` |
| Account preference per model (optional) | settings file `~/.config/openclaw-inference-failover/settings.json` (`OCF_SETTINGS` overrides) |
| Claude account limits (optional, `usage` in the settings) | Claude's own endpoints, as Claude Code reads them: `GET /api/oauth/usage?cedar_ember=1&skip_spend=1`, `GET /api/oauth/account`, token refresh at `platform.claude.com/v1/oauth/token`, with a Claude Code login per account (`claude_usage.py`) |
| Continuing a chat | documented CLI: `openclaw agent --session-key ...` (`--deliver` only for external chats) |
| Whether a failed continuation was only stopped | the unit's own output (`journalctl --user -u <unit> -t openclaw`): a last line `aborted` is a stop, not a failure |
| Account status | Gateway RPC `models.probe` |
| Whether a held model@account answers again | documented CLI: `openclaw infer model run --local --model provider/model@account --prompt ... --json` |
| Session state; moving and aborting a chat run | Gateway RPC `sessions.describe`, `sessions.patch`, `sessions.abort` |
| Model, fallbacks, account order and declared accounts (the targets) | config file (`~/.openclaw/openclaw.json`) |
| Accounts saved only in the agent's auth store (targets of a provider without a non-empty `auth.order`) | documented CLI: `openclaw models status --json --agent <id>`, `auth.oauth.profiles[]` `profileId`, `provider`, `type` (only when needed) |
| Chats' pinned model/account and queued input | internal: tables `session_nodes` and `session_pending_inputs` in each agent database (read-only) |
| The recorded account of a job whose `payload.model` names no account | internal: table `cron_jobs` (`inferenceBinding.profileId`) in `state/openclaw.sqlite` (read-only); `cron list` does not show it |
| Stalls and failed turns | internal: Gateway log lines (`/tmp/openclaw*/openclaw-*.log`) |

Everything that depends on the OpenClaw version (CLI arguments, JSON field names, RPC names, table and column names,
log line anchors, error wording) is in one marked section at the top of `failover.py`.

**After an OpenClaw update** the next pass sees the new version and runs a self-check: each item of that section is
looked up in the installed OpenClaw (CLI answers, one read-only `sessions.describe`, database schemas, a text search of
the installed package). If anything is missing, the pass changes nothing and fails with the exact missing items (two passes in a row, then
once a day while it stays the same, and again when the missing set changes). The same happens when a read returns an
unexpected shape. Fix the section, and the next pass works again. The one exception is `openclaw models status`
(accounts saved only in the agent's store): if it fails or changes shape, the pass goes on with the config's accounts
and reports the failure as a problem.

When OpenClaw does not answer, or answers so slowly that a pass is cut off (a slow or restarting Gateway), the pass
keeps what it already did and stays quiet; the rest continues next pass. It becomes a problem only after ten minutes of
this, counted from the start of the first pass it hit. A part that does not run every pass (the 24 h check every 30
minutes, the disabled jobs) keeps its own count while it is cut off, until it finishes once, so ordinary passes between its
runs do not hide it.

The script discovers the OpenClaw state directory (`~/.openclaw`, or `OPENCLAW_STATE_DIR`), the config file, the
agents (one database per directory under `agents/`), the log directory (`logging.file` in the config, else
`/tmp/openclaw` or `/tmp/openclaw-<uid>`, whichever holds the newest log) and the package (`openclaw` on `PATH`,
its wrapper text, or `npm root -g`). It needs no settings file; the optional one holds only the account preference:

```json
{"prefer": {"anthropic/claude-opus-5-5": ["anthropic:a", "anthropic:b", "anthropic:c"],
            "anthropic/claude-fable-5-2": ["anthropic:a", "anthropic:c", "anthropic:b"]}}
```

Optional `usage` logins (one Claude Code login per account, made once with
`CLAUDE_CONFIG_DIR=<dir> claude auth login --claudeai`; OpenClaw's setup-tokens cannot read limits):

```json
{"usage": {"anthropic:a": {"dir": "~/.config/claude-usage/a", "email": "a@example.com"},
           "anthropic:b": {"dir": "~/.config/claude-usage/b", "email": "b@example.com", "borrow": "~/.claude"}}}
```

`dir` is a login the script owns and refreshes; `borrow` is another Claude Code login of the same account, read only
while its token is valid and never refreshed (a refresh would rotate the token under the Claude Code that owns it),
shown but never deciding the order (it comes and goes with that token);
`email` must match the login's account, else the reading is not used (without it the reading is shown, unverified,
and not used). An account without a usable reading is a problem after an hour when its own login read before, after
a day otherwise (no login yet, no email set, only a borrowed login); one failed read keeps the last good one in use. Readings go to `usage.json` in the
state directory.

Accounts it names that are not targets are ignored; targets it leaves out come after, in target order. A file that is
not valid is a problem, and until it is fixed no account is preferred. `OCF_*` environment variables override these for tests
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
account-mode job on an exhausted account is reported, not moved (a disabled one is skipped silently).

Requirements and limits:

- Python 3.8+ and the `openclaw` CLI on the job's `PATH`.
- Continuing a chat starts `openclaw agent` as a transient user unit (`systemd-run --user`), so that part needs
  Linux with a user systemd session. Without it the chat is still moved, and the failed start is reported. A
  continuation whose run someone stopped (`openclaw agent` ends with `aborted`) is journaled `continue_stopped`, not
  reported as a failure.
- `openclaw cron list` returns one page of 200 jobs and has no paging option. With more jobs the script reports it
  and watches only the first page.
- Targets: with a non-empty `auth.order` list for a provider, an account is chosen only when that list names it,
  whatever the probe reports about it and however it is billed. Without one (no key, or an empty list) the targets are
  every subscription account of the provider: its `oauth` and `token` profiles in `auth.profiles` (config order) and
  in the agent's saved auth store (`openclaw models status --json`, field `auth.oauth.profiles`), each once, `oauth`
  first. `models status` is run at most once per pass and only when such a provider is needed; only the profile id,
  provider and type are read, never the labels (they show masked keys and emails). If it fails, the config profiles
  are the targets and the failure is reported as a problem. A provider whose saved types disagree with the store's
  per-type counts (a plugin credential can override the reported type) or a row whose id names another provider is
  left out the same way. OpenClaw breaks ties by last use; without a preference this script prefers no account, so that order stands. An
  `api_key` profile is billed per token and is a target only when `auth.order` lists it. Under an empty list
  OpenClaw itself uses no account for unpinned work and its probe answers `unknown` for every one; the script still
  moves pinned work onto those accounts (any order rather than stopping) when the saved store calls their credential
  usable (ok, expiring or static), and a real limit comes back as a hold from the runs. Without an empty list, an order saved with `openclaw models auth order set` is not read: profiles it
  leaves out probe `unknown` and are never chosen. Work that can go nowhere else is reported as having no live
  account.
- Holds: a model@account is held until the time its provider named (the delay of a stalled run), or for an hour after
  a failure that named no time. A probe that answers ok (it tests another model) does not end a hold, and nothing
  moves back onto a model while it is held there. A provider can end a limit early (Anthropic resets a weekly limit
  before the time it named), so a hold with more than an hour left is re-tested with one real minimal request of that
  model on that account: `openclaw infer model run --local --model provider/model@account`, which uses the account's
  own saved credential inside OpenClaw. At most once an hour per hold, two per pass, only while the pass has time; a
  dry run makes none. An answer clears the hold (journal `hold_cleared`) and the next pass moves work back by the
  usual rules; a limit keeps it, at the newly named time if the provider names one; any other failure keeps it and
  becomes a problem after three in a row (journal `hold_retest`). `--status` lists the holds and their last re-test.
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
- If a chat's or job's original model leaves the non-empty `agents.defaults.models` allowlist, its saved return is
  dropped (`home_gone` in the journal), leaving it on its current model without a problem. A permanent
  `model not allowed` refusal during a return does the same.
- Times in the journal and in `--status` are the host's local time; set `TZ` on the job (`--command-env`) if needed.

Set `OCF_LOCAL_MODEL_CHECKS=0` in the job environment to disable isolated local model requests. Regular account
probes, holds and their recorded expiry, urgent failover, and native terminal detection continue. Early clearing
of long holds and unverified returns to preferred accounts wait; the script never treats an untested account as
verified. The default remains enabled for installations that allow local model checks.

## Use

- `failover.py`: dry run. Prints what a pass would do as JSON; takes no lock and writes no state.
- `failover.py --apply`: one real pass (what the job runs).
- `failover.py --check`: self-check plus a check of the last 24 hours; prints `{"openclaw": version, "missing": [...],
  "saved_profiles": "ok" or the failure}` and exits 1 with problems.
- `failover.py --adopt KEY[=MODEL]...`: records that the operator set the current account of these chats by hand, so the
  preference may move them; with `=MODEL` the operator moved the chat off its own model MODEL to a fallback, and it
  goes back to MODEL like a chat this script parked once an account of MODEL is live. Changes no chat itself (journal
  `chat_adopt`).
- `failover.py --status`: open problems, held model@account pairs, the preference, and jobs and chats parked on a fallback model, as
  JSON.

Tests: `python3 test_failover.py`. They use a fake `openclaw` and fixture files and never touch a Gateway.

The package compatibility check accepts the older generic automation account/route refusal or the complete
runtime/account guidance introduced in 2026.11.10-beta.1. Legacy refusal receipts remain recognized. New runtime,
CLI-login, and replaced-authorization refusals are operational errors, not usage-limit signals; they do not cause
a model/account limit hold. Provider rate-limit detection keeps its existing checks.

## Removal

Remove the job (`openclaw cron list` shows its id, then `openclaw cron rm <id>`), then delete the clone and
`~/.local/state/openclaw-inference-failover`. Jobs and chats keep the binding they have at that moment; check
`failover.py --status` first for work still parked on a fallback model.

## Status

Original surface: OpenClaw 2026.10.10; exact-run recovery receipts were checked against 2026.11.3. Run the self-check
on the executing install. A temporary workaround: remove each part as its upstream issue is fixed.

License: MIT.
