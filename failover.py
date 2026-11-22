#!/usr/bin/env python3
"""openclaw-inference-failover: moves OpenClaw work off AI accounts that hit a provider usage limit.

Run once a minute by an OpenClaw command job. A limit is always one model on one account (model@account), never a
whole account: a probe that answers ok for one model says nothing about another model on that account.
  jobs   a cron job whose model@account is held or probed out moves to the same model on the next live account
         of auth.order, else to the first live entry of agents.defaults.model.fallbacks. The targets of a provider are
         the accounts its non-empty auth.order lists; a provider without one (no key or an empty list) has all its
         subscription accounts: its oauth and token profiles in auth.profiles and in the agent's saved auth store
         (openclaw models status), oauth first, then config order, then saved order. An api_key profile is a target
         only when auth.order lists it. A move to another MODEL is temporary: the job goes back to its own model once an
         account of it is live again. A job is never rerun.
  prefer optional per-model account preference (settings file, see SETTINGS): a move off a held account goes to the
         most preferred live account, and work of that model returns to a more preferred account once it is live again
         (its hold ran out or a re-test cleared it) and one real request of that model there answered within the hour.
         Moved back: operator cron jobs, and chats this script moved, parked or was told to adopt (--adopt); never a
         chat whose account or model a person chose, nor one whose account OpenClaw chose itself. At most one such move
         per item an hour. The first account of auth.order follows the preferred live account of the most used models, so
         new unpinned chats and subagents start there. Without a preference for a model: no return between accounts.
         A disabled job moves too, after the chats, and stays disabled, so a run started by hand ('cron run') finds a
         live account. On probing passes it discovers its own accounts and fallbacks; other passes use existing
         answers only. No target is not a problem; disabled one-shot and legacy account-mode jobs are left alone.
  usage  optional ('usage' logins in the settings, claude_usage.py): each Claude account's limits are read at most every
         5 min; while every account named for a model has a fresh, verified reading, that model's preference is
         computed from them: accounts with room for it first, earliest reset first (owner's rule: fill what resets
         soonest), then the full ones, back soonest first. Else the settings' order. Never claims a free reset.
  chats  a pinned human chat (a model or account the user chose, or this script set) is moved the same way on the same
         passes, whatever its runs did.
  strict chats  session keys listed in OCF_STRICT_CHATS (comma-separated) never change model: they move only to
         another account of their own model. While no account of it is live the chat is left alone, and a turn that
         died on a limit waits like any other (then is a problem); it is continued only on its own model.
  stalls a run sleeping out a long same-model rate-limit wait is aborted, and its model@account is held until the time
         the provider named. One-shot (at) jobs are never aborted: an abort disables them silently, their timeout is
         retried by the scheduler on the new binding. An aborted cron run is not replayed.
  holds  a model@account is held until the time its provider named. A provider may end a limit early, so a hold with
         more than an hour left is re-tested at most hourly with one real request of that model on that account
         (openclaw infer model run --local --model provider/model@account); an answer clears the hold.
  turns  a human chat turn whose run died on a limit (the limit was the run's last word in the log) is continued on a
         live model@account, unless the user already wrote again or a person stopped it. The continuation prompt asks
         the agent to open its reply with one line, in the chat's own language, saying the chat moved to another model.
         A turn that merely timed out is not continued.
  check  every 30 min it compares the last 24 h of limit failures with what it did.
  problems  anything it cannot fix fails the run (exit 1, reason on stderr) on two passes in a row, then stays quiet
         while it is open, and fails again only if it is still open after 6 h (a missing OpenClaw surface: after
         24 h). OpenClaw not answering is a problem only after 10 min. It never fails every minute: ten errors in a
         row would make the scheduler disable this job.

  failover.py            dry run (no lock, no state written)
  failover.py --apply    act
  failover.py --check    self-check of the OpenClaw surfaces it reads plus the 24 h check; exit 1 with problems
  failover.py --status   open problems, held model@account pairs, work parked on a fallback model (JSON)
  failover.py --adopt KEY...   record that the current account of these chats was set by the operator, not a person
State and journal: ~/.local/state/openclaw-inference-failover (OCF_STATE overrides).
Settings (optional): ~/.config/openclaw-inference-failover/settings.json (OCF_SETTINGS overrides):
  {"prefer": {"provider/model": ["provider:account", ...]}}
"""
import calendar, fcntl, glob, hashlib, json, os, pathlib, re, signal, sqlite3, subprocess, sys, time

import claude_usage as CU      # sibling module: Claude subscription limits per account (optional 'usage' settings)

# =====================================================================================================================
# OPENCLAW SURFACE. Everything that depends on the installed OpenClaw version is named here and only here; the rest of
# the script uses these names. --check (and a pass after an OpenClaw update, or after a read of the wrong shape)
# verifies each item against the installed OpenClaw and changes nothing while any is missing.
# Written against OpenClaw 2026.10.10.
# =====================================================================================================================
# Documented CLI (argv after 'openclaw')
CLI_VERSION = ['--version']                                         # installed version: a change triggers the self-check
CLI_JOBS = ['cron', 'list', '--all', '--json', '--timeout', '60000']   # job list with binding and last-run state
CLI_RUNS = ['cron', 'runs', '--all', '--status', 'error', '--sort', 'desc', '--limit', '200', '--json',
            '--timeout', '60000']                                   # recent failed runs, newest first (200 = cap)
CLI_EDIT = lambda jid, ref: ['cron', 'edit', jid, '--model', ref, '--timeout', '120000']   # rebind a job to model@account
CLI_AGENT = lambda key, msg, deliver=True: ['agent', '--session-key', key, '--message', msg] + (['--deliver'] if deliver else []) + ['--timeout', '3600']  # continue on the stored route, or locally
CLI_MODELS = lambda agent: ['models', 'status', '--json', '--agent', agent]   # the agent's accounts, saved ones too
AGENT_STOPPED = 'aborted'   # last output line of 'openclaw agent' when its run was stopped (sessions.abort, a stop button)
AGENT_OUTPUT = lambda unit: ['journalctl', '--user', '-u', unit, '-t', 'openclaw', '-o', 'cat', '--no-pager', '-n', '5']
                            # what 'openclaw agent' printed in a continuation unit (syslog identifier 'openclaw')
CLI_ORDER_SET = lambda provider, new, old: ['config', 'set', 'auth.order.%s' % provider, json.dumps(new), '--strict-json',
                                           '--expect-current-json', json.dumps(old)]   # compare-and-set of one order
ORDER_RACES = ('expectation did not match', 'changed while this command was writing')   # its answers to a lost race
ORDER_CHANGED = 'declined: the config changed since it was read'
CLI_CALL = lambda method, params, ms: ['gateway', 'call', method, '--params', json.dumps(params), '--json', '--timeout', str(ms)]  # one RPC
# One real minimal request of exactly this model on exactly this account, through OpenClaw with the account's own saved
# credential ('provider/model@profile' locks the profile; --local runs it in the CLI process, no chat session).
RETEST_PROMPT = 'Reply with exactly: ok'
CLI_RETEST = lambda ref, agent: ['infer', 'model', 'run', '--local', '--model', ref, '--prompt', RETEST_PROMPT, '--json',
                                 '--agent', agent]
RETEST_HELP = (['infer', 'model', 'run', '--help'], ('--local', '--model', '--agent', '--json'))   # self-check
RETEST_OK, RETEST_OUTPUTS, RETEST_ERROR = 'ok', 'outputs', 'error'  # answer {ok: true, outputs: [{text}]} | {ok: false, error: {message}}
RETRY_AFTER_RE = re.compile(r'Retry-After:\s*(\d+)\s*seconds', re.I)   # the provider's wait in the CLI's error message
# JSON fields of the CLI answers ('a.b' = nested)
JOBS_LIST, RUNS_LIST = 'jobs', 'entries'                            # top-level lists of cron list / cron runs
LIST_MORE = 'hasMore'                                               # true when the job list did not fit one page
JOB_F = {'id': 'id', 'enabled': 'enabled', 'name': 'name',          # per job in cron list
         'model': 'payload.model',                                  #   binding 'provider/model@account'
         'kind': 'schedule.kind', 'mode': 'scheduledToolPolicy.mode', 'next': 'state.nextRunAtMs',
         'last': 'state.lastRunStatus', 'last_at': 'state.lastRunAtMs', 'streak': 'state.consecutiveErrors',
         'pkind': 'payload.kind'}                                   #   'agentTurn' for AI jobs (command jobs have no account)
AGENT_TURN = 'agentTurn'
AT_KIND = 'at'                                                      # schedule.kind of a one-shot job
JOB_REQUIRED = ('id', 'enabled', 'model', 'kind', 'next', 'mode')   # fields every job list must show somewhere
JOB_REQUIRED_RUN = ('last', 'streak')                               # and these once any job has run (shows 'last_at')
JOB_TRUSTED = 'trusted'                                             # scheduledToolPolicy.mode of an operator job
RUN_F = {'job': 'jobId', 'at': 'runAtMs', 'at_alt': 'ts', 'error': 'error'}   # per entry in cron runs
# Gateway RPC (no CLI exists for these)
RPC_PROBE = 'models.probe'                                          # account status per provider: {results:[{profileId,status}]}
RPC_DESCRIBE = 'sessions.describe'                                  # one session: {session: {...} | null}
RPC_PATCH = 'sessions.patch'                                        # set a chat's model@account
RPC_ABORT = 'sessions.abort'                                        # abort a stalled run
PROBE_RESULTS, PROBE_PROFILE, PROBE_STATUS = 'results', 'profileId', 'status'
PROBE_ARGS = lambda provider, agent: {'provider': provider, 'agentId': agent}   # models.probe params
DESCRIBE_ARGS = lambda key: {'key': key}                            # sessions.describe params
PATCH_ARGS = lambda key, ref: {'key': key, 'model': ref}            # sessions.patch params
ABORT_ARGS = lambda key, run: {'key': key, 'runId': run}            # sessions.abort params
S_ROOT = 'session'                                                  # sessions.describe wrapper
S_ACTIVE, S_LAST_RUN = 'hasActiveRun', 'lastRunId'                  # run state of a session (its status word is not read)
S_DELIVERY, S_TO = 'deliveryContext', 'lastTo'
S_ID = 'sessionId'                                                  # persisted session id
S_WRITER, S_LIFECYCLE, S_ACTIVE_IDS = 'activeWriterRunId', 'lifecycleRunId', 'activeRunIds'
S_INTERACTION, A_ABORTED_RUN = 'lastInteractionAt', 'abortedRunId'  # sessions.abort answers the run it stopped
S_PLACEMENT, S_BACKGROUND, S_DEPTH = 'placement', 'isBackground', 'spawnDepth'
P_PROVIDER, P_ENV, WORKER_PROVIDER, WORKER_ENV = 'providerId', 'environmentId', 'device', 'worker:'
WORKER_FIELDS = ('spawnedCwd', 'worktree', 'spawnedBy')               # worker/subagent metadata (also on dashboard keys)
# Agent databases (chat model/account selection and queued input: no CLI or RPC exposes them)
AGENT_DB_PATH = 'agents/{agent}/agent/openclaw-agent.sqlite'        # under the OpenClaw state dir
T_NODES, C_NODE_KEY, C_NODE_ENTRY = 'session_nodes', 'session_key', 'entry_json'           # session entry per key
T_PENDING, C_PEND_KEY, C_PEND_STATE = 'session_pending_inputs', 'session_key', 'state'     # input waiting for a session
PEND_QUEUED = 'queued'                                              # state of a user message not yet answered
# Cron store (a job's recorded account when payload.model names no account: cron list does not show it)
STATE_DB_PATH = 'state/openclaw.sqlite'                             # under the OpenClaw state dir
T_CRON, C_CRON_ID, C_CRON_JSON = 'cron_jobs', 'job_id', 'job_json'  # one row per job
JOB_BINDING = 'inferenceBinding.profileId'                          # the account the job is bound to
E_MODEL, E_PROVIDER, E_MODEL_SRC = 'modelOverride', 'providerOverride', 'modelOverrideSource'   # entry_json keys
E_PROFILE, E_PROFILE_SRC, E_USER = 'authProfileOverride', 'authProfileOverrideSource', 'user'   # 'user' = pinned by hand
# openclaw models status --json: only these fields are read. Never the label next to them, nor
# auth.providers[].profiles.labels: they show masked keys and emails.
MS_PROFILES = ('auth', 'oauth', 'profiles')                          # every profile of the agent's auth store, all types
MS_ID, MS_PROVIDER, MS_TYPE = 'profileId', 'provider', 'type'        # per profile; type is oauth, token or api_key
MS_COUNTS, MS_COUNT_FIELD = ('auth', 'providers'), 'profiles'      # per provider: raw store counts by type
MS_COUNT_TYPES = {'oauth': 'oauth', 'token': 'token', 'api_key': 'apiKey'}   # type -> count field
MS_STATE = 'status'                                                 # per profile: ok, expiring, static, expired, missing
MS_USABLE = ('ok', 'expiring', 'static')                            # credential states OpenClaw can use
MS_TIMEOUT_S = 60                                                    # the command reads local files; ~8 s seen
# Config file
CFG_MODEL = ('agents', 'defaults', 'model')                         # {primary, fallbacks}
CFG_MODELS = ('agents', 'defaults', 'models')                       # non-empty map: allowed model keys; empty: unrestricted
CFG_ORDER = ('auth', 'order')                                       # account order per provider: its targets when not empty
CFG_PROFILES = ('auth', 'profiles')                                 # optional: declared accounts {id: {provider, mode}}
PROFILE_PROVIDER, PROFILE_MODE = 'provider', 'mode'
SUB_MODES = ('oauth', 'token')     # subscription modes, in the order OpenClaw's automatic selection ranks them; never
                                   # api_key (billed per token) unless auth.order lists it
CFG_LOG_FILE = ('logging', 'file')                                  # optional: Gateway log file
CFG_AGENTS = ('agents', 'list')                                     # optional: agent definitions with 'default'
# Gateway log (JSON lines; '1' and '2' are the logger's argument slots)
LOG_GLOB = 'openclaw-*.log'                                         # daily log files
LOG_TMP_DIRS = ('/tmp/openclaw', '/tmp/openclaw-{uid}')             # default log directories
L1, L2, L_META, L_DATE = '1', '2', '_meta', 'date'
L_HTTP, L_BODY, L_PROV, L_MODEL = 'status', 'body', 'provider', 'model'   # fields of a failed model request / run end
EV_AUTH_LIMIT = 'auth_profile_failure_state_updated'              # native-runtime account bookkeeping
NATIVE_OUTCOME = 'recorded message-tool-only run outcome'           # terminal delivery receipt
EV_END = 'embedded_run_agent_end'                                   # a run ended: {runId, isError, failoverReason, provider, model}
L_EVENT, L_RUN, L_ERROR, L_REASON, L_STATUS, L_DONE, L_MILESTONE = 'event', 'runId', 'isError', 'failoverReason', 'runStatus', 'completed', 'milestone'
RUN_END_RE = re.compile(r'\[agent\] run (\S+) ended with stopReason=(\w+)')   # agent-command's plain-text run end
LIMITED = ('rate_limit', 'billing')                                 # failoverReason values that mean a usage limit
RETRY_RE = re.compile(r'transient same-model retry \d+/\d+ for (\S+) reason=rate_limit: delayMs=(\d+) '
                      r'runId="([^"]+)" sessionId="([^"]*)"')      # a run sleeping out a same-model rate-limit wait
REQ_RE = re.compile(r'Model request failed sessionKey="([^"]+)" runId="([^"]+)"')   # a failed model request -> session
KEY_RE = re.compile(r'sessionKey[=:\s\\"]+(agent:[^"\\\s,]+)')         # session key inside any log line
HAND_STOP_RE = re.compile(r'\b(?:chat\.abort requested|sessions\.abort requested: actor=(?!cli\b))')
                                                                    # a person pressed stop (Control UI logs sessions.abort,
                                                                    # actor=openclaw-control-ui). actor=cli is this script.
ANCHORS = ('transient same-model retry', 'Model request failed', EV_END)   # self-check anchors
ANCHOR_DIRS = ('dist', 'node_modules/@openclaw/ai/dist')            # where the anchors live in the package
# Session keys
CHAT_RE = re.compile(r'^agent:[^:]+:(?:main$|dashboard:[^:]+$|(?!a2a:|cron:|subagent:|worker:)[^:]+:(?:group|direct|channel):)')  # human keys; metadata is checked too
LOCAL_CHAT_RE = re.compile(r'^agent:[^:]+:(?:main$|dashboard:[^:]+$)')
GROUP_RE = re.compile(r'^agent:[^:]+:[^:]+:(group|channel):')        # a chat session that is a group or channel
SUB_RE = re.compile(r'^agent:[^:]+:subagent:')                       # a subagent session (its parent waits for it)
CRON_RE = re.compile(r'^agent:[^:]+:cron:([0-9a-f-]{36}):run:')      # a cron run session -> job id
# Error wording. LIMIT_RE: OpenClaw's and the providers' own limit wording only ("quota exceeded" or "429" also appear
# in business errors). Name and pattern are used by other scripts that import this file.
ROUTE_ERR = 'incompatible with its recorded inference account and route'   # raised before the run does anything
LIMIT_RE = re.compile(r"incompatible with its recorded inference account and route|rate_limit_error|"
                      r"exceed your account's rate limit|needs a short break|asking us to slow down|"
                      r"insufficient_quota", re.I)
ERROR_TEXTS = (ROUTE_ERR, 'needs a short break', 'asking us to slow down', 'rate_limit_error', 'insufficient_quota')
#   ^ the parts of LIMIT_RE that OpenClaw itself contains; "exceed your account's rate limit" is provider wording
MODEL_NOT_ALLOWED = 'model not allowed:'                            # permanent model-selection refusal, not a limit
RETRYABLE_RE = re.compile(r'[\"\']?retryable[\"\']?\s*:\s*true\b', re.I)
# =====================================================================================================================

HOME = pathlib.Path.home()
SDIR = pathlib.Path(os.environ.get('OCF_STATE') or os.environ.get('OCF_STATE_DIR')
                    or HOME / '.local/state/openclaw-inference-failover')
STATE, JOURNAL = SDIR / 'state.json', SDIR / 'journal.jsonl'
USAGE = SDIR / 'usage.json'   # claude_usage snapshot: limits, resets and free limit resets per account
OC_DIR = pathlib.Path(os.environ.get('OCF_OPENCLAW_DIR') or os.environ.get('OPENCLAW_STATE_DIR') or HOME / '.openclaw')
CONFIG = os.environ.get('OCF_CONFIG') or os.environ.get('OPENCLAW_CONFIG_PATH') or str(OC_DIR / 'openclaw.json')
FAKE = json.load(open(os.environ['OCF_FAKE'])) if os.environ.get('OCF_FAKE') else None  # tests only
APPLY = '--apply' in sys.argv or '--adopt' in sys.argv
SETTINGS = pathlib.Path(os.environ.get('OCF_SETTINGS') or pathlib.Path(os.environ.get('XDG_CONFIG_HOME') or HOME / '.config')
                        / 'openclaw-inference-failover' / 'settings.json')
NOW = lambda: int(time.time() * 1000)
LOCAL = lambda fmt: time.strftime(fmt, time.localtime())

WINDOW_MS = 90 * 60_000        # run history looked at
PROBE_EVERY_MS = 5 * 60_000    # accounts are asked at least this often while any job or pinned chat uses them
CHECK_EVERY_MS = 30 * 60_000   # cadence of the 24 h check inside --apply passes
FRESH_MS = 20 * 60_000         # a failed chat turn older than this when first seen is not continued
WAIT_MS = 2 * 3600_000         # a failed chat turn waits this long for any live account, then is a problem
STRICT = frozenset(k.strip() for k in os.environ.get('OCF_STRICT_CHATS', '').split(',') if k.strip())
STALL_MS = 300_000             # same-model retry waits above 5 min are the #143274 stall
PASS_SOFT_S = 200              # no new Gateway call is started after this
PASS_HARD_S = int(os.environ.get('OCF_PASS_HARD_S', 270))   # a running call is cut off here: the job's own timeout is 300 s
GONE_MS = 70 * 60_000          # a problem not seen for this long (two 30-min checks) and seen again is a new one
PASS_BUDGET_S = 40             # stop starting new edits after this; the next minute continues
DISABLED_BUDGET_S = float(os.environ.get('OCF_DISABLED_BUDGET_S', 10))   # edits of disabled jobs per pass, after chats
CHAT_MAX_PER_HOUR = 2
HOLD_MS = 3600_000             # a model@account that failed on a limit without naming a wait is not a target this long
RETEST_LEFT_MS = 3600_000      # a hold with more than this left is re-tested with a real request (a provider may reset early)
RETEST_EVERY_MS = 60 * 60_000  # at most one re-test per hold this often
LOCAL_MODEL_CHECKS = os.environ.get('OCF_LOCAL_MODEL_CHECKS', '1') != '0'  # opt out of isolated --local requests
RETEST_PER_PASS = 2            # re-tests per pass at most (one takes ~15 s)
RETEST_TIMEOUT_S = 60
RETEST_ERRORS = 3              # re-tests in a row that failed other than on a limit before it is a problem
VERIFY_MS = 60 * 60_000        # a return to a preferred account needs a real request there answered within this long
VERIFY_PER_PASS = 2            # such requests per pass at most (separate from the hold re-tests)
RETURN_EVERY_MS = 60 * 60_000  # a job or chat is moved by the preference at most once in this long
RETURN_CHATS_PER_PASS = 10     # chats moved by the preference per pass at most (~5 s each)
RETURN_JOBS_PER_PASS = 10      # jobs moved by the preference per pass at most (the Gateway allows ~27 cron writes a window)
ORDER_EVERY_MS = 15 * 60_000   # auth.order of a provider is rewritten at most once in this long
USAGE_EVERY_MS = 5 * 60_000    # an account's limits are read at most once in this long
USAGE_FRESH_MS = 20 * 60_000   # a reading older than this is stale: that model's order falls back to the settings
USAGE_LOST_MS = 60 * 60_000    # an account whose own login read before and has had no usable reading this long: problem
USAGE_INERT_MS = 24 * 3600_000 # the same for an account that never had one (no login yet, no email, only borrowed)
USAGE_BUDGET_S = 30            # no further account is read in a pass after this many seconds of reading
RERAISE_MS = 6 * 3600_000      # an unfixed problem fails the run once, then again only after this long
DOWN_MS = 10 * 60_000          # OpenClaw not answering is a problem only after this long: one slow call is not
PROF = lambda ref: ref.split('@', 1)[1]     # account ids may contain '@' (openai:name@example.com)
MODEL = lambda ref: ref.split('@', 1)[0]


_CACHE = {}


def dig(d, path):
    """Nested value at path (tuple or list of keys), or None."""
    for k in path:
        d = d.get(k) if isinstance(d, dict) else None
    return d


def cached(f):
    """Discovery results do not change within one pass."""
    return lambda: _CACHE[f.__name__] if f.__name__ in _CACHE else _CACHE.setdefault(f.__name__, f())


@cached
def log_dir():
    """Directory of the Gateway's daily logs: logging.file in the config if set, else the per-user temp directory
    (/tmp/openclaw or /tmp/openclaw-<uid>) that holds the newest openclaw-*.log."""
    if os.environ.get('OCF_LOG_DIR'):
        return os.environ['OCF_LOG_DIR']
    try:
        f = (dig(json.load(open(CONFIG)), CFG_LOG_FILE) or '')
        if f:
            return os.path.dirname(os.path.expanduser(f))
    except Exception:
        pass
    dirs = [d.format(uid=os.getuid()) for d in LOG_TMP_DIRS]
    best, t = dirs[0], -1
    for d in dirs:
        for f in glob.glob(os.path.join(d, LOG_GLOB)):
            if os.path.getmtime(f) > t:
                best, t = d, os.path.getmtime(f)
    return best


def agent_ids():
    """Agents of this install, from the agents directory (no agent id is assumed)."""
    return sorted(p.name for p in (OC_DIR / AGENT_DB_PATH.split('/')[0]).glob('*') if (OC_DIR / AGENT_DB_PATH.format(agent=p.name)).exists())


@cached
def default_agent():
    """Agent used for account probes: the config's default agent, else 'main' (OpenClaw's default id), else the first."""
    try:
        for e in dig(json.load(open(CONFIG)), CFG_AGENTS) or []:
            if isinstance(e, dict) and e.get('default') and e.get('id'):
                return e['id']
    except Exception:
        pass
    ids = agent_ids()
    return 'main' if 'main' in ids or not ids else ids[0]


def agent_db(key):
    """Read-only URI of the agent database that holds the session (agent id taken from the session key)."""
    if os.environ.get('OCF_AGENT_DB'):
        return os.environ['OCF_AGENT_DB']
    aid = key.split(':')[1] if key.startswith('agent:') else default_agent()
    return 'file:%s?mode=ro' % (OC_DIR / AGENT_DB_PATH.format(agent=aid))


def pkg_dir():
    """Installed OpenClaw package directory (holds dist/). 'openclaw' on PATH may be a small shell wrapper, so its
    text, every PATH entry and 'npm root -g' are tried."""
    if os.environ.get('OCF_DIST'):
        return pathlib.Path(os.environ['OCF_DIST']).parent
    cands = []
    for d in os.environ.get('PATH', '').split(os.pathsep):
        f = pathlib.Path(d or '.') / 'openclaw'
        if not f.is_file():
            continue
        r = pathlib.Path(os.path.realpath(f))
        cands += [r.parent, r.parent.parent]
        try:
            txt = f.read_text(errors='ignore')[:4000] if f.stat().st_size < 20000 else ''
        except Exception:
            txt = ''
        for m in re.finditer(r'(/[^\s"\'$]*?/openclaw)(?=/|["\'\s])', txt):
            cands.append(pathlib.Path(m.group(1)))
    rc, o, _ = run(['npm', 'root', '-g'], 20) if FAKE is None else (1, '', '')
    if rc == 0 and o.strip():
        cands.append(pathlib.Path(o.strip()) / 'openclaw')
    for c in cands:
        try:
            if (c / 'dist').is_dir() and json.load(open(c / 'package.json')).get('name') == 'openclaw':
                return c
        except Exception:
            continue
    return None


def journal(kind, **kw):
    if not APPLY:
        return
    SDIR.mkdir(parents=True, exist_ok=True)
    with open(JOURNAL, 'a') as f:
        f.write(json.dumps({'t': NOW(), 'at': LOCAL('%Y-%m-%d %H:%M:%S'), 'kind': kind, **kw}, ensure_ascii=False) + '\n')


T0 = time.time()
late = lambda: time.time() - T0 > PASS_SOFT_S
# A call that changes something starts only when its whole timeout fits before the pass deadline: a call cut off
# half-way may still be applied by the Gateway while this script never records it.
fits = lambda timeout_s: PASS_HARD_S - (time.time() - T0) > timeout_s + 10


class Deadline(BaseException):
    """Raised by SIGALRM at PASS_HARD_S, so a pass on a slow Gateway ends through the ledger and save(), not through
    the scheduler's kill (which counts as an error every minute and loses what the pass did)."""


def _alarm(*_):
    raise Deadline()


def prob(out, key, text):
    """A problem this pass could not fix. key is stable across passes (no counters or times)."""
    out['problems'].append((key, text))


def raise_due(state, out):
    """Problems that must fail this pass. Each new one fails two passes in a row (a failure monitor may need a streak of two for a
    frequent job), then stays silent while it is open, until RERAISE_MS. Never three failing passes in a row: the
    scheduler backs a recurring job off 30 s and 60 s after errors one and two, then 5, 15 and 60 min."""
    led, now = state.setdefault('problems', {}), NOW()
    for key, text in out['problems']:
        e = led.get(key)
        if not e or now - e['raised'] >= (4 * RERAISE_MS if key.startswith('selfcheck:') else RERAISE_MS) \
                or now - e.get('seen', e['raised']) > GONE_MS:     # an unchanged self-check finding is raised once a day
            led[key] = {'raised': now, 'seen': now, 'left': 2, 'text': text}
            journal('problem', key=key, text=text)
        else:
            e['text'], e['seen'] = text, now
    for k in [k for k, e in led.items() if now - e.get('seen', e['raised']) > 2 * RERAISE_MS]:
        led.pop(k)
    streak, due = state.get('fail_streak', 0), [e for e in led.values() if e['left'] > 0]
    if streak >= 2 or not due:
        state['fail_streak'] = 0           # a quiet pass resets the scheduler's error count; the rest is raised next
        return []
    for e in due:
        e['left'] = (2 if streak == 0 else e['left']) - 1      # whatever is raised gets its full pair of passes
    state['fail_streak'] = streak + 1
    return sorted(e['text'] for e in due)


def run(argv, timeout=120):
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, stdin=subprocess.DEVNULL)
        return p.returncode, p.stdout, (p.stderr or '')
    except subprocess.TimeoutExpired:
        return 124, '', 'timeout'
    except OSError as e:               # the program is not installed (npm, systemd-run) or cannot start
        return 127, '', str(e)


def gw(method, params, timeout=120):
    rc, out, err = run(['openclaw'] + CLI_CALL(method, params, timeout * 1000 - 5000), timeout)
    try:
        d = json.loads(out[out.index('{'):])
    except Exception:
        return None, (err or out).strip()[-300:] or 'rc=%s' % rc
    if rc != 0 or d.get('ok') is False:
        return None, json.dumps(d.get('error') or d)[:300]
    return d, ''


def ts_ms(rec):
    d = (rec.get(L_META) or {}).get(L_DATE) or ''
    try:
        return calendar.timegm(time.strptime(d[:19], '%Y-%m-%dT%H:%M:%S')) * 1000
    except Exception:
        return 0


def log_files():
    return sorted(glob.glob(os.path.join(log_dir(), LOG_GLOB)), key=os.path.getmtime)[-2:]


def log_lines(needle):
    files = log_files()
    out = []
    for raw in (run(['grep', '-hF', needle] + files, 30)[1] if files else '').splitlines():
        try:
            out.append(json.loads(raw))
        except Exception:
            pass
    return out


def native_limit_terminal(rec):
    """Exact native run quota + failed terminal receipt. Never infer quota from a missing assistant alone.
    Native runs can already have performed tools; only rebind their next turn, never launch a continuation.
    Read matching run metadata across pass/file boundaries; a completed run after profile rotation is excluded.
    """
    one = rec.get(L1)
    if not isinstance(one, dict) or one.get(L_PROV) != 'openai' or one.get(L_STATUS) != 'errored':
        return None
    run_id, key = one.get(L_RUN), one.get('sessionKey')
    if not run_id or not key or not one.get(L_MODEL):
        return None
    t = ts_ms(rec)
    limits, rows = [], log_lines(run_id)
    for row in rows:
        data = row.get(L1)
        if not isinstance(data, dict) or data.get(L_RUN) != run_id:
            continue
        if data.get(L_STATUS) == L_DONE:
            return None
        if (data.get(L_EVENT) == EV_AUTH_LIMIT and data.get(L_PROV) == one[L_PROV]
                and data.get('reason') in LIMITED and ts_ms(row) <= t
                and data.get(L_MODEL, one[L_MODEL]) == one[L_MODEL]):
            limits.append((ts_ms(row), data.get('profileId')))
    if not limits:
        return None
    last_limit = max(at for at, _ in limits)
    for row in rows:
        if ts_ms(row) <= last_limit:
            continue
        data = row.get(L1)
        if isinstance(data, str) and 'post-run auth-profile success bookkeeping completed' in data:
            return None
        if isinstance(data, dict) and data.get(L_RUN) == run_id:
            if data.get(L_MILESTONE) and ts_ms(row) > last_limit + 2000:
                return None
            if data.get(L_EVENT) == EV_END and data.get(L_REASON) not in LIMITED:
                return None
            if (data.get(L_EVENT) == EV_AUTH_LIMIT and data.get(L_PROV) == one[L_PROV]
                    and data.get('reason') not in LIMITED):
                return None
    return {'t': t, 'model': '%s/%s' % (one[L_PROV], one[L_MODEL]),
            'profile_hashes': list(dict.fromkeys(p for _, p in limits if isinstance(p, str))),
            'no_continue': True, 'key': key}


def scan_logs(state):
    """New complete log lines since the last pass -> stalls, failed turns, run->session keys, providers seen limited."""
    ev = {'stalls': [], 'failed': {}, 'run_key': {}, 'providers': set()}
    offs = state.setdefault('offsets', {})
    files = log_files()
    for f in files:
        size = os.path.getsize(f)
        start = offs.get(f, 0) if offs.get(f, 0) <= size else 0
        with open(f, 'rb') as fh:
            fh.seek(start)
            chunk = fh.read()
        chunk = chunk[:chunk.rfind(b'\n') + 1]       # a half-written last line is read next pass
        offs[f] = start + len(chunk)
        for raw in chunk.split(b'\n'):
            if not any(x.encode() in raw for x in LIMITED + (ANCHORS[1], NATIVE_OUTCOME)):
                continue
            try:
                rec = json.loads(raw)
            except Exception:
                continue
            one, two, t = rec.get(L1), rec.get(L2), ts_ms(rec)
            if two == NATIVE_OUTCOME:
                native = native_limit_terminal(rec)
                if native:
                    ev['failed'][one[L_RUN]] = native
                    ev['run_key'][one[L_RUN]] = native['key']
                    ev['providers'].add(one[L_PROV])
            m = REQ_RE.search(two) if isinstance(two, str) else None
            if m and isinstance(one, dict):
                ev['run_key'][m.group(2)] = m.group(1)
                if one.get(L_HTTP) == 429 or LIMITED[0] in json.dumps(one.get(L_BODY) or ''):
                    ev['providers'].add(one.get(L_PROV))
            if isinstance(one, str):
                r = RETRY_RE.search(one)
                if r and int(r.group(2)) >= STALL_MS:
                    k = KEY_RE.search(one)
                    ev['stalls'].append({'model': r.group(1), 'delay': int(r.group(2)), 'run': r.group(3), 't': t,
                                         'key': k.group(1) if k else None, 'session': r.group(4)})
                    ev['providers'].add(r.group(1).split('/')[0])
            elif isinstance(one, dict) and one.get(L_EVENT) == EV_END and one.get(L_ERROR) \
                    and one.get(L_REASON) in LIMITED:
                ev['failed'][one.get(L_RUN)] = {'t': t, 'model': '%s/%s' % (one.get(L_PROV), one.get(L_MODEL))}
                ev['providers'].add(one.get(L_PROV))
    for k in [k for k in offs if k not in files]:
        offs.pop(k)
    ev['providers'].discard(None)
    return ev


def cfg():
    c = json.load(open(CONFIG))
    m = dig(c, CFG_MODEL) or {}
    if not m.get('primary'):
        raise Shape('config %s: no %s.primary' % (CONFIG, '.'.join(CFG_MODEL)))
    prefer, err = load_settings()
    usage, uerr = load_usage()
    eff, src = effective_prefer(prefer, usage)
    return {'primary': m['primary'], 'chain': m.get('fallbacks') or [], 'order': dig(c, CFG_ORDER) or {},
            'profiles': dig(c, CFG_PROFILES) or {}, 'allowed': dig(c, CFG_MODELS) or {},
            'prefer': eff, 'prefer_static': prefer, 'prefer_from': src, 'settings_err': err, 'usage_err': uerr}


def load_settings():
    """The operator's optional settings file: ({model: [accounts in order of preference]}, error or None). A missing
    file is no preference. An unreadable or wrong one is a problem (main raises it) and counts as no preference, so a
    typo never stops the rest of the failover. Accounts of another provider than the model's are an error too."""
    if not SETTINGS.exists():
        return {}, None
    try:
        d = json.loads(SETTINGS.read_text())
    except (OSError, ValueError) as e:
        return {}, 'settings %s: not readable JSON (%s)' % (SETTINGS, str(e)[:80])
    p = d.get('prefer', {}) if isinstance(d, dict) else None
    ok = isinstance(p, dict) and all(isinstance(m, str) and '/' in m and isinstance(a, list) and a and all(
        isinstance(x, str) and x.split(':')[0] == m.split('/')[0] for x in a) for m, a in p.items())
    if not ok:
        return {}, ('settings %s: "prefer" must map "provider/model" to a list of that provider\'s accounts' % SETTINGS)
    return {m: list(dict.fromkeys(a)) for m, a in p.items()}, None


def load_usage():
    """The settings' optional 'usage' logins: ({account: {'dir', 'borrow', 'email'}}, error or None). See
    claude_usage.py. Missing is no usage reading; a wrong one is a problem and counts as none."""
    try:
        d = json.loads(SETTINGS.read_text()) if SETTINGS.exists() else {}
    except (OSError, ValueError):
        return {}, None                    # load_settings reports an unreadable file
    u = d.get('usage', {}) if isinstance(d, dict) else {}
    ok = isinstance(u, dict) and all(isinstance(a, str) and ':' in a and isinstance(s, dict)
                                     and (isinstance(s.get('dir'), str) or isinstance(s.get('borrow'), str))
                                     and all(isinstance(s.get(k), (str, type(None))) for k in ('dir', 'borrow', 'email'))
                                     for a, s in u.items())
    if not ok:
        return {}, ('settings %s: "usage" must map "provider:account" to {"dir": ..., "borrow": ..., "email": ...}; '
                    'the order follows "prefer" until it is fixed' % SETTINGS)
    return u, None


def effective_prefer(prefer, usage):
    """The account order per model that the rest of the script follows, and where it came from ({model: 'usage'}).
    Owner's rule (Alex, IT topic 6601 msg 6885, 18 Nov 2026): get the most out of what we pay, fill the account
    that resets earliest first. So when every account the settings name for a model has a usable reading
    (claude_usage.usable: fresh, verified), the order is computed from the readings: accounts with room left for
    that model (weekly under 100 %, and for Fable its own weekly limit under 100 %) first, earliest reset first, then
    the full ones, the one back soonest first. Otherwise the settings' order stands for that model."""
    if not usage or not prefer:
        return prefer, {}
    snap, now, eff, src = CU.load(USAGE), NOW(), dict(prefer), {}
    for model, accts in prefer.items():
        if all(a in usage for a in accts):
            try:
                o = CU.order(accts, snap, model, now, USAGE_FRESH_MS)
            except (TypeError, ValueError, AttributeError, KeyError):   # a reading of a shape no test has seen
                o = None
            if o:
                eff[model], src[model] = o, 'usage'
    return eff, src


def _fake_http(url, token=None, body=None, timeout=None):
    """Tests: the claude_usage HTTP answers from the fixture (FAKE['usage_api'][token])."""
    api = (FAKE or {}).get('usage_api', {})
    if url == CU.TOKEN_URL:
        new = api.get('refresh', {}).get(body.get('refresh_token'))
        return (200, new) if new else (400, {'_error': 'invalid_grant'})
    one = api.get(token)
    if not one:
        return 401, {'_error': 'unknown token'}
    return 200, one['account' if url.endswith(CU.ACCOUNT_PATH) else 'usage']


def usage_pass(state, out):
    """Re-reads the accounts' limits (at most every USAGE_EVERY_MS each) before the pass decides anything, so the
    order it follows is current. Never fails the pass: an account that cannot be read only makes its models fall
    back to the settings' order. An account without a usable reading (claude_usage.usable) is a problem once that
    lasts USAGE_LOST_MS when its own login read before, else USAGE_INERT_MS (never logged in, no email set, only a
    borrowed login), so an order that never switches on does not go unnoticed."""
    usage = load_usage()[0]
    if not usage:
        return
    t = time.time()
    try:
        snap = CU.collect(usage, USAGE, USAGE_EVERY_MS, _fake_http if FAKE is not None else CU._http,
                          budget_s=USAGE_BUDGET_S)
    except OSError as e:
        journal('usage_failed', error=str(e)[:200])
        return
    errs, inert = state.setdefault('usage_err', {}), state.setdefault('usage_inert', {})
    for p, e in (snap.get('profiles') or {}).items():
        err = None if e.get('ok') else e.get('error')
        if err != errs.get(p):
            journal('usage', account=p, ok=bool(e.get('ok')), error=err, took_s=round(time.time() - t, 1))
        errs[p] = err
        if CU.usable(e, NOW(), USAGE_FRESH_MS):
            inert.pop(p, None)
            continue
        since = inert.setdefault(p, NOW())
        good = CU.current(e)
        if good.get('source') == 'own' and good.get('email_ok') is True and good.get('t'):   # it was usable once
            since, limit = max(since, good['t']), USAGE_LOST_MS
        else:
            limit = USAGE_INERT_MS
        if NOW() - since >= limit:
            why = err or ('the login is not of a verified account: set its "email"' if good.get('email_ok') is not True
                          else 'only a borrowed login: make its own one')
            prob(out, 'usage:' + p, 'no usable reading of the limits of %s since %s (%s); models that name it follow '
                 'the settings\' order' % (p, time.strftime('%d %b %H:%M', time.localtime(since / 1000)), why))
    for d in (errs, inert):
        for p in [p for p in d if p not in usage]:
            d.pop(p)


def note_prefer(state, conf, cfg_err=None):
    """Journals each change of the order a model follows (prefer_order), with where it came from. state's prefer_eff is
    read by others (the SLA judge) as the order the failover follows now, next to last_pass; a pass whose config check
    failed (conf None) follows no order, so it drops prefer_eff, journaled once with the reason, and readers fall back
    to the settings' order instead of a stale one. The next good pass journals every order again."""
    if conf is None:
        if state.pop('prefer_eff', None):
            journal('prefer_order_dropped', why='config check failed: readers use the settings\' order',
                    error=str(cfg_err or '')[:200])
        return
    now = {m: [a, (conf.get('prefer_from') or {}).get(m, 'settings')] for m, a in (conf.get('prefer') or {}).items()}
    for m, v in now.items():
        if (state.get('prefer_eff') or {}).get(m) != v:
            journal('prefer_order', model=m, order=v[0], source=v[1])
    state['prefer_eff'] = now


class Health:
    """Account status per provider from models.probe, asked at most once per pass. A probe tests one model of the
    provider, so 'ok' means the account answers, not that every model on it has quota left."""

    def __init__(self, conf, limited=None):
        # limited: {'model@account': until_ms}, kept in state. The probe tests one model per provider and forgets
        # between passes, so limits seen in real runs (stall delay, failed turns, job receipts) are held here, for the
        # model they were seen on only, until the time the provider named. A probe answer never clears a hold; a real
        # request of that model on that account does (retest_holds), since a provider may reset a limit early.
        self.conf, self.limited, self.cache, self.failed = conf, limited if limited is not None else {}, {}, []
        self.saved, self.saved_err = None, None    # the agent's saved accounts, read once per pass when first needed
        self.saved_state, self.saved_skipped = {}, False   # {profile: credential status}; read skipped for time
        self.no_probe = False                # True: a provider not asked yet this pass answers nothing, unasked

    def probe(self, provider):
        if provider not in self.cache and self.no_probe:
            return {}
        if provider not in self.cache:
            if FAKE is not None:
                time.sleep(FAKE.get('slow_probe', 0))
                res = dict(FAKE.get('probe', {}).get(provider, {}))
            else:
                d, err = gw(RPC_PROBE, PROBE_ARGS(provider, default_agent()), 120)
                res = {r[PROBE_PROFILE]: r.get(PROBE_STATUS) for r in (d or {}).get(PROBE_RESULTS, []) if r.get(PROBE_PROFILE)}
                if d is None:
                    self.failed.append(provider)
                    journal('probe_failed', provider=provider, error=err)
            self.cache[provider] = res
            journal('probe', provider=provider, result=res)
        return self.cache[provider]

    def status(self, profile):
        return self.probe(profile.split(':')[0]).get(profile)

    def held(self, profile, model):
        return self.limited.get('%s@%s' % (model, profile), 0) > NOW()

    def hold(self, profile, until, model):
        k = '%s@%s' % (model, profile)
        self.limited[k] = max(self.limited.get(k, 0), until)

    def alive(self, profile, model):
        # Under an empty auth.order list OpenClaw's probe excludes every account ('unknown', excluded_by_auth_order),
        # so it can prove none live. The owner's rule is to use them all in any order rather than stop: 'unknown' then
        # counts as usable when the saved store says the credential itself is usable (the probe checks nothing else
        # there), and a real limit on the account comes back as a hold from its runs.
        s = self.status(profile)
        usable = s == 'ok' or (s == 'unknown' and self.empty_order(profile.split(':')[0])
                               and self.saved_state.get(profile) in MS_USABLE)
        return usable and not self.held(profile, model)

    def dead(self, profile, model):
        return self.held(profile, model) or self.status(profile) in LIMITED

    def ordered(self, provider):
        """True when a non-empty auth.order lists the provider's targets."""
        return bool((self.conf.get('order') or {}).get(provider))

    def empty_order(self, provider):
        """True when auth.order has the provider's key with an empty list."""
        order = self.conf.get('order') or {}
        return provider in order and not order[provider]

    def saved_store(self):
        """[(profile id, provider, type)] of the agent's saved accounts, read at most once per pass. A failed read is
        journaled and raised as a problem by finish(); the targets are then the config profiles only. Under no_probe a
        read not made yet is not made: the config profiles serve, and nothing is skipped or failed."""
        if self.saved is None and self.no_probe:
            return []
        if self.saved is None:
            self.saved = []
            if not fits(MS_TIMEOUT_S):       # a late pass: the config profiles serve this once, the next pass reads it
                journal('saved_profiles_skipped', why='no time left in this pass')
                self.saved_skipped = True
                return self.saved
            try:
                self.saved, bad, self.saved_state = saved_profiles()
                if bad:
                    self.saved_err = 'saved types disagree with the store for %s' % ', '.join(bad)
                    journal('saved_profiles_failed', error=self.saved_err)
            except (Shape, Unreachable) as e:
                self.saved_err = quiet_error(e)
                journal('saved_profiles_failed', error=self.saved_err)
        return self.saved

    def accounts(self, provider):
        """The accounts that are targets for provider, in order; the one answer for every caller. auth.order[provider]
        when it is a non-empty list, api_key profiles included. Otherwise (no key, or an empty list) every subscription
        account of the provider: its oauth and token profiles in auth.profiles (config order) and in the agent's saved
        auth store (models status order), each once, oauth first. A profile declared in config keeps its config mode.
        OpenClaw breaks ties by last use; this script prefers no account, so that order stands. models status is asked
        only here, so a pass whose providers all have a non-empty auth.order never runs it. An order saved with
        'openclaw models auth order set' is not read; profiles it leaves out probe 'unknown', so they are never chosen."""
        order = (self.conf.get('order') or {}).get(provider)
        if self.ordered(provider):
            return list(order)
        modes = {p: v.get(PROFILE_MODE) for p, v in (self.conf.get('profiles') or {}).items()
                 if isinstance(v, dict) and v.get(PROFILE_PROVIDER) == provider and p.split(':')[0] == provider}
        for p, prov, kind in self.saved_store():
            if prov == provider:
                modes.setdefault(p, kind)
        return [p for m in SUB_MODES for p, pm in modes.items() if pm == m]

    def prefers(self, model):
        """True when the settings name a preference for this model."""
        return bool((self.conf.get('prefer') or {}).get(model))

    def pref(self, model):
        """The model's targets in order of preference: the settings' accounts that are targets, then the other targets in
        accounts() order. Without a preference for the model: accounts() unchanged."""
        accts = self.accounts(model.split('/')[0])
        want = [p for p in (self.conf.get('prefer') or {}).get(model) or [] if p in accts]
        return want + [p for p in accts if p not in want]

    def target(self, model, avoid=(), chain=True):
        """First live model@account: the same model on the first live of its pref() accounts, then the config fallback
        chain. avoid is the account this model just failed on, and it is skipped for this model only: a later model may
        use that account, because a limit on one model is not a limit on the next."""
        for m in dict.fromkeys([model] + (list(self.conf['chain']) if chain else [])):
            blocked = avoid if m == model else ()
            for p in self.pref(m):
                if p not in blocked and self.alive(p, m):
                    return m + '@' + p
        return None

    def unknown(self):
        """True when a probe failed or timed out for a target account (accounts()): 'nothing alive' is then unproven.
        auth, no_model and format are definite answers (that account cannot be used), so an expired token does not hide
        'nothing alive'. An account that is not a target is never chosen, so its answer does not matter. For a provider
        without a non-empty auth.order, 'unknown' counts as a definite answer: OpenClaw reports it for a profile that an
        order saved with 'openclaw models auth order set' leaves out (excluded_by_auth_order), which this script cannot
        read. A failed models status read does not count: the config profiles are then the targets. A read skipped
        because the pass ran out of time does: saved-only accounts were not looked at."""
        if self.saved_skipped:
            return True
        def open_(prov):
            return ('timeout', 'unknown', None) if self.ordered(prov) else ('timeout', None)
        return bool(self.failed) or any(s in open_(prov) for prov, r in list(self.cache.items())
                                        for p, s in r.items() if p in self.accounts(prov))


def retest_one(ref):
    """One real minimal request of this model on this account (CLI_RETEST). Returns ('ok' | 'limit' | 'error', the time
    the provider named for a limit in ms or None, a short reason)."""
    if FAKE is not None:                   # tests: {ref: {rc, out, err}}
        time.sleep(FAKE.get('slow_retest', 0))
        a = FAKE.get('retest', {}).get(ref) or {'rc': 1, 'err': 'fake: no answer for ' + ref}
        rc, out, err = a.get('rc', 0), a.get('out', ''), a.get('err', '')
        with open(os.environ['OCF_FAKE_CALLS'], 'a') as f:
            f.write(' '.join(CLI_RETEST(ref, 'main')) + '\n')
    else:
        rc, out, err = run(['openclaw'] + CLI_RETEST(ref, default_agent()), RETEST_TIMEOUT_S)
    d = None
    for m in re.finditer(r'(?m)^\{', out or ''):          # the answer is one JSON object; log lines may come before it
        try:
            d = json.JSONDecoder().raw_decode(out[m.start():])[0]
            break
        except ValueError:
            continue
    d = d if isinstance(d, dict) else {}
    texts = [o.get('text') for o in d.get(RETEST_OUTPUTS) or [] if isinstance(o, dict)]
    if rc == 0 and d.get(RETEST_OK) is True and any(isinstance(t, str) and t.strip() for t in texts):
        return 'ok', None, ''
    e = d.get(RETEST_ERROR)
    msg = (e.get('message') if isinstance(e, dict) else e if isinstance(e, str) else None) or \
        (err or out or '').strip()[-300:] or ('timeout' if rc == 124 else 'rc=%s' % rc)
    both = '%s\n%s' % (msg, err or '')
    if LIMIT_RE.search(both) or re.search(r'status=429\b|^429\b', both, re.M):
        w = RETRY_AFTER_RE.search(both)
        return 'limit', (NOW() + int(w.group(1)) * 1000) if w else None, msg[:300]
    return 'error', None, msg[:300]


def retest_holds(state, health, out):
    """A provider may end a limit before the time it named (Anthropic resets a weekly limit early), and the probe tests
    another model, so it cannot see that. A hold with more than RETEST_LEFT_MS left is re-tested with one real request
    of that model on that account, at most once per RETEST_EVERY_MS and RETEST_PER_PASS per pass, only while the pass
    has time. An answer clears the hold (the next pass moves work back by the usual rules); a limit keeps it, at the
    newly named time if any; anything else keeps it and becomes a problem after RETEST_ERRORS in a row."""
    if not LOCAL_MODEL_CHECKS:
        return
    hm = lambda t: time.strftime('%d %b %H:%M', time.localtime(t / 1000))
    limited, now = health.limited, NOW()
    tested, errs = state.setdefault('retested', {}), state.setdefault('retest_errors', {})
    for led in (tested, errs):
        for k in [k for k in led if k not in limited]:
            led.pop(k)
    due = sorted((tested.get(k, 0), k) for k, u in limited.items()
                 if u - now > RETEST_LEFT_MS and now - tested.get(k, 0) >= RETEST_EVERY_MS)
    for _, k in due[:RETEST_PER_PASS]:
        if late() or not fits(RETEST_TIMEOUT_S):
            break
        if not APPLY:                      # a dry run makes no request
            out['retest_due'].append(k)
            continue
        tested[k] = NOW()                  # first: a re-test cut off by the deadline still counts
        res, until, why = retest_one(k)
        if res == 'ok':
            was = limited.pop(k, 0)
            errs.pop(k, None)
            state.setdefault('verified', {})[k] = NOW()   # a return there needs no second request this hour
            state['retry'] = True          # the next pass asks the accounts and moves work back
            journal('hold_cleared', key=k, why='a real request of this model on this account answered; the hold ran '
                                                'until %s' % hm(was))
            out['cleared'].append(k)
        elif res == 'limit':
            errs.pop(k, None)
            if until:
                limited[k] = until
            journal('hold_retest', key=k, result='limit', until=hm(limited.get(k, 0)), error=why)
        else:
            n = errs[k] = errs.get(k, 0) + 1
            journal('hold_retest', key=k, result='error', in_a_row=n, error=why)
            if n >= RETEST_ERRORS:
                prob(out, 'retest:' + k, 'hold %s: its re-test failed %d times in a row (%s); the hold stays'
                     % (k, n, why[:120]))


_VERIFY = {'n': 0, 'failed': set()}    # this pass: real requests made for returns, and pairs that did not answer


def verify(state, health, ref, out, request=True):
    """True when a real request of this model on this account answered within VERIFY_MS. A return to a preferred
    account waits for that proof: the probe tests another model, and a hold that ran out (or an hour's hold for a limit
    that named no time) does not show the limit is over, so without it work would bounce back onto a model still at
    its limit every hour. Otherwise, when request is set and the pass has room, makes that request now (at most
    VERIFY_PER_PASS per pass, once an hour per pair after one that did not answer; a dry run makes none). A limit holds
    the pair until the time the provider named (an hour when it names none)."""
    v, tried = state.setdefault('verified', {}), state.setdefault('verify_tried', {})
    if NOW() - v.get(ref, 0) < VERIFY_MS:
        return True
    if not LOCAL_MODEL_CHECKS or not request or _VERIFY.get('busy') or ref in _VERIFY['failed'] or NOW() - tried.get(ref, 0) < RETEST_EVERY_MS:
        return False                       # busy: a pass with fresh limits keeps its time for them; returns wait
    if not APPLY:
        if ref not in out['verify_due']:
            out['verify_due'].append(ref)
        return False
    if _VERIFY['n'] >= VERIFY_PER_PASS or late() or not fits(RETEST_TIMEOUT_S):
        return False
    _VERIFY['n'] += 1
    tried[ref] = NOW()                     # first: a request cut off by the deadline still counts
    res, until, why = retest_one(ref)
    journal('return_check', key=ref, result=res, **({'error': why} if why else {}))
    errs = state.setdefault('verify_errors', {})
    if res == 'ok':
        v[ref] = NOW()
        tried.pop(ref, None)
        errs.pop(ref, None)
        return True
    _VERIFY['failed'].add(ref)
    if res == 'error':                     # not a limit: returns there stop, so a streak of these must reach someone
        n = errs[ref] = [(errs.get(ref) or [0])[0] + 1, NOW()]
        if n[0] >= RETEST_ERRORS:
            prob(out, 'return_check:' + ref, 'return to %s: its check failed %d times in a row (%s); work stays where it is'
                 % (ref, n[0], why[:120]))
    else:
        errs.pop(ref, None)
    if res == 'limit':
        health.hold(PROF(ref), until or NOW() + HOLD_MS, MODEL(ref))
        state.setdefault('retested', {})[ref] = NOW()    # this was the hold's re-test too: not again within the hour
    return False


def better(state, health, model, acct, out, request=True):
    """model@account to move work of this model to from acct by the preference, or None: the first account of
    pref(model) above acct that is live (probe ok, not held for this model) and verified by a real request. Only for a
    model the settings name; never while acct itself is out (that is a move off a held account, not a return)."""
    if not health.prefers(model) or not acct or acct.split(':')[0] != model.split('/')[0] or health.dead(acct, model):
        return None                        # a stored account of another provider: the model is not what it runs
    order = health.pref(model)
    for p in order[:order.index(acct) if acct in order else len(order)]:
        if health.alive(p, model) and verify(state, health, '%s@%s' % (model, p), out, request):
            return '%s@%s' % (model, p)
    return None


def cleanup_prefer(state):
    """Forget proofs and per-item marks once they no longer matter."""
    now = NOW()
    for led, ttl in (('verified', VERIFY_MS), ('verify_tried', RETEST_EVERY_MS), ('pref_moved', RETURN_EVERY_MS)):
        d = state.get(led) or {}
        for k in [k for k, t in d.items() if now - t >= ttl]:
            d.pop(k)
    errs = state.get('verify_errors') or {}
    for k in [k for k, (n, t) in errs.items() if now - t > 2 * RETEST_EVERY_MS]:
        errs.pop(k)                        # no check of it for two hours (nothing to return there): the streak is over


def cli_json(argv, timeout=75):
    """One documented CLI command with JSON output, at most once per pass (cached). Raises Shape when the answer is not
    the expected JSON object, so a pass never acts on a misread job list."""
    k = ' '.join(argv)
    if k not in _CACHE:
        rc, out, err = run(['openclaw'] + argv, timeout)
        why = (err or out).strip()[-200:]
        if rc != 0 and re.search(r'unknown (option|command)|too many arguments|invalid argument', why, re.I):
            raise Shape('cli: openclaw %s is not accepted (%s)' % (k, why))
        if rc != 0:
            raise Unreachable('openclaw %s: rc=%s %s' % (k, rc, why))
        try:
            d = json.loads(out[out.index('{'):])
        except Exception:
            raise Unreachable('openclaw %s: no complete JSON answer' % k)
        if not isinstance(d, dict):
            raise Shape('cli: openclaw %s: answer is not a JSON object' % k)
        _CACHE[k] = d
    return _CACHE[k]


def saved_profiles():
    """[(profile id, provider, type)] of every account in the default agent's auth store, saved-only ones included.
    Reads id, provider and type only: a profile's label shows a masked key or an email.
    Returns (rows, providers whose rows were dropped). A plugin's runtime credential can override the reported type for
    a whole provider (e.g. an API key in the environment), so a provider's rows are kept only when their types match
    the raw store counts in auth.providers[].profiles; a row whose id prefix is not its provider is skipped, because
    the probe of that account would ask another provider."""
    d = cli_json(CLI_MODELS(default_agent()), MS_TIMEOUT_S)
    rows = dig(d, MS_PROFILES)
    if not isinstance(rows, list) or not all(isinstance(r, dict) and all(isinstance(r.get(k), str) for k in
                                                                         (MS_ID, MS_PROVIDER, MS_TYPE)) for r in rows):
        raise Shape(MS_SHAPE_ERR)
    counts = {}
    for p in dig(d, MS_COUNTS) or []:
        c = p.get(MS_COUNT_FIELD) if isinstance(p, dict) else None
        if isinstance(c, dict) and isinstance(p.get(MS_PROVIDER), str):
            counts[p[MS_PROVIDER]] = {t: c.get(f) for t, f in MS_COUNT_TYPES.items()}
    rows = [(r[MS_ID], r[MS_PROVIDER], r[MS_TYPE]) for r in rows]
    bad = sorted(pv for pv in {pv for _, pv, _ in rows}
                 if counts.get(pv) != {t: sum(1 for _, q, k in rows if q == pv and k == t) for t in MS_COUNT_TYPES})
    state = {r[MS_ID]: r.get(MS_STATE) for r in dig(d, MS_PROFILES)}
    return [r for r in rows if r[1] not in bad and r[0].split(':')[0] == r[1]], bad, state


MS_SHAPE_ERR = 'cli: openclaw %s: no list %s of {%s, %s, %s}' % (' '.join(CLI_MODELS('<agent>')), '.'.join(MS_PROFILES),
                                                                 MS_ID, MS_PROVIDER, MS_TYPE)


def quiet_error(e):
    """A failed models status read without the command's output, which may carry account labels."""
    s = str(e)
    if s == MS_SHAPE_ERR:
        return s
    for t in ('timeout', 'no time left in this pass', 'no complete JSON answer', 'is not accepted', 'not a JSON object'):
        if t in s:
            return t
    m = re.search(r'rc=-?\d+', s)
    return m.group(0) if m else type(e).__name__


class Shape(Exception):
    """An OpenClaw answer or file does not have the shape this script was written for."""


class Unreachable(Exception):
    """The Gateway or the CLI did not answer: nothing is known, nothing is changed this pass."""


def openclaw_version():
    rc, out, _ = run(['openclaw'] + CLI_VERSION, 30)
    m = re.search(r'\d{4}\.\d+\.\d+\S*', out)
    if rc != 0 or not m:
        raise Unreachable('openclaw %s: rc=%s' % (' '.join(CLI_VERSION), rc))
    return m.group(0)


def selfcheck():
    """Checks every item of the OPENCLAW SURFACE section against the installed OpenClaw; returns the missing ones.
    Reads only: the job list and run history (cached for the pass), one sessions.describe of a session that does not
    exist, the config, the agent database schemas, the log directory, a text search of the installed package and the
    help of the re-test command."""
    miss = []

    def guard(f):
        try:
            f()
        except Shape as e:
            miss.append(str(e))

    def jobs_ok():
        jobs = cron_jobs()
        has = lambda k: any(dig(j, JOB_F[k].split('.')) is not None for j in jobs)
        for k in (JOB_REQUIRED + (JOB_REQUIRED_RUN if has('last_at') else ())) if jobs else ():
            if not has(k):
                miss.append('cli: openclaw %s: field %s' % (' '.join(CLI_JOBS), JOB_F[k]))
        refs = [r for r in (dig(j, JOB_F['model'].split('.')) for j in jobs) if isinstance(r, str)]
        if refs and not any('@' in r for r in refs):
            miss.append('cli: openclaw %s: %s carries no @account' % (' '.join(CLI_JOBS), JOB_F['model']))

    def runs_ok():
        e = (cron_errors() or [{}])[0]
        if e and (RUN_F['job'] not in e or not (RUN_F['at'] in e or RUN_F['at_alt'] in e) or RUN_F['error'] not in e):
            miss.append('cli: openclaw %s: fields %s' % (' '.join(CLI_RUNS), '/'.join(sorted(set(RUN_F.values())))))

    def describe_ok():
        if FAKE is not None:
            d, err = FAKE.get('describe_shape', {S_ROOT: None}), ''
        else:
            d, err = gw(RPC_DESCRIBE, DESCRIBE_ARGS('agent:%s:ocf-selfcheck-none' % default_agent()), 60)
        if d is None and ('unknown method' in err.lower() or 'not found' in err.lower() and 'method' in err.lower()):
            miss.append('rpc: %s' % RPC_DESCRIBE)
        elif d is None:
            raise Unreachable('%s: %s' % (RPC_DESCRIBE, err))
        elif S_ROOT not in d:
            miss.append('rpc: %s: no "%s" in the answer' % (RPC_DESCRIBE, S_ROOT))

    def config_ok():
        try:
            c = json.load(open(CONFIG))
        except Exception:
            return miss.append('config: %s is not readable JSON' % CONFIG)
        if not (dig(c, CFG_MODEL) or {}).get('primary'):
            miss.append('config: %s.primary' % '.'.join(CFG_MODEL))

    def tables_ok():
        ids = agent_ids() if not os.environ.get('OCF_AGENT_DB') else ['(OCF_AGENT_DB)']
        if not ids:
            miss.append('files: no agent database %s under %s' % (AGENT_DB_PATH, OC_DIR))
        for aid in ids:
            uri = os.environ.get('OCF_AGENT_DB') or 'file:%s?mode=ro' % (OC_DIR / AGENT_DB_PATH.format(agent=aid))
            try:
                c = sqlite3.connect(uri, uri=True)
                for t, cols in ((T_NODES, (C_NODE_KEY, C_NODE_ENTRY)), (T_PENDING, (C_PEND_KEY, C_PEND_STATE))):
                    have = {r[1] for r in c.execute('pragma table_info(%s)' % t)}
                    for col in cols:
                        if col not in have:
                            miss.append('table: %s.%s (agent %s)' % (t, col, aid))
                c.close()
            except sqlite3.Error as e:
                miss.append('table: agent %s database not readable (%s)' % (aid, e))
        try:
            c = sqlite3.connect(os.environ.get('OCF_STATE_DB') or 'file:%s?mode=ro' % (OC_DIR / STATE_DB_PATH), uri=True)
            have = {r[1] for r in c.execute('pragma table_info(%s)' % T_CRON)}
            c.close()
            for col in (C_CRON_ID, C_CRON_JSON):
                if col not in have:
                    miss.append('table: %s.%s (cron store)' % (T_CRON, col))
        except sqlite3.Error as e:
            miss.append('table: cron store %s not readable (%s)' % (STATE_DB_PATH, e))

    def logs_ok():
        if not log_files():
            miss.append('log: no %s in %s' % (LOG_GLOB, log_dir()))

    def package_ok():
        pkg = pkg_dir()
        dirs = [str(pkg / d) for d in ANCHOR_DIRS if pkg and (pkg / d).is_dir()]
        if not dirs:
            return miss.append('package: installed OpenClaw package (dist) not found')
        want = ANCHORS + ERROR_TEXTS + (RPC_PROBE, RPC_DESCRIBE, RPC_PATCH, RPC_ABORT)
        rc, o, _ = run(['grep', '-rhoF'] + [x for a in want for x in ('-e', a)] + ['--include=*.js', '--include=*.mjs'] + dirs, 120)
        if rc == 124:
            raise Unreachable('package search timed out')
        found = set(o.split('\n'))
        miss.extend('log: anchor "%s"' % a for a in ANCHORS if a not in found)
        miss.extend('error text: "%s"' % a for a in ERROR_TEXTS if a not in found)
        miss.extend('rpc: %s' % a for a in (RPC_PROBE, RPC_DESCRIBE, RPC_PATCH, RPC_ABORT) if a not in found)

    def retest_ok():
        if FAKE is not None:
            return
        for argv, opts in (RETEST_HELP,):   # not CLI_ORDER_SET: only its own write fails without it (a problem)
            rc, o, e = run(['openclaw'] + argv, 60)
            if rc == 124:
                raise Unreachable('openclaw %s timed out' % ' '.join(argv))
            miss.extend('cli: openclaw %s: option %s' % (' '.join(argv[:-1]), x) for x in opts if x not in o + e)

    for f in (config_ok, jobs_ok, runs_ok, describe_ok, tables_ok, logs_ok, package_ok, retest_ok):
        guard(f)
    return sorted(set(miss))


def down(state, out, e, slow=False, part=None):
    """OpenClaw did not answer, or (slow) answered so slowly that the pass was cut off after saving what it did.
    Either is one outage: one clock from the start of the first pass it hit, one ledger key, a problem only once it
    has lasted DOWN_MS. Cleared by a pass that gets all its answers, or by a self-check finding (raised on its own).
    A part that does not run every pass (part: the 30-min check, the disabled jobs) keeps its own clock as well, which
    only that part finishing clears: the ordinary passes between its runs get their answers and stop the main one."""
    start = NOW() - int((time.time() - T0) * 1000)                                     # this pass's start, on NOW's clock
    since = state.setdefault('down_since', start)
    if part:
        since = min(since, state.setdefault('cut_since', {}).setdefault(part, start))
    if NOW() - since >= DOWN_MS:
        prob(out, 'gateway', ('OpenClaw has answered too slowly for %d min: %s' if slow else
                              'OpenClaw has not answered for %d min, nothing was changed: %s')
             % ((NOW() - since) // 60_000, str(e)[:200]))


def part_done(state, part):
    """A part that does not run every pass finished: its own clock (see down) stops."""
    cut = state.get('cut_since') or {}
    cut.pop(part, None)
    if not cut:
        state.pop('cut_since', None)


jobs_partial = lambda: bool(cli_json(CLI_JOBS).get(LIST_MORE))   # the job list did not fit one page


def cron_jobs():
    d = cli_json(CLI_JOBS)
    if not isinstance(d.get(JOBS_LIST), list):
        raise Shape('openclaw %s: no "%s" list' % (' '.join(CLI_JOBS), JOBS_LIST))
    return d[JOBS_LIST]


def cron_errors():
    """Recent failed runs of all jobs, newest first (one page; history older than the page is not needed)."""
    d = cli_json(CLI_RUNS)
    if not isinstance(d.get(RUNS_LIST), list):
        raise Shape('openclaw %s: no "%s" list' % (' '.join(CLI_RUNS), RUNS_LIST))
    return d[RUNS_LIST]


CRON_STORE_ERR = []                       # a failed read of the cron store this pass, raised as a problem by main


def cron_bindings():
    """{job id: recorded account} from the cron store, read-only, for jobs whose payload.model names no account."""
    uri = os.environ.get('OCF_STATE_DB') or 'file:%s?mode=ro' % (OC_DIR / STATE_DB_PATH)
    try:
        c = sqlite3.connect(uri, uri=True)
        try:
            rows = c.execute('select %s, %s from %s' % (C_CRON_ID, C_CRON_JSON, T_CRON)).fetchall()
        finally:
            c.close()
    except sqlite3.Error as e:            # only these jobs go unwatched; jobs naming their account and chats carry on
        CRON_STORE_ERR.append('cron store %s not readable: %s' % (STATE_DB_PATH, str(e)[:120]))
        return {}
    out = {}
    for jid, j in rows:
        try:
            p = dig(json.loads(j or '{}'), JOB_BINDING.split('.'))
        except ValueError:
            continue
        if isinstance(p, str) and p:
            out[jid] = p
    return out


def load_jobs(state):
    """Jobs with their binding (payload.model = 'provider/model@account') and per model@account the limit failures in a
    row. A failure from before the job's last rebinding belongs to the previous binding; the time of a rebinding is this
    script's own record (state 'bindings'), not the job's updated time."""
    seen, now = state.setdefault('bindings', {}), NOW()
    start = state.setdefault('started', now)   # first pass ever: history from before it was not watched by this script
    jobs, bound = {}, None
    for d in cron_jobs():
        f = lambda k: dig(d, JOB_F[k].split('.'))
        jid, ref = f('id'), f('model')
        if not jid:
            raise Shape('openclaw %s: a job without "%s"' % (' '.join(CLI_JOBS), JOB_F['id']))
        profile = None
        if ref and '@' in ref:
            profile = PROF(ref)
        elif ref or f('pkind') == AGENT_TURN:   # bound to an account without naming it: the cron store records it
            bound = cron_bindings() if bound is None else bound
            profile = bound.get(jid)
        full = ref if ref and '@' in ref else \
            ('%s@%s' % (MODEL(ref) if ref else cfg()['primary'], profile) if profile else ref)   # model@account
        rec = seen.get(jid)
        if not rec or rec.get('model') != full:  # first sight, or rebound by someone else since (model or account)
            # t: when the job was last rebound (0 = unknown). Failures and stalls from before t, or from before this
            # script's first pass, do not count against the current binding.
            rec = seen[jid] = {'model': full, 't': 0 if not rec else now}
        jobs[jid] = {'enabled': bool(f('enabled')), 'name': f('name') or '', 'model': ref, 'profile': profile,
                     'ref': full if profile else None, 'since': max(rec['t'], start),
                     'mode': f('mode'), 'next': f('next') or 0, 'kind': f('kind'),
                     'last': f('last'), 'last_at': f('last_at') or 0, 'streak_n': f('streak') or 0, 'errors': []}
    for k in [k for k in seen if k not in jobs and not jobs_partial()]:
        seen.pop(k)
    # The run history is read only when some job is in a fresh error streak: a quiet pass makes one CLI call.
    if any(j['last'] == 'error' and j['last_at'] > now - WINDOW_MS for j in jobs.values()):
        for e in cron_errors():
            j, at = jobs.get(e.get(RUN_F['job'])), e.get(RUN_F['at']) or e.get(RUN_F['at_alt']) or 0
            if j is not None and at > now - WINDOW_MS:
                j['errors'].append((at, bool(LIMIT_RE.search(e.get(RUN_F['error']) or ''))))
        for j in jobs.values():
            j['errors'] = sorted(j['errors'], reverse=True)[:j['streak_n'] if j['last'] == 'error' else 0]
    return jobs, streaks(jobs)


def streaks(jobs):
    """Per model@account: limit failures in a row, newest first, across the jobs bound to it since their rebinding. A
    job's newest ok run (lastRunStatus) ends the streak; so does a failure for another reason. Jobs of another model on
    the same account are another streak: a limit on one model is not a limit on the account."""
    last_ok, ev = {}, {}
    for j in jobs.values():
        p = j['ref'] if j['profile'] else None    # model@account, also when payload.model names no account
        if not p:
            continue
        if j['last'] == 'ok' and j['last_at'] >= j['since']:
            last_ok[p] = max(last_ok.get(p, 0), j['last_at'])
        ev.setdefault(p, []).extend(e for e in j['errors'] if e[0] >= j['since'])
    out = {}
    for p, seq in ev.items():
        n = 0
        for at, bad in sorted(seq, reverse=True):
            if not bad or at <= last_ok.get(p, 0):
                break
            n += 1
        if n:
            out[p] = n
    return out


def cron_edit(jid, model, timeout=180):
    if not APPLY:
        return 'dry-run'
    if FAKE is not None:          # tests: rebind in the fixture database
        time.sleep(FAKE.get('slow_edit', 0))
        c = sqlite3.connect(os.environ['OCF_FAKE_DB'])
        d = json.loads(c.execute('select job_json from cron_jobs where job_id=?', (jid,)).fetchone()[0])
        d['payload']['model'], d['inferenceBinding'] = model, {'profileId': PROF(model)}
        c.execute('update cron_jobs set job_json=?, updated_at=? where job_id=?', (json.dumps(d), NOW(), jid))
        c.commit()
        return FAKE.get('edit_result', 'ok')
    rc, out, err = run(['openclaw'] + CLI_EDIT(jid, model), timeout)
    return 'ok' if rc == 0 else ('write_limited' if 'retry after' in (err + out).lower() else 'rc=%s %s' % (rc, (err or out).strip()[-200:]))


def park(state, kind, ident, own, tgt):
    """Remember that a job or chat sits on a fallback model. The first time it goes home as soon as an account of its
    own model is live; every repeat within a day waits an hour longer, so a model that the probe cannot see as
    exhausted does not make it bounce. The count starts again once a day passes without a park."""
    seen, now = state.setdefault('parks', {}), NOW()
    n = seen[ident]['n'] + 1 if now - seen.get(ident, {}).get('t', 0) < 86400_000 else 1
    seen[ident] = {'n': n, 't': now}
    state[kind][ident] = {'model': own, 'parked': tgt, 'at': now, 'wait': HOLD_MS * (n - 1)}
    for k in [k for k, v in seen.items() if now - v['t'] > 86400_000]:
        seen.pop(k)


ripe = lambda h: NOW() - h.get('at', 0) >= h.get('wait', 0)


def forget_home(state, kind, ident, reason):
    """A removed model cannot come back through a provider probe; keep the working fallback and retire its return."""
    h = state.get(kind, {}).pop(ident, None)
    if h is not None:
        journal('home_gone', **{'session' if kind == 'chat_home' else 'job': ident}, model=h['model'], reason=reason)
        for prefix in (('chat_return:', 'chat_move:') if kind == 'chat_home' else ('rebind:',)):
            key = prefix + ident
            text = state.get('problems', {}).get(key, {}).get('text', '')
            if prefix == 'chat_return:' or 'to %s@' % h['model'] in text:
                state.get('problems', {}).pop(key, None)   # a failure to move elsewhere is still a problem


def prune_homes(state, kind, conf):
    """The model allowlist, not a provider's health, decides whether a saved home still exists."""
    allowed = conf.get('allowed')
    for ident, h in list(state.get(kind, {}).items()):
        if allowed and h['model'] not in allowed:
            forget_home(state, kind, ident, 'model removed from ' + '.'.join(CFG_MODELS))


def home_rejected(state, kind, ident, tgt, err):
    """Backstop a config change after discovery without hiding transient or unrelated return failures."""
    h = state.get(kind, {}).get(ident)
    if h and MODEL(tgt) == h['model'] and MODEL_NOT_ALLOWED in err.lower() and not RETRYABLE_RE.search(err):
        forget_home(state, kind, ident, err)
        return True
    return False


def jobs_pass(conf, jobs, health, state, t0, out, disabled=False):
    """Every enabled job whose model@account is held or probed out moves to a live one, soonest-due first. A job parked
    on a fallback model goes back to its own model once an account of that model is live and not held for it. A job
    is only rebound, never run: its next scheduled run uses the new binding.
    disabled=True covers trusted recurring disabled jobs after the enabled work and chats; they stay disabled.
    On probing passes they may discover missing source and fallback providers, otherwise the caller sets
    health.no_probe to use existing answers only. Their edits get DISABLED_BUDGET_S, excluding discovery, so a
    slow probe cannot spend every pass's edit allowance. No target is not a problem; a failed edit still is.
    Disabled one-shot (at) and legacy account-mode jobs are left alone."""
    edit_s = 0
    home = state.setdefault('home', {})      # {job: {'model': its own model, 'parked': 'fallback@account'}}
    for jid, u in list(state.setdefault('unsure', {}).items()) if not disabled else ():   # once, before any edit
        state['unsure'].pop(jid)             # an edit that reported failure (a timeout) may still have landed
        if jid in jobs and jobs[jid]['model'] == u['tgt']:
            journal('job_move', job=jid, name=jobs[jid]['name'][:60], frm=u['frm'], to=u['tgt'], result='ok',
                    note='the edit reported a failure but was applied')
            if u.get('why') == 'preference':
                state.setdefault('pref_moved', {})[jid] = NOW()
                if jid in home:
                    home[jid]['parked'] = u['tgt']    # same fallback model, preferred account: still on its way home
            elif MODEL(u['tgt']) != u['own']:
                park(state, 'home', jid, u['own'], u['tgt'])   # also when it was parked elsewhere before: new place
            else:
                home.pop(jid, None)
            out['moved'].append((jid[:8], u['tgt']))            # counts as done: the pass state is not rolled back
            state.get('problems', {}).pop('rebind:' + jid, None)   # the failure reported last pass did not happen
    prune_homes(state, 'home', conf)       # also covers disabled jobs and homes restored by an uncertain edit
    todo, pref_moved = [], state.setdefault('pref_moved', {})
    for jid, j in jobs.items():
        if j['enabled'] == disabled:
            continue                         # the other call's job
        if disabled and j['mode'] != JOB_TRUSTED:
            continue                         # no discovery or edits for legacy account-mode jobs
        if not j['profile'] or disabled and j['kind'] == AT_KIND:
            home.pop(jid, None)
            continue
        ref = j['model'] or conf['primary'] + '@' + j['profile']
        if jid in home and home[jid]['parked'] != ref:
            home.pop(jid)                    # somebody rebound it since: that choice stands
        if health.dead(j['profile'], MODEL(ref)):
            todo.append((j['next'], jid, None, 'out'))
        elif jid in home and ripe(home[jid]) and health.target(home[jid]['model'], chain=False):
            todo.append((j['next'], jid, health.target(home[jid]['model'], chain=False), 'home'))
        elif j['mode'] == JOB_TRUSTED and health.prefers(MODEL(ref)) and NOW() - pref_moved.get(jid, 0) >= RETURN_EVERY_MS:
            todo.append((j['next'], jid, None, 'preference'))   # decided below, after every move that must happen
    for jid in [k for k in home if k not in jobs and not jobs_partial()]:
        home.pop(jid)
    n_pref, rank = 0, {'out': 0, 'home': 1}
    for _, jid, back, why in sorted(todo, key=lambda x: (rank.get(x[3], 2), x[0], x[1])):
        j = jobs[jid]
        if j['mode'] != JOB_TRUSTED:
            if disabled:
                continue
            prob(out, 'legacy:' + jid, 'legacy account-mode job %s (%s) is on exhausted %s; recreate it as an operator job'
                 % (jid[:8], j['name'][:40], j['profile']))
            continue
        if (edit_s >= DISABLED_BUDGET_S) if disabled else (time.time() - t0 > PASS_BUDGET_S):
            if why != 'preference':          # a return may wait for the next probing pass; it forces no early one
                out['later' if disabled else 'deferred'] += 1
            continue
        model = MODEL(j['model'] or conf['primary'])
        own = home.get(jid, {}).get('model', model)
        if why == 'preference':              # a return: its real request, if any, comes after the urgent moves
            if n_pref >= RETURN_JOBS_PER_PASS:
                continue                     # leaves cron writes of this window to moves off held accounts
            back = better(state, health, model, j['profile'], out, request=not disabled)
            if not back:
                continue
            n_pref += 1
        tgt = back or health.target(own, avoid=(j['profile'],))
        if not tgt:
            if not disabled:
                out['no_target'].append((jid[:8], own))
            continue
        timeout = min(180, DISABLED_BUDGET_S - edit_s) if disabled else 180
        if not fits(timeout):
            out['later' if disabled else 'deferred'] += 1
            continue
        edit_start = time.time()
        r = cron_edit(jid, tgt, timeout=timeout)
        edit_s += time.time() - edit_start
        journal('job_move', job=jid, name=j['name'][:60], frm=model + '@' + j['profile'], to=tgt, why=why, result=r)
        if r == 'write_limited':
            out['later' if disabled else 'deferred'] += 1
            break
        if home_rejected(state, 'home', jid, tgt, r):
            state.get('unsure', {}).pop(jid, None)
            save(state)
            continue
        if r in ('ok', 'dry-run'):
            out['moved'].append((jid[:8], tgt))
            if why == 'preference':
                pref_moved[jid] = NOW()
                if jid in home:
                    home[jid]['parked'] = tgt  # same fallback model, preferred account: still on its way home
            elif MODEL(tgt) != own:
                park(state, 'home', jid, own, tgt)
            else:
                home.pop(jid, None)
            j['profile'], j['model'], j['ref'], j['since'] = PROF(tgt), tgt, tgt, NOW()
            if r == 'ok':
                state.setdefault('bindings', {})[jid] = {'model': tgt, 't': j['since']}
            save(state)                      # a pass cut off later must still know where this job came from
        else:
            prob(out, 'rebind:' + jid, 'rebind of job %s to %s failed: %s' % (jid[:8], tgt, r))
            state.setdefault('unsure', {})[jid] = {'own': own, 'tgt': tgt, 'frm': model + '@' + j['profile'], 'why': why}
            save(state)


def session(key):
    """Exact session row, or None when it does not exist. Raises when the Gateway cannot be asked."""
    if FAKE is not None:
        if FAKE.get('describe_error'):
            raise RuntimeError(RPC_DESCRIBE + ' failed: ' + FAKE['describe_error'])
        return FAKE.get('sessions', {}).get(key)
    d, err = gw(RPC_DESCRIBE, DESCRIBE_ARGS(key), 60)
    if d is None:
        if 'not found' in err.lower() or 'unknown session' in err.lower():
            return None
        raise RuntimeError(RPC_DESCRIBE + ' failed: ' + err)
    if S_ROOT in d:
        return d[S_ROOT] or None           # {session: null}: no such session
    raise Shape('%s: no "%s" in the answer' % (RPC_DESCRIBE, S_ROOT))


def entry(key):
    """Stored model/account selection of a session and whether a newer user message is already queued."""
    if FAKE is not None:
        return FAKE.get('entries', {}).get(key, {}), bool(FAKE.get('queued', {}).get(key))
    c = sqlite3.connect(agent_db(key), uri=True)
    r = c.execute('select %s from %s where %s=?' % (C_NODE_ENTRY, T_NODES, C_NODE_KEY), (key,)).fetchone()
    q = c.execute('select count(*) from %s where %s=? and %s=?' % (T_PENDING, C_PEND_KEY, C_PEND_STATE),
                  (key, PEND_QUEUED)).fetchone()[0]
    c.close()
    return (json.loads(r[0]) if r and r[0] else {}), q > 0


def stored_entries(match=None):
    """Stored session entries of every agent, read-only: {key: entry}; with match, only the keys it accepts."""
    if FAKE is not None:
        return {k: e for k, e in FAKE.get('entries', {}).items() if not match or match(k)}
    uris = [os.environ['OCF_AGENT_DB']] if os.environ.get('OCF_AGENT_DB') else [
        agent_db('agent:%s:main' % aid) for aid in agent_ids()]
    out = {}
    for uri in uris:
        c = sqlite3.connect(uri, uri=True)
        try:
            for key, raw in c.execute('select %s,%s from %s' % (C_NODE_KEY, C_NODE_ENTRY, T_NODES)):
                if isinstance(key, str) and (not match or match(key)):
                    try:
                        e = json.loads(raw or '{}')
                    except ValueError:
                        continue
                    if isinstance(e, dict):
                        out[key] = e
        finally:
            c.close()
    return out


def key_for_session(session_id):
    """A stall line may name only its sessionId. Resolve it read-only, and never guess between matching rows."""
    if not session_id:
        return None
    matches = [k for k, e in stored_entries().items() if e.get(S_ID) == session_id]
    return matches[0] if len(matches) == 1 else None


def human_chat(key, s=None, e=None):
    """Human main/WebChat/channel session, not an internal worker reusing a dashboard key."""
    if not CHAT_RE.match(key):
        return False
    for data in (s or {}, e or {}):
        placement = data.get(S_PLACEMENT) or {}
        if any(data.get(f) for f in WORKER_FIELDS) or (data.get(S_DEPTH) or 0) > 0 \
                or data.get(S_BACKGROUND) or placement.get(P_PROVIDER) == WORKER_PROVIDER \
                or str(placement.get(P_ENV, '')).startswith(WORKER_ENV):
            return False
    return True


def last_run(s, e):
    """Describe omits lastRunId on some builds; entry_json is the persisted terminal receipt."""
    return s.get(S_LAST_RUN) or e.get(S_LAST_RUN)


def active_run(s, e, run_id):
    if not s.get(S_ACTIVE):
        return False
    for field in (S_WRITER, S_LIFECYCLE):
        ids = [d[field] for d in (e, s) if d.get(field)]
        if ids:
            return all(r == run_id for r in ids)
    return run_id in (s.get(S_ACTIVE_IDS) or [])


def newer_input(s, e, queued, t):
    # lastInteractionAt also catches a user turn consumed since the failure (no longer queued).
    return queued or any(isinstance(d.get(S_INTERACTION), (int, float))
                         and d[S_INTERACTION] > t + 2000 for d in (s, e))


selection = lambda e: [e.get(f) for f in (E_MODEL, E_PROVIDER, E_MODEL_SRC, E_PROFILE, E_PROFILE_SRC)]
pinned = lambda e: E_USER in (e.get(E_MODEL_SRC), e.get(E_PROFILE_SRC))   # chosen by hand (or by this script's patch)


def chat_ref(conf, e):
    """A chat's selected (model, account): its model override, else the configured primary (sessions.patch to the
    primary clears the override), on its stored account (None when no account is stored)."""
    acct = e.get(E_PROFILE)
    if not e.get(E_MODEL):
        return conf['primary'], acct
    return '%s/%s' % (e.get(E_PROVIDER) or str(acct or '').split(':')[0], e[E_MODEL]), acct


def reconcile_chat_abort(state, key, s, e, run_id, p):
    """Adopt a lost abort receipt only for our durable exact-session/run intent."""
    intent = p.get('abort_intent') or {}
    sid = s.get(S_ID) or e.get(S_ID)
    if (p['kind'] == 'stall' and not p.get('aborted') and intent.get('session') and
            intent == {'key': key, 'session': sid, 'run': run_id} and
            not s.get(S_ACTIVE) and last_run(s, e) in (run_id, None)):
        p['aborted'] = True
        save(state)


def limit_death(s, e, run_id, p):
    """Where a chat's limit event stands, decided by one log test: the limit was the run's last word. The session's
    status word is not read (an abort is stored as 'killed', 'aborted' or nothing, depending on the build).
    'live': the run still sleeps out the wait; it is aborted. 'dead': the run is over and the limit was its last word,
    or our own abort ended it; the turn is continued. 'wait': look again next pass. Any other answer is the reason the
    event is dropped."""
    if s.get(S_ACTIVE):
        if p['kind'] == 'failed':
            return 'wait'                      # OpenClaw may still be rotating accounts; look again next minute
        if not active_run(s, e, run_id):
            return 'a newer run is active'
        if p.get('aborted') or p.get('abort_refused'):
            return 'wait'                      # our abort was accepted (or found no such run); let it settle
        if not any(ts_ms(x) > p['t'] + 5000 for x in log_lines(run_id)):
            return 'live'
        return 'wait' if died_on_limit(run_id, p['t']) else 'it woke up and went on'
    if p['kind'] == 'failed' and NOW() - p['t'] < 20_000:
        return 'wait'
    last = last_run(s, e)
    if last and last != run_id:
        return 'a newer run took over'
    if p.get('aborted'):
        return 'dead'
    if stopped_by_hand(run_id, p['t']):
        return 'a person stopped it'
    if not died_on_limit(run_id, p['t']):
        return 'the limit was not its last word: it went on or finished'
    return 'dead'


def stopped_by_hand(run_id, t):
    """A person pressed stop in a chat for this run at or after t: an intentional stop, not a limit death."""
    return any(ts_ms(x) >= t and any(isinstance(x.get(k), str) and HAND_STOP_RE.search(x[k]) for k in (L1, L2))
               for x in log_lines(run_id))


def died_on_limit(run_id, t):
    """The limit was the run's last word: no later progress and no completed outcome after it."""
    for rec in log_lines(run_id):
        one = rec.get(L1)
        m = RUN_END_RE.search(one) if isinstance(one, str) else None
        if m and m.group(1) == run_id and m.group(2) != 'error' and ts_ms(rec) > t + 2000:
            return False                   # the run rotated to another account and ended normally after the limit
        if not isinstance(one, dict):
            continue
        if one.get(L_STATUS) == L_DONE:
            return False
        if ts_ms(rec) > t + 2000 and (one.get(L_MILESTONE) or (one.get(L_EVENT) == EV_END
                                                                 and one.get(L_REASON) not in LIMITED)):
            return False
    return True


def has_chat(key, s):
    """Local WebChat needs no external delivery route; channel chats keep their stored route."""
    dc = s.get(S_DELIVERY) or {}
    return bool(LOCAL_CHAT_RE.match(key) or GROUP_RE.match(key) or dc.get('to') or s.get(S_TO))


def continue_turn(key, model, moved, run_id=''):
    """Starts the continuation as its own user unit; returns (result, unit). No --reply-to: an explicit target drops
    the session's topic/thread, the session's own stored route keeps it. When the model changed, the agent itself
    opens its reply with one line in the chat's language (this script sends nothing to a chat)."""
    tell = ('Start your reply with one short line, in the language this chat uses, saying that the AI account hit its '
            'usage limit and this chat now runs on %s; then answer. If nothing is unanswered, reply exactly NO_REPLY and '
            'nothing else, without that line.' % model) if moved else 'If nothing is unanswered, reply exactly NO_REPLY. Do not mention this note.'
    msg = ('[Inference failover, automatic] The turn with run ID %s in this chat was interrupted by a provider usage '
           'limit. This session now runs on %s. Review the transcript and reconcile completed tool actions and delivery '
           'receipts with current state before acting. Continue only the unfinished part of that interrupted '
           'request, in the usual language and format of this chat. Do not replay the entire request or repeat '
           'completed actions, sends, payments or writes. Verify uncertain side effects before retrying them. '
           'Respect intentional stops and any newer user instruction. %s' % (run_id, model, tell))
    if not APPLY or FAKE is not None:
        return ('dry-run' if not APPLY else 'started'), None
    unit = 'ocf-continue-' + hashlib.sha256((key + ':' + run_id).encode()).hexdigest()[:24]
    # Retain a completed unit until handled state is durable. A retry after a lost launch receipt cannot run twice.
    argv = ['systemd-run', '--user', '--quiet', '--unit', unit, '-p', 'MemoryMax=2G',
            '-p', 'Type=oneshot', '-p', 'RemainAfterExit=yes', '--no-block', 'openclaw'] + CLI_AGENT(
        key, msg, deliver=not LOCAL_CHAT_RE.match(key))
    rc, out, err = run(argv, 20)
    if rc != 0 and run(['systemctl', '--user', 'show', unit, '-p', 'LoadState', '--value'], 15)[1].strip() == 'loaded':
        return 'started', unit             # reconcile an already accepted deterministic launch
    return ('started', unit) if rc == 0 else ('rc=%s %s' % (rc, (err or out).strip()[-200:]), None)


def continued_check(state, out):
    """A continuation that ended in failure is a problem; a finished one is forgotten (systemd drops the unit). One whose
    run was stopped (by a person, or by an operator moving the chat by hand) is an intentional stop, not a failure."""
    c = state.setdefault('continued', {})
    for unit, key in list(c.items()):
        st = FAKE.get('units', {}).get(unit, 'inactive') if FAKE is not None else \
            (run(['systemctl', '--user', 'is-active', unit], 15)[1].strip() or 'inactive')
        if st in ('active', 'activating'):
            sub = '' if FAKE is not None else run(['systemctl', '--user', 'show', unit, '-p', 'SubState', '--value'], 15)[1].strip()
            if sub != 'exited':
                continue
            if APPLY:
                run(['systemctl', '--user', 'stop', unit], 15)
        c.pop(unit)
        if st != 'failed':
            continue
        if stopped_continuation(unit):
            journal('continue_stopped', session=key, unit=unit, why='its run was stopped, not failed')
        else:
            prob(out, 'continue:' + unit, 'chat %s: the continued turn failed (journalctl --user -u %s)' % (key[-40:], unit))
        if FAKE is None and APPLY:
            run(['systemctl', '--user', 'reset-failed', unit], 15)


def stopped_continuation(unit):
    out = FAKE.get('unit_output', {}).get(unit, '') if FAKE is not None else run(AGENT_OUTPUT(unit), 15)[1]
    lines = [x.strip() for x in (out or '').splitlines() if x.strip()]
    return bool(lines) and lines[-1] == AGENT_STOPPED


def patch_chat(key, tgt, e=None):
    """sessions.patch of a chat to model@account; '' when it landed, else the error. With e (the entry read just
    before), the patch is conditional on its session id and lifecycle revision, so an intervening reset or new turn
    makes it fail instead of landing on the wrong session."""
    args = PATCH_ARGS(key, tgt)
    for source, guard in ((S_ID, 'expectedSessionId'), ('lifecycleRevision', 'expectedLifecycleRevision')):
        if (e or {}).get(source):
            args[guard] = e[source]
    if not APPLY:
        return ''
    if FAKE is not None:                   # tests: the stored selection follows the patch, as OpenClaw's does
        model = MODEL(tgt)
        FAKE.setdefault('entries', {}).setdefault(key, {}).update({
            E_PROVIDER: model.split('/')[0], E_MODEL: model.split('/', 1)[1], E_MODEL_SRC: E_USER,
            E_PROFILE: PROF(tgt), E_PROFILE_SRC: E_USER})
        with open(os.environ['OCF_FAKE'], 'w') as f:
            json.dump(FAKE, f)
        return ''
    d, err = gw(RPC_PATCH, args, 60)
    return '' if d else (err or 'failed')


def chat_plan(state, health, key, e, unknown_dead=False):
    """Whether a chat must move and where, by the same rule as a job: it moves when its own model@account is held or
    probed out (with unknown_dead also when it stores no account: a limit event whose account is unknown). own is the
    model it goes home to; tgt the first live target (None when nothing is live)."""
    model, acct = chat_ref(health.conf, e)
    ref = '%s@%s' % (model, acct)
    prune_homes(state, 'chat_home', health.conf)   # recovery also plans from saved homes, before chats_home runs
    home = state.setdefault('chat_home', {})
    if key in home and home[key]['parked'] != ref:
        home.pop(key)                      # the user chose something else since: that choice stands
    own = home.get(key, {}).get('model', model)
    needed = health.dead(acct, model) if acct else unknown_dead
    return {'ref': ref, 'own': own, 'needed': needed, 'strict': key in STRICT,   # strict: its own model only
            'tgt': health.target(own, avoid=(acct,) if acct else (), chain=key not in STRICT) if needed else None}


def move_chat(state, key, plan, e, out, why):
    """Patch the chat to plan['tgt'] and remember the move; returns 'ok' ('dry-run') or the error."""
    err = patch_chat(key, plan['tgt'], e)
    res = err or ('ok' if APPLY else 'dry-run')
    journal('chat_move', session=key, frm=plan['ref'], to=plan['tgt'], why=why, result=res)
    if err:
        if home_rejected(state, 'chat_home', key, plan['tgt'], err):
            save(state)
        else:
            prob(out, 'chat_move:' + key, 'chat %s: could not move to %s: %s' % (key[-40:], plan['tgt'], err[:120]))
        return res
    home = state.setdefault('chat_home', {})
    if MODEL(plan['tgt']) != plan['own']:
        if home.get(key, {}).get('parked') != plan['tgt']:
            park(state, 'chat_home', key, plan['own'], plan['tgt'])   # a fallback model is temporary: chats_home moves it back
    else:
        home.pop(key, None)
    state.setdefault('chat_moved', {})[key] = {'to': plan['tgt'], 't': NOW()}   # this script's choice, not the user's
    own(state, key, plan['tgt'], e)        # kept while the chat stays there: the preference may move it
    out['chats'].append((key[-40:], plan['tgt']))
    save(state)
    return res


def chats_pass(state, health, out):
    """A pinned human chat is moved like a job: on every probing pass, a chat whose own model@account is held or probed
    out is patched to the next live target. Pinned: a model or account chosen by hand (OpenClaw applies no fallbacks
    to it), or a chat this script parked on a fallback. What its runs did is not asked. A chat with a run in progress
    is looked at next pass; a run that sleeps out a limit wait is aborted, and its chat moved, by its limit event."""
    home, moved = state.setdefault('chat_home', {}), state.setdefault('chat_moved', {})
    for k in [k for k, v in moved.items() if NOW() - v.get('t', 0) > 86400_000]:
        moved.pop(k)
    for key, e in sorted(stored_entries(CHAT_RE.match).items()):
        if not (pinned(e) or key in home) or not human_chat(key, None, e):
            continue
        model, acct = chat_ref(health.conf, e)
        if not acct or not health.dead(acct, model):
            continue
        if not fits(60):
            out['deferred'] += 1
            break
        try:
            s = session(key)
            if not s or s.get(S_ACTIVE) or not human_chat(key, s, e):
                continue
            plan = chat_plan(state, health, key, e)
            if not plan['tgt']:
                if not plan['strict']:         # a strict chat waiting for its own model is expected, not a problem
                    out['no_target'].append((key[-40:], plan['own']))
                continue
            fresh, (fresh_e, _) = session(key) or {}, entry(key)   # probes take time: a new run or choice wins
        except RuntimeError:
            out['deferred'] += 1               # the Gateway is busy: look again next pass
            continue
        if not fresh or fresh.get(S_ACTIVE) or selection(fresh_e) != selection(e):
            continue
        move_chat(state, key, plan, fresh_e, out, 'held' if health.held(acct, model) else 'probe: %s' % health.status(acct))


def chats_home(state, health, conf, out):
    """A chat parked on a fallback model goes home, without disturbing active work or new input."""
    prune_homes(state, 'chat_home', conf)
    home = state.setdefault('chat_home', {})
    for key, h in list(home.items()):
        if not fits(60):
            break
        e, queued = entry(key)
        s = session(key) or {}
        if not s or not human_chat(key, s, e) or s.get(S_ACTIVE) or queued:
            continue
        if '%s@%s' % chat_ref(conf, e) != h['parked']:
            home.pop(key)                  # the user chose something else since: that choice stands
            continue
        tgt = health.target(h['model'], chain=False) if ripe(h) else None
        if not tgt:
            continue
        fresh = session(key) or {}
        fresh_e, queued = entry(key)
        if not fresh or fresh.get(S_ACTIVE) or queued or fresh_e != e or not human_chat(key, fresh, fresh_e):
            continue                      # a user choice or new run during the probe wins
        err = patch_chat(key, tgt, fresh_e)   # conditional on the session read just now
        journal('chat_return', session=key, frm=h['parked'], to=tgt, result=err or 'ok')
        if not err:
            home.pop(key)
            state.setdefault('chat_moved', {})[key] = {'to': tgt, 't': NOW()}
            own(state, key, tgt, fresh_e)
            out['chats'].append((key[-40:], tgt))
        elif not home_rejected(state, 'chat_home', key, tgt, err):
            prob(out, 'chat_return:' + key, 'chat %s: could not move back to %s: %s' % (key[-40:], tgt, err[:120]))


def own(state, key, ref, e):
    """Remember that this script (or the operator, --adopt) set this chat's model@account, in this session."""
    state.setdefault('owned', {})[key] = {'ref': ref, 'session': (e or {}).get(S_ID)}


def owns(state, conf, key, e):
    """The chat still sits where this script put it, in the same session. A person who picks something else, or a
    new session under the key, ends it. (A person picking exactly the same model and account is not visible.)"""
    o = (state.get('owned') or {}).get(key)
    return isinstance(o, dict) and o.get('ref') == '%s@%s' % chat_ref(conf, e) and \
        (not o.get('session') or o['session'] == e.get(S_ID))


def chats_prefer(state, health, conf, out):
    """Return by the preference: a human chat whose account this script set moves to a more preferred live account
    of the same model (better()): one it moved, parked on a fallback or was told to adopt (state 'owned' or
    'chat_home', while the chat still sits where it was put). A different selection since then is a person's choice
    and is never touched, nor is a chat whose account OpenClaw chose itself; the move off a held account (chats_pass)
    is unchanged. The model never changes here. A chat with a run in progress or queued input waits; at most one such
    move per chat an hour and RETURN_CHATS_PER_PASS per pass."""
    owned, home = state.setdefault('owned', {}), state.setdefault('chat_home', {})
    moved, n = state.setdefault('pref_moved', {}), 0
    entries = stored_entries(CHAT_RE.match)
    for key in [k for k in owned if k not in entries or not owns(state, conf, k, entries[k])]:
        owned.pop(key)                     # gone, or a person chose something else since: that choice stands
    if not conf.get('prefer'):
        return
    for key, e in sorted(entries.items()):
        model, acct = chat_ref(conf, e)
        ref = '%s@%s' % (model, acct)
        mine = key in owned or home.get(key, {}).get('parked') == ref
        if not acct or not mine or not health.prefers(model) or not human_chat(key, None, e) \
                or NOW() - moved.get(key, 0) < RETURN_EVERY_MS:
            continue
        if n >= RETURN_CHATS_PER_PASS or late() or not fits(60):
            out['deferred'] += 1
            break
        tgt = better(state, health, model, acct, out)
        if not tgt:
            continue
        try:
            s = session(key)
            fresh_e, queued = entry(key)
        except RuntimeError:
            out['deferred'] += 1               # the Gateway is busy: look again next pass
            continue
        if not s or s.get(S_ACTIVE) or queued or selection(fresh_e) != selection(e) or not human_chat(key, s, fresh_e):
            continue                           # a run, new input or a person's choice wins
        n += 1
        err = patch_chat(key, tgt, fresh_e)
        res = err or ('ok' if APPLY else 'dry-run')
        journal('chat_move', session=key, frm=ref, to=tgt, why='preference', result=res)
        if err:
            prob(out, 'chat_move:' + key, 'chat %s: could not move to %s: %s' % (key[-40:], tgt, err[:120]))
            continue
        moved[key] = NOW()
        own(state, key, tgt, fresh_e)
        if key in home:
            home[key]['parked'] = tgt          # same fallback model, preferred account: still on its way home
        state.setdefault('chat_moved', {})[key] = {'to': tgt, 't': NOW()}
        out['chats'].append((key[-40:], tgt))
        save(state)


def adopt(state, keys):
    """--adopt KEY...: the operator moved these chats by hand, so their current account is not a person's choice:
    record it as this script's own (state 'owned'), and the preference may move them. Only human chats with a stored
    account; anything else is listed as skipped. Changes no chat."""
    conf, res = cfg(), {}
    for key in keys:
        e, _ = entry(key)
        model, acct = chat_ref(conf, e)
        if not e or not acct or not human_chat(key, None, e):
            res[key] = 'skipped: no human chat with a stored account'
            continue
        res[key] = '%s@%s' % (model, acct)
        own(state, key, res[key], e)
        journal('chat_adopt', session=key, ref=res[key])
    save(state)
    print(json.dumps(res, indent=1, ensure_ascii=False))


def order_pass(state, health, conf, jobs, out):
    """auth.order decides only where unpinned work starts (new chats, subagents), and OpenClaw does not skip an account
    this script holds. So per provider with a non-empty order and a preference for some of its models: each such model
    votes for its first live account in pref() with its weight (enabled jobs plus pinned human chats on it; the config
    primary, which new chats use, one more). The winner goes first when it beats the current first account; the rest
    keep their order. Written with a compare-and-set of the documented CLI, at most once per ORDER_EVERY_MS per
    provider, and only when it must change; a dry run only reports it."""
    prefs = conf.get('prefer') or {}
    weight = {}
    for j in jobs.values():
        if j['enabled'] and j['ref']:
            weight[MODEL(j['ref'])] = weight.get(MODEL(j['ref']), 0) + 1
    for key, e in stored_entries(CHAT_RE.match).items():
        if pinned(e) and e.get(E_PROFILE) and human_chat(key, None, e):
            m = chat_ref(conf, e)[0]
            weight[m] = weight.get(m, 0) + 1
    weight[conf['primary']] = weight.get(conf['primary'], 0) + 1
    last = state.setdefault('order_set', {})
    for prov, order in sorted((conf.get('order') or {}).items()):
        models = [m for m in prefs if m.split('/')[0] == prov and weight.get(m)]
        if not isinstance(order, list) or not order or not models or NOW() - last.get(prov, 0) < ORDER_EVERY_MS:
            continue
        score = {}
        tried = state.get('verify_tried') or {}      # a pair whose return check failed this hour is no start either
        for m in models:
            best = next((p for p in health.pref(m) if health.alive(p, m) and '%s@%s' % (m, p) not in tried), None)
            if best:
                score[best] = score.get(best, 0) + weight[m]
        first = max(order, key=lambda p: (score.get(p, 0), -order.index(p)))   # a tie keeps the current first
        if first == order[0] or score.get(first, 0) <= score.get(order[0], 0):
            continue
        new = [first] + [p for p in order if p != first]
        if not APPLY:
            out['order'].append((prov, new))
            continue
        if not fits(60):
            out['deferred'] += 1
            continue
        last[prov] = NOW()
        r = order_set(prov, new, order)
        journal('order_set', provider=prov, frm=order, to=new, votes=score, result=r)
        if r == 'ok':
            out['order'].append((prov, new))
            conf['order'][prov] = new
        lost = state.setdefault('order_lost', {})
        if r == ORDER_CHANGED and lost.get(prov, 0) + 1 < RETEST_ERRORS:
            lost[prov] = lost.get(prov, 0) + 1   # someone changed it since it was read: the next pass decides anew
        elif r == 'ok':
            lost.pop(prov, None)
        else:
            prob(out, 'order:' + prov, 'auth.order.%s could not be set to %s: %s' % (prov, ', '.join(new), r[:120]))


def order_set(provider, new, old):
    """Writes auth.order.<provider> = new only while it still equals old (the CLI's compare-and-set). Returns 'ok',
    ORDER_CHANGED when someone changed the config meanwhile (nothing written), else the error."""
    rc, out, err = run(['openclaw'] + CLI_ORDER_SET(provider, new, old), 60)
    if rc == 0:
        return 'ok'
    if any(t in err + out for t in ORDER_RACES):
        return ORDER_CHANGED
    return 'rc=%s %s' % (rc, (err or out).strip()[-200:])


def chat_changed(state, health, key, s, e, queued, p):
    """Why something newer wins over a chat's limit event, or '': it is no human chat, the user wrote, the session was
    replaced, or the user chose another model or account (a move by this script is not a choice)."""
    if not human_chat(key, s, e):
        return 'not a human chat'
    if newer_input(s, e, queued, p['t']):
        return 'the user already wrote'
    sid = s.get(S_ID) or e.get(S_ID)
    if p.get('session') and sid and sid != p['session']:
        return 'a new session replaced it'
    ref = '%s@%s' % chat_ref(health.conf, e)
    if ref != p.get('ref', ref) and ref != state.get('chat_moved', {}).get(key, {}).get('to'):
        return 'the user chose another model or account'
    return ''


def recover_chat(state, health, key, s, run_id, p, out):
    """A human chat's limit event. A run still sleeping out the wait is aborted; a run that is over with the limit as
    its last word (or ended by our abort) is continued. Before that, the chat is put on a live model@account by the
    same rule as chats_pass: it moves only when its own model@account is held or probed out. True: the event is
    finished (continued, or dropped with the reason journaled). False: look again next pass."""
    if not fits(60 + 30 + 20):              # patch + abort + continuation: all of it, or next pass
        return False
    dropped = lambda why: journal('event_drop', session=key, run=run_id, event=p['kind'], why=why) or True
    e, queued = entry(key)
    p.setdefault('session', s.get(S_ID) or e.get(S_ID))
    p.setdefault('profile', e.get(E_PROFILE))      # normally noted with the event, before any pass moved the chat
    p.setdefault('ref', '%s@%s' % chat_ref(health.conf, e))
    why = chat_changed(state, health, key, s, e, queued, p)
    if why:
        return dropped(why)
    reconcile_chat_abort(state, key, s, e, run_id, p)
    st = limit_death(s, e, run_id, p)
    if st == 'wait':
        return False
    if st not in ('live', 'dead'):
        return dropped(st)
    hits = [t for t in state.setdefault('chat_hits', {}).get(key, []) if t > NOW() - 3600_000]
    if len(hits) >= CHAT_MAX_PER_HOUR:
        prob(out, 'chat_loop:' + key, 'chat %s keeps failing on a limit after recovery (%d times within an hour)'
             % (key[-40:], len(hits) + 1))
        return dropped('it died on a limit again after %d continuations within an hour' % len(hits))
    # Native holds use exact logged profile hashes; the current selection may already be another account.
    if p.get('profile') and not p.get('no_continue'):
        health.hold(p['profile'], p['until'] if p['kind'] == 'stall' else NOW() + HOLD_MS, p['model'])
    plan = chat_plan(state, health, key, e, unknown_dead=True)
    if plan['needed'] and not plan['tgt']:
        if not plan['strict']:
            out['no_target'].append((key[-40:], plan['own']))
        return False                       # stays pending and a sleeping run is not aborted; the next minute asks again
    # Probes can take minutes: read the chat again before any change. A user turn, a newer run or a choice wins.
    latest = session(key) or {}
    latest_e, queued = entry(key)
    why = (chat_changed(state, health, key, latest, latest_e, queued, p) if latest else 'the session no longer exists') \
        or ('the user chose another model or account' if selection(latest_e) != selection(e) else '')
    if why:
        return dropped(why)
    st = limit_death(latest, latest_e, run_id, p)
    if st == 'wait':
        return False
    if st not in ('live', 'dead'):
        return dropped(st)
    if st == 'live':
        p['abort_intent'] = {'key': key, 'session': p.get('session'), 'run': run_id}
        save(state)                       # write-ahead intent survives a lost RPC receipt or process exit
        d, err = ({A_ABORTED_RUN: run_id}, '') if (FAKE is not None or not APPLY) else \
            gw(RPC_ABORT, ABORT_ARGS(key, run_id), 30)
        if d is None:
            prob(out, 'chat_recover:' + run_id, 'chat %s: could not abort the stalled run: %s' % (key[-40:], err[:120]))
            journal('chat_recover', session=key, run=run_id, why=p['kind'], abort=err)
            return False                  # the intent stays: a receipt that was lost is adopted next pass
        if d.get(A_ABORTED_RUN) != run_id:    # it ended meanwhile: not aborted again; once over, its log decides
            p['abort_refused'] = True
            save(state)
            journal('chat_wait', session=key, run=run_id, why='the abort found no such active run; its log decides')
            return False
        p['aborted'] = True
        save(state)
        if FAKE is None and APPLY:
            latest = session(key) or {}
            latest_e, queued = entry(key)
            if latest.get(S_ACTIVE):
                return False              # abort accepted; wait for the old execution to settle
    res = {'abort': ('ok' if APPLY else 'dry-run') if p.get('aborted') else 'not needed', 'patch': 'not needed'}
    now = plan['ref']
    if plan['needed']:
        res['patch'] = move_chat(state, key, plan, latest_e, out, 'limit event')
        if res['patch'] not in ('ok', 'dry-run'):
            journal('chat_recover', session=key, run=run_id, why=p['kind'], to=plan['tgt'], **res)
            return False                  # retain pending and an accepted abort receipt across partial failure
        now = plan['tgt']
    # Re-read after abort and patch, which can take time. A user arriving during recovery wins.
    latest = session(key) or {}
    latest_e, queued = entry(key)
    newer = (latest.get(S_ACTIVE) and not active_run(latest, latest_e, run_id)) or \
        (not latest.get(S_ACTIVE) and last_run(latest, latest_e) not in (run_id, None))
    if not latest or newer or newer_input(latest, latest_e, queued, p['t']) or not human_chat(key, latest, latest_e):
        res['continue'] = 'user already wrote'
    elif p.get('no_continue'):
        res['continue'] = 'not replayed: native run may have completed tool actions'
    elif not has_chat(key, s):
        res['continue'] = 'no chat'
    else:
        res['continue'], unit = continue_turn(key, MODEL(now).split('/')[-1], MODEL(now) != p['model'], run_id)
        if unit:
            state.setdefault('continued', {})[unit] = key
        if res['continue'] not in ('started', 'dry-run'):
            prob(out, 'continue:' + run_id, 'chat %s: could not start the continued turn: %s' % (key[-40:], res['continue'][:120]))
            journal('chat_recover', session=key, run=run_id, why=p['kind'], to=now, **res)
            return False
    state['handled'][run_id] = {'t': NOW()}
    if not p.get('no_continue'):
        state['chat_hits'][key] = hits + [NOW()]
    save(state)
    journal('chat_recover', session=key, run=run_id, why=p['kind'], frm='%s@%s' % (p['model'], p.get('profile')),
            to=now, **res)
    out.setdefault('rebound' if p.get('no_continue') else 'continued', []).append((key[-40:], now))
    return True


def key_of(rk, r, session_id=None):
    if not rk.get(r):
        for rec in log_lines(r):
            m = REQ_RE.search(rec.get(L2)) if isinstance(rec.get(L2), str) else None
            k = m.group(1) if m else (KEY_RE.search(rec[L1]).group(1) if isinstance(rec.get(L1), str) and KEY_RE.search(rec[L1]) else None)
            if k:
                rk[r] = k
                break
        if not rk.get(r) and session_id:
            key = key_for_session(session_id)
            if key:
                rk[r] = key
    return rk.get(r)


def note_events(state, ev, jobs, health):
    """New log events -> pending list. A stalled cron run also holds its job's model@account until the time the
    provider named, so jobs_pass moves the job in this same pass even while the probe of another model still says ok.
    For a chat, the model@account its run was on is noted now, before chats_pass may move the chat."""
    pend, rk = state.setdefault('pending', {}), state.setdefault('run_key', {})
    rk.update(ev['run_key'])
    new = []
    for st in ev['stalls']:
        if st['key']:
            rk.setdefault(st['run'], st['key'])
        old = pend.get(st['run']) or {}
        p = pend[st['run']] = {'kind': 'stall', 't': st['t'], 'until': st['t'] + st['delay'], 'model': st['model'],
                               'session_hint': st.get('session')}
        key = key_of(rk, st['run'], st.get('session'))
        m = CRON_RE.match(key or '')
        j = jobs.get(m.group(1)) if m else None
        if j and j['profile'] and j['since'] < st['t']:
            health.hold(j['profile'], st['t'] + st['delay'], st['model'])
        new.append((p, key, old))
    for r, f in ev['failed'].items():
        if (pend.get(r) or {}).get('kind') != 'stall' and NOW() - f['t'] < FRESH_MS:
            old = pend.get(r) or {}
            p = pend[r] = {'kind': 'failed', 't': f['t'], 'until': f['t'] + WAIT_MS, 'model': f['model']}
            if f.get('no_continue'):
                p['no_continue'] = True
                matches = [a for a in health.accounts(f['model'].split('/')[0])
                           if ('sha256:' + hashlib.sha256(a.strip().encode()).hexdigest()[:12])
                           in f.get('profile_hashes', []) or a in f.get('profile_hashes', [])]
                for account in matches:
                    health.hold(account, f['t'] + HOLD_MS, f['model'])
            new.append((p, key_of(rk, r), old))
    for p, key, old in new:
        if old.get('ref'):                 # the same run seen again: it was on the route noted the first time
            p.update({k: old[k] for k in ('ref', 'profile', 'session') if k in old})
        elif key and (CHAT_RE.match(key) or SUB_RE.match(key)):
            e, _ = entry(key)
            p['profile'], p['ref'] = e.get(E_PROFILE), '%s@%s' % chat_ref(health.conf, e)


def drop_event(pend, r, p, key, why):
    pend.pop(r, None)
    journal('event_drop', session=key, run=r, event=p.get('kind'), why=why)


def stalls_and_chats(state, health, jobs, out):
    """Pending limit events. A run still sleeping out a long wait is aborted (never a one-shot job's run: an abort
    disables the job); a human chat turn that died on the limit is continued. Nothing is rerun or replayed otherwise,
    and moving work off a held or probed-out model@account is left to jobs_pass and chats_pass. Every event that is
    dropped is journaled (event_drop) with the reason."""
    pend, handled = state.setdefault('pending', {}), state.setdefault('handled', {})
    rk = state.setdefault('run_key', {})
    for r, p in list(pend.items()):
        if late():
            break
        if p.get('kind') not in ('stall', 'failed'):
            drop_event(pend, r, p, None, 'not a limit event (left by an older version)')
            continue
        if r in handled:
            drop_event(pend, r, p, rk.get(r), 'already handled')
            continue
        key = key_of(rk, r, p.get('session_hint') or p.get('session'))
        if not key:
            if NOW() <= p['until']:
                continue                      # log may precede the terminal DB commit; resolve again next pass
            drop_event(pend, r, p, None, 'no session found for the run')
            continue
        cron, sub = CRON_RE.match(key), SUB_RE.match(key)
        if sub and p['kind'] != 'stall':
            drop_event(pend, r, p, key, 'a subagent run that failed on a limit: its parent sees the failure')
            continue
        if not cron and not sub and not CHAT_RE.match(key):
            drop_event(pend, r, p, key, 'neither a human chat, a subagent nor a cron run')
            continue
        if cron and p['kind'] != 'stall':
            drop_event(pend, r, p, key, 'a cron run that failed on a limit: its job is rebound, not rerun')
            continue
        if NOW() > p['until']:
            if p['kind'] == 'failed':
                prob(out, 'chat_wait:' + r, 'chat %s: turn failed on a limit and no live account appeared for %d h'
                     % (key[-40:], WAIT_MS // 3600_000))
            drop_event(pend, r, p, key, 'its wait is over' if p['kind'] == 'stall' else 'no live account appeared in time')
            continue
        try:
            s = session(key)
        except RuntimeError as e:
            if NOW() - p['t'] > 600_000:
                prob(out, 'gateway', 'Gateway did not answer for 10 min while a limit event waits: ' + str(e)[:160])
            continue                                   # Gateway busy: keep it pending
        if not s:
            drop_event(pend, r, p, key, 'the session no longer exists')
            continue
        if cron:
            cron_stall(state, jobs, key, cron.group(1), s, r, p, out)
        elif sub:
            sub_stall(state, health, key, s, r, p, out)
        elif recover_chat(state, health, key, s, r, p, out):
            pend.pop(r, None)
    for d in (handled, rk):
        for k in list(d)[:-300]:
            d.pop(k)


def cron_stall(state, jobs, key, jid, s, r, p, out):
    """A cron run sleeping out a long rate-limit wait is aborted so it does not sit for hours. Its job was rebound by
    jobs_pass (the stall holds the model@account), so the next scheduled run uses the new binding; the aborted run is
    not replayed. A one-shot (at) job's run is left alone: an abort disables the job without a notice, while its
    timeout is retried by the scheduler on the new binding."""
    pend, handled = state['pending'], state['handled']
    if not s.get(S_ACTIVE) or any(ts_ms(x) > p['t'] + 5000 for x in log_lines(r)):
        return drop_event(pend, r, p, key, 'the cron run woke up or ended')
    if not fits(30):
        return
    handled[r] = {'t': NOW()}
    pend.pop(r, None)
    j = jobs.get(jid)
    if not j or j.get('kind') == 'at':
        return journal('cron_stall_left', session=key, run=r, why='one-shot job: its timeout retry runs on the new '
                       'binding' if j else 'job not in the job list: it may be a one-shot job')
    d, err = (1, '') if (FAKE is not None or not APPLY) else gw(RPC_ABORT, ABORT_ARGS(key, r), 30)
    journal('cron_stall_abort', session=key, run=r, wait_ms=p['until'] - p['t'], result='ok' if d else err)
    out['aborted'].append(key[-50:])
    if not d:
        prob(out, 'abort:' + r, 'could not abort stalled cron run %s: %s' % (key[-50:], err[:120]))


def sub_stall(state, health, key, s, r, p, out):
    """A subagent run sleeping out a long rate-limit wait is aborted, so its parent gets the failure now instead of
    when the wait ends (hours or days later) and can act on it. It is not continued or moved: the parent chose its
    model, and the model@account stays held until the time the provider named."""
    pend, handled = state['pending'], state['handled']
    if not s.get(S_ACTIVE) or any(ts_ms(x) > p['t'] + 5000 for x in log_lines(r)):
        return drop_event(pend, r, p, key, 'the subagent run woke up or ended')
    if not fits(30):
        return
    handled[r] = {'t': NOW()}
    pend.pop(r, None)
    if p.get('profile'):                   # the route it slept on stays out until the provider's time
        health.hold(p['profile'], p['until'], p['model'])
    d, err = ({A_ABORTED_RUN: r}, '') if (FAKE is not None or not APPLY) else gw(RPC_ABORT, ABORT_ARGS(key, r), 30)
    if d and d.get(A_ABORTED_RUN) != r:    # it ended meanwhile: nothing was aborted
        return journal('subagent_stall_abort', session=key, run=r, wait_ms=p['until'] - p['t'], result='no-active-run')
    journal('subagent_stall_abort', session=key, run=r, wait_ms=p['until'] - p['t'], result='ok' if d else err)
    out['aborted'].append(key[-50:])
    if not d:
        prob(out, 'abort:' + r, 'could not abort stalled subagent run %s: %s' % (key[-50:], err[:120]))


def check(state, out, jobs=None):
    """Did limits get handled in the last 24 h? Independent of the journal: reads the run history and the Gateway log."""
    jobs = jobs if jobs is not None else load_jobs(state)[0]
    streak = streaks(jobs)
    for ref, n in streak.items():
        if n >= 2:
            names = [j['name'][:30] for j in jobs.values() if j['enabled'] and j['ref'] == ref][:3]
            prob(out, 'streak:' + ref, 'jobs on %s keep failing on a limit after their last edit (%s)' % (ref, ', '.join(names)))
    died, keys = {}, {}
    for f in log_files():
        for raw in run(['grep', '-hF', '-e', ANCHORS[1], '-e', EV_END, f], 60)[1].splitlines():
            try:
                rec = json.loads(raw)
            except Exception:
                continue
            one, two = rec.get(L1), rec.get(L2)
            m = REQ_RE.search(two) if isinstance(two, str) else None
            if m:
                keys[m.group(2)] = m.group(1)
            if isinstance(one, dict) and one.get(L_EVENT) == EV_END and one.get(L_REASON) in LIMITED:
                died[one.get(L_RUN)] = ts_ms(rec)
    last = {}
    for r, t in died.items():
        key = keys.get(r)
        if key and CHAT_RE.match(key) and NOW() - 86400_000 < t < NOW() - 600_000 and t > last.get(key, (0, 0))[0]:
            last[key] = (t, r)
    for key, (t, r) in last.items():
        try:
            s = session(key) or {}
        except RuntimeError:
            continue
        e, _ = entry(key)
        if human_chat(key, s, e) and not s.get(S_ACTIVE) and last_run(s, e) == r and died_on_limit(r, t) \
                and not stopped_by_hand(r, t):
            prob(out, 'unanswered:' + r, 'chat %s: turn failed on a limit at %s UTC and is still unanswered'
                 % (key[-40:], time.strftime('%H:%M', time.gmtime(t / 1000))))


def load_state():
    try:
        s = json.loads(STATE.read_text()) if STATE.exists() else {}
    except Exception:
        s = {}             # a corrupt file only loses log offsets and dedup memory; the next pass rebuilds them
    return s               # no 'started' yet (new or imported state): load_jobs sets it, earlier history is not counted


def save(state):
    if not APPLY:
        return
    SDIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE.with_suffix('.tmp')
    tmp.write_text(json.dumps(state, indent=1, ensure_ascii=False))
    tmp.replace(STATE)


def status(state):
    now = NOW()
    static = load_settings()[0]
    eff, src = effective_prefer(static, load_usage()[0])
    hm = lambda ms: time.strftime('%d %b %H:%M', time.localtime(ms / 1000))
    print(json.dumps({
        'last_pass': hm(state.get('last_pass', 0)),
        'open_problems': [{'since': hm(e['raised']), 'text': e['text']} for e in state.get('problems', {}).values()
                          if now - max(e['raised'], e.get('seen', 0)) < CHECK_EVERY_MS + 5 * 60_000],
        'held': {k: hm(u) for k, u in state.get('limited', {}).items() if u > now},   # model@account: until
        'hold_retested': {k: hm(t) for k, t in state.get('retested', {}).items()
                          if state.get('limited', {}).get(k, 0) > now},       # last real re-test of a held pair
        'prefer': eff,                                                      # model: accounts, most preferred first
        'prefer_from': src,                                                 # model: 'usage' when computed
        'prefer_settings': static,                                          # the settings' order (the fallback)
        'usage': CU.rows(CU.load(USAGE)),                                   # per account: limits, resets, free resets
        'verified': {k: hm(t) for k, t in state.get('verified', {}).items()},  # last real answer of a return target
        'chats_owned': len(state.get('owned', {})),        # chats whose account this script set: the preference moves them
        'jobs_on_fallback_model': state.get('home', {}),
        'chats_on_fallback_model': state.get('chat_home', {}),
        'pending_events': state.get('pending', {}),
    }, indent=1, ensure_ascii=False))


def main():
    out = {'moved': [], 'chats': [], 'continued': [], 'aborted': [], 'no_target': [], 'problems': [], 'deferred': 0, 'later': 0,
           'cleared': [], 'retest_due': [], 'verify_due': [], 'order': []}
    state = load_state()
    if '--status' in sys.argv:
        return status(state)
    if '--adopt' in sys.argv:
        SDIR.mkdir(parents=True, exist_ok=True)
        lock = open(SDIR / 'lock', 'w')
        fcntl.flock(lock, fcntl.LOCK_EX)   # waits for a running pass, so its save cannot overwrite this one
        return adopt(load_state(), sys.argv[sys.argv.index('--adopt') + 1:])
    if '--check' in sys.argv:
        try:
            ver, missing = openclaw_version(), selfcheck()
            try:                   # optional: without it the targets are the config profiles, so it is not 'missing'
                bad = saved_profiles()[1]
                saved = 'ok' if not bad else 'saved types disagree with the store for %s' % ', '.join(bad)
                if bad:
                    prob(out, 'saved-profiles', 'openclaw %s: %s' % (' '.join(CLI_MODELS('<agent>')), saved))
            except (Shape, Unreachable) as e:
                saved = quiet_error(e)
                prob(out, 'saved-profiles', 'openclaw %s could not be read: %s' % (' '.join(CLI_MODELS('<agent>')), saved))
            print(json.dumps({'openclaw': ver, 'missing': missing, 'saved_profiles': saved}, indent=1, ensure_ascii=False))
            if missing:
                prob(out, 'selfcheck', 'OpenClaw %s lacks what this script reads: %s' % (ver, '; '.join(missing)))
            else:
                check(state, out)
        except (Unreachable, Shape) as e:
            prob(out, 'gateway', 'self-check could not run: %s' % str(e)[:200])
        if out['problems']:
            sys.stderr.write('[inference failover] ' + '; '.join(sorted({t for _, t in out['problems']})[:12]) + '\n')
        sys.exit(1 if out['problems'] else 0)
    if APPLY:
        SDIR.mkdir(parents=True, exist_ok=True)
        lock = open(SDIR / 'lock', 'w')
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return             # the previous pass is still working
        state = load_state()   # read under the lock: an --adopt that just finished is not overwritten
        if JOURNAL.exists() and JOURNAL.stat().st_size > 5_000_000:
            JOURNAL.replace(JOURNAL.with_suffix('.jsonl.1'))
    if APPLY:
        usage_pass(state, out)  # before the order is computed from it
    t0, before = time.time(), json.dumps(state)
    try:
        conf, cfg_err = cfg(), None
    except Shape as e:
        conf, cfg_err = None, str(e)
    except (OSError, ValueError) as e:     # an unreadable config is a crash (the ledger below), but no order is
        if APPLY:                          # followed either: drop prefer_eff first, saved, so it is journaled once
            note_prefer(state, None, 'config %s not readable: %s' % (CONFIG, str(e)[:120]))
            save(state)
        raise
    if APPLY:
        note_prefer(state, conf, cfg_err)
    if APPLY:                  # armed before the self-check: after an update it alone can take minutes on a slow Gateway
        signal.signal(signal.SIGALRM, _alarm)
        signal.alarm(max(1, int(PASS_HARD_S - (time.time() - T0))))
    try:
        ok = surface_ok(state, out, cfg_err)
    except Deadline:
        ok = False             # a slow Gateway right after an update: a problem only once it lasts DOWN_MS
        down(state, out, 'self-check cut off after %d s' % PASS_HARD_S)
    if not ok or conf is None:
        signal.alarm(0)
        return finish(state, out, {'providers': set()}, {}, Health(conf or {}, {}))
    if conf.get('settings_err'):
        prob(out, 'settings', conf['settings_err'] + '; no account is preferred until it is fixed')
    if conf.get('usage_err'):
        prob(out, 'settings-usage', conf['usage_err'])
    limited = state.setdefault('limited', {})
    for k in [k for k, u in limited.items() if u <= NOW() or '/' not in k.split('@', 1)[0]]:
        state.get('verified', {}).pop(k, None)   # a proof from before this limit says nothing about after it
        if limited.pop(k) <= NOW() and conf.get('prefer'):
            state['retry'] = True          # a limit ran out: this pass asks the accounts, so work can return now
    cleanup_prefer(state)
    for k in ('rerun', 'rerun_wanted', 'run_req'):
        state.pop(k, None)     # an older version's rerun bookkeeping
    ev, streak, health = {'providers': set()}, {}, Health(conf, limited)
    part = None                # the part running now that does not run every pass: a cut-off there keeps its own clock
    try:
        del CRON_STORE_ERR[:]
        jobs, streak = load_jobs(state)
        for e in CRON_STORE_ERR[:1]:
            prob(out, 'cron-store', e + '; jobs whose model names no account are not watched')
        if jobs_partial():
            prob(out, 'partial', 'the job list has more than one page (%d jobs shown); jobs beyond it are not watched'
                 % len(jobs))
        ev = scan_logs(state)
        for ref, n in streak.items():
            if n >= 2:         # held for an hour after the last failure, so jobs do not bounce straight back
                health.hold(PROF(ref), NOW() + HOLD_MS, MODEL(ref))
        note_events(state, ev, jobs, health)
        # Ask the accounts when there is a fresh limit signal, an unfinished pass, or every 5 min while work uses them.
        _VERIFY['busy'] = bool(ev['providers'] or any(NOW() - p.get('t', 0) < 600_000            # fresh limits first:
                                                      for p in (state.get('pending') or {}).values()))   # no return checks now
        probe_due = bool(ev['providers'] or streak or state.get('retry') or state.get('unsure')
                         or NOW() - state.get('probed_at', 0) >= PROBE_EVERY_MS)
        if probe_due:
            jobs_pass(conf, jobs, health, state, t0, out)
            chats_pass(state, health, out)
            chats_home(state, health, conf, out)
            chats_prefer(state, health, conf, out)
            order_pass(state, health, conf, jobs, out)
            state['probed_at'] = NOW()
            state['retry'] = bool(out['deferred'] or out['no_target'] or health.failed)
        stalls_and_chats(state, health, jobs, out)
        _VERIFY['busy'] = False
        continued_check(state, out)
        retest_holds(state, health, out)
        if probe_due or health.cache:        # disabled jobs last, including providers no enabled work uses
            health.no_probe = not probe_due  # discover only on probing passes; 'later' forces no early probe
            part = 'disabled'
            try:
                jobs_pass(conf, jobs, health, state, t0, out, disabled=True)
            finally:
                health.no_probe = False
            part_done(state, part)
            part = None
        if APPLY and not late() and NOW() - state.get('checked_at', 0) >= CHECK_EVERY_MS:
            state['checked_at'] = NOW()      # set first, so a check that is cut off is not retried on every pass
            part = 'check'
            check(state, out, jobs)
            part_done(state, part)
            part = None
        state.pop('down_since', None)
    except Deadline:           # a slow Gateway is OpenClaw not answering: a problem only once it lasts DOWN_MS
        down(state, out, 'pass cut off after %d s; what was done is saved, the rest continues next pass' % PASS_HARD_S,
             slow=True, part=part)
    except (Shape, Unreachable) as e:
        if not (out['moved'] or out['chats'] or out['continued'] or out['aborted'] or out['cleared']):
            eff = state.get('prefer_eff')
            state = json.loads(before)    # nothing was done: keep log offsets and pending events for the next pass
            if eff is not None:
                state['prefer_eff'] = eff  # but the order this pass follows (and journaled), not the last pass's
        if isinstance(e, Unreachable):
            down(state, out, e)
        else:
            state.setdefault('selfcheck', {})['version'] = None     # the next pass runs the full self-check
            _CACHE.clear()
            surface_ok(state, out, str(e))
    finally:
        signal.alarm(0)
    return finish(state, out, ev, streak, health)


def surface_ok(state, out, shape_error=None):
    """Runs the self-check when the installed OpenClaw version changed since the last good check, while a finding is
    open, or after a read of the wrong shape. A finding fails the run through the ledger, once per missing set."""
    try:
        ver = openclaw_version()
        sc = state.get('selfcheck') or {}
        if not shape_error and sc.get('version') == ver and not sc.get('missing'):
            return True
        missing = selfcheck() or ([shape_error] if shape_error else [])
    except Unreachable as e:
        down(state, out, e)
        return False
    state['selfcheck'] = {'version': ver, 'missing': missing, 'at': NOW()}
    if missing:                            # OpenClaw answered and the finding is raised on its own; a clean self-check
        state.pop('down_since', None)      # leaves the clock to the rest of the pass, which may still be cut off
        journal('selfcheck', version=ver, missing=missing)
        prob(out, 'selfcheck:' + '|'.join(missing), 'OpenClaw %s lacks what this script reads, so it changed nothing: %s'
             % (ver, '; '.join(missing)))
    return not missing


def finish(state, out, ev, streak, health):
    if out['no_target'] and not health.unknown():
        models = sorted({m for _, m in out['no_target']})
        prob(out, 'no_target:' + ','.join(models), 'no live account or fallback model for %s: %s'
             % (', '.join(models), ', '.join(i for i, _ in out['no_target'][:8])))
    if health.saved_err:
        prob(out, 'saved-profiles', 'openclaw %s could not be used (%s): accounts only in the agent\'s saved auth store '
             'are not targets until it can' % (' '.join(CLI_MODELS('<agent>')), health.saved_err))
    state['last_pass'] = NOW()
    due = raise_due(state, out) if APPLY else sorted({t for _, t in out['problems']})
    save(state)
    if not APPLY:
        out['problems'] = due
        print(json.dumps({'signals': sorted(ev['providers']), 'streaks': streak, 'probes': health.cache,
                          'held': health.limited, **out}, indent=1, ensure_ascii=False))
    elif due:
        sys.stderr.write('[inference failover] ' + '; '.join(due[:12]) + '\n')
        sys.exit(1)


if __name__ == '__main__':
    try:
        main()
    except SystemExit:
        raise
    except (Exception, Deadline) as e:     # a crash goes through the same ledger: it fails two passes, not every minute forever
        import traceback
        tb = traceback.format_exc()
        if not APPLY:
            raise
        state, out = load_state(), {'problems': []}
        prob(out, 'crash:' + type(e).__name__, 'failover.py crashed: %s' % tb.strip().splitlines()[-1][:200])
        due = raise_due(state, out)
        save(state)
        journal('crash', error=tb[-1500:])
        if due:
            sys.stderr.write('[inference failover] ' + '; '.join(due) + '\n' + tb[-1500:])
            sys.exit(1)
