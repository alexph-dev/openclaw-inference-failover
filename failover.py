#!/usr/bin/env python3
"""openclaw-inference-failover: moves OpenClaw work off AI accounts that hit a provider usage limit.

Run once a minute by an OpenClaw command job.
  jobs   an enabled cron job bound to an account that is out of limit moves to the same model on a live account,
         else to the first live entry of agents.defaults.model.fallbacks. No return between accounts (no account is
         preferred). A move to another MODEL is temporary: the job goes back to its own model once any account of it
         answers. A job that lost a run to the limit and is not due again within 30 min is run once after the move.
  stalls a run sleeping out a long same-model rate-limit wait is aborted; a chat is then continued; a recurring job is
         rerun as above. One-shot (at) jobs are never aborted: an abort disables them silently, their timeout is
         retried by the scheduler on the new binding.
  chats  a chat turn that failed on a limit (all accounts of a pinned model out) is moved the same way and continued
         unless the user already wrote again. The continuation prompt asks the agent to open its reply with one line,
         in the chat's own language, saying the chat moved to another model or account.
  check  every 30 min it compares the last 24 h of limit failures with what it did.
  problems  anything it cannot fix fails the run (exit 1, reason on stderr) on two passes in a row, then stays quiet
         while it is open, and fails again only if it is still open after 6 h (a missing OpenClaw surface: after
         24 h). OpenClaw not answering is a problem only after 10 min. It never fails every minute: ten errors in a
         row would make the scheduler disable this job.

  failover.py            dry run (no lock, no state written)
  failover.py --apply    act
  failover.py --check    self-check of the OpenClaw surfaces it reads plus the 24 h check; exit 1 with problems
  failover.py --status   open problems, held accounts, work parked on a fallback model (JSON)
State and journal: ~/.local/state/openclaw-inference-failover (OCF_STATE overrides).
"""
import calendar, fcntl, glob, json, os, pathlib, re, signal, sqlite3, subprocess, sys, time

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
CLI_RUN = lambda jid: ['cron', 'run', jid]                          # run a job once now
CLI_AGENT = lambda key, msg: ['agent', '--session-key', key, '--message', msg, '--deliver', '--timeout', '3600']  # continue a chat
CLI_CALL = lambda method, params, ms: ['gateway', 'call', method, '--params', json.dumps(params), '--json', '--timeout', str(ms)]  # one RPC
# JSON fields of the CLI answers ('a.b' = nested)
JOBS_LIST, RUNS_LIST = 'jobs', 'entries'                            # top-level lists of cron list / cron runs
LIST_MORE = 'hasMore'                                               # true when the job list did not fit one page
JOB_F = {'id': 'id', 'enabled': 'enabled', 'name': 'name',          # per job in cron list
         'model': 'payload.model',                                  #   binding 'provider/model@account'
         'kind': 'schedule.kind', 'mode': 'scheduledToolPolicy.mode', 'next': 'state.nextRunAtMs',
         'last': 'state.lastRunStatus', 'last_at': 'state.lastRunAtMs', 'streak': 'state.consecutiveErrors',
         'error': 'state.lastError'}
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
S_ACTIVE, S_STATUS, S_LAST_RUN = 'hasActiveRun', 'status', 'lastRunId'          # run state of a session
S_DELIVERY, S_TO, S_MODEL, S_PROVIDER = 'deliveryContext', 'lastTo', 'model', 'modelProvider'
S_FAILED = 'failed'                                                 # session status after a failed turn
# Agent databases (chat model/account selection and queued input: no CLI or RPC exposes them)
AGENT_DB_PATH = 'agents/{agent}/agent/openclaw-agent.sqlite'        # under the OpenClaw state dir
T_NODES, C_NODE_KEY, C_NODE_ENTRY = 'session_nodes', 'session_key', 'entry_json'           # session entry per key
T_PENDING, C_PEND_KEY, C_PEND_STATE = 'session_pending_inputs', 'session_key', 'state'     # input waiting for a session
PEND_QUEUED = 'queued'                                              # state of a user message not yet answered
E_MODEL, E_PROVIDER, E_MODEL_SRC = 'modelOverride', 'providerOverride', 'modelOverrideSource'   # entry_json keys
E_PROFILE, E_PROFILE_SRC, E_USER = 'authProfileOverride', 'authProfileOverrideSource', 'user'   # 'user' = pinned by hand
# Config file
CFG_MODEL = ('agents', 'defaults', 'model')                         # {primary, fallbacks}
CFG_ORDER = ('auth', 'order')                                       # account order per provider
CFG_LOG_FILE = ('logging', 'file')                                  # optional: Gateway log file
CFG_AGENTS = ('agents', 'list')                                     # optional: agent definitions with 'default'
# Gateway log (JSON lines; '1' and '2' are the logger's argument slots)
LOG_GLOB = 'openclaw-*.log'                                         # daily log files
LOG_TMP_DIRS = ('/tmp/openclaw', '/tmp/openclaw-{uid}')             # default log directories
L1, L2, L_META, L_DATE = '1', '2', '_meta', 'date'
L_HTTP, L_BODY, L_PROV, L_MODEL = 'status', 'body', 'provider', 'model'   # fields of a failed model request / run end
EV_END = 'embedded_run_agent_end'                                   # a run ended: {runId, isError, failoverReason, provider, model}
L_EVENT, L_RUN, L_ERROR, L_REASON, L_STATUS, L_DONE, L_MILESTONE = 'event', 'runId', 'isError', 'failoverReason', 'runStatus', 'completed', 'milestone'
LIMITED = ('rate_limit', 'billing')                                 # failoverReason values that mean a usage limit
RETRY_RE = re.compile(r'transient same-model retry \d+/\d+ for (\S+) reason=rate_limit: delayMs=(\d+) '
                      r'runId="([^"]+)" sessionId="([^"]*)"')      # a run sleeping out a same-model rate-limit wait
REQ_RE = re.compile(r'Model request failed sessionKey="([^"]+)" runId="([^"]+)"')   # a failed model request -> session
REQ_N_RE = re.compile(r'requestNumber=(\d+)')                       # how far into the run that request was
KEY_RE = re.compile(r'sessionKey[=:\s\\"]+(agent:[^"\\\s,]+)')         # session key inside any log line
ANCHORS = ('transient same-model retry', 'Model request failed', EV_END)   # log anchors the self-check looks for
ANCHOR_DIRS = ('dist', 'node_modules/@openclaw/ai/dist')            # where the anchors live in the package
# Session keys
CHAT_RE = re.compile(r'^agent:[^:]+:(?!a2a:)[^:]+:(group|direct|channel):')  # a chat session with people (not agent-to-agent)
GROUP_RE = re.compile(r'^agent:[^:]+:[^:]+:(group|channel):')        # a chat session that is a group or channel
CRON_RE = re.compile(r'^agent:[^:]+:cron:([0-9a-f-]{36}):run:')      # a cron run session -> job id
CRON_KEY = ':cron:%s:run:'                                          # log needle for one job's runs
# Error wording. LIMIT_RE: OpenClaw's and the providers' own limit wording only ("quota exceeded" or "429" also appear
# in business errors). Name and pattern are used by other scripts that import this file.
ROUTE_ERR = 'incompatible with its recorded inference account and route'   # raised before the run does anything
LIMIT_RE = re.compile(r"incompatible with its recorded inference account and route|rate_limit_error|"
                      r"exceed your account's rate limit|needs a short break|asking us to slow down|"
                      r"insufficient_quota", re.I)
ERROR_TEXTS = (ROUTE_ERR, 'needs a short break', 'asking us to slow down', 'rate_limit_error', 'insufficient_quota')
#   ^ the parts of LIMIT_RE that OpenClaw itself contains; "exceed your account's rate limit" is provider wording
# =====================================================================================================================

HOME = pathlib.Path.home()
SDIR = pathlib.Path(os.environ.get('OCF_STATE') or os.environ.get('OCF_STATE_DIR')
                    or HOME / '.local/state/openclaw-inference-failover')
STATE, JOURNAL = SDIR / 'state.json', SDIR / 'journal.jsonl'
OC_DIR = pathlib.Path(os.environ.get('OCF_OPENCLAW_DIR') or os.environ.get('OPENCLAW_STATE_DIR') or HOME / '.openclaw')
CONFIG = os.environ.get('OCF_CONFIG') or os.environ.get('OPENCLAW_CONFIG_PATH') or str(OC_DIR / 'openclaw.json')
FAKE = json.load(open(os.environ['OCF_FAKE'])) if os.environ.get('OCF_FAKE') else None  # tests only
APPLY = '--apply' in sys.argv
NOW = lambda: int(time.time() * 1000)
LOCAL = lambda fmt: time.strftime(fmt, time.localtime())

WINDOW_MS = 90 * 60_000        # run history looked at
PROBE_EVERY_MS = 5 * 60_000    # accounts are asked at least this often while any job or pinned chat uses them
CHECK_EVERY_MS = 30 * 60_000   # cadence of the 24 h check inside --apply passes
FRESH_MS = 20 * 60_000         # a failed chat turn older than this when first seen is not continued
WAIT_MS = 2 * 3600_000         # a failed chat turn waits this long for any live account, then is a problem
STALL_MS = 300_000             # same-model retry waits above 5 min are the #143274 stall
PASS_SOFT_S = 200              # no new Gateway call is started after this
PASS_HARD_S = int(os.environ.get('OCF_PASS_HARD_S', 270))   # a running call is cut off here: the job's own timeout is 300 s
GONE_MS = 70 * 60_000          # a problem not seen for this long (two 30-min checks) and seen again is a new one
PASS_BUDGET_S = 40             # stop starting new edits after this; the next minute continues
CHAT_MAX_PER_HOUR = 2
HOLD_MAX_MS = 6 * 3600_000     # a provider's own wait is trusted up to this long, then the account is tried again
HOLD_MS = 3600_000             # an account that failed on a limit while its probe says ok is not a target for this long
RERAISE_MS = 6 * 3600_000      # an unfixed problem fails the run once, then again only after this long
DOWN_MS = 10 * 60_000          # OpenClaw not answering is a problem only after this long: one slow call is not
RERUN_MAX_AGE_MS = 30 * 60_000  # a lost run older than this is not rerun automatically
FREQUENT_MS = 30 * 60_000      # a job due again within this is left to the scheduler: its next run replaces the lost one
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


def scan_logs(state):
    """New complete log lines since the last pass -> stalls, failed turns, run->session keys, providers seen limited."""
    ev = {'stalls': [], 'failed': {}, 'run_key': {}, 'run_req': {}, 'providers': set()}
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
            if not any(x.encode() in raw for x in LIMITED + (ANCHORS[1],)):
                continue
            try:
                rec = json.loads(raw)
            except Exception:
                continue
            one, two, t = rec.get(L1), rec.get(L2), ts_ms(rec)
            m = REQ_RE.search(two) if isinstance(two, str) else None
            if m and isinstance(one, dict):
                ev['run_key'][m.group(2)] = m.group(1)
                n = REQ_N_RE.search(two)
                ev['run_req'][m.group(2)] = max(ev['run_req'].get(m.group(2), 0), int(n.group(1)) if n else 1)
                if one.get(L_HTTP) == 429 or LIMITED[0] in json.dumps(one.get(L_BODY) or ''):
                    ev['providers'].add(one.get(L_PROV))
            if isinstance(one, str):
                r = RETRY_RE.search(one)
                if r and int(r.group(2)) >= STALL_MS:
                    k = KEY_RE.search(one)
                    ev['stalls'].append({'model': r.group(1), 'delay': int(r.group(2)), 'run': r.group(3), 't': t,
                                         'key': k.group(1) if k else None})
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
    return {'primary': m['primary'], 'chain': m.get('fallbacks') or [], 'order': dig(c, CFG_ORDER) or {}}


class Health:
    """Account status per provider from models.probe, asked at most once per pass. A probe tests one model of the
    provider, so 'ok' means the account answers, not that every model on it has quota left."""

    def __init__(self, conf, limited=None):
        # limited: {'account' or 'model@account': until_ms}, kept in state. The probe tests one model per provider and
        # forgets between passes, so limits seen in real runs (stall delay, failed turns, job receipts) are held here.
        self.conf, self.limited, self.cache, self.failed = conf, limited if limited is not None else {}, {}, []

    def probe(self, provider):
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

    def held(self, profile, model=None):
        return max(self.limited.get(profile, 0), self.limited.get('%s@%s' % (model, profile), 0)) > NOW()

    def hold(self, profile, until, model=None):
        k = '%s@%s' % (model, profile) if model else profile
        self.limited[k] = max(self.limited.get(k, 0), until)

    def alive(self, profile, model=None):
        return self.status(profile) == 'ok' and not self.held(profile, model)

    def dead(self, profile, model=None):
        return self.held(profile, model) or self.status(profile) in LIMITED

    def target(self, model, avoid=(), chain=True):
        """First live model@account: the same model on another account, then the config fallback chain."""
        for m in dict.fromkeys([model] + (list(self.conf['chain']) if chain else [])):
            prov = m.split('/')[0]
            res = self.probe(prov)
            order = [p for p in self.conf['order'].get(prov, []) if p in res] + [p for p in res if p not in self.conf['order'].get(prov, [])]
            for p in order:
                if p not in avoid and self.alive(p, m):
                    return m + '@' + p
        return None

    def unknown(self):
        """True when a probe failed or timed out: 'nothing alive' is then unproven. auth, no_model and format are
        definite answers (that account cannot be used), so an expired token does not hide 'nothing alive'."""
        return bool(self.failed) or any(s in ('timeout', 'unknown', None) for r in self.cache.values() for s in r.values())


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
    exist, the config, the agent database schemas, the log directory and a text search of the installed package."""
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
        if any(dig(j, JOB_F['last'].split('.')) == 'error' for j in jobs) and not has('error'):
            miss.append('cli: openclaw %s: field %s' % (' '.join(CLI_JOBS), JOB_F['error']))   # a failed job shows why
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

    for f in (config_ok, jobs_ok, runs_ok, describe_ok, tables_ok, logs_ok, package_ok):
        guard(f)
    return sorted(set(miss))


def down(state, out, e):
    """OpenClaw did not answer: nothing is changed this pass. It is a problem only once it has lasted DOWN_MS."""
    since = state.setdefault('down_since', NOW())
    if NOW() - since >= DOWN_MS:
        prob(out, 'gateway', 'OpenClaw has not answered for %d min, nothing was changed: %s'
             % ((NOW() - since) // 60_000, str(e)[:200]))


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


def load_jobs(state):
    """Jobs with their binding (payload.model = 'provider/model@account'), the run each lost to a limit, and per account
    the limit failures in a row. A failure from before the job's last rebinding belongs to the previous account; the
    time of a rebinding is this script's own record (state 'bindings'), not the job's updated time."""
    seen, now = state.setdefault('bindings', {}), NOW()
    start = state.setdefault('started', now)   # first pass ever: history from before it was not watched by this script
    jobs = {}
    for d in cron_jobs():
        f = lambda k: dig(d, JOB_F[k].split('.'))
        jid, ref = f('id'), f('model')
        if not jid:
            raise Shape('openclaw %s: a job without "%s"' % (' '.join(CLI_JOBS), JOB_F['id']))
        rec = seen.get(jid)
        if not rec or rec.get('model') != ref:   # first sight, or rebound by someone else since
            # t: when the job was last rebound (0 = unknown, so a lost run is never rerun on a guess). Failures and
            # stalls from before t, or from before this script's first pass, do not count against the current account.
            rec = seen[jid] = {'model': ref, 't': 0 if not rec else now}
        jobs[jid] = {'enabled': bool(f('enabled')), 'name': f('name') or '', 'model': ref,
                     'profile': PROF(ref) if ref and '@' in ref else None, 'updated': rec['t'],
                     'since': max(rec['t'], start),
                     'mode': f('mode'), 'next': f('next') or 0, 'kind': f('kind'), 'lost': 0, 'lost_route': False,
                     'last': f('last'), 'last_at': f('last_at') or 0, 'streak_n': f('streak') or 0, 'errors': []}
        err = f('error') or ''
        if jobs[jid]['last'] == 'error' and jobs[jid]['last_at'] > now - WINDOW_MS and LIMIT_RE.search(err):
            jobs[jid]['lost'], jobs[jid]['lost_route'] = jobs[jid]['last_at'], ROUTE_ERR in err
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
            if j['lost'] and j['errors'] and j['errors'][0][1]:
                j['lost'] = j['errors'][0][0]      # the run's own start time, as the run history gives it
    return jobs, streaks(jobs)


def streaks(jobs):
    """Per account: limit failures in a row, newest first, across the jobs bound to it since their rebinding. A job's
    newest ok run (lastRunStatus) ends the streak; so does a failure for another reason."""
    last_ok, ev = {}, {}
    for j in jobs.values():
        p = j['profile']
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


def cron_edit(jid, model):
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
    rc, out, err = run(['openclaw'] + CLI_EDIT(jid, model), 180)
    return 'ok' if rc == 0 else ('write_limited' if 'retry after' in (err + out).lower() else 'rc=%s %s' % (rc, (err or out).strip()[-200:]))


def park(state, kind, ident, own, tgt):
    """Remember that a job or chat sits on a fallback model. The first time it goes home as soon as an account of its
    own model answers; every repeat within a day waits an hour longer (max 6 h), so a model that the probe cannot see
    as exhausted does not make it bounce."""
    seen, now = state.setdefault('parks', {}), NOW()
    n = seen[ident]['n'] + 1 if now - seen.get(ident, {}).get('t', 0) < 86400_000 else 1
    seen[ident] = {'n': n, 't': now}
    state[kind][ident] = {'model': own, 'parked': tgt, 'at': now, 'wait': min(HOLD_MS * (n - 1), HOLD_MAX_MS)}
    for k in [k for k, v in seen.items() if now - v['t'] > 86400_000]:
        seen.pop(k)


ripe = lambda h: NOW() - h.get('at', 0) >= h.get('wait', 0)


def cron_run(jid):
    if not APPLY or FAKE is not None:
        return 'ok' if APPLY else 'dry-run'
    rc, out, err = run(['openclaw'] + CLI_RUN(jid), 120)
    return 'ok' if rc == 0 else 'rc=%s %s' % (rc, (err or out).strip()[-200:])


def jobs_pass(conf, jobs, health, state, t0, out):
    """Every enabled job on an account that is out of limit moves to a live one, soonest-due first. A job parked on
    a fallback model goes back to its own model once any account of that model answers."""
    home = state.setdefault('home', {})      # {job: {'model': its own model, 'parked': 'fallback@account'}}
    for jid, u in list(state.setdefault('unsure', {}).items()):
        state['unsure'].pop(jid)             # an edit that reported failure (a timeout) may still have landed
        if jid in jobs and jobs[jid]['model'] == u['tgt']:
            journal('job_move', job=jid, name=jobs[jid]['name'][:60], frm=u['frm'], to=u['tgt'], result='ok',
                    note='the edit reported a failure but was applied')
            if MODEL(u['tgt']) != u['own'] and jid not in home:
                park(state, 'home', jid, u['own'], u['tgt'])
    todo = []
    for jid, j in jobs.items():
        if not (j['enabled'] and j['profile']):
            home.pop(jid, None)
            continue
        ref = j['model'] or conf['primary'] + '@' + j['profile']
        if jid in home and home[jid]['parked'] != ref:
            home.pop(jid)                    # somebody rebound it since: that choice stands
        if health.dead(j['profile'], MODEL(ref)):
            todo.append((j['next'], jid, None))
        elif jid in home and ripe(home[jid]) and health.target(home[jid]['model'], chain=False):
            todo.append((j['next'], jid, health.target(home[jid]['model'], chain=False)))
    for jid in [k for k in home if k not in jobs and not jobs_partial()]:
        home.pop(jid)
    for _, jid, back in sorted(todo, key=lambda x: x[:2]):
        j = jobs[jid]
        if j['mode'] != JOB_TRUSTED:
            prob(out, 'legacy:' + jid, 'legacy account-mode job %s (%s) is on exhausted %s; recreate it as an operator job'
                 % (jid[:8], j['name'][:40], j['profile']))
            continue
        if time.time() - t0 > PASS_BUDGET_S:
            out['deferred'] += 1
            continue
        model = MODEL(j['model'] or conf['primary'])
        own = home.get(jid, {}).get('model', model)
        tgt = back or health.target(own, avoid=(j['profile'],))
        if not tgt:
            out['no_target'].append((jid[:8], own))
            continue
        if not fits(180):
            out['deferred'] += 1
            continue
        r = cron_edit(jid, tgt)
        journal('job_move', job=jid, name=j['name'][:60], frm=model + '@' + j['profile'], to=tgt, result=r)
        if r == 'write_limited':
            out['deferred'] += 1
            break
        if r in ('ok', 'dry-run'):
            out['moved'].append((jid[:8], tgt))
            if MODEL(tgt) != own:
                park(state, 'home', jid, own, tgt)
            else:
                home.pop(jid, None)
            j['profile'], j['model'], j['updated'] = PROF(tgt), tgt, NOW()
            j['since'] = j['updated']
            if r == 'ok':
                state.setdefault('bindings', {})[jid] = {'model': tgt, 't': j['updated']}
            save(state)                      # a pass cut off later must still know where this job came from
        else:
            prob(out, 'rebind:' + jid, 'rebind of job %s to %s failed: %s' % (jid[:8], tgt, r))
            state.setdefault('unsure', {})[jid] = {'own': own, 'tgt': tgt, 'frm': model + '@' + j['profile']}
            save(state)


def requests_made(jid, since):
    """Highest model request number the job's runs reached since that time (1 when none is logged)."""
    n = 1
    for rec in log_lines(CRON_KEY % jid):
        two = rec.get(L2)
        m = REQ_N_RE.search(two) if isinstance(two, str) and REQ_RE.search(two) else None
        if m and ts_ms(rec) >= since - 1000:
            n = max(n, int(m.group(1)))
    return n


def rerun_lost(conf, jobs, health, state, out):
    """A job whose last run died on a limit and that is not due again soon is run once after its move (the scheduler
    does not retry the route error, so such a run is otherwise lost)."""
    done = state.setdefault('rerun', {})
    for jid, j in jobs.items():
        t = j['lost']
        if not (t and j['enabled'] and j['profile']) or done.get(jid) == t or j['updated'] <= t:
            continue                         # nothing lost, already rerun, or not moved yet
        if health.dead(j['profile'], MODEL(j['model'] or conf['primary'])):
            continue
        if not fits(120):
            break
        done[jid] = t
        if j['next'] and j['next'] - NOW() < FREQUENT_MS:
            continue                         # due soon, overdue or firing right now: the scheduler runs it
        if NOW() - t > RERUN_MAX_AGE_MS:      # a late rerun can message people at the wrong hour: a human decides
            prob(out, 'lost_late:%s:%d' % (jid, t), 'job %s (%s) lost its run to a limit %d min ago and was moved only '
                 'now; not rerun automatically: decide whether it still should run' % (jid[:8], j['name'][:40], (NOW() - t) // 60000))
            continue
        if not j['lost_route']:              # any other limit error can come after the run already sent something
            prob(out, 'lost_mid:%s:%d' % (jid, t), 'job %s (%s) died on a provider limit after it had started; it may have '
                 'acted already: check its transcript before rerunning' % (jid[:8], j['name'][:40]))
            continue
        n = requests_made(jid, t)
        if n > 1:                            # it got past its first model request: it may have sent something
            prob(out, 'lost_mid:%s:%d' % (jid, t), 'job %s (%s) died on a limit at model request %d; it may have acted '
                 'already: check its transcript before rerunning' % (jid[:8], j['name'][:40], n))
            continue
        save(state)                          # before the run: a pass cut off here must not run it twice
        r = cron_run(jid)
        journal('job_rerun', job=jid, name=j['name'][:60], result=r)
        out['rerun'].append(jid[:8])
        if r not in ('ok', 'dry-run'):
            prob(out, 'rerun:' + jid, 'job %s lost its run to a limit and the rerun failed: %s' % (jid[:8], r))
    for k in list(done)[:-200]:
        done.pop(k)
    wanted = state.setdefault('rerun_wanted', {})        # stalled on the first model request and aborted last pass
    for jid, t in list(wanted.items()):
        j = jobs.get(jid)
        if not j or not j['enabled'] or NOW() - t > 3600_000:
            wanted.pop(jid)
            continue
        if health.dead(j['profile'], MODEL(j['model'] or conf['primary'])):
            continue                         # not moved yet
        if not fits(120):
            break
        wanted.pop(jid)
        if j['next'] and j['next'] - NOW() < FREQUENT_MS:
            continue
        save(state)
        r = cron_run(jid)
        journal('job_rerun', job=jid, name=j['name'][:60], why='stall aborted', result=r)
        out['rerun'].append(jid[:8])
        if r not in ('ok', 'dry-run'):
            prob(out, 'rerun:' + jid, 'job %s was aborted in a limit stall and the rerun failed: %s' % (jid[:8], r))


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


def died_on_limit(run_id, t):
    """The limit was the run's last word: no later progress and no completed outcome after it."""
    for rec in log_lines(run_id):
        one = rec.get(L1)
        if not isinstance(one, dict):
            continue
        if one.get(L_STATUS) == L_DONE:
            return False
        if ts_ms(rec) > t + 2000 and (one.get(L_MILESTONE) or (one.get(L_EVENT) == EV_END
                                                                 and one.get(L_REASON) not in LIMITED)):
            return False
    return True


def has_chat(key, s):
    """The session delivers to a chat: a group/channel key, or a stored delivery route."""
    dc = s.get(S_DELIVERY) or {}
    return bool(GROUP_RE.match(key) or dc.get('to') or s.get(S_TO))


def continue_turn(key, model, moved):
    """Starts the continuation as its own user unit; returns (result, unit). No --reply-to: an explicit target drops
    the session's topic/thread, the session's own stored route keeps it. When the model changed, the agent itself
    opens its reply with one line in the chat's language (this script sends nothing to a chat)."""
    tell = ('Start your reply with one short line, in the language this chat uses, saying that the AI account hit its '
            'usage limit and this chat now runs on %s; then answer. If nothing is unanswered, reply exactly NO_REPLY and '
            'nothing else, without that line.' % model) if moved else 'If nothing is unanswered, reply exactly NO_REPLY. Do not mention this note.'
    msg = ('[Inference failover, automatic] Your previous turn in this chat was cut off by a provider usage limit '
           'before you answered. This session now runs on %s. Continue that turn now: answer the latest unanswered '
           'request in this chat as if it had just arrived, in the usual language and format of this chat. %s'
           % (model, tell))
    if not APPLY or FAKE is not None:
        return ('dry-run' if not APPLY else 'started'), None
    unit = 'ocf-continue-%d' % NOW()
    argv = ['systemd-run', '--user', '--quiet', '--unit', unit, '-p', 'MemoryMax=2G', 'openclaw'] + CLI_AGENT(key, msg)
    rc, out, err = run(argv, 20)
    return ('started', unit) if rc == 0 else ('rc=%s %s' % (rc, (err or out).strip()[-200:]), None)


def continued_check(state, out):
    """A continuation that ended in failure is a problem; a finished one is forgotten (systemd drops the unit)."""
    c = state.setdefault('continued', {})
    for unit, key in list(c.items()):
        st = FAKE.get('units', {}).get(unit, 'inactive') if FAKE is not None else \
            (run(['systemctl', '--user', 'is-active', unit], 15)[1].strip() or 'inactive')
        if st in ('active', 'activating'):
            continue
        c.pop(unit)
        if st == 'failed':
            prob(out, 'continue:' + unit, 'chat %s: the continued turn failed (journalctl --user -u %s)' % (key[-40:], unit))
            if FAKE is None:
                run(['systemctl', '--user', 'reset-failed', unit], 15)


def recover_chat(state, health, key, s, run_id, p, out):
    if not fits(60 + 30 + 20):              # patch + abort + continuation: all of it, or next pass
        return False
    hits = [t for t in state.setdefault('chat_hits', {}).get(key, []) if t > NOW() - 3600_000]
    if len(hits) >= CHAT_MAX_PER_HOUR:
        prob(out, 'chat_loop:' + key, 'chat %s keeps failing on a limit after recovery (%d times within an hour)'
             % (key[-40:], len(hits) + 1))
        return True
    e, queued = entry(key)
    model = p.get('model') or '%s/%s' % (s.get(S_PROVIDER), s.get(S_MODEL))
    cur = e.get(E_PROFILE)
    if cur:        # the account this turn ran on is out for this model, whatever a probe of another model says
        health.hold(cur, min(p['until'], NOW() + HOLD_MAX_MS) if p['kind'] == 'stall' else NOW() + HOLD_MS, model)
    home = state.setdefault('chat_home', {})
    own = home.get(key, {}).get('model', model)
    pinned = (e.get(E_MODEL_SRC) == E_USER and bool(e.get(E_MODEL))) or e.get(E_PROFILE_SRC) == E_USER
    tgt = health.target(own, avoid=(cur,) if cur else ())
    if not tgt:
        out['no_target'].append((key[-40:], own))
        return False                       # stays pending; the next minute asks again
    res = {'patch': 'not needed', 'abort': 'not needed'}
    if pinned:                             # an unpinned session rotates by itself once the stuck run is gone
        d, err = (1, '') if (FAKE is not None or not APPLY) else gw(RPC_PATCH, PATCH_ARGS(key, tgt), 60)
        res['patch'] = ('ok' if APPLY else 'dry-run') if d else err
    if p['kind'] == 'stall':
        d, err = (1, '') if (FAKE is not None or not APPLY) else gw(RPC_ABORT, ABORT_ARGS(key, run_id), 30)
        res['abort'] = ('ok' if APPLY else 'dry-run') if d else err
    if res['patch'] not in ('ok', 'dry-run', 'not needed') or res['abort'] not in ('ok', 'dry-run', 'not needed'):
        prob(out, 'chat_recover:' + run_id, 'chat %s: recovery failed %s' % (key[-40:], json.dumps(res)[:200]))
        journal('chat_recover', session=key, run=run_id, why=p['kind'], to=tgt, **res)
        return True
    if pinned and MODEL(tgt) != own:
        park(state, 'chat_home', key, own, tgt)        # a fallback model is temporary: chats_home moves it back
    elif pinned:
        home.pop(key, None)
    state['handled'][run_id] = NOW()
    state['chat_hits'][key] = hits + [NOW()]
    save(state)                            # before anything is sent: a pass killed below must not send twice
    chat = has_chat(key, s)
    short = MODEL(tgt).split('/')[-1]
    if queued or not chat:
        res['continue'] = 'user already wrote' if queued else 'no chat'
    else:
        res['continue'], unit = continue_turn(key, short, MODEL(tgt) != model)   # the line is asked only for a new model
        if unit:
            state.setdefault('continued', {})[unit] = key
        if res['continue'] not in ('started', 'dry-run'):
            prob(out, 'continue:' + run_id, 'chat %s: could not start the continued turn: %s' % (key[-40:], res['continue'][:120]))
    journal('chat_recover', session=key, run=run_id, why=p['kind'], frm=model + '@' + str(cur), to=tgt, **res)
    out['chats'].append((key[-40:], tgt))
    return True


def chats_home(state, health, conf, out):
    """A pinned chat parked on a fallback model goes back to its own model once an account of it answers."""
    home = state.setdefault('chat_home', {})
    for key, h in list(home.items()):
        if not fits(60):
            break
        e, _ = entry(key)
        # sessions.patch to the config's primary model stores no modelOverride at all (it clears the field)
        cur = '%s/%s' % (e.get(E_PROVIDER), e.get(E_MODEL)) if e.get(E_MODEL) else conf['primary']
        if '%s@%s' % (cur, e.get(E_PROFILE)) != h['parked']:
            home.pop(key)                  # the user chose something else since: that choice stands
            continue
        tgt = health.target(h['model'], chain=False) if ripe(h) else None
        if not tgt:
            continue
        d, err = (1, '') if (FAKE is not None or not APPLY) else gw(RPC_PATCH, PATCH_ARGS(key, tgt), 60)
        journal('chat_return', session=key, frm=h['parked'], to=tgt, result='ok' if d else err)
        if d:
            home.pop(key)
            out['chats'].append((key[-40:], tgt))
        else:
            prob(out, 'chat_return:' + key, 'chat %s: could not move back to %s: %s' % (key[-40:], tgt, err[:120]))


def key_of(rk, r):
    if not rk.get(r):
        for rec in log_lines(r):
            m = REQ_RE.search(rec.get(L2)) if isinstance(rec.get(L2), str) else None
            k = m.group(1) if m else (KEY_RE.search(rec[L1]).group(1) if isinstance(rec.get(L1), str) and KEY_RE.search(rec[L1]) else None)
            if k:
                rk[r] = k
                break
    return rk.get(r)


def note_events(state, ev, jobs, health):
    """New log events -> pending list. A stalled cron run also marks its job's account as out for that model, so
    jobs_pass moves the job in this same pass even while the probe of another model still says ok."""
    pend, rk = state.setdefault('pending', {}), state.setdefault('run_key', {})
    rk.update(ev['run_key'])
    state.setdefault('run_req', {}).update(ev['run_req'])
    for st in ev['stalls']:
        if st['key']:
            rk.setdefault(st['run'], st['key'])
        pend[st['run']] = {'kind': 'stall', 't': st['t'], 'until': st['t'] + st['delay'], 'model': st['model']}
        m = CRON_RE.match(key_of(rk, st['run']) or '')
        j = jobs.get(m.group(1)) if m else None
        if j and j['profile'] and j['since'] < st['t']:
            health.hold(j['profile'], min(st['t'] + st['delay'], NOW() + HOLD_MAX_MS), st['model'])
    for r, f in ev['failed'].items():
        if (pend.get(r) or {}).get('kind') != 'stall' and NOW() - f['t'] < FRESH_MS:
            pend[r] = {'kind': 'failed', 't': f['t'], 'until': f['t'] + WAIT_MS, 'model': f['model']}


def stalls_and_chats(state, health, jobs, out):
    """Pending limit events: stalled runs (chat or cron) and chat turns that died on a limit."""
    pend, handled = state.setdefault('pending', {}), state.setdefault('handled', {})
    rk, req = state.setdefault('run_key', {}), state.setdefault('run_req', {})
    for r, p in list(pend.items()):
        if late():
            break
        key = key_of(rk, r)
        if r in handled or not key or not (CHAT_RE.match(key) or (CRON_RE.match(key) and p['kind'] == 'stall')):
            pend.pop(r, None)              # a cron run that FAILED on a limit is handled through its receipts
            continue
        if NOW() > p['until']:
            if p['kind'] == 'failed' and CHAT_RE.match(key):
                prob(out, 'chat_wait:' + r, 'chat %s: turn failed on a limit and no live account appeared for 2 h' % key[-40:])
            pend.pop(r, None)
            continue
        try:
            s = session(key)
        except RuntimeError as e:
            if NOW() - p['t'] > 600_000:
                prob(out, 'gateway', 'Gateway did not answer for 10 min while a limit event waits: ' + str(e)[:160])
            continue                                   # Gateway busy: keep it pending
        if not s:
            pend.pop(r, None)
            continue
        if p['kind'] == 'stall':
            if not s.get(S_ACTIVE) or any(ts_ms(x) > p['t'] + 5000 for x in log_lines(r)):
                pend.pop(r, None)                      # woke up, ended or was aborted
                continue
            if CRON_RE.match(key):
                jid = CRON_RE.match(key).group(1)
                j = jobs.get(jid) or {}
                if not fits(30):
                    continue
                handled[r] = NOW()
                pend.pop(r, None)
                if j.get('kind') == 'at':              # an abort disables a one-shot job without a notice; its
                    journal('cron_stall_left', session=key, run=r, why='one-shot job: timeout retry runs on the new binding')
                    continue                           # timeout is retried by the scheduler, on the new binding
                d, err = (1, '') if (FAKE is not None or not APPLY) else gw(RPC_ABORT, ABORT_ARGS(key, r), 30)
                journal('cron_stall_abort', session=key, run=r, wait_ms=p['until'] - p['t'], result='ok' if d else err)
                out['aborted'].append(key[-50:])
                if not d:
                    prob(out, 'abort:' + r, 'could not abort stalled cron run %s: %s' % (key[-50:], err[:120]))
                elif req.get(r, 1) <= 1:               # stalled on its first model request: nothing was done yet
                    state.setdefault('rerun_wanted', {})[jid] = NOW()
                else:
                    prob(out, 'stall_mid:' + r, 'job %s (%s) stalled on a limit at model request %d and was aborted; it may '
                         'have acted already: check its transcript before rerunning' % (jid[:8], j.get('name', '')[:40], req[r]))
                continue
        else:
            if s.get(S_ACTIVE) or NOW() - p['t'] < 20_000:
                continue                               # still rotating accounts; look again next minute
            if s.get(S_LAST_RUN) != r or s.get(S_STATUS) != S_FAILED or not died_on_limit(r, p['t']):
                pend.pop(r, None)                      # answered, a newer turn ran, or it failed for another reason
                continue
        if recover_chat(state, health, key, s, r, p, out):
            handled[r] = NOW()
            pend.pop(r, None)
    for d in (handled, rk, req):
        for k in list(d)[:-300]:
            d.pop(k)


def check(state, out, jobs=None):
    """Did limits get handled in the last 24 h? Independent of the journal: reads the run history and the Gateway log."""
    jobs = jobs if jobs is not None else load_jobs(state)[0]
    streak = streaks(jobs)
    for p, n in streak.items():
        if n >= 2:
            names = [j['name'][:30] for j in jobs.values() if j['enabled'] and j['profile'] == p][:3]
            prob(out, 'streak:' + p, 'jobs on %s keep failing on a limit after their last edit (%s)' % (p, ', '.join(names)))
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
        if s.get(S_LAST_RUN) == r and s.get(S_STATUS) == S_FAILED and died_on_limit(r, t):
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
    hm = lambda ms: time.strftime('%d %b %H:%M', time.localtime(ms / 1000))
    print(json.dumps({
        'last_pass': hm(state.get('last_pass', 0)),
        'open_problems': [{'since': hm(e['raised']), 'text': e['text']} for e in state.get('problems', {}).values()
                          if now - max(e['raised'], e.get('seen', 0)) < CHECK_EVERY_MS + 5 * 60_000],
        'held_accounts': {k: hm(u) for k, u in state.get('limited', {}).items() if u > now},
        'jobs_on_fallback_model': state.get('home', {}),
        'chats_on_fallback_model': state.get('chat_home', {}),
        'pending_events': state.get('pending', {}),
    }, indent=1, ensure_ascii=False))


def main():
    out = {'moved': [], 'chats': [], 'aborted': [], 'rerun': [], 'no_target': [], 'problems': [], 'deferred': 0}
    state = load_state()
    if '--status' in sys.argv:
        return status(state)
    if '--check' in sys.argv:
        try:
            ver, missing = openclaw_version(), selfcheck()
            print(json.dumps({'openclaw': ver, 'missing': missing}, indent=1, ensure_ascii=False))
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
        if JOURNAL.exists() and JOURNAL.stat().st_size > 5_000_000:
            JOURNAL.replace(JOURNAL.with_suffix('.jsonl.1'))
    t0, before = time.time(), json.dumps(state)
    try:
        conf, cfg_err = cfg(), None
    except Shape as e:
        conf, cfg_err = None, str(e)
    if APPLY:                  # armed before the self-check: after an update it alone can take minutes on a slow Gateway
        signal.signal(signal.SIGALRM, _alarm)
        signal.alarm(max(1, int(PASS_HARD_S - (time.time() - T0))))
    try:
        ok = surface_ok(state, out, cfg_err)
    except Deadline:
        ok = False
        prob(out, 'slow', 'self-check cut off after %d s because OpenClaw answers slowly; nothing was changed'
             % PASS_HARD_S)
    if not ok or conf is None:
        signal.alarm(0)
        return finish(state, out, {'providers': set()}, {}, Health(conf or {}, {}))
    limited = state.setdefault('limited', {})
    for k in [k for k, u in limited.items() if u <= NOW()]:
        limited.pop(k)
    ev, streak, health = {'providers': set()}, {}, Health(conf, limited)
    try:
        jobs, streak = load_jobs(state)
        if jobs_partial():
            prob(out, 'partial', 'the job list has more than one page (%d jobs shown); jobs beyond it are not watched'
                 % len(jobs))
        ev = scan_logs(state)
        for p, n in streak.items():
            if n >= 2:         # held for an hour after the last failure, so jobs do not bounce straight back
                health.hold(p, NOW() + HOLD_MS)
        note_events(state, ev, jobs, health)
        # Ask the accounts when there is a fresh limit signal, an unfinished pass, or every 5 min while jobs use them.
        if ev['providers'] or streak or state.get('retry') or state.get('unsure') \
                or NOW() - state.get('probed_at', 0) >= PROBE_EVERY_MS:
            jobs_pass(conf, jobs, health, state, t0, out)
            chats_home(state, health, conf, out)
            state['probed_at'] = NOW()
            state['retry'] = bool(out['deferred'] or out['no_target'] or health.failed)
        rerun_lost(conf, jobs, health, state, out)
        stalls_and_chats(state, health, jobs, out)
        continued_check(state, out)
        if APPLY and not late() and NOW() - state.get('checked_at', 0) >= CHECK_EVERY_MS:
            state['checked_at'] = NOW()
            check(state, out, jobs)
        state.pop('down_since', None)
    except Deadline:
        prob(out, 'slow', 'pass cut off after %d s because the Gateway answers slowly; what was done is saved, the rest '
             'continues next pass' % PASS_HARD_S)
    except (Shape, Unreachable) as e:
        if not (out['moved'] or out['chats'] or out['aborted'] or out['rerun']):
            state = json.loads(before)    # nothing was done: keep log offsets and pending events for the next pass
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
    if missing:
        journal('selfcheck', version=ver, missing=missing)
        prob(out, 'selfcheck:' + '|'.join(missing), 'OpenClaw %s lacks what this script reads, so it changed nothing: %s'
             % (ver, '; '.join(missing)))
    return not missing


def finish(state, out, ev, streak, health):
    if out['no_target'] and not health.unknown():
        models = sorted({m for _, m in out['no_target']})
        prob(out, 'no_target:' + ','.join(models), 'no live account or fallback model for %s: %s'
             % (', '.join(models), ', '.join(i for i, _ in out['no_target'][:8])))
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
