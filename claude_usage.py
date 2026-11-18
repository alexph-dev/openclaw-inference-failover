#!/usr/bin/env python3
"""claude_usage.py: what is left on each Claude subscription account, read the way Claude Code reads it.

OpenClaw runs Claude subscriptions on setup-tokens, which carry only the inference scope: they cannot read the
account's limits (GET /api/oauth/usage answers 403, scope user:profile missing). This module keeps one full Claude
Code login (CLAUDE_CONFIG_DIR) per OpenClaw account, only to read, per account:
  weekly     share of the weekly limit used and when it resets
  scoped     the weekly limits of one model inside it (today: Fable, up to half of the weekly) and their resets
  five_hour  the 5-hour window, when the account has one
  resets     how many free limit resets the account holds ("cedar_ember" grants: resets_left > 0, not paused, not
             expired). It never claims one: claiming is the owner's decision (Claude Code /limit-reset).
It never sends a model request and never writes anything but its own snapshot file and the logins it owns.

Logins. Settings 'usage' (in the failover's settings file) maps an OpenClaw account to its login:
  {"anthropic:chit": {"dir": "~/.config/claude-usage/chit", "email": "chit@example.com"},
   "anthropic:sam":  {"dir": "~/.config/claude-usage/sam", "email": "sam@example.com", "borrow": "~/.claude"}}
  dir     a login this module owns: made once with
          CLAUDE_CONFIG_DIR=<dir> claude auth login --claudeai
          Its access token is refreshed here when it is about to expire (the same refresh-token grant Claude Code
          sends), under an flock on <dir>/.usage.lock, and written back to <dir>/.credentials.json (mode 0600).
  borrow  optional: another Claude Code login of the same account, read only and never refreshed (refreshing it
          would rotate the refresh token under the Claude Code that owns it). Used while 'dir' has no login yet,
          and only while its access token is still valid, so it comes and goes: its readings are shown but never
          decide the order (an order that flips each time that token lapses would move work back and forth).
  email   the account's email (GET /api/oauth/account). A login of another account is an error and is not used;
          without 'email' the reading is shown but marked unverified, and routing does not use it.
Tokens are never printed or logged.

  claude_usage.py            table of the last snapshot (no network)
  claude_usage.py --refresh  read every account now, then the table
  claude_usage.py --json     the snapshot as JSON
"""
import fcntl, json, os, pathlib, signal, sys, time, urllib.error, urllib.request
from datetime import datetime

API = 'https://api.anthropic.com'
USAGE_PATH = '/api/oauth/usage?cedar_ember=1&skip_spend=1'   # cedar_ember=1 adds the reset grants (Claude Code 2.1.322)
ACCOUNT_PATH = '/api/oauth/account'
TOKEN_URL = 'https://platform.claude.com/v1/oauth/token'     # Claude Code's TOKEN_URL and CLIENT_ID (2.1.322)
CLIENT_ID = '9d1c250a-e61b-44d9-88ed-5944d1962f5e'
# The server lists the reset grants only to the Claude Code surface (another User-Agent gets cedar_ember
# ineligible_reason 'surface' and no grants), so the reads present themselves as the Claude Code that made the login.
HEADERS = {'anthropic-beta': 'oauth-2025-04-20', 'anthropic-version': '2023-06-01', 'Accept': 'application/json',
           'User-Agent': 'claude-cli/2.1.322 (external, cli)'}
REFRESH_BEFORE_MS = 5 * 60_000   # an access token with less than this left is refreshed first
TIMEOUT_S = 15
NOW = lambda: int(time.time() * 1000)


def iso_ms(s):
    """'2026-11-19T03:59:59.587854+00:00' -> epoch ms, or None."""
    if not s:
        return None
    try:
        return int(datetime.fromisoformat(str(s).replace('Z', '+00:00')).timestamp() * 1000)
    except ValueError:
        return None


EXPECT_SCOPED = ('fable',)   # models with their own weekly limit: a reading without it is not used for them


def num(x):
    """A percentage as a number, or None for anything else (a changed API shape must not pass as a number)."""
    return float(x) if isinstance(x, (int, float)) and not isinstance(x, bool) and x == x else None


def parse(d, now=None):
    """The parts of a GET /api/oauth/usage?cedar_ember=1 answer that decide where work goes. 'resets' is None when
    the answer does not say (no grant list, or it was not shown to this surface), never a guessed 0."""
    now = NOW() if now is None else now
    out = {'weekly': None, 'scoped': {}, 'five_hour': None, 'resets': 0, 'grants': [], 'juniper': None}
    ce = d.get('cedar_ember')
    if not isinstance(ce, dict) or not isinstance(ce.get('grants'), list) \
            or ce.get('ineligible_reason') in ('surface', 'unavailable'):
        out['resets'] = None
    for lim in d.get('limits') or []:
        if not isinstance(lim, dict):
            continue
        one = {'pct': num(lim.get('percent')), 'resets_at': iso_ms(lim.get('resets_at')), 'active': lim.get('is_active')}
        if lim.get('kind') == 'weekly_all':
            out['weekly'] = one
        elif lim.get('kind') == 'weekly_scoped':
            name = (((lim.get('scope') or {}).get('model') or {}).get('display_name') or '').strip()
            if name:
                out['scoped'][name] = one
    sd = d.get('seven_day')
    if out['weekly'] is None and isinstance(sd, dict):
        out['weekly'] = {'pct': num(sd.get('utilization')), 'resets_at': iso_ms(sd.get('resets_at')), 'active': None}
    fh = d.get('five_hour')
    if isinstance(fh, dict):
        out['five_hour'] = {'pct': num(fh.get('utilization')), 'resets_at': iso_ms(fh.get('resets_at'))}
    for g in ((ce or {}).get('grants') or []) if out['resets'] is not None else []:
        if not isinstance(g, dict):
            continue
        ends = iso_ms(g.get('ends_at'))
        left = g.get('resets_left') if isinstance(g.get('resets_left'), int) else 0
        out['grants'].append({'id': g.get('id'), 'label': g.get('label'), 'type': g.get('type'), 'left': left,
                              'ends_at': ends, 'usable_now': g.get('usable_now'), 'paused': g.get('paused'),
                              'clears': g.get('clears')})
        if left > 0 and not g.get('paused') and (ends is None or ends > now):
            out['resets'] += left
    jt = d.get('juniper_tide')
    if isinstance(jt, dict):
        out['juniper'] = {'available': jt.get('available'), 'next_available_at': iso_ms(jt.get('next_available_at')),
                          'per_week': jt.get('resets_per_week')}
    return out


def scoped_for(entry, model):
    """The scoped weekly limit that covers this model ('Fable' covers anthropic/claude-fable-5-2), or None."""
    m = model.split('/', 1)[-1].lower()
    for name, lim in ((entry or {}).get('scoped') or {}).items():
        if name.lower() in m:
            return lim
    return None


def applicable(entry, model):
    """The limits that bind this model on this account: weekly, the model's own weekly limit, the 5-hour window."""
    e = entry or {}
    return [x for x in (e.get('weekly'), scoped_for(e, model), e.get('five_hour')) if x]


def live(entry, model):
    """True when the reading leaves room for this model: every limit that binds it (weekly, its own weekly limit
    such as Fable's, the 5-hour window) under 100 %."""
    w = (entry or {}).get('weekly') or {}
    if w.get('pct') is None:
        return False
    return all(x.get('pct') is None or x['pct'] < 100 for x in applicable(entry, model))


def reset_for(entry, model):
    """For an account with room: when its unused room for this model is lost (the model's own weekly limit resets
    when it has one, else the weekly). For a full one: when it is back, the latest reset of the limits it is at."""
    inf = float('inf')
    if not live(entry, model):
        full = [x.get('resets_at') or inf for x in applicable(entry, model) if (x.get('pct') or 0) >= 100]
        return max(full) if full else inf
    s = scoped_for(entry, model)
    if s and s.get('resets_at'):
        return s['resets_at']
    return ((entry or {}).get('weekly') or {}).get('resets_at') or inf


def current(entry):
    """The account's latest good reading: this one, or the last good one kept when this read failed."""
    entry = entry or {}
    return entry if entry.get('ok') else (entry.get('last_ok') or {})


def usable(entry, now, fresh_ms):
    """A reading routing may use: the latest good one (a single failed read does not flip the order) read through
    an owned login, of the verified account, not older than fresh_ms."""
    e = current(entry)
    return bool(e.get('ok') and e.get('email_ok') is True and e.get('source') == 'own' and e.get('t')
                and now - e['t'] < fresh_ms and (e.get('weekly') or {}).get('pct') is not None)


def order(accounts, snap, model, now, fresh_ms):
    """Accounts in the order to spend them for this model, or None when any of them has no usable reading (the
    caller then keeps its static order): live ones first, earliest reset first (fill what is lost soonest), then
    the full ones, the one back soonest first. Ties keep the given order."""
    prof = (snap or {}).get('profiles') or {}
    if not accounts or not all(usable(prof.get(a), now, fresh_ms) for a in accounts):
        return None
    pos, cur = {a: i for i, a in enumerate(accounts)}, {a: current(prof[a]) for a in accounts}
    if any(n in model.lower() for n in EXPECT_SCOPED) and not all(scoped_for(cur[a], model) for a in accounts):
        return None                        # its own weekly limit is missing from a reading: the API changed
    key = lambda a: (not live(cur[a], model), reset_for(cur[a], model), pos[a])
    return sorted(accounts, key=key)


# --------------------------------------------------------------------------------------------------- reading


def _http(url, token=None, body=None, timeout=TIMEOUT_S):
    h = dict(HEADERS)
    if token:
        h['Authorization'] = 'Bearer ' + token
    data = None
    if body is not None:
        data, h['Content-Type'] = json.dumps(body).encode(), 'application/json'
    req = urllib.request.Request(url, data=data, headers=h, method='POST' if body is not None else 'GET')
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode() or 'null')
    except urllib.error.HTTPError as e:
        try:
            msg = json.loads(e.read().decode() or 'null')
        except ValueError:
            msg = None
        err = (msg or {}).get('error') if isinstance(msg, dict) else None
        detail = (err.get('message') or err.get('type')) if isinstance(err, dict) else (err or '')
        return e.code, {'_error': str(detail)[:160]}


def _creds(path):
    try:
        o = json.loads(path.read_text()).get('claudeAiOauth')
    except (OSError, ValueError, AttributeError):
        return None
    return o if isinstance(o, dict) and o.get('accessToken') else None


def _write_creds(path, oauth):
    try:
        whole = json.loads(path.read_text())
    except (OSError, ValueError):
        whole = {}
    whole['claudeAiOauth'] = oauth
    tmp = path.with_name(path.name + '.tmp')
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(whole, f)
        f.flush()
        os.fsync(f.fileno())               # the old refresh token is already spent: the new one must survive
    os.replace(str(tmp), str(path))
    dfd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


def _refresh(oauth, http, timeout):
    """The refresh-token grant Claude Code sends. Returns the updated credentials (other fields kept)."""
    body = {'grant_type': 'refresh_token', 'refresh_token': oauth['refreshToken'], 'client_id': CLIENT_ID}
    if oauth.get('scopes'):
        body['scope'] = ' '.join(oauth['scopes'])
    code, d = http(TOKEN_URL, None, body, timeout=timeout)
    if code != 200 or not isinstance(d, dict) or not d.get('access_token'):
        raise RuntimeError('token refresh answered %s %s' % (code, (d or {}).get('_error', '') if isinstance(d, dict) else ''))
    new = dict(oauth, accessToken=d['access_token'], expiresAt=NOW() + int(d.get('expires_in') or 3600) * 1000)
    if d.get('refresh_token'):
        new['refreshToken'] = d['refresh_token']
    if d.get('refresh_token_expires_in'):
        new['refreshTokenExpiresAt'] = NOW() + int(d['refresh_token_expires_in']) * 1000
    if d.get('scope'):
        new['scopes'] = d['scope'].split()
    return new


def _left(deadline):
    """Seconds an HTTP call may take before the deadline; raises when there is no time for one."""
    s = min(TIMEOUT_S, deadline - time.time()) if deadline else TIMEOUT_S
    if s < 2:
        raise RuntimeError('no time left in this pass')
    return s


def _token(spec, http, deadline=None):
    """(access token, source) of the account's login, refreshing an owned one that is about to expire."""
    own = pathlib.Path(os.path.expanduser(spec['dir'])) / '.credentials.json' if spec.get('dir') else None
    if own and own.exists():
        with open(own.with_name('.usage.lock'), 'w') as lock:
            try:                                          # one refresh at a time: a refresh token is used once
                fcntl.flock(lock, fcntl.LOCK_EX | (fcntl.LOCK_NB if deadline else 0))
            except BlockingIOError:
                raise RuntimeError('login %s is being refreshed by another reader' % own.parent)
            oauth = _creds(own)
            if not oauth:
                raise RuntimeError('login %s is unreadable' % own.parent)
            if (oauth.get('expiresAt') or 0) - NOW() < REFRESH_BEFORE_MS:
                if not oauth.get('refreshToken'):
                    raise RuntimeError('login %s has no refresh token' % own.parent)
                t = _left(deadline)
                # A kill between the refresh and the write would lose the rotated refresh token, and the login with it.
                old = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM, signal.SIGTERM, signal.SIGINT})
                try:
                    oauth = _refresh(oauth, http, t)
                    _write_creds(own, oauth)
                finally:
                    signal.pthread_sigmask(signal.SIG_SETMASK, old)
            return oauth['accessToken'], 'own'
    if spec.get('borrow'):
        b = pathlib.Path(os.path.expanduser(spec['borrow'])) / '.credentials.json'
        oauth = _creds(b) if b.exists() else None
        if oauth and (oauth.get('expiresAt') or 0) - NOW() > 60_000:
            return oauth['accessToken'], 'borrowed'
        if oauth:
            raise RuntimeError('no own login yet, and the borrowed one (%s) has an expired access token' % b.parent)
    raise RuntimeError('no login: run CLAUDE_CONFIG_DIR=%s claude auth login --claudeai' % (spec.get('dir') or '?'))


def read_one(spec, http=_http, deadline=None):
    """One account's reading: {'ok', 't', 'source', 'email', 'email_ok', 'weekly', 'scoped', ...} or an error.
    deadline (epoch s): no HTTP call is started or allowed to run past it."""
    now, e = NOW(), {'t': NOW(), 'ok': False}
    try:
        token, e['source'] = _token(spec, http, deadline)
        code, acct = http(API + ACCOUNT_PATH, token, timeout=_left(deadline))
        if code != 200 or not isinstance(acct, dict):
            raise RuntimeError('account read answered %s %s' % (code, (acct or {}).get('_error', '')))
        e['email'] = acct.get('email_address') or acct.get('email')
        want = (spec.get('email') or '').strip().lower()
        e['email_ok'] = (e['email'] or '').lower() == want if want else None
        if e['email_ok'] is False:
            raise RuntimeError('login belongs to %s, not %s' % (e['email'], spec.get('email')))
        code, d = http(API + USAGE_PATH, token, timeout=_left(deadline))
        if code != 200 or not isinstance(d, dict):
            raise RuntimeError('usage read answered %s %s' % (code, (d or {}).get('_error', '')))
        e.update(parse(d, now), ok=True)
    except Exception as x:                                  # one account's failure never stops the others
        e['error'] = str(x)[:240]
    return e


def load(path):
    try:
        d = json.loads(pathlib.Path(path).read_text())
        return d if isinstance(d, dict) else {}
    except (OSError, ValueError):
        return {}


def collect(spec, path, every_ms, http=_http, now=None, budget_s=None):
    """Re-read every account whose reading is at least every_ms old; write the snapshot; return it. An account
    whose read fails keeps its last good numbers under 'last_ok' so the table and the order can still use them.
    Reading stops at budget_s seconds: no HTTP call runs past it; the next call reads the rest. One writer at a
    time (flock on <path>.lock), so a reading by hand during a failover pass loses nothing."""
    now = NOW() if now is None else now
    deadline = time.time() + budget_s if budget_s is not None else None
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(str(path) + '.lock', 'w') as lock:
        try:                               # with a budget (the failover pass) never wait for a reading by hand
            fcntl.flock(lock, fcntl.LOCK_EX | (fcntl.LOCK_NB if deadline else 0))
        except BlockingIOError:
            return load(path)
        return _collect(spec, path, every_ms, http, now, deadline)


def _collect(spec, path, every_ms, http, now, deadline):
    snap = load(path)
    prof = snap.setdefault('profiles', {})
    for p in [p for p in prof if p not in spec]:
        prof.pop(p)
    changed = False
    for p, s in spec.items():
        old = prof.get(p) or {}
        if now - (old.get('t') or 0) < every_ms:
            continue
        if deadline is not None and deadline - time.time() < 2:
            break
        new = read_one(s, http, deadline)
        if not new.get('ok'):
            good = old if old.get('ok') else old.get('last_ok')
            if good:
                new['last_ok'] = {k: v for k, v in good.items() if k != 'last_ok'}
        prof[p], changed = new, True
    if changed:
        snap['t'] = now
        tmp = path.with_name(path.name + '.tmp')          # safe: writers hold the lock
        tmp.write_text(json.dumps(snap, indent=1, ensure_ascii=False))
        tmp.replace(path)
    return snap


# --------------------------------------------------------------------------------------------------- table


def pct(x):
    return '?' if x is None else '%g' % x


def hm(ms):
    return time.strftime('%a %d %b %H:%M %Z', time.localtime(ms / 1000)) if ms else '-'


def rows(snap, now=None):
    """One plain line per account: the shape both the failover's --status and the daily spend report show."""
    now = NOW() if now is None else now
    out = []
    for p, e in sorted(((snap or {}).get('profiles') or {}).items()):
        src = current(e)
        w, parts = src.get('weekly') or {}, []
        if w:
            parts.append('weekly %s%% (resets %s)' % (pct(w.get('pct')), hm(w.get('resets_at'))))
        for name, s in sorted((src.get('scoped') or {}).items()):
            parts.append('%s %s%% (resets %s)' % (name, pct(s.get('pct')), hm(s.get('resets_at'))))
        if src.get('five_hour') and src['five_hour'].get('pct') is not None:
            parts.append('5-hour %s%%' % pct(src['five_hour']['pct']))
        if src:
            n = src.get('resets')
            parts.append('resets available %s' % ('?' if n is None else n))
        age = '%d min ago' % ((now - src['t']) // 60_000) if src.get('t') else 'never read'
        flag = '' if e.get('ok') else ' [last read failed: %s]' % e.get('error', '?')
        if e.get('ok') and e.get('email_ok') is None:
            flag = ' [account not verified: no email set]'
        out.append({'account': p, 'email': src.get('email') or e.get('email'), 'source': e.get('source'),
                    'text': '; '.join(parts) or 'no reading', 'read': age + flag})
    return out


def main():
    here = pathlib.Path(__file__).resolve().parent
    sys.path.insert(0, str(here))
    import failover as F                                    # the failover owns the settings and the state dir
    spec = F.load_usage()[0]
    path = F.USAGE
    if '--refresh' in sys.argv:
        collect(spec, path, 0)
    snap = load(path)
    if '--json' in sys.argv:
        print(json.dumps(snap, indent=1, ensure_ascii=False))
        return
    for r in rows(snap):
        print('%-18s %-28s %s  [%s, %s]' % (r['account'], r['email'] or '?', r['text'], r['source'] or '-', r['read']))
    if not spec:
        print('no "usage" logins in %s' % F.SETTINGS)


if __name__ == '__main__':
    main()
