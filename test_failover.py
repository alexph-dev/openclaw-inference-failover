#!/usr/bin/env python3
"""Fixture tests for failover.py: no Gateway, no real edits, no messages.
Run: python3 test_failover.py"""
import functools, json, os, pathlib, sqlite3, subprocess, sys, tempfile, time
from unittest.mock import patch

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
import failover as F                                                  # names of the OpenClaw surface only


def offline(test):
    """Unit tests that mock APPLY must never write to the operator's actual state/journal. Each starts its own pass
    clock: F.T0 is set at import, so a late test in a long suite would otherwise look like a pass out of time."""
    @functools.wraps(test)
    def wrapped():
        with tempfile.TemporaryDirectory(prefix='ocf-unit-') as tmp:
            d = pathlib.Path(tmp)
            with patch.object(F, 'SDIR', d), patch.object(F, 'STATE', d / 'state.json'), \
                    patch.object(F, 'JOURNAL', d / 'journal.jsonl'), patch.object(F, 'T0', time.time()):
                return test()
    return wrapped
ERR = 'Automation candidate is incompatible with its recorded inference account and route.'
OPUS, FABLE = 'anthropic/claude-opus-5-5', 'anthropic/claude-fable-5-2'
PA, PB, PC = 'anthropic:a', 'anthropic:b', 'anthropic:c'
OA = 'openai:a'
ms = lambda: int(time.time() * 1000)
DEAD = {PA: 'rate_limit', PB: 'rate_limit', PC: 'rate_limit'}


FAKE_CLI = r'''
import json, os, sqlite3, sys
"""Fake 'openclaw' for the tests: serves the documented CLI JSON from the fixture database."""
a = sys.argv[1:]
open(os.environ['OCF_FAKE_CALLS'], 'a').write(' '.join(a) + '\n')
if a == ['--version']:
    print('OpenClaw %s (test)' % os.environ.get('OCF_FAKE_VERSION', '2026.10.10'))
    sys.exit(0)
if os.environ.get('OCF_FAKE_CLI_DOWN'):
    sys.stderr.write('gateway timeout after 60000ms\n')
    sys.exit(1)
c = sqlite3.connect(os.environ['OCF_FAKE_DB'])
runs = [dict(zip(('jobId', 'status', 'error', 'runAtMs'), r)) for r in
        c.execute('select job_id, status, error_text, started_at_ms from cron_run_receipts order by started_at_ms desc')]
if a[:2] == ['cron', 'list']:
    jobs = []
    for jid, j, en, st in c.execute('select job_id, job_json, enabled, state_json from cron_jobs'):
        if not en and '--all' not in a:
            continue
        d, s = json.loads(j), json.loads(st or '{}')
        mine = [r for r in runs if r['jobId'] == jid]
        n = 0
        for r in mine:
            if r['status'] != 'error':
                break
            n += 1
        if mine:
            s.update(lastRunStatus=mine[0]['status'], lastRunAtMs=mine[0]['runAtMs'], lastError=mine[0]['error'],
                     consecutiveErrors=n)
        d.pop('inferenceBinding', None)
        jobs.append(dict(d, id=jid, enabled=bool(en), agentId='main', state=s))
    print(json.dumps({os.environ.get('OCF_FAKE_JOBS_KEY', 'jobs'): jobs, 'hasMore': bool(os.environ.get('OCF_FAKE_MORE'))}))
elif a[:2] == ['models', 'status']:
    f = json.load(open(os.environ['OCF_FAKE']))
    print(json.dumps(f.get('models_status', {'auth': {'oauth': {'warnAfterMs': 86400000, 'profiles': [], 'providers': []}}})))
    sys.exit(f.get('models_status_rc', 0))
elif a[:4] == ['models', 'auth', 'order', 'get']:
    sys.stderr.write('Cannot admit models.auth: This operation requires exclusive offline state ownership\n')
    sys.exit(1)
elif a[:2] == ['config', 'set']:
    f = os.environ['OCF_CONFIG']
    c = json.load(open(f))
    path, new = a[2].split('.'), json.loads(a[3])
    cur = c
    for k in path[:-1]:
        cur = cur.setdefault(k, {})
    if '--expect-current-json' in a and cur.get(path[-1]) != json.loads(a[a.index('--expect-current-json') + 1]):
        sys.stderr.write('Config change declined (conditional config set expectation did not match the authored config). '
                         'No settings were saved. Review the current config and any conditional expectations before retrying.\n')
        sys.exit(1)
    cur[path[-1]] = new
    json.dump(c, open(f, 'w'))
elif a[:2] == ['cron', 'runs']:
    lim = int(a[a.index('--limit') + 1])
    print(json.dumps({'entries': [r for r in runs if r['status'] == 'error'][:lim], 'total': 0, 'hasMore': False}))
else:
    sys.stderr.write('fake openclaw: unsupported %s\n' % a)
    sys.exit(2)
'''


def setup(jobs=None, receipts=(), probe=None, sessions=None, entries=None, log=(), edited_ago=3600, sched=None):
    sessions, entries = dict(sessions or {}), dict(entries or {})
    for key in entries:
        sessions.setdefault(key, {'key': key, 'hasActiveRun': False})
    # Real active session entries expose the writer run even when describe omits activeRunIds/lastRunId.
    for rec in log:
        two, one = rec.get('2'), rec.get('1')
        request = F.REQ_RE.search(two) if isinstance(two, str) else None
        retry = F.RETRY_RE.search(one) if isinstance(one, str) else None
        key_match = F.KEY_RE.search(one) if isinstance(one, str) else None
        key, run = (request.group(1), request.group(2)) if request else \
            (key_match.group(1), retry.group(3)) if retry and key_match else (None, None)
        if key in sessions and sessions[key].get('hasActiveRun'):
            entries[key] = dict(entries.get(key, {}), activeWriterRunId=run)
    d = pathlib.Path(tempfile.mkdtemp(prefix='ocf-test-'))
    c = sqlite3.connect(d / 'state.sqlite')
    c.execute('create table cron_jobs(job_id, job_json, enabled, state_json, updated_at)')
    c.execute('create table cron_run_receipts(job_id, status, error_text, started_at_ms)')
    c.execute('create table config_machine_state(state_key primary key, value_json)')
    c.execute('insert into config_machine_state values(?,?)', ('auth.sharedStore', '{"location":"state-db"}'))
    for jid, (model, prof, mode) in (jobs or {}).items():
        c.execute('insert into cron_jobs values(?,?,1,?,?)', (jid, json.dumps({
            'name': jid, 'payload': {'model': model + '@' + prof}, 'inferenceBinding': {'profileId': prof},
            'scheduledToolPolicy': {'mode': mode}, 'schedule': {'kind': (sched or {}).get(jid, ('cron', 0))[0]}}),
            json.dumps({'nextRunAtMs': ms() + (sched or {}).get(jid, ('cron', 0))[1] * 1000}), ms() - edited_ago * 1000))
    c.commit()
    c.close()
    pkg_strings(d / 'pkg', F.ANCHORS + F.ERROR_TEXTS + (F.RPC_PROBE, F.RPC_DESCRIBE, F.RPC_PATCH, F.RPC_ABORT))
    a = d / 'oc' / 'agents' / 'main' / 'agent'
    a.mkdir(parents=True)
    c = sqlite3.connect(a / 'openclaw-agent.sqlite')
    c.execute('create table session_nodes(session_key, entry_json)')
    c.execute('create table session_pending_inputs(session_key, state)')
    c.execute('create table auth_profile_state(state_key primary key, state_json)')
    c.commit()
    c.close()
    (d / 'bin').mkdir()
    (d / 'bin' / 'openclaw').write_text('#!%s\n%s' % (sys.executable, FAKE_CLI))
    (d / 'bin' / 'openclaw').chmod(0o755)
    (d / 'logs').mkdir()
    (d / 'logs' / 'openclaw-2026-01-01.log').write_text(''.join(json.dumps(x) + '\n' for x in log))
    (d / 'cfg.json').write_text(json.dumps({'agents': {'defaults': {'model': {'primary': OPUS, 'fallbacks': [
        'xai/grok-4.7', OPUS, 'openai/gpt-6.1-sol']}}}, 'auth': {'order': {'anthropic': [PB, PA, PC], 'openai': ['openai:default', OA, 'openai:b'], 'xai': ['xai:x']}}}))
    fake(d, probe=probe or {}, sessions=sessions or {}, entries=entries or {})
    receipt(d, receipts)
    return d


def pkg_strings(pkg, strings):
    """A fake installed OpenClaw package whose code contains these strings."""
    for sub in ('dist', 'node_modules/@openclaw/ai/dist'):
        (pkg / sub).mkdir(parents=True, exist_ok=True)
        (pkg / sub / 'x.js').write_text('')
    (pkg / 'dist' / 'a.js').write_text('\n'.join('log(%s);' % json.dumps(s) for s in strings))


def fake(d, **kw):
    if 'saved_orders' in kw:
        # Persist native nonsecret auth state in actual SQLite fixtures. The auth
        # CLI deliberately refuses online access, exactly as the installed build.
        for db in (d / 'oc' / 'agents').glob('*/agent/openclaw-agent.sqlite'):
            with sqlite3.connect(db) as c:
                c.execute('delete from auth_profile_state')
        for agent, orders in kw.pop('saved_orders').items():
            db = d / 'oc' / 'agents' / agent / 'agent' / 'openclaw-agent.sqlite'
            db.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(db) as c:
                c.execute('create table if not exists session_nodes(session_key, entry_json)')
                c.execute('create table if not exists session_pending_inputs(session_key, state)')
                c.execute('create table if not exists auth_profile_state(state_key primary key, state_json)')
                c.execute('insert or replace into auth_profile_state values(?,?)',
                          ('primary', json.dumps({'version': 1, 'order': orders})))
    f = d / 'fake.json'
    cur = json.loads(f.read_text()) if f.exists() else {}
    cur.update(kw)
    f.write_text(json.dumps(cur))


def receipt(d, rows):
    c = sqlite3.connect(d / 'state.sqlite')
    for jid, st, err, ago in rows:
        c.execute('insert into cron_run_receipts values(?,?,?,?)', (jid, st, err, ms() - ago * 1000))
    c.commit()
    c.close()


def binding(d, jid):
    c = sqlite3.connect(d / 'state.sqlite')
    j = json.loads(c.execute('select job_json from cron_jobs where job_id=?', (jid,)).fetchone()[0])
    return j['payload']['model']


def go(d, stale_probe=False, *extra, env=None):
    env_extra = env
    st = d / 'st' / 'state.json'
    if not st.exists():                                               # the script has been running for a while
        st.parent.mkdir(exist_ok=True)
        st.write_text(json.dumps({'started': 0}))
    if stale_probe and st.exists():
        s = json.loads(st.read_text())
        s['probed_at'] = 0
        st.write_text(json.dumps(s))
    env = dict(os.environ, OCF_STATE=str(d / 'st'), OCF_STATE_DB='file:%s?mode=ro' % (d / 'state.sqlite'),
               OCF_FAKE_DB=str(d / 'state.sqlite'), OCF_CONFIG=str(d / 'cfg.json'), OCF_LOG_DIR=str(d / 'logs'),
               OCF_FAKE=str(d / 'fake.json'), OCF_DIST=str(d / 'pkg' / 'dist'), OCF_OPENCLAW_DIR=str(d / 'oc'), OCF_FAKE_CALLS=str(d / 'calls.log'), OCF_SETTINGS=str(d / 'settings.json'),
               PATH=str(d / 'bin') + os.pathsep + os.environ.get('PATH', ''))
    env.update(env_extra or {})
    p = subprocess.run([sys.executable, str(HERE / os.environ.get('OCF_SCRIPT', 'failover.py')), '--apply', *extra], env=env, capture_output=True, text=True)
    assert p.returncode in (0, 1), p.stderr
    jf = d / 'st' / 'journal.jsonl'
    j = [json.loads(x) for x in open(jf)] if jf.exists() else []
    return p.returncode, p.stderr, j


def kinds(j, kind):
    return [r for r in j if r['kind'] == kind]


JOBS = {'a': (OPUS, PA, 'trusted'), 'b': (OPUS, PA, 'trusted'), 'c': ('openai/gpt-6.1-sol', 'openai:default', 'trusted')}


def test_dead_account_jobs_move_to_same_model_and_stay():
    d = setup(dict(JOBS, old=(OPUS, PA, 'account')), [('a', 'error', ERR, 60)], {'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}})
    rc, err, j = go(d)
    assert {(r['job'], r['to']) for r in kinds(j, 'job_move')} == {('a', OPUS + '@' + PB), ('b', OPUS + '@' + PB)}, j
    assert binding(d, 'c') == 'openai/gpt-6.1-sol@openai:default'
    assert rc == 1 and 'legacy account-mode job old' in err            # legacy job is reported, untouched
    fake(d, probe={'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}})
    go(d, True)
    assert binding(d, 'a') == OPUS + '@' + PB                       # no return when the first account recovers


def test_quiet_dead_account_is_still_found_by_the_periodic_probe():
    # Jobs stayed on an exhausted account because nothing new was failing.
    d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}})
    st = d / 'st'
    st.mkdir()
    (st / 'state.json').write_text(json.dumps({'probed_at': ms() - 60_000}))
    assert not kinds(go(d)[2], 'job_move')                            # no signal, probed a minute ago: no probe
    assert len(kinds(go(d, True)[2], 'job_move')) == 2                 # 5 min later the probe finds the account out


def test_fallback_chain_and_nothing_alive():
    d = setup(JOBS, [], {'anthropic': DEAD, 'xai': {'xai:x': 'ok'}, 'openai': {'openai:default': 'ok'}})
    assert {r['to'] for r in kinds(go(d)[2], 'job_move')} == {'xai/grok-4.7@xai:x'}
    d = setup(JOBS, [], {'anthropic': DEAD, 'xai': {'xai:x': 'rate_limit'}, 'openai': {'openai:default': 'rate_limit', OA: 'ok'}})
    assert {r['to'] for r in kinds(go(d)[2], 'job_move')} == {'openai/gpt-6.1-sol@' + OA}   # '@' inside an account id
    d = setup(JOBS, [], {'anthropic': DEAD, 'xai': {'xai:x': 'rate_limit'}, 'openai': {'openai:default': 'rate_limit'}})
    rc, err, j = go(d)
    assert rc == 1 and 'no live account' in err and not kinds(j, 'job_move')
    d = setup(JOBS, [], {'anthropic': DEAD, 'xai': {'xai:x': 'timeout'}, 'openai': {'openai:default': 'rate_limit'}})
    rc, err, j = go(d)
    assert rc == 0, err                                               # a probe timeout is unknown, not "nothing alive"


def test_no_false_alarm():
    d = setup(JOBS, [('a', 'error', ERR, 60)], {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}})
    assert not kinds(go(d)[2], 'job_move')                            # one failure while the account answers: wait
    d = setup(JOBS, [('a', 'error', 'Quota exceeded for quota metric Read requests', 120), ('a', 'error', 'Notion 429', 60)],
              {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}})
    rc, err, j = go(d)
    assert rc == 0 and not kinds(j, 'job_move')                       # business errors are not limits
    d = setup(JOBS, [('a', 'error', ERR, 600), ('a', 'error', ERR, 500)], {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}})
    (d / 'st').mkdir()                                                # this script rebound 'a' a minute ago
    (d / 'st' / 'state.json').write_text(json.dumps({'bindings': {'a': {'model': OPUS + '@' + PA, 't': ms() - 60_000}}}))
    rc, err, j = go(d, False, '--check')
    assert rc == 0, err                                               # failures from before the rebinding don't count


K = 'agent:main:telegram:group:-1001:topic:7'
PIN = {'providerOverride': 'anthropic', 'modelOverride': 'claude-fable-5-2', 'modelOverrideSource': 'user',
       'authProfileOverride': PB, 'authProfileOverrideSource': 'user'}


def line(one, two=None, ago=40):
    return {'1': one, '2': two, '_meta': {'date': time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(time.time() - ago)) + '.000Z'}}


def req(key, run, ago=40):
    return line({'provider': 'anthropic', 'model': 'claude-fable-5-2', 'status': 429},
                'Model request failed sessionKey="%s" runId="%s" requestId="x"' % (key, run), ago)


def end(run, ago=40):
    return line({'event': 'embedded_run_agent_end', 'runId': run, 'isError': True, 'failoverReason': 'rate_limit',
                 'provider': 'anthropic', 'model': 'claude-fable-5-2'}, 'embedded run agent end', ago)


def stall(model, run, key=None, delay=3600000):
    return line('transient same-model retry 1/9 for %s reason=rate_limit: delayMs=%d runId="%s" sessionId="s"%s'
                % (model, delay, run, (' sessionKey="%s"' % key) if key else ''))


def test_stalled_pinned_chat_moves_aborts_and_continues():
    d = setup(log=[req(K, 'R1'), stall(FABLE, 'R1')], probe={'anthropic': {PB: 'rate_limit', PA: 'ok', PC: 'rate_limit'}},
              sessions={K: {'key': K, 'hasActiveRun': True, 'lastRunId': 'R0'}}, entries={K: PIN})
    rc, err, j = go(d)
    r, = kinds(j, 'chat_recover')
    assert (r['why'], r['to'], r['patch'], r['abort'], r['continue']) == ('stall', FABLE + '@' + PA, 'ok', 'ok', 'started'), r
    assert len(kinds(go(d)[2], 'chat_recover')) == 1                  # handled once


def test_model_limit_does_not_block_the_same_account_for_the_next_model():
    # The account the stalled model ran on still answers. Other accounts of that model are out, and the first
    # fallback model is served by the same account. It must be used. Skipping the account for every later model
    # lands on Sol even though Opus on that account is fine.
    d = setup(log=[req(K, 'R4'), stall(FABLE, 'R4')],
              probe={'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'},
                     'xai': {'xai:x': 'unknown'}, 'openai': {'openai:default': 'ok'}},
              sessions={K: {'key': K, 'hasActiveRun': True, 'lastRunId': 'R4'}}, entries={K: PIN})
    r, = kinds(go(d)[2], 'chat_recover')
    assert r['to'] == OPUS + '@' + PB, r


def test_failed_chat_all_accounts_dead_goes_down_the_chain():
    ses = {K: {'key': K, 'hasActiveRun': False, 'lastRunId': 'R2', 'status': 'failed'}}
    d = setup(log=[req(K, 'R2'), end('R2')], probe={'anthropic': DEAD, 'xai': {'xai:x': 'ok'}}, sessions=ses, entries={K: PIN})
    j = go(d)[2]
    r, = kinds(j, 'chat_recover')
    assert (r['why'], r['to'], r['abort'], r['continue']) == ('failed', 'xai/grok-4.7@xai:x', 'not needed', 'started'), r
    assert 'note' not in r                                            # the script itself sends nothing to a chat
    m, = kinds(j, 'chat_move')                                        # moved once, by the pass or by the event
    assert m['to'] == 'xai/grok-4.7@xai:x', m


def test_unpinned_stalled_chat_applies_healthy_target_before_continuation():
    d = setup(log=[req(K, 'R3'), stall(OPUS, 'R3')], probe={'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'ok'}},
              sessions={K: {'key': K, 'hasActiveRun': True}}, entries={K: {'authProfileOverride': PA, 'authProfileOverrideSource': 'auto'}})
    r, = kinds(go(d)[2], 'chat_recover')
    assert (r['patch'], r['abort'], r['continue']) == ('ok', 'ok', 'started'), r


def test_user_already_wrote_again():
    # The newer message wins over the continuation, but the pinned chat still leaves the dead account, so that
    # message does not fail on it too.
    ses = {K: {'key': K, 'hasActiveRun': False, 'lastRunId': 'R4', 'status': 'failed'}}
    d = setup(log=[req(K, 'R4'), end('R4')], probe={'anthropic': DEAD, 'xai': {'xai:x': 'ok'}}, sessions=ses, entries={K: PIN})
    fake(d, queued={K: True})
    j = go(d)[2]
    drop, = kinds(j, 'event_drop')
    assert (drop['run'], drop['why']) == ('R4', 'the user already wrote') and not kinds(j, 'chat_recover'), j
    assert [m['to'] for m in kinds(j, 'chat_move')] == ['xai/grok-4.7@xai:x'], j


def test_stalled_cron_run_is_aborted_not_continued():
    # A cron run sleeping a 44 h same-model wait until its own timeout.
    ck = 'agent:main:cron:00000000-0000-4000-8000-00000000000a:run:00000000-0000-4000-8000-0000000000f1'
    d = setup({'00000000-0000-4000-8000-00000000000a': (OPUS, PA, 'trusted')},
              log=[line({'provider': 'anthropic', 'model': 'claude-opus-5-5', 'status': 429},
                        'Model request failed sessionKey="%s" runId="26cafcf4" requestId="x"' % ck),
                   stall(OPUS, '26cafcf4', delay=158394000)],
              probe={'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}},
              sessions={ck: {'key': ck, 'hasActiveRun': True}})
    rc, err, j = go(d)
    a, = kinds(j, 'cron_stall_abort')
    assert a['run'] == '26cafcf4' and a['result'] == 'ok' and not kinds(j, 'chat_recover'), j
    assert binding(d, '00000000-0000-4000-8000-00000000000a') == OPUS + '@' + PB
    held = json.loads((d / 'st' / 'state.json').read_text())['limited']
    assert held[OPUS + '@' + PA] > ms() + 40 * 3600_000, held        # until the time the provider named: no cap


def test_chat_cases_left_alone():
    d = setup(log=[req(K, 'R5'), end('R5')], probe={'anthropic': DEAD, 'xai': {'xai:x': 'ok'}},
              sessions={K: {'key': K, 'hasActiveRun': False, 'lastRunId': 'R9', 'status': 'done'}}, entries={K: PIN})
    j = go(d)[2]
    assert not kinds(j, 'chat_recover') and kinds(j, 'event_drop')[0]['why'] == 'a newer run took over', j
    later = line({'runId': 'R6', 'milestone': 'tool_execution_started'}, 'agent turn milestone', ago=10)
    d = setup(log=[req(K, 'R6'), end('R6'), later], probe={'anthropic': DEAD, 'xai': {'xai:x': 'ok'}},
              sessions={K: {'key': K, 'hasActiveRun': False, 'lastRunId': 'R6', 'status': 'failed'}}, entries={K: PIN})
    j = go(d)[2]
    assert not kinds(j, 'chat_recover') and 'last word' in kinds(j, 'event_drop')[0]['why'], j   # went on after the limit
    d = setup(log=[req(K, 'R7', 3600), end('R7', 3600)], probe={'anthropic': DEAD, 'xai': {'xai:x': 'ok'}},
              sessions={K: {'key': K, 'hasActiveRun': False, 'lastRunId': 'R7', 'status': 'failed'}}, entries={K: PIN})
    rc, err, j = go(d)
    assert not kinds(j, 'chat_recover')                               # an hour-old failure is not continued...
    assert rc == 1 and 'still unanswered' in err                      # ...but the check reports it


def test_gateway_busy_keeps_the_chat_pending():
    ses = {K: {'key': K, 'hasActiveRun': False, 'lastRunId': 'R8', 'status': 'failed'}}
    d = setup(log=[req(K, 'R8'), end('R8')], probe={'anthropic': DEAD, 'xai': {'xai:x': 'ok'}}, sessions=ses, entries={K: PIN})
    fake(d, describe_error='gateway restarting')
    rc, err, j = go(d)
    assert not kinds(j, 'chat_recover') and 'R8' in json.loads((d / 'st' / 'state.json').read_text())['pending']
    fake(d, describe_error='')
    assert len(kinds(go(d)[2], 'chat_recover')) == 1


def test_check_reports_jobs_still_failing_after_a_move():
    d = setup(JOBS, [('a', 'error', ERR, 120), ('a', 'error', ERR, 60)], {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}}, edited_ago=600)
    rc, err, j = go(d, False, '--check')
    assert rc == 1 and 'keep failing on a limit' in err, err


def test_problem_fails_two_passes_then_stays_quiet():
    # Ten errors in a row make the scheduler disable a recurring job; a failure monitor may need two in a row.
    d = setup(JOBS, [], {'anthropic': DEAD, 'xai': {'xai:x': 'rate_limit'}, 'openai': {'openai:default': 'rate_limit'}})
    assert [go(d, True)[0] for _ in range(5)] == [1, 1, 0, 0, 0]
    s = json.loads((d / 'st' / 'state.json').read_text())
    (k, e), = s['problems'].items()
    assert k.startswith('no_target:') and e['left'] == 0


def test_expired_token_does_not_hide_nothing_alive():
    d = setup(JOBS, [], {'anthropic': {PB: 'rate_limit', PA: 'rate_limit', PC: 'auth'}, 'xai': {'xai:x': 'rate_limit'},
                         'openai': {'openai:default': 'rate_limit'}})
    rc, err, j = go(d)
    assert rc == 1 and 'no live account' in err, err


def test_job_parked_on_a_fallback_model_returns_to_its_own_model():
    d = setup(JOBS, [], {'anthropic': DEAD, 'xai': {'xai:x': 'ok'}})
    go(d)
    assert binding(d, 'a') == 'xai/grok-4.7@xai:x'
    go(d, True)
    assert binding(d, 'a') == 'xai/grok-4.7@xai:x'                    # still nothing alive for its own model
    fake(d, probe={'anthropic': {PB: 'rate_limit', PA: 'ok', PC: 'rate_limit'}, 'xai': {'xai:x': 'ok'}})
    go(d, True)
    assert binding(d, 'a') == OPUS + '@' + PA and binding(d, 'b') == OPUS + '@' + PA
    assert not json.loads((d / 'st' / 'state.json').read_text())['home']


def test_job_that_lost_a_run_to_a_limit_is_moved_and_never_rerun():
    # Daily or frequent, overdue, lost long ago, or after it had started: the job is rebound and its next scheduled
    # run uses the new binding. The watchdog never runs a job itself and reports nothing for the lost run.
    mid = line({'provider': 'anthropic', 'model': 'claude-opus-5-5', 'status': 429},
               'Model request failed sessionKey="agent:main:cron:a:run:s1" runId="RA" requestId="x" requestNumber=5', ago=100)
    for receipts, sched in (([('a', 'error', ERR, 120), ('b', 'error', ERR, 60)], {'a': ('cron', 20 * 3600), 'b': ('every', 600)}),
                            ([('a', 'error', ERR, 50 * 60)], {'a': ('cron', 20 * 3600)}),
                            ([('a', 'error', ERR, 120)], {'a': ('cron', -120)}),
                            ([('a', 'error', "429 rate_limit_error: This request would exceed your account's rate limit", 120)],
                             {'a': ('cron', 20 * 3600)})):
        d = setup(JOBS, receipts, {'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}}, sched=sched, log=[mid])
        rc1, err1, _ = go(d)
        rc2, err2, j = go(d, True)
        assert binding(d, 'a') == OPUS + '@' + PB and not kinds(j, 'job_rerun'), j
        assert rc1 == rc2 == 0 and not err1 + err2, (err1, err2)
        assert not [c for c in calls(d) if c.split()[:2] == ['cron', 'run']], calls(d)   # no job is run


STALL_JOB = '00000000-0000-4000-8000-00000000000a'
STALL_KEY = 'agent:main:cron:%s:run:00000000-0000-4000-8000-0000000000f1' % STALL_JOB


def stall_log(request_number=1):
    return [line({'provider': 'anthropic', 'model': 'claude-opus-5-5', 'status': 429},
                 'Model request failed sessionKey="%s" runId="26cafcf4" requestId="x" requestNumber=%d' % (STALL_KEY, request_number)),
            stall(OPUS, '26cafcf4', delay=158394000)]


def test_stall_while_the_probe_still_says_ok_moves_the_job():
    # The probe tests one model per provider; a stall is the real evidence for this model on this account.
    d = setup({STALL_JOB: (OPUS, PA, 'trusted')}, log=stall_log(), probe={'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}},
              sessions={STALL_KEY: {'key': STALL_KEY, 'hasActiveRun': True}}, sched={STALL_JOB: ('cron', 20 * 3600)})
    rc, err, j = go(d)
    assert binding(d, STALL_JOB) == OPUS + '@' + PB and len(kinds(j, 'cron_stall_abort')) == 1, j
    held = json.loads((d / 'st' / 'state.json').read_text())['limited']
    assert list(held) == [OPUS + '@' + PA] and held[OPUS + '@' + PA] > ms() + 40 * 3600_000, held   # this model only
    j = go(d, True)[2]
    assert binding(d, STALL_JOB) == OPUS + '@' + PB and not kinds(j, 'job_rerun')   # held: no bounce back, no rerun


def test_stalled_cron_run_is_aborted_and_never_rerun():
    # Wherever the run stalled, it is aborted so it does not sleep for hours; the job only gets its new binding, and
    # the next scheduled run uses it. Nothing replays the aborted run, and nothing is reported for it.
    for n in (1, 7):
        d = setup({STALL_JOB: (OPUS, PA, 'trusted')}, log=stall_log(n), probe={'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'ok'}},
                  sessions={STALL_KEY: {'key': STALL_KEY, 'hasActiveRun': True}}, sched={STALL_JOB: ('cron', 20 * 3600)})
        rc1, err1, _ = go(d)
        rc2, err2, j = go(d, True)
        assert rc1 == rc2 == 0 and len(kinds(j, 'cron_stall_abort')) == 1, (err1, err2, j)
        assert binding(d, STALL_JOB) == OPUS + '@' + PB and not kinds(j, 'job_rerun')
        assert not [c for c in calls(d) if c.split()[:2] == ['cron', 'run']], calls(d)   # no job is run


def test_stalled_one_shot_job_is_moved_but_never_aborted():
    # An aborted "at" job is disabled by the scheduler without a notice; its timeout is retried on the new binding.
    d = setup({STALL_JOB: (OPUS, PA, 'trusted')}, log=stall_log(), probe={'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'ok'}},
              sessions={STALL_KEY: {'key': STALL_KEY, 'hasActiveRun': True}}, sched={STALL_JOB: ('at', 0)})
    rc, err, j = go(d)
    assert not kinds(j, 'cron_stall_abort') and len(kinds(j, 'cron_stall_left')) == 1
    assert binding(d, STALL_JOB) == OPUS + '@' + PB


def sleeping_cron_log(*later):
    # As OpenClaw 2026.11.3 logs it: the attempt ends on the 429, a cleanup line of the same attempt follows within a
    # second or two, then the run sleeps out the provider's wait. Nothing else names the run while it sleeps.
    return stall_log() + [line({'event': 'embedded_run_agent_end', 'runId': '26cafcf4', 'isError': True,
                                'failoverReason': 'rate_limit', 'provider': 'anthropic', 'model': 'claude-opus-5-5'},
                               'embedded run agent end', ago=41),
                          line({'runId': '26cafcf4', 'failurePhase': 'bridge'}, 'Code Mode execution failed', ago=38)] + list(later)


def test_cron_run_sleeping_between_attempts_is_aborted_though_its_session_is_not_active():
    # A cron run's session reports no active run while it sleeps out a same-model retry: OpenClaw clears the attempt's
    # run handle before the sleep. Only a later log line of the run means it woke up or ended.
    for sched in (('cron', 20 * 3600), ('every', 3600)):
        d = setup({STALL_JOB: (OPUS, PA, 'trusted')}, log=sleeping_cron_log(),
                  probe={'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'ok'}},
                  sessions={STALL_KEY: {'key': STALL_KEY, 'hasActiveRun': False}}, sched={STALL_JOB: sched})
        rc, err, j = go(d)
        a, = kinds(j, 'cron_stall_abort')
        assert (a['run'], a['result']) == ('26cafcf4', 'ok') and not kinds(j, 'event_drop'), (err, j)
        assert binding(d, STALL_JOB) == OPUS + '@' + PB
        assert len(kinds(go(d, True)[2], 'cron_stall_abort')) == 1        # handled once


def test_cron_run_that_woke_up_or_ended_after_the_stall_is_left_alone():
    later = (line('lane task error: lane=cron-nested error="aborted" runId=26cafcf4', ago=10),   # it ended
             line('[model-fetch] response status=200 runId="26cafcf4"', ago=10))               # it woke up
    for x in later:
        for active in (False, True):
            d = setup({STALL_JOB: (OPUS, PA, 'trusted')}, log=sleeping_cron_log(x),
                      probe={'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'ok'}},
                      sessions={STALL_KEY: {'key': STALL_KEY, 'hasActiveRun': active}}, sched={STALL_JOB: ('cron', 3600)})
            rc, err, j = go(d)
            drop, = kinds(j, 'event_drop')
            assert drop['why'] == 'the cron run woke up or ended' and not kinds(j, 'cron_stall_abort'), j


def test_sleeping_one_shot_cron_run_is_left_even_when_its_session_is_not_active():
    d = setup({STALL_JOB: (OPUS, PA, 'trusted')}, log=sleeping_cron_log(),
              probe={'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'ok'}},
              sessions={STALL_KEY: {'key': STALL_KEY, 'hasActiveRun': False}}, sched={STALL_JOB: ('at', 0)})
    rc, err, j = go(d)
    assert not kinds(j, 'cron_stall_abort') and len(kinds(j, 'cron_stall_left')) == 1 and not kinds(j, 'event_drop'), j


def test_cron_stall_abort_that_stops_nothing_is_reported():
    # sessions.abort answers ok but names no aborted run: the run still sleeps. That is a problem, not an abort.
    d = setup({STALL_JOB: (OPUS, PA, 'trusted')}, log=sleeping_cron_log(),
              probe={'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'ok'}},
              sessions={STALL_KEY: {'key': STALL_KEY, 'hasActiveRun': False}}, sched={STALL_JOB: ('cron', 3600)})
    fake(d, abort_answer={'ok': True, 'abortedRunId': None, 'status': 'no-active-run'})
    rc, err, j = go(d)
    a, = kinds(j, 'cron_stall_abort')
    assert a['result'] == 'no-active-run' and rc == 1 and 'found no active run' in err, (rc, err, j)
    assert binding(d, STALL_JOB) == OPUS + '@' + PB                     # the job still moves for its next run


def test_failed_continuation_is_reported():
    d = setup(probe={'anthropic': {PB: 'ok'}})
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text(json.dumps({'continued': {'ocf-continue-1': K, 'ocf-continue-2': K}, 'probed_at': ms()}))
    fake(d, units={'ocf-continue-1': 'failed', 'ocf-continue-2': 'active'})
    rc, err, j = go(d)
    assert rc == 1 and 'continued turn failed' in err and 'ocf-continue-1' in err
    assert list(json.loads((d / 'st' / 'state.json').read_text())['continued']) == ['ocf-continue-2']


def test_continuation_stopped_by_a_person_is_not_a_failure():
    # 17 Nov 2026: the operator stopped a running continuation by hand to move the chat; 'openclaw agent' printed
    # 'aborted' and exited 1. That is a stop, not a failed continuation, and must not fail the job.
    d = setup(probe={'anthropic': {PB: 'ok'}})
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text(json.dumps({'continued': {'ocf-continue-1': K, 'ocf-continue-2': K}, 'probed_at': ms()}))
    fake(d, units={'ocf-continue-1': 'failed', 'ocf-continue-2': 'failed'},
         unit_output={'ocf-continue-1': F.AGENT_STOPPED + '\n', 'ocf-continue-2': 'Error: gateway closed\n'})
    rc, err, j = go(d)
    assert rc == 1 and 'ocf-continue-2' in err and 'ocf-continue-1' not in err, err
    s, = kinds(j, 'continue_stopped')
    assert s['unit'] == 'ocf-continue-1', s
    assert json.loads((d / 'st' / 'state.json').read_text())['continued'] == {}


# The real Gateway log lines of a Control UI (dashboard) chat whose Fable request got a 5.4-day Retry-After
# (/tmp/openclaw/openclaw-2026-11-17.log lines 6490-6493). The retry line names only runId and sessionId.
DASH = 'agent:main:dashboard:2c2d08a7-2ef4-474d-a940-8ea5113fab4f'
DASH_RUN = 'b6cffd71-d0f6-44a6-a9f7-6f10dff3e906'
DASH_LOG = [
    {'0': '{"subsystem":"provider-transport-fetch"}', '1': '[model-fetch] response provider=anthropic api=anthropic-messages model=claude-fable-5-2 status=429 elapsedMs=317 dispatcher=reused contentType=application/json httpRequestId=unknown sessionKey="%s" runId="%s" requestId="%s:model:1" requestNumber=1' % (DASH, DASH_RUN, DASH_RUN)},
    {'0': '{"subsystem":"provider-error"}', '1': {'provider': 'anthropic', 'model': 'claude-fable-5-2', 'status': 429, 'message': '429 status code (no body)', 'body': {'type': 'error', 'error': {'type': 'rate_limit_error', 'message': "This request would exceed your account's rate limit. Please try again later."}, 'request_id': 'req_011Ch8fQH9WJxo7eU1BuvQBy'}},
     '2': 'Model request failed sessionKey="%s" runId="%s" requestId="%s:model:1" requestNumber=1' % (DASH, DASH_RUN, DASH_RUN)},
    {'0': '{"subsystem":"agent/embedded"}', '1': {'event': 'embedded_run_agent_end', 'tags': ['error_handling', 'lifecycle', 'agent_end', 'assistant_error'], 'runId': DASH_RUN, 'isError': True, 'error': '\u26a0\ufe0f The AI service needs a short break. Please try again in a few minutes.', 'failoverReason': 'rate_limit', 'model': 'claude-fable-5-2', 'provider': 'anthropic', 'rawErrorPreview': '429: {"error":{"message":"This request would exceed your account\'s rate limit. Please try again later.","type":"rate_limit_error"},"request_id":"req_011Ch8fQH9WJxo7eU1BuvQBy","type":"error"}; Retry-After: 469674 seconds', 'rawErrorHash': 'sha256:84a976a9aa41', 'providerRuntimeFailureKind': 'rate_limit'}, '2': 'embedded run agent end'},
    {'0': '{"subsystem":"agent/embedded"}', '1': 'transient same-model retry 1/9 for anthropic/claude-fable-5-2 reason=rate_limit: delayMs=469674000 runId="%s" sessionId="f8f4654a-e634-4e9d-86b2-1dba88c01b0c"' % DASH_RUN},
]


def test_dashboard_chat_sleeping_a_long_wait_is_recovered_from_the_real_log_lines():
    log = [dict(x, _meta=line(None)['_meta']) for x in DASH_LOG]
    d = setup(log=log, probe={'anthropic': {PB: 'ok', PA: 'ok'}},     # the probe does not see a per-model limit
              sessions={DASH: {'key': DASH, 'hasActiveRun': True, 'sessionId': 'f8f4654a-e634-4e9d-86b2-1dba88c01b0c'}},
              entries={DASH: PIN})
    rc, err, j = go(d)
    r, = kinds(j, 'chat_recover')
    assert (r['session'], r['run'], r['why'], r['frm'], r['to'], r['abort'], r['patch'], r['continue']) == \
        (DASH, DASH_RUN, 'stall', FABLE + '@' + PB, FABLE + '@' + PA, 'ok', 'ok', 'started'), r
    held = json.loads((d / 'st' / 'state.json').read_text())['limited'][FABLE + '@' + PB]
    assert abs(held - (ms() + 469674000)) < 120_000, held                # held until the time the provider named
    assert len(kinds(go(d)[2], 'chat_recover')) == 1                  # handled once (the journal is cumulative)


def test_log_format_drift_after_an_update_is_reported():
    d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}})
    pkg_strings(d / 'pkg', tuple(s for s in F.ANCHORS + F.ERROR_TEXTS if s != F.EV_END))
    rc, err, j = go(d, False, '--check')
    assert rc == 1 and 'anchor "embedded_run_agent_end"' in err and 'Model request failed' not in err, err


def test_beta_runtime_account_refusal_surface_accepts_the_new_mjs_artifact():
    d = setup()
    strings = tuple(s for s in F.ANCHORS + F.ERROR_TEXTS if s != F.ROUTE_ERR) + F.ROUTE_ERROR_TEXTS + \
        (F.RPC_PROBE, F.RPC_DESCRIBE, F.RPC_PATCH, F.RPC_ABORT)
    pkg_strings(d / 'pkg', strings)
    (d / 'pkg' / 'dist' / 'a.js').rename(d / 'pkg' / 'dist' / 'a.mjs')
    rc, err, _ = go(d, False, '--check', env={'OCF_FAKE_VERSION': '2026.11.10-beta.1'})
    assert rc == 0 and not err, (rc, err)


def test_partial_beta_refusal_guidance_still_blocks_all_changes():
    for missing in F.ROUTE_ERROR_TEXTS:
        d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}})
        strings = tuple(s for s in F.ANCHORS + F.ERROR_TEXTS if s != F.ROUTE_ERR) + \
            tuple(s for s in F.ROUTE_ERROR_TEXTS if s != missing) + \
            (F.RPC_PROBE, F.RPC_DESCRIBE, F.RPC_PATCH, F.RPC_ABORT)
        pkg_strings(d / 'pkg', strings)
        rc, err, j = go(d, True, env={'OCF_FAKE_VERSION': '2026.11.10-beta.1'})
        assert rc == 1 and 'complete runtime/account refusal guidance' in err, (rc, err)
        assert not kinds(j, 'job_move'), j


@offline
def test_beta_runtime_refusals_are_not_usage_limits_and_legacy_receipts_still_are():
    refusals = [
        "Automation test runs anthropic on account anthropic:a and can't run it on runtime codex. "
        "That account was recorded for runtime openclaw, which this run can no longer use.",
        F.ROUTE_ERROR_TEXTS[0], F.ROUTE_ERROR_TEXTS[2],
        'Automation test runs on account anthropic:a, which was signed in again or replaced since the job recorded it.',
        'Automation test runs on account anthropic:a, which was removed or signed out.',
    ]
    for message in refusals + [ERR]:
        with patch.object(F, 'FAKE', {'retest': {OPUS + '@' + PA: {'rc': 1, 'err': message}}}), \
                patch.dict(os.environ, {'OCF_FAKE_CALLS': str(F.SDIR / 'calls')}):
            result, _, _ = F.retest_one(OPUS + '@' + PA)
        assert result == ('limit' if message == ERR else 'error'), (message, result)


def test_after_an_update_with_a_missing_item_nothing_changes_and_it_is_reported_once():
    d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}})
    pkg_strings(d / 'pkg', F.ANCHORS + F.ERROR_TEXTS + (F.RPC_PROBE, F.RPC_DESCRIBE, F.RPC_PATCH))   # no sessions.abort
    v2 = {'OCF_FAKE_VERSION': '2026.11.1'}
    res = [go(d, True, env=v2) for _ in range(4)]
    assert [r[0] for r in res] == [1, 1, 0, 0], [r[:2] for r in res]          # the ledger's pair, then quiet
    assert 'rpc: sessions.abort' in res[0][1] and not any(kinds(r[2], 'job_move') for r in res), res[0][1]
    pkg_strings(d / 'pkg', F.ANCHORS + F.ERROR_TEXTS + (F.RPC_PROBE, F.RPC_DESCRIBE))       # the missing set changed
    assert go(d, True, env=v2)[0] == 1
    pkg_strings(d / 'pkg', F.ANCHORS + F.ERROR_TEXTS + (F.RPC_PROBE, F.RPC_DESCRIBE, F.RPC_PATCH, F.RPC_ABORT))
    rc, err, j = go(d, True, env=v2)                                  # fixed: the next pass acts again
    assert len(kinds(j, 'job_move')) == 2, (rc, err)


def test_first_pass_ignores_failures_and_stalls_from_before_it():
    d = setup({STALL_JOB: (OPUS, PA, 'trusted')}, [(STALL_JOB, 'error', ERR, 120), (STALL_JOB, 'error', ERR, 60)],
              log=stall_log(), probe={'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}},
              sessions={STALL_KEY: {'key': STALL_KEY, 'hasActiveRun': True}}, sched={STALL_JOB: ('cron', 20 * 3600)})
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text('{}')                        # a fresh install
    rc, err, j = go(d)
    assert binding(d, STALL_JOB) == OPUS + '@' + PA and 'keep failing' not in err, (err, j)


def test_job_list_of_the_wrong_shape_changes_nothing():
    d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}})
    rc, err, j = go(d, env={'OCF_FAKE_JOBS_KEY': 'automations'})
    assert rc == 1 and 'no "jobs" list' in err and not kinds(j, 'job_move'), err
    assert len(kinds(go(d, True)[2], 'job_move')) == 2                # answer fixed: works again


def test_pinned_chat_parked_on_a_fallback_model_returns_to_its_own_model():
    grok = {'providerOverride': 'xai', 'modelOverride': 'grok-4.7', 'modelOverrideSource': 'user', 'authProfileOverride': 'xai:x'}
    d = setup(probe={'anthropic': {PB: 'rate_limit', PA: 'ok', PC: 'rate_limit'}, 'xai': {'xai:x': 'ok'}}, entries={K: grok})
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text(json.dumps({'chat_home': {K: {'model': FABLE, 'parked': 'xai/grok-4.7@xai:x'}}}))
    r, = kinds(go(d)[2], 'chat_return')
    assert (r['to'], r['result']) == (FABLE + '@' + PA, 'ok'), r
    (d / 'st' / 'state.json').write_text(json.dumps({'chat_home': {K: {'model': FABLE, 'parked': 'xai/grok-4.7@xai:x'}}}))
    fake(d, entries={K: dict(grok, modelOverride='grok-5')})          # the user picked another model meanwhile
    assert len(kinds(go(d)[2], 'chat_return')) == 1                   # no second move: that choice stands


def test_chat_the_operator_moved_to_a_fallback_model_returns_after_adopt_with_its_model():
    # The operator moved a chat off Fable to Grok by hand; --adopt KEY=MODEL makes it go home like a parked chat.
    grok = {'providerOverride': 'xai', 'modelOverride': 'grok-4.7', 'modelOverrideSource': 'user', 'authProfileOverride': 'xai:x'}
    d = setup(probe={'anthropic': {PB: 'rate_limit', PA: 'ok', PC: 'rate_limit'}, 'xai': {'xai:x': 'ok'}}, entries={K: grok})
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text('{}')
    assert not kinds(go(d)[2], 'chat_return')                         # a person's pick: never moved without adopt
    c = json.loads((d / 'cfg.json').read_text())
    c['agents']['defaults']['models'] = {OPUS: {}, FABLE: {}, 'xai/grok-4.7': {}}
    (d / 'cfg.json').write_text(json.dumps(c))
    rc, err, j = go(d, False, '--adopt', K + '=openai/old-model')     # a home the allowlist dropped is refused
    assert rc == 0 and not kinds(j, 'chat_adopt') and not state_of(d).get('chat_home'), (err, j)
    rc, err, j = go(d, False, '--adopt', K + '=' + FABLE)
    a, = kinds(j, 'chat_adopt')
    assert rc == 0 and (a['session'], a['home']) == (K, FABLE), (err, j)
    r, = kinds(go(d, True)[2], 'chat_return')                         # stale probe: anthropic is probed again
    assert (r['session'], r['to'], r['result']) == (K, FABLE + '@' + PA, 'ok'), r
    assert K not in state_of(d).get('chat_home', {})


def test_adopt_with_a_bare_model_or_the_current_model_parks_nothing():
    grok = {'providerOverride': 'xai', 'modelOverride': 'grok-4.7', 'modelOverrideSource': 'user', 'authProfileOverride': 'xai:x'}
    d = setup(probe={'anthropic': {PB: 'rate_limit', PA: 'ok', PC: 'rate_limit'}, 'xai': {'xai:x': 'ok'}}, entries={K: grok})
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text(json.dumps({'chat_home': {K: {'model': FABLE, 'parked': 'xai/grok-4.7@xai:x'}}}))
    rc, err, j = go(d, False, '--adopt', K + '=claude-fable-5-2')      # no provider: refused, the old home stays
    assert rc == 0 and not kinds(j, 'chat_adopt') and state_of(d)['chat_home'][K]['model'] == FABLE, (err, j)
    rc, err, j = go(d, False, '--adopt', K + '=xai/grok-4.7')         # the operator says grok is home now
    a, = kinds(j, 'chat_adopt')
    assert rc == 0 and 'home' not in a and K not in state_of(d).get('chat_home', {}), (err, j)
    assert not kinds(go(d, True)[2], 'chat_return')


def test_cron_run_that_failed_on_a_limit_is_never_continued_like_a_chat():
    d = setup({STALL_JOB: (OPUS, PB, 'trusted')}, log=[req(STALL_KEY, 'R9'), end('R9')], probe={'anthropic': DEAD, 'xai': {'xai:x': 'ok'}},
              sessions={STALL_KEY: {'key': STALL_KEY, 'hasActiveRun': False, 'lastRunId': 'R9', 'status': 'failed',
                                    'deliveryContext': {'to': 'telegram:-1001'}}})
    rc, err, j = go(d)
    assert not kinds(j, 'chat_recover') and 'R9' not in json.loads((d / 'st' / 'state.json').read_text())['pending']
    drop, = kinds(j, 'event_drop')
    assert drop['run'] == 'R9' and 'not rerun' in drop['why'], drop       # the reason is journaled
    assert binding(d, STALL_JOB) == 'xai/grok-4.7@xai:x'


def test_limit_wave_never_fails_three_passes_in_a_row():
    # The scheduler backs a recurring job off 30 s and 60 s after errors one and two, then 5, 15 and 60 min.
    d = setup(probe={'anthropic': {PB: 'ok'}})
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text('{}')
    rcs, errs = [], ''
    for i in range(9):
        if i < 6:                                                     # a new problem on each of six passes
            s = json.loads((d / 'st' / 'state.json').read_text())
            s.setdefault('continued', {})['ocf-continue-%d' % i] = K
            (d / 'st' / 'state.json').write_text(json.dumps(s))
            fake(d, units={'ocf-continue-%d' % i: 'failed'})
        rc, err, _ = go(d)
        rcs.append(rc)
        errs += err
    assert '111' not in ''.join(map(str, rcs)), rcs
    assert all('ocf-continue-%d' % i in errs for i in range(6)), (rcs, errs)       # and nothing is swallowed


def test_pinned_chat_parked_on_the_primary_model_returns_too():
    # sessions.patch to the config's primary model clears modelOverride instead of storing it.
    cleared = {'modelOverrideSource': 'default', 'authProfileOverride': PB, 'authProfileOverrideSource': 'user'}
    d = setup(probe={'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}}, entries={K: cleared})
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text(json.dumps({'chat_home': {K: {'model': FABLE, 'parked': OPUS + '@' + PB}}}))
    r, = kinds(go(d)[2], 'chat_return')
    assert (r['to'], r['result']) == (FABLE + '@' + PB, 'ok'), r


def test_chat_home_removed_from_config_is_forgotten_without_a_patch():
    d = setup(probe={'anthropic': {PB: 'ok'}, 'openai': {'openai:default': 'ok'}}, entries={K: PIN})
    c = json.loads((d / 'cfg.json').read_text())
    c['agents']['defaults']['models'] = {OPUS: {}, FABLE: {}}
    (d / 'cfg.json').write_text(json.dumps(c))
    (d / 'st').mkdir()
    (d / 'st/state.json').write_text(json.dumps({'chat_home': {K: {'model': 'openai/old-model', 'parked': FABLE + '@' + PB}}}))
    rc, err, j = go(d)
    assert not kinds(j, 'chat_return') and not kinds(j, 'chat_move'), j
    assert rc == 0 and not err, err
    assert not json.loads((d / 'st/state.json').read_text())['chat_home']
    assert json.loads((d / 'fake.json').read_text())['entries'][K] == PIN
    r, = kinds(j, 'home_gone')
    assert r['session'] == K and r['model'] == 'openai/old-model' and r['reason'], r
    assert len(kinds(go(d, True)[2], 'home_gone')) == 1


def test_job_home_removed_from_config_is_forgotten_without_an_edit():
    for enabled in (True, False):
        d = setup({'a': (OPUS, PB, 'trusted')}, probe={'anthropic': {PB: 'ok'}, 'openai': {'openai:default': 'ok'}})
        c = json.loads((d / 'cfg.json').read_text())
        c['agents']['defaults']['models'] = {OPUS: {}}
        (d / 'cfg.json').write_text(json.dumps(c))
        with sqlite3.connect(d / 'state.sqlite') as db:
            db.execute('update cron_jobs set enabled=?', (enabled,))
        (d / 'st').mkdir()
        (d / 'st/state.json').write_text(json.dumps({'home': {'a': {'model': 'openai/old-model', 'parked': OPUS + '@' + PB}}}))
        rc, err, j = go(d)
        assert not kinds(j, 'job_move'), j
        assert rc == 0 and not err and binding(d, 'a') == OPUS + '@' + PB, err
        assert not json.loads((d / 'st/state.json').read_text())['home']
        r, = kinds(j, 'home_gone')
        assert r['job'] == 'a' and r['model'] == 'openai/old-model' and r['reason'], r
        assert len(kinds(go(d, True)[2], 'home_gone')) == 1


@offline
def test_return_model_not_allowed_backstop_retires_chat_and_job_homes():
    for path in ('chat_return', 'chat_move', 'job_return'):
        kind = 'home' if path == 'job_return' else 'chat_home'
        for error, gone in ((json.dumps({'type': 'gateway_request_error', 'code': 'INVALID_REQUEST',
                'message': 'model not allowed: ' + OPUS, 'retryable': False}), True),
                ('rc=1 Error: model not allowed: ' + OPUS, True),
                (json.dumps({'message': 'model not allowed: ' + OPUS, 'retryable': True}), False),
                ('gateway timeout', False)):
            ident, parked = (K if kind == 'chat_home' else 'a'), FABLE + '@' + PB
            problem = ('rebind:' if kind == 'home' else path + ':') + ident
            state = {kind: {ident: {'model': OPUS, 'parked': parked}}, 'problems': {
                problem: {'raised': ms(), 'seen': ms(), 'left': 1, 'text': 'could not move to ' + OPUS + '@' + PA}}}
            conf = {'primary': OPUS, 'chain': [], 'order': {}, 'allowed': {OPUS: {}, FABLE: {}}}
            health = F.Health(conf)
            out = {'problems': [], 'chats': [], 'moved': [], 'deferred': 0, 'later': 0}
            F.JOURNAL.write_text('')
            with patch.object(F, 'APPLY', True), patch.object(F, 'FAKE', None), \
                    patch.object(F, 'entry', return_value=(PIN, False)), \
                    patch.object(F, 'session', return_value={'hasActiveRun': False}), \
                    patch.object(health, 'dead', return_value=False), \
                    patch.object(health, 'target', return_value=OPUS + '@' + PA), \
                    patch.object(F, 'gw', return_value=(None, error)) as gateway, \
                    patch.object(F, 'run', return_value=(1, '', error)) as cli:
                if kind == 'chat_home':
                    if path == 'chat_return':
                        F.chats_home(state, health, conf, out)
                    else:
                        F.move_chat(state, ident, {'ref': parked, 'own': OPUS, 'tgt': OPUS + '@' + PA}, PIN, out, 'held')
                    gateway.assert_called_once()
                else:
                    jobs = {'a': {'enabled': True, 'profile': PB, 'model': parked, 'next': 0,
                                 'mode': 'trusted', 'name': 'a'}}
                    F.jobs_pass(conf, jobs, health, state, time.time(), out)
                    cli.assert_called_once()
                assert (ident not in state[kind]) == gone, (kind, error, state)
                assert bool(out['problems']) == (not gone), (kind, error, out)
                assert not out['chats'] and not out['moved'], out
                if gone:
                    assert not state.get('unsure')
                    assert not F.raise_due(state, out)              # no cron failure for a permanent refusal
                    rows = [json.loads(x) for x in F.JOURNAL.read_text().splitlines()]
                    r, = kinds(rows, 'home_gone')
                    assert r['model'] == OPUS and r['reason'] and r['session' if kind == 'chat_home' else 'job'] == ident


def test_empty_or_matching_allowlist_keeps_chat_and_job_returns():
    for allowed in ({}, {OPUS: {}, FABLE: {}}):
        d = setup({'a': (FABLE, PB, 'trusted')}, probe={'anthropic': {PB: 'ok'}}, entries={K: PIN})
        c = json.loads((d / 'cfg.json').read_text())
        c['agents']['defaults']['models'] = allowed
        (d / 'cfg.json').write_text(json.dumps(c))
        parked = {'model': OPUS, 'parked': FABLE + '@' + PB}
        (d / 'st').mkdir()
        (d / 'st/state.json').write_text(json.dumps({'home': {'a': parked}, 'chat_home': {K: parked}}))
        rc, err, j = go(d)
        assert rc == 0 and not err and not kinds(j, 'home_gone'), (err, j)
        assert binding(d, 'a') == OPUS + '@' + PB and kinds(j, 'chat_return')[0]['to'] == OPUS + '@' + PB


def test_removed_home_is_not_reused_when_fallback_account_dies():
    d = setup({'a': (FABLE, PB, 'trusted')}, probe={'anthropic': {PB: 'rate_limit', PA: 'ok'},
              'openai': {'openai:default': 'ok'}}, entries={K: PIN})
    c = json.loads((d / 'cfg.json').read_text())
    c['agents']['defaults']['models'] = {OPUS: {}, FABLE: {}}
    (d / 'cfg.json').write_text(json.dumps(c))
    parked = {'model': 'openai/old-model', 'parked': FABLE + '@' + PB}
    (d / 'st').mkdir()
    (d / 'st/state.json').write_text(json.dumps({'home': {'a': parked}, 'chat_home': {K: parked}}))
    rc, err, j = go(d)
    assert rc == 0 and not err and len(kinds(j, 'home_gone')) == 2, (err, j)
    assert binding(d, 'a') == FABLE + '@' + PA
    assert kinds(j, 'chat_move')[0]['to'] == FABLE + '@' + PA
    s = json.loads((d / 'st/state.json').read_text())
    assert not s['home'] and not s['chat_home']


def test_second_park_within_a_day_waits_before_going_home():
    d = setup(JOBS, [], {'anthropic': DEAD, 'xai': {'xai:x': 'ok'}})
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text(json.dumps({'parks': {'a': {'n': 1, 't': ms() - 3600_000}}}))
    go(d)                                                             # a: second park today, b: first
    fake(d, probe={'anthropic': {PB: 'rate_limit', PA: 'ok', PC: 'rate_limit'}, 'xai': {'xai:x': 'ok'}})
    go(d, True)
    assert binding(d, 'b') == OPUS + '@' + PA and binding(d, 'a') == 'xai/grok-4.7@xai:x'


def test_slow_gateway_pass_is_cut_off_and_saved_before_the_job_timeout():
    # A slow Gateway is OpenClaw not answering: the cut-off pass saves its state and stays quiet; ten minutes of it fail.
    d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}})
    fake(d, slow_probe=5)
    t = time.time()
    rc, err, j = go(d, False, env={'OCF_PASS_HARD_S': '2'})
    assert rc == 0 and not err and time.time() - t < 5, (rc, err)
    s = json.loads((d / 'st' / 'state.json').read_text())
    assert s['down_since'] and s['last_pass'] and not s.get('problems'), s      # saved, nothing raised yet
    s['down_since'] = ms() - 11 * 60_000
    (d / 'st' / 'state.json').write_text(json.dumps(s))
    rc, err, j = go(d, False, env={'OCF_PASS_HARD_S': '2'})
    assert rc == 1 and 'too slowly for 11 min' in err and 'cut off' in err, (rc, err)
    fake(d, slow_probe=0)                                             # answers again: the clock stops, the moves go
    rc, err, j = go(d, True)
    assert 'down_since' not in json.loads((d / 'st' / 'state.json').read_text()) and len(kinds(j, 'job_move')) == 2, j


def test_slow_gateway_clock_starts_when_the_first_cut_off_pass_started():
    # The grace counts from the start of the first pass that could not finish, not from the moment it was cut off.
    d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}})
    fake(d, slow_probe=5)
    t = ms()
    go(d, False, env={'OCF_PASS_HARD_S': '2'})
    s = json.loads((d / 'st' / 'state.json').read_text())
    assert t <= s['down_since'] and s['last_pass'] - s['down_since'] >= 1000, (t, s['down_since'], s['last_pass'])


def test_update_with_a_clean_self_check_does_not_restart_the_slow_gateway_clock():
    # After a version change the self-check gets through but the pass is cut off again: the outage goes on.
    d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}})
    go(d)
    s = json.loads((d / 'st' / 'state.json').read_text())
    s['down_since'] = ms() - 11 * 60_000
    (d / 'st' / 'state.json').write_text(json.dumps(s))
    fake(d, slow_probe=5)
    rc, err, j = go(d, True, env={'OCF_PASS_HARD_S': '2', 'OCF_FAKE_VERSION': '2026.11.1'})
    assert rc == 1 and 'too slowly for 11 min' in err, (rc, err)


def test_one_outage_that_turns_from_no_answer_to_slow_fails_only_two_passes():
    # One clock, one ledger entry: a Gateway that first does not answer and then answers slowly is one problem.
    d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}})
    go(d)
    s = json.loads((d / 'st' / 'state.json').read_text())
    s['down_since'] = ms() - 11 * 60_000
    (d / 'st' / 'state.json').write_text(json.dumps(s))
    rcs = [go(d, True, env={'OCF_FAKE_CLI_DOWN': '1'})[0]]
    fake(d, slow_probe=5)
    rcs += [go(d, True, env={'OCF_PASS_HARD_S': '2'})[0] for _ in range(4)]
    led = json.loads((d / 'st' / 'state.json').read_text())['problems']
    assert rcs == [1, 1, 0, 0, 0] and list(led) == ['gateway'], (rcs, led)


def test_cut_off_30_min_check_keeps_its_clock_across_ordinary_passes():
    # The check runs every 30 min and the passes between it finish: a check cut off each time still fails after
    # ten minutes, the passes between do not retry it, and a check that finishes stops its clock.
    d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}})
    go(d)
    st = d / 'st' / 'state.json'
    (d / 'bin' / 'grep').write_text('#!/bin/sh\nif [ "$1" = "-hF" ]; then sleep 6; fi\nexec /usr/bin/grep "$@"\n')
    (d / 'bin' / 'grep').chmod(0o755)                                  # only the check's log search is slow

    def check_pass():
        s = json.loads(st.read_text())
        s['checked_at'] = 0                                           # 30 min since the last check
        st.write_text(json.dumps(s))
        return go(d, env={'OCF_PASS_HARD_S': '3'})
    rc, err, j = check_pass()
    s = json.loads(st.read_text())
    assert rc == 0 and not err and not s.get('problems'), (rc, err, s)  # one cut-off: quiet
    at = s['checked_at']
    rc, err, j = go(d)                                                 # an ordinary pass: no retry of the check
    s = json.loads(st.read_text())
    assert rc == 0 and 'down_since' not in s and s['checked_at'] == at, (rc, err, s)
    for part in s.get('cut_since', {}):                                # 30 min of ordinary passes go by
        s['cut_since'][part] -= 30 * 60_000
    st.write_text(json.dumps(s))
    rc, err, j = check_pass()
    assert rc == 1 and 'too slowly for 30 min' in err, (rc, err)
    (d / 'bin' / 'grep').unlink()                                      # the log search answers again
    rc, err, j = check_pass()
    assert 'cut_since' not in json.loads(st.read_text()), st.read_text()


def test_prompt_edit_of_a_parked_job_does_not_cancel_the_return():
    d = setup(JOBS, [], {'anthropic': DEAD, 'xai': {'xai:x': 'ok'}})
    go(d)
    c = sqlite3.connect(d / 'state.sqlite')                           # somebody edits the prompt: same model and account
    c.execute("update cron_jobs set updated_at=? where job_id='a'", (ms() + 60_000,))
    c.commit()
    c.close()
    fake(d, probe={'anthropic': {PB: 'rate_limit', PA: 'ok', PC: 'rate_limit'}, 'xai': {'xai:x': 'ok'}})
    go(d, True)
    assert binding(d, 'a') == OPUS + '@' + PA and binding(d, 'b') == OPUS + '@' + PA


def test_a_change_that_cannot_finish_before_the_deadline_is_not_started():
    d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}})
    rc, err, j = go(d, False, env={'OCF_PASS_HARD_S': '100'})
    assert rc == 0 and not kinds(j, 'job_move') and json.loads((d / 'st' / 'state.json').read_text())['retry'] is True
    assert len(kinds(go(d)[2], 'job_move')) == 2



def calls(d):
    f = d / 'calls.log'
    return f.read_text().splitlines() if f.exists() else []


def test_each_cli_read_is_made_once_per_pass_and_run_history_only_when_needed():
    d = setup(JOBS, [('a', 'ok', None, 60)], {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}})
    go(d)                                                             # first pass: self-check of a new version
    (d / 'calls.log').unlink()
    go(d)
    assert [c for c in calls(d) if c.startswith('cron')] == [' '.join(F.CLI_JOBS)], calls(d)       # quiet pass
    d = setup(JOBS, [('a', 'error', ERR, 120), ('b', 'error', ERR, 60)], {'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'ok'}})
    s = d / 'st'
    s.mkdir()
    s.joinpath('state.json').write_text(json.dumps({'checked_at': 0}))                    # the 30-min check runs too
    go(d)
    c = [x for x in calls(d) if x.startswith('cron')]
    assert sorted(c) == sorted([' '.join(F.CLI_JOBS), ' '.join(F.CLI_RUNS)]), c


def test_state_imported_from_an_older_install_does_not_count_earlier_failures():
    # Two limit failures on account a before this script's first pass: they say nothing about what it should do now.
    d = setup(JOBS, [('a', 'error', ERR, 600), ('b', 'error', ERR, 300)], {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}})
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text(json.dumps({'limited': {}, 'home': {}}))     # no 'started', no 'bindings'
    rc, err, j = go(d)
    assert not kinds(j, 'job_move') and 'streak' not in err, (err, j)
    assert json.loads((d / 'st' / 'state.json').read_text())['started'] > ms() - 60_000


def test_one_pass_without_an_answer_is_not_a_problem_but_ten_minutes_are():
    d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}})
    go(d)
    rc, err, j = go(d, env={'OCF_FAKE_CLI_DOWN': '1'})
    assert rc == 0 and not err, err
    s = json.loads((d / 'st' / 'state.json').read_text())
    s['down_since'] = ms() - 11 * 60_000
    (d / 'st' / 'state.json').write_text(json.dumps(s))
    rc, err, j = go(d, env={'OCF_FAKE_CLI_DOWN': '1'})
    assert rc == 1 and 'has not answered for 11 min' in err, err
    go(d)
    assert 'down_since' not in json.loads((d / 'st' / 'state.json').read_text())


def test_job_list_longer_than_one_page_is_reported_and_keeps_what_it_knows():
    d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}})
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text(json.dumps({'started': 0, 'home': {'zz': {'model': OPUS, 'parked': 'xai/grok-4.7@xai:x'}}, 'probed_at': 0}))
    rc, err, j = go(d, env={'OCF_FAKE_MORE': '1'})
    assert rc == 1 and 'more than one page' in err, err
    assert 'zz' in json.loads((d / 'st' / 'state.json').read_text())['home']       # a job off the page is not forgotten


def test_edit_that_reported_a_failure_but_landed_is_adopted_as_parked():
    d = setup(JOBS, [], {'anthropic': DEAD, 'xai': {'xai:x': 'ok'}})
    fake(d, edit_result='rc=1 gateway timeout')                        # the fixture still applies the edit
    rc, err, j = go(d)
    assert 'rebind of job' in err
    fake(d, edit_result='ok')
    go(d)
    home = json.loads((d / 'st' / 'state.json').read_text())['home']
    assert home['a']['model'] == OPUS and home['a']['parked'] == 'xai/grok-4.7@xai:x', home


def test_only_accounts_listed_in_auth_order_are_targets():
    # An account the probe reports but auth.order does not list is never chosen, however it is billed; a provider
    # without an auth.order entry and without subscription profiles yields no target at all, not a guessed account.
    probe = {'anthropic': {PA: 'rate_limit', PB: 'ok', PC: 'rate_limit'}, 'xai': {'xai:x': 'ok'}}
    for order, want in (({'anthropic': [PA], 'xai': ['xai:x']}, {'xai/grok-4.7@xai:x'}),
                        ({'anthropic': [PA, PB], 'xai': ['xai:x']}, {OPUS + '@' + PB}),
                        ({'anthropic': [PA]}, set())):
        d = setup(JOBS, [], probe)
        c = json.loads((d / 'cfg.json').read_text())
        c['auth'] = {'order': order, 'profiles': {PB: {'provider': 'anthropic', 'mode': 'token'}}}
        (d / 'cfg.json').write_text(json.dumps(c))
        rc, err, j = go(d)
        assert {r['to'] for r in kinds(j, 'job_move')} == want, (order, j)
        if not want:
            assert rc == 1 and 'no live account' in err, err


SECRET = 'LABEL-VALUE-9f3c'      # stands for the masked key or email in a label: it must never leave models status


def models_status(rows):
    """The shape of the real 'openclaw models status --json' for [(profile id, type)] in the agent's store: per provider
    counts and labels, and auth.oauth.profiles sorted by provider then id, as OpenClaw builds them."""
    lab = {'oauth': 'OAuth (%s@example.invalid)', 'token': 'token:%s...', 'api_key': 'sk-%s...'}
    rows = sorted(rows, key=lambda r: (r[0].split(':')[0], r[0]))
    provs = {}
    for pid, kind in rows:
        provs.setdefault(pid.split(':')[0], []).append((pid, kind))
    prof = lambda pid, kind: {'profileId': pid, 'provider': pid.split(':')[0], 'type': kind, 'source': 'store',
                              'label': '%s (%s@example.invalid)' % (pid, SECRET), 'status': 'ok' if kind == 'oauth' else 'static'}
    n = lambda ps, k: sum(1 for _, t in ps if t == k)
    return {'configPath': 'cfg.json', 'agentDir': 'agent', 'defaultModel': OPUS, 'resolvedDefault': OPUS, 'fallbacks': [],
            'auth': {'storePath': 'auth.json', 'shellEnvFallback': {'enabled': False, 'appliedKeys': []},
                     'providersWithOAuth': ['%s (%d)' % (pv, n(ps, 'oauth') + n(ps, 'token')) for pv, ps in provs.items()],
                     'missingProvidersInUse': [], 'modelRouteIssues': [], 'runtimeAuthRoutes': [],
                     'providers': [{'provider': pv, 'effective': {'kind': 'profiles', 'detail': 'auth.json'},
                                    'profiles': {'count': len(ps), 'oauth': n(ps, 'oauth'), 'token': n(ps, 'token'),
                                                 'apiKey': n(ps, 'api_key'),
                                                 'labels': ['%s=%s' % (pid, lab[k] % SECRET) for pid, k in ps]}}
                                   for pv, ps in provs.items()],
                     'unusableProfiles': [],
                     'oauth': {'warnAfterMs': 86400000, 'profiles': [prof(*r) for r in rows],
                               'providers': [{'provider': pv, 'status': 'ok', 'profiles': [prof(*r) for r in ps],
                                              'effectiveProfiles': [prof(*r) for r in ps]} for pv, ps in provs.items()]}}}


def saved_cli(rows, calls=None, error=None):
    """In-process models status: [(profile id, type)] saved in the agent's store, or a failure."""
    def cli(argv, timeout=75):
        assert argv[:3] == ['models', 'status', '--json'], argv
        if calls is not None:
            calls.append(argv)
        if error:
            raise error
        return models_status(rows)
    return patch.object(F, 'cli_json', cli)


_run = F.run
def _guarded_run(argv, timeout=120):
    assert argv[1:3] != ['models', 'status'], 'a unit test ran the real openclaw models status'
    return _run(argv, timeout)
F.run = _guarded_run


SUBS = {'anthropic:k': {'provider': 'anthropic', 'mode': 'api_key'}, PA: {'provider': 'anthropic', 'mode': 'token'},
        PC: {'provider': 'anthropic', 'mode': 'token'}, PB: {'provider': 'anthropic', 'mode': 'oauth'},
        OA: {'provider': 'openai', 'mode': 'oauth'}}


@offline
@saved_cli([])
def test_without_auth_order_targets_are_the_subscription_profiles():
    # No non-empty auth.order for a provider: the targets are its oauth and token profiles in config order, oauth
    # first. An api_key profile never is. An empty list is the same as no key.
    for order in ({}, {'openai': [OA]}, {'anthropic': []}):
        h = F.Health({'primary': OPUS, 'chain': [], 'order': order, 'profiles': SUBS})
        assert h.accounts('anthropic') == [PB, PA, PC], (order, h.accounts('anthropic'))
    # With auth.order: that list in its order, an api_key account included when listed, unlisted profiles ignored.
    h = F.Health({'primary': OPUS, 'chain': [], 'order': {'anthropic': [PC, 'anthropic:k']}, 'profiles': SUBS})
    assert h.accounts('anthropic') == [PC, 'anthropic:k'] and h.target(OPUS) is None
    # Config declares no profile for the provider: no targets, and no probe is made to look for any.
    with patch.object(F, 'FAKE', {'probe': {'xai': {'xai:x': 'ok'}}}):
        h = F.Health({'primary': OPUS, 'chain': [], 'order': {}, 'profiles': SUBS})
        assert h.accounts('xai') == [] and 'xai' not in h.cache
    # No non-empty auth.order: a profile that probes 'unknown' (left out by a CLI-saved order) does not make 'nothing
    # alive' unproven; with a non-empty config auth.order it still does. Under an empty list OpenClaw's probe answers
    # 'unknown' for every account, and those accounts are used anyway (any order rather than stopping).
    h = F.Health({'primary': OPUS, 'chain': [], 'order': {}, 'profiles': SUBS})
    h.cache['anthropic'] = {PA: 'rate_limit', PB: 'unknown', PC: 'unknown'}
    assert not h.unknown() and h.target(OPUS) is None
    with saved_cli([(PA, 'token'), (PB, 'oauth'), (PC, 'token')]):
        h = F.Health({'primary': OPUS, 'chain': [], 'order': {'anthropic': []}, 'profiles': SUBS})
        h.cache['anthropic'] = {PA: 'unknown', PB: 'unknown', PC: 'unknown'}
        h.hold(PB, F.NOW() + 3600000, OPUS)
        assert not h.unknown() and h.target(OPUS, avoid=(PA,)) == OPUS + '@' + PC
    # ... but not an account whose saved credential is expired or missing: the probe checks nothing under [].
    d = models_status([(PA, 'token'), (PB, 'oauth'), (PC, 'token')])
    for r in d['auth']['oauth']['profiles']:
        if r['profileId'] == PC:
            r['status'] = 'expired'
    with patch.object(F, 'cli_json', lambda argv, timeout=75: d):
        h = F.Health({'primary': OPUS, 'chain': [], 'order': {'anthropic': []}, 'profiles': SUBS})
        h.cache['anthropic'] = {PA: 'unknown', PB: 'unknown', PC: 'unknown'}
        h.hold(PB, F.NOW() + 3600000, OPUS)
        assert h.target(OPUS, avoid=(PA,)) is None
    h = F.Health({'primary': OPUS, 'chain': [], 'order': {'anthropic': [PA, PB]}, 'profiles': SUBS})
    h.cache['anthropic'] = {PA: 'rate_limit', PB: 'unknown'}
    assert h.unknown()


@offline
def test_saved_rows_are_dropped_when_types_differ_from_store_counts_or_ids_name_another_provider():
    # A plugin runtime credential reports every xai account as api_key while the store holds an oauth one: the counts
    # disagree, so no xai row is trusted and the mismatch is a problem. A row whose id names another provider is
    # skipped; its provider's other rows stay.
    d = models_status([('xai:x', 'oauth'), (OA, 'oauth')])
    for r in d['auth']['oauth']['profiles']:
        if r['provider'] == 'xai':
            r['type'] = 'api_key'
    d['auth']['oauth']['profiles'].append({'profileId': 'codex:w', 'provider': 'openai', 'type': 'oauth'})
    d['auth']['providers'][[p['provider'] for p in d['auth']['providers']].index('openai')]['profiles']['oauth'] += 1
    with patch.object(F, 'cli_json', lambda argv, timeout=75: d):
        rows, bad, _ = F.saved_profiles()
    assert bad == ['xai'] and rows == [(OA, 'openai', 'oauth')], (rows, bad)
    with patch.object(F, 'cli_json', lambda argv, timeout=75: d):
        h = F.Health({'primary': OPUS, 'chain': [], 'order': {}, 'profiles': {}})
        assert h.accounts('xai') == [] and h.accounts('openai') == [OA] and 'xai' in h.saved_err


@offline
def test_a_late_pass_skips_models_status_without_a_problem():
    calls = []
    with saved_cli([(OA, 'oauth')], calls), patch.object(F, 'fits', lambda s: False):
        h = F.Health({'primary': OPUS, 'chain': [], 'order': {}, 'profiles': {}})
        assert h.accounts('openai') == [] and not calls and h.saved_err is None
        assert h.unknown()             # saved-only accounts were not looked at: 'nothing alive' is unproven
    # A config profile whose id names another provider is not a target either (its probe would ask that provider).
    with saved_cli([]):
        h = F.Health({'primary': OPUS, 'chain': [], 'order': {}, 'profiles': {'x:n': {'provider': 'anthropic', 'mode': 'token'}}})
        assert h.accounts('anthropic') == []


@offline
def test_saved_store_profiles_are_targets_once_and_api_keys_only_when_listed():
    # Config declares PB (oauth) and PA (token); the saved store repeats them and adds PC (token), an api_key and an
    # oauth account of a provider config does not mention. Each subscription is a target once, oauth first.
    saved = [(PA, 'token'), (PC, 'token'), (PB, 'oauth'), ('anthropic:k', 'api_key'), (OA, 'oauth'), ('openai:k', 'api_key')]
    calls = []
    with saved_cli(saved, calls):
        h = F.Health({'primary': OPUS, 'chain': [], 'order': {}, 'profiles': {PB: SUBS[PB], PA: SUBS[PA]}})
        assert h.accounts('anthropic') == [PB, PA, PC], h.accounts('anthropic')
        assert h.accounts('openai') == [OA] and h.accounts('xai') == []
        assert len(calls) == 1, calls                                  # read once per pass
    # A non-empty auth.order is the whole answer: an api_key it lists is a target, and the saved store is not read.
    calls = []
    with saved_cli(saved, calls):
        h = F.Health({'primary': OPUS, 'chain': [], 'order': {'anthropic': ['anthropic:k', PC]}, 'profiles': {}})
        assert h.accounts('anthropic') == ['anthropic:k', PC] and calls == []
    # The read fails: the config profiles are the targets, the failure is kept for finish() and is not retried, and an
    # account the probe calls 'unknown' still does not make 'nothing alive' unproven.
    calls = []
    with saved_cli(saved, calls, F.Unreachable('openclaw models status --json: rc=1 %s=%s' % (PC, SECRET))):
        h = F.Health({'primary': OPUS, 'chain': [], 'order': {}, 'profiles': {PA: SUBS[PA]}})
        assert h.accounts('anthropic') == [PA] and h.accounts('anthropic') == [PA] and len(calls) == 1
        assert h.saved_err == 'rc=1' and SECRET not in h.saved_err, h.saved_err
        h.cache['anthropic'] = {PA: 'rate_limit', PC: 'unknown'}
        assert not h.unknown() and h.target(OPUS) is None
    with saved_cli(saved, error=None), patch.object(F, 'cli_json', lambda argv, timeout=75: {'auth': {'oauth': {}}}):
        h = F.Health({'primary': OPUS, 'chain': [], 'order': {}, 'profiles': {PA: SUBS[PA]}})
        assert h.accounts('anthropic') == [PA] and h.saved_err == F.MS_SHAPE_ERR


def test_empty_auth_order_and_saved_only_account_move_the_job():
    # anthropic: auth.order [] (same as no key), PA limited, PB only in the saved store; openai has no auth.order and
    # its live accounts are an api_key and an oauth account, both only in the saved store.
    d = setup(JOBS, [], {'anthropic': {PA: 'rate_limit', PB: 'unknown', 'anthropic:k': 'unknown'},
                         'openai': {'openai:default': 'rate_limit', 'openai:k': 'ok', OA: 'ok'}, 'xai': {'xai:x': 'rate_limit'}})
    c = json.loads((d / 'cfg.json').read_text())
    c['auth'] = {'order': {'anthropic': [], 'xai': ['xai:x']}, 'profiles': {PA: SUBS[PA], 'openai:default': {'provider': 'openai', 'mode': 'oauth'}}}
    (d / 'cfg.json').write_text(json.dumps(c))
    fake(d, models_status=models_status([('anthropic:k', 'api_key'), (PA, 'token'), (PB, 'token'), ('openai:default', 'oauth'),
                                         ('openai:k', 'api_key'), (OA, 'oauth')]))
    rc, err, j = go(d, True)
    assert {(r['job'], r['to']) for r in kinds(j, 'job_move')} == {
        ('a', OPUS + '@' + PB), ('b', OPUS + '@' + PB), ('c', 'openai/gpt-6.1-sol@' + OA)}, (err, j)
    assert rc == 0 and not err, err
    assert [x for x in calls(d) if x.startswith('models status')] == ['models status --json --agent main'], calls(d)
    st = (d / 'st' / 'journal.jsonl').read_text() + (d / 'st' / 'state.json').read_text()
    assert SECRET not in st and 'example.invalid' not in st


def test_models_status_is_not_read_when_every_provider_has_an_order():
    d = setup(JOBS, [], {'anthropic': DEAD, 'xai': {'xai:x': 'ok'}, 'openai': {'openai:default': 'ok'}})
    fake(d, models_status_rc=1)
    rc, err, j = go(d, True)
    assert {r['to'] for r in kinds(j, 'job_move')} == {'xai/grok-4.7@xai:x'}, (err, j)
    assert not [x for x in calls(d) if x.startswith('models status')] and 'models status' not in err, (calls(d), err)


def test_failed_models_status_falls_back_to_config_and_never_hides_no_target():
    # The read fails with the full answer (labels included) on stdout: the config profile is the only target, it is
    # limited, so the alert fires; the failure is a problem of its own; no label value reaches the journal or stderr.
    for broken in (dict(models_status_rc=1), dict(models_status={'auth': {'oauth': {'profiles': [{'profileId': PB}]}}})):
        d = setup(JOBS, [], {'anthropic': {PA: 'rate_limit', PB: 'ok'}, 'openai': {'openai:default': 'rate_limit'},
                             'xai': {'xai:x': 'rate_limit'}})
        c = json.loads((d / 'cfg.json').read_text())
        c['auth'] = {'order': {'xai': ['xai:x'], 'openai': ['openai:default']}, 'profiles': {PA: SUBS[PA]}}
        (d / 'cfg.json').write_text(json.dumps(c))
        fake(d, models_status=models_status([(PA, 'token'), (PB, 'token')]))
        fake(d, **broken)
        rc, err, j = go(d, True)
        assert not kinds(j, 'job_move'), j
        assert rc == 1 and 'no live account' in err and 'models status' in err, err
        assert len([x for x in calls(d) if x.startswith('models status')]) == 1, calls(d)
        st = (d / 'st' / 'journal.jsonl').read_text() + (d / 'st' / 'state.json').read_text() + err
        assert SECRET not in st and 'example.invalid' not in st, st


def test_check_reports_an_unreadable_models_status_without_its_output():
    d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}})
    fake(d, models_status=models_status([(PA, 'token')]))
    rc, err, j = go(d, False, '--check')
    assert rc == 0 and not err, err
    fake(d, models_status_rc=1)
    rc, err, j = go(d, False, '--check')
    assert rc == 1 and 'models status' in err and 'rc=1' in err and SECRET not in err, err


def test_pinned_account_limited_without_auth_order_moves_to_a_declared_subscription():
    # The reported install: no auth.order for anthropic, the job's account limited, two other subscriptions ok. The
    # job moves to the first of them in config order instead of reporting no target; the ok api_key account is skipped.
    d = setup(JOBS, [], {'anthropic': {'anthropic:k': 'ok', PA: 'rate_limit', PB: 'ok', PC: 'ok'}})
    c = json.loads((d / 'cfg.json').read_text())
    c['auth']['order'].pop('anthropic')
    c['auth']['profiles'] = {k: dict(v, mode='token') if k == PB else v for k, v in SUBS.items()}
    (d / 'cfg.json').write_text(json.dumps(c))
    rc, err, j = go(d, True)
    assert {(r['job'], r['to']) for r in kinds(j, 'job_move')} == {('a', OPUS + '@' + PC), ('b', OPUS + '@' + PC)}, (err, j)
    assert 'no live account' not in err and 'no_target' not in err, err


def test_landed_edit_of_a_job_already_on_a_fallback_keeps_its_way_home():
    SOL = 'openai/gpt-6.1-sol'
    d = setup({'a': (SOL, 'openai:default', 'trusted')}, [], {'anthropic': DEAD, 'openai': {'openai:default': 'rate_limit', 'openai:b': 'ok'}})
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text(json.dumps({'started': 0, 'probed_at': 0, 'home': {
        'a': {'model': OPUS, 'parked': SOL + '@openai:default', 'at': 0, 'wait': 0}}}))
    fake(d, edit_result='rc=1 gateway timeout')                        # reported as failed, applied all the same
    rc, err, j = go(d)
    assert rc == 1 and 'rebind of job' in err and binding(d, 'a') == SOL + '@openai:b', (err, binding(d, 'a'))
    fake(d, edit_result='ok')
    rc, err, j = go(d)
    s = json.loads((d / 'st' / 'state.json').read_text())
    assert s['home']['a']['model'] == OPUS and s['home']['a']['parked'] == SOL + '@openai:b', s['home']
    assert rc == 0 and not err and not s.get('unsure') and 'rebind:a' not in s['problems'], (rc, err, s)
    assert len([r for r in kinds(j, 'job_move') if r['result'] == 'ok']) == 1, j


def test_agent_to_agent_sessions_are_not_chats():
    assert F.CHAT_RE.match('agent:main:telegram:direct:1') and F.CHAT_RE.match('agent:x:slack:channel:c1')
    assert not F.CHAT_RE.match('agent:main:a2a:direct:peer') and not F.CHAT_RE.match('agent:main:cron:j:run:s')


WEB = 'agent:main:dashboard:00000000-0000-4000-8000-000000000001'


def test_main_and_webchat_stalls_continue_locally_without_a_delivery_route():
    for key in ('agent:main:main', WEB):
        d = setup(log=[req(key, 'human-run'), stall(FABLE, 'human-run')],
                  probe={'anthropic': {PB: 'rate_limit', PA: 'ok'}},
                  sessions={key: {'key': key, 'hasActiveRun': True}}, entries={key: PIN})
        r, = kinds(go(d)[2], 'chat_recover')
        assert (r['patch'], r['abort'], r['continue'], r['to']) == ('ok', 'ok', 'started', FABLE + '@' + PA), r
        assert len(kinds(go(d)[2], 'chat_recover')) == 1


def test_runtime_timeout_is_neither_continued_nor_held():
    # A turn that merely timed out was not stopped by a limit: no continuation, no hold, no move.
    e = dict(PIN, sessionId='session-1', lastRunId='timeout-run', status='timeout')
    d = setup(log=[line('embedded run timeout: runId=timeout-run sessionId=session-1 timeoutMs=899999')],
              probe={'anthropic': {PA: 'ok', PB: 'ok'}}, sessions={WEB: {'hasActiveRun': False}}, entries={WEB: e})
    (d / 'st').mkdir()                                                # a timeout event an older version left pending
    (d / 'st' / 'state.json').write_text(json.dumps({'started': 0, 'pending': {'timeout-run': {
        'kind': 'timeout', 't': ms() - 40_000, 'until': ms() + 3600_000, 'session': 'session-1'}}}))
    j = go(d, True)[2]
    state = json.loads((d / 'st' / 'state.json').read_text())
    assert not kinds(j, 'chat_recover') and not kinds(j, 'chat_move'), j
    assert not state['pending'] and not state['limited'] and not state.get('continued'), state
    drop, = kinds(j, 'event_drop')                                    # dropped, with the reason journaled
    assert drop['run'] == 'timeout-run' and 'older version' in drop['why'], drop


def test_worker_dashboard_and_internal_keys_are_not_human_chats():
    for key in ('agent:main:cron:x:run:y', 'agent:main:a2a:direct:peer',
                'agent:main:subagent:worker', 'agent:main:worker:direct:peer', 'agent:main:heartbeat'):
        assert not F.human_chat(key), key
    for metadata in ({'spawnedCwd': '/worktrees/worker'}, {'worktree': {'id': 'worker'}},
                     {'placement': {'providerId': 'device'}}, {'placement': {'environmentId': 'worker:worker'}},
                     {'spawnedBy': 'agent:main:main'}, {'spawnDepth': 1}, {'isBackground': True}):
        assert not F.human_chat(WEB, metadata)
        assert not F.human_chat(WEB, {}, metadata)
        d = setup(log=[req(WEB, 'worker-run'), stall(FABLE, 'worker-run')],
                  probe={'anthropic': {PB: 'rate_limit', PA: 'ok'}},
                  sessions={WEB: dict(metadata, hasActiveRun=True)}, entries={WEB: PIN})
        assert not kinds(go(d)[2], 'chat_recover'), metadata
    assert F.human_chat(WEB, {'placement': {'state': 'local'}, 'spawnDepth': 0})
    assert F.human_chat(WEB, {'placement': None, 'spawnDepth': None})


def test_session_id_resolver_reads_the_database_and_never_guesses():
    d = setup()
    db = d / 'oc/agents/main/agent/openclaw-agent.sqlite'
    c = sqlite3.connect(db)
    c.execute('insert into session_nodes values(?,?)', (WEB, json.dumps({'sessionId': 'sid', 'lastRunId': 'run'})))
    c.commit()
    with patch.object(F, 'FAKE', None), patch.dict(os.environ, OCF_AGENT_DB='file:%s?mode=ro' % db):
        assert F.key_for_session('sid') == WEB and F.key_for_session('other') is None
        c.execute('insert into session_nodes values(?,?)', ('agent:main:main', json.dumps({'sessionId': 'sid'})))
        c.commit()
        assert F.key_for_session('sid') is None                        # two sessions claim it: no guess
    c.close()


def recovery_fixture():
    run = 'original-run'
    e = dict(PIN, sessionId='sid', activeWriterRunId=run, lifecycleRevision='revision-before-abort')
    s = {'hasActiveRun': True, 'lastRunId': run}
    p = {'kind': 'stall', 't': ms() - 40_000, 'until': ms() + 3600_000, 'model': FABLE}
    state = {'handled': {}, 'pending': {run: p}}
    health = F.Health({'primary': OPUS, 'chain': [], 'order': {}})
    health.saved = []                                                  # no saved-only accounts
    out = {'problems': [], 'chats': [], 'continued': [], 'no_target': []}
    return run, e, s, p, state, health, out


@offline
def test_new_input_or_model_choice_during_probe_prevents_all_mutation():
    for change in ('queued', 'new-run', 'model'):
        run, e, s, p, state, health, out = recovery_fixture()
        fresh_e = dict(e, modelOverride='new-model') if change == 'model' else dict(e)
        fresh_s = dict(s, hasActiveRun=True, activeRunIds=['new-run']) if change == 'new-run' else s
        if change == 'new-run':
            fresh_e['activeWriterRunId'] = 'new-run'
        with patch.object(F, 'APPLY', True), patch.object(F, 'FAKE', None), \
                patch.object(F, 'entry', side_effect=[(e, False), (fresh_e, change == 'queued')]), \
                patch.object(F, 'session', return_value=fresh_s), patch.object(F, 'log_lines', return_value=[]), \
                patch.object(health, 'target', return_value=FABLE + '@' + PA), patch.object(F, 'gw') as mutate:
            assert F.recover_chat(state, health, WEB, s, run, p, out)
            mutate.assert_not_called()


@offline
def test_abort_is_exact_and_precedes_patch_and_partial_failure_stays_pending():
    run, e, s, p, state, health, out = recovery_fixture()
    stopped = {'hasActiveRun': False, 'lastRunId': run, 'status': 'aborted'}
    post_abort = dict(e, lifecycleRevision='revision-after-abort')
    with patch.object(F, 'APPLY', True), patch.object(F, 'FAKE', None), \
            patch.object(F, 'entry', side_effect=[(e, False), (e, False), (post_abort, False)]), \
            patch.object(F, 'session', side_effect=[s, stopped]), \
            patch.object(F, 'log_lines', return_value=[]), patch.object(F, 'save'), \
            patch.object(health, 'target', return_value=FABLE + '@' + PA), \
            patch.object(F, 'gw', side_effect=[({'status': 'aborted', 'abortedRunId': run}, ''), (None, 'patch failed')]) as mutate:
        assert not F.recover_chat(state, health, WEB, s, run, p, out)
        assert [c.args[0] for c in mutate.call_args_list] == [F.RPC_ABORT, F.RPC_PATCH]
        assert mutate.call_args_list[0].args[1] == {'key': WEB, 'runId': run}
        assert mutate.call_args_list[1].args[1]['expectedSessionId'] == 'sid'
        assert mutate.call_args_list[1].args[1]['expectedLifecycleRevision'] == 'revision-after-abort'
        assert p['aborted'] and run not in state['handled'] and run in state['pending']


@offline
def test_abort_answer_without_our_run_changes_nothing_and_leaves_the_log_to_decide():
    # sessions.abort answered that no such run was active: it ended meanwhile. Nothing is patched or continued now,
    # and the event is not marked handled: the next pass reads the run's log.
    run, e, s, p, state, health, out = recovery_fixture()
    with patch.object(F, 'APPLY', True), patch.object(F, 'FAKE', None), \
            patch.object(F, 'entry', return_value=(e, False)), patch.object(F, 'session', return_value=s), \
            patch.object(F, 'log_lines', return_value=[]), patch.object(health, 'target', return_value=FABLE + '@' + PA), \
            patch.object(F, 'gw', return_value=({'status': 'no-active-run', 'abortedRunId': None}, '')) as mutate, \
            patch.object(F, 'continue_turn') as send:
        assert not F.recover_chat(state, health, WEB, s, run, p, out)
        assert not F.recover_chat(state, health, WEB, s, run, p, out)   # the row still says active: wait, no second abort
        assert mutate.call_count == 1 and mutate.call_args.args[0] == F.RPC_ABORT
        send.assert_not_called()
        assert not p.get('aborted') and run not in state['handled']


@offline
def test_abort_accepted_before_a_status_is_stored_still_moves_and_continues():
    # Our abort stopped the run, but the session row shows no terminal status word yet ('running', or none at all).
    # The status word decides nothing: the run is over and our abort ended it, so the chat is moved and continued.
    for status in ('running', None):
        run, e, s, p, state, health, out = recovery_fixture()
        unsettled = {'hasActiveRun': False, 'lastRunId': run}
        if status:
            unsettled['status'] = status
        with patch.object(F, 'APPLY', True), patch.object(F, 'FAKE', None), \
                patch.object(F, 'entry', return_value=(e, False)), \
                patch.object(F, 'session', side_effect=[s, unsettled, unsettled]), \
                patch.object(F, 'log_lines', return_value=[]), patch.object(F, 'save'), \
                patch.object(health, 'target', return_value=FABLE + '@' + PA), \
                patch.object(F, 'gw', return_value=({'status': 'aborted', 'abortedRunId': run}, '')) as mutate, \
                patch.object(F, 'continue_turn', return_value=('started', None)) as send:
            assert F.recover_chat(state, health, WEB, s, run, p, out)
            assert [c.args[0] for c in mutate.call_args_list] == [F.RPC_ABORT, F.RPC_PATCH]
            send.assert_called_once()
            assert p['aborted'] and run in state['handled'], status


@offline
def test_abort_intent_recovers_lost_receipt_and_process_interruption_once():
    for interrupted in (False, True):
        run, e, s, p, state, health, out = recovery_fixture()
        state['run_key'] = {run: WEB}
        stopped = dict(s, hasActiveRun=False, status='aborted')
        def abort(rpc, args, timeout):
            assert rpc == F.RPC_ABORT
            durable = json.loads(F.STATE.read_text())['pending'][run]
            assert durable['abort_intent'] == {'key': WEB, 'session': 'sid', 'run': run}
            assert not durable.get('aborted')
            if interrupted:
                raise SystemExit('process stopped after accepted abort')
            return None, 'lost transport receipt'
        with patch.object(F, 'APPLY', True), patch.object(F, 'FAKE', None), \
                patch.object(F, 'entry', return_value=(e, False)), patch.object(F, 'session', return_value=s), \
                patch.object(F, 'log_lines', return_value=[]), \
                patch.object(health, 'target', return_value=FABLE + '@' + PA), patch.object(F, 'gw', side_effect=abort):
            try:
                assert not F.recover_chat(state, health, WEB, s, run, p, out)
            except SystemExit:
                assert interrupted
        state = json.loads(F.STATE.read_text())  # real restart: only the pre-RPC state survived
        with patch.object(F, 'APPLY', True), patch.object(F, 'FAKE', None), \
                patch.object(F, 'entry', return_value=(e, False)), patch.object(F, 'session', return_value=stopped), \
                patch.object(F, 'log_lines', return_value=[]), \
                patch.object(health, 'target', return_value=FABLE + '@' + PA), \
                patch.object(F, 'gw', return_value=({'ok': True}, '')) as mutate, \
                patch.object(F, 'continue_turn', return_value=('started', None)) as send:
            F.stalls_and_chats(state, health, {}, out)
            F.stalls_and_chats(state, health, {}, out)
            send.assert_called_once()
            assert [c.args[0] for c in mutate.call_args_list] == [F.RPC_PATCH]
            assert run in state['handled'] and run not in state['pending']


@offline
def test_stopped_run_without_our_exact_intent_or_with_newer_choice_is_untouched():
    # A run stopped from the Control UI is not a limit death, and an abort intent that is not exactly ours does not
    # make it one. A newer model choice or user turn wins too.
    stop = line('sessions.abort requested: actor=openclaw-control-ui session=%s runId=original-run' % WEB, ago=5)
    for change in ('manual', 'session', 'run', 'key', 'model', 'input'):
        run, e, s, p, state, health, out = recovery_fixture()
        state['run_key'] = {run: WEB}
        stopped = dict(s, hasActiveRun=False, status='aborted')
        p.update(session='sid', ref=FABLE + '@' + PB)
        if change != 'manual':
            p['abort_intent'] = {'key': WEB, 'session': 'sid', 'run': run}
        if change in ('session', 'run', 'key'):
            p['abort_intent'][change] = 'unrelated'
        if change == 'model':
            e['modelOverride'] = 'new-user-model'
        if change == 'input':
            e['lastInteractionAt'] = ms()
        with patch.object(F, 'entry', return_value=(e, False)), \
                patch.object(F, 'session', return_value=stopped), patch.object(F, 'log_lines', return_value=[stop]), \
                patch.object(F, 'gw') as mutate, patch.object(F, 'continue_turn') as send:
            F.stalls_and_chats(state, health, {}, out)
            mutate.assert_not_called()
            send.assert_not_called()
            assert run not in state['pending'] and run not in state['handled'], change


@offline
def test_a_stopped_run_with_no_hand_stop_and_no_progress_is_a_limit_death():
    # No abort intent of ours and no status word to go by: the limit was the run's last word, so it is continued.
    run, e, s, p, state, health, out = recovery_fixture()
    state['run_key'] = {run: WEB}
    stopped = {'hasActiveRun': False, 'lastRunId': run}
    with patch.object(F, 'APPLY', True), patch.object(F, 'FAKE', None), \
            patch.object(F, 'entry', return_value=(e, False)), patch.object(F, 'session', return_value=stopped), \
            patch.object(F, 'log_lines', return_value=[]), patch.object(health, 'target', return_value=FABLE + '@' + PA), \
            patch.object(F, 'gw', return_value=({'ok': True}, '')) as mutate, \
            patch.object(F, 'continue_turn', return_value=('started', None)) as send:
        F.stalls_and_chats(state, health, {}, out)
        assert [c.args[0] for c in mutate.call_args_list] == [F.RPC_PATCH]
        send.assert_called_once()
        assert run in state['handled'] and run not in state['pending'] and health.held(PB, FABLE)


def test_continuation_prompt_reconciles_side_effects_and_local_route_and_launch_identity():
    calls = []
    def launch(argv, timeout):
        calls.append(argv)
        return 0, '', ''
    with patch.object(F, 'APPLY', True), patch.object(F, 'FAKE', None), patch.object(F, 'run', side_effect=launch):
        first = F.continue_turn(WEB, 'test-model', True, 'interrupted-run')
        assert F.continue_turn(WEB, 'test-model', True, 'interrupted-run') == first
        F.continue_turn(K, 'test-model', True, 'another-run')
    local, external = calls[0], calls[2]
    assert '--deliver' not in local and '--deliver' in external
    assert local[local.index('--session-key') + 1] == WEB
    assert not any(flag in local for flag in ('--reply-to', '--reply-channel', '--to'))
    prompt = local[local.index('--message') + 1]
    assert 'interrupted-run' in prompt and 'usage limit' in prompt and 'timeout' not in prompt
    assert 'reconcile completed tool actions and delivery receipts' in prompt
    assert 'Do not replay the entire request' in prompt and 'Verify uncertain side effects' in prompt
    assert 'RemainAfterExit=yes' in local and '--no-block' in local


def test_uncertain_continuation_launch_adopts_existing_deterministic_unit():
    with patch.object(F, 'APPLY', True), patch.object(F, 'FAKE', None), \
            patch.object(F, 'run', side_effect=[(1, '', 'transport lost'), (0, 'loaded\n', '')]):
        result, unit = F.continue_turn(WEB, 'test-model', False, 'interrupted-run')
        assert result == 'started' and unit.startswith('ocf-continue-')


def test_chat_return_does_not_touch_active_or_queued_sessions():
    parked = {'providerOverride': 'xai', 'modelOverride': 'grok-4.7', 'authProfileOverride': 'xai:x'}
    for active, queued in ((True, False), (False, True)):
        d = setup(probe={'anthropic': {PA: 'ok'}}, entries={WEB: parked},
                  sessions={WEB: {'hasActiveRun': active}})
        fake(d, queued={WEB: queued})
        (d / 'st').mkdir()
        (d / 'st/state.json').write_text(json.dumps({'chat_home': {WEB: {'model': OPUS, 'parked': 'xai/grok-4.7@xai:x'}}}))
        assert not kinds(go(d)[2], 'chat_return')


def test_unpinned_default_fallback_returns_to_primary_even_when_override_was_absent():
    key = 'agent:main:main'
    d = setup(log=[req(key, 'default-run'), stall(OPUS, 'default-run')],
              probe={'anthropic': DEAD, 'xai': {'xai:x': 'ok'}},
              sessions={key: {'hasActiveRun': True}}, entries={key: {'authProfileOverride': PA}})
    r, = kinds(go(d)[2], 'chat_recover')
    assert r['to'] == 'xai/grok-4.7@xai:x' and r['patch'] == 'ok'
    state = json.loads((d / 'st/state.json').read_text())
    assert state['chat_home'][key]['model'] == OPUS
    fake(d, probe={'anthropic': {PB: 'ok'}, 'xai': {'xai:x': 'ok'}},
         sessions={key: {'hasActiveRun': False}},
         entries={key: {'providerOverride': 'xai', 'modelOverride': 'grok-4.7', 'authProfileOverride': 'xai:x'}})
    returned, = kinds(go(d, True)[2], 'chat_return')
    # sessions.patch(primary@profile) clears modelOverride in OpenClaw; no permanent fallback pin remains.
    assert returned['to'] == OPUS + '@' + PB


@offline
def test_partial_recovery_retry_never_holds_the_new_healthy_target_or_loses_failed_launch():
    run, e, s, p, state, health, out = recovery_fixture()
    p.update(aborted=True, profile=PB, ref=FABLE + '@' + PB)
    state['chat_moved'] = {WEB: {'to': FABLE + '@' + PA, 't': ms()}}   # last pass moved it, then the launch failed
    e['authProfileOverride'] = PA
    s.update(hasActiveRun=False, status='aborted')
    health.cache['anthropic'] = {PA: 'ok', PB: 'rate_limit'}
    with patch.object(F, 'APPLY', True), patch.object(F, 'FAKE', None), \
            patch.object(F, 'entry', return_value=(e, False)), patch.object(F, 'session', return_value=s), \
            patch.object(F, 'log_lines', return_value=[]), \
            patch.object(F, 'gw', return_value=({'ok': True}, '')) as mutate, \
            patch.object(F, 'continue_turn', return_value=('rc=1 launch failed', None)):
        assert not F.recover_chat(state, health, WEB, s, run, p, out)
        mutate.assert_not_called()                                    # already on the live target: no second patch
        assert health.held(PB, FABLE) and not health.held(PA, FABLE)
        assert run not in state['handled'] and run in state['pending']


def test_stalled_run_identity_prefers_current_writer_over_old_lifecycle():
    assert not F.active_run({'hasActiveRun': True}, {'activeWriterRunId': 'new', 'lifecycleRunId': 'old'}, 'old')
    assert not F.active_run({'hasActiveRun': True}, {}, 'old')
    assert F.active_run({'hasActiveRun': True, 'activeRunIds': ['old']}, {}, 'old')


def aborted_lines(run, key=K, ago=25, by='sessions.abort', actor='cli'):
    """What the Gateway logs when a run is stopped: who asked for it, then the turn's lane error."""
    return [line('%s requested: actor=%s device=d ip=i conn=c session=%s runId=%s' % (by, actor, key, run), ago=ago),
            line({'errorName': 'AbortError', 'taskKind': 'turn', 'sessionKey': key, 'runId': run},
                 'lane task error: lane=main durationMs=25000 error="aborted | agent run aborted"', ago=ago)]


def killed(run):
    """Session row after an abort: OpenClaw stores the end as 'killed' (the abort RPC itself answers 'aborted')."""
    return {'key': K, 'hasActiveRun': False, 'lastRunId': run, 'status': 'killed', 'abortedLastRun': True}


ONE_LIVE = {'anthropic': {PB: 'rate_limit', PA: 'ok', PC: 'rate_limit'}}


def test_stall_aborted_before_the_pass_is_moved_and_continued_without_another_abort():
    # Fallbacks are off for a pinned model: the run sleeps out an 87 h wait and is aborted within seconds. The pin
    # stays on the limited model and the turn is unanswered.
    log = [req(K, 'D1'), end('D1'), stall(FABLE, 'D1', delay=313935000)] + aborted_lines('D1')
    d = setup(log=log, probe=ONE_LIVE, sessions={K: dict(killed('D1'), hasActiveRun=True)}, entries={K: PIN})
    j = go(d)[2]                                                      # still stopping: wait for its end, keep it
    assert not kinds(j, 'chat_recover') and 'D1' in json.loads((d / 'st' / 'state.json').read_text())['pending'], j
    fake(d, sessions={K: killed('D1')})
    r, = kinds(go(d)[2], 'chat_recover')
    assert (r['why'], r['frm'], r['to'], r['patch'], r['abort'], r['continue']) == \
        ('stall', FABLE + '@' + PB, FABLE + '@' + PA, 'ok', 'not needed', 'started'), r
    state = json.loads((d / 'st' / 'state.json').read_text())
    assert state['limited'].get(FABLE + '@' + PB, 0) > ms() and 'D1' not in state['pending'], state
    assert len(kinds(go(d)[2], 'chat_recover')) == 1                  # handled once


def test_pinned_chat_an_older_version_left_on_a_dead_account_is_moved_without_reopening_its_event():
    # An older version marked the run handled without moving the chat. Nothing reconstructs that death any more: the
    # pinned chat is moved by the next pass because its model@account is probed out. The old turn is not continued.
    log = [req(K, 'D2'), end('D2'), stall(FABLE, 'D2', delay=313935000)] + aborted_lines('D2')
    d = setup(log=log, probe=ONE_LIVE, sessions={K: killed('D2')}, entries={K: PIN})
    f = d / 'logs' / 'openclaw-2026-01-01.log'
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text(json.dumps({
        'started': 0, 'handled': {'D2': ms() - 30_000, 'OTHER': ms() - 60_000},
        'offsets': {str(f): f.stat().st_size}, 'run_key': {'D2': K}}))
    j = go(d)[2]
    m, = kinds(j, 'chat_move')
    assert (m['session'], m['frm'], m['to'], m['result']) == (K, FABLE + '@' + PB, FABLE + '@' + PA, 'ok'), m
    assert not kinds(j, 'chat_recover') and not kinds(j, 'chat_reopen'), j
    assert len(kinds(go(d, True)[2], 'chat_move')) == 1                # the pin now points at a live account


def test_pinned_chat_is_moved_each_pass_like_a_job_without_any_run_event():
    # No log line at all: a pinned chat whose model@account is probed out, or held, moves to the next live target.
    d = setup(probe=ONE_LIVE, entries={K: PIN})
    j = go(d, True)[2]
    m, = kinds(j, 'chat_move')
    assert (m['frm'], m['to'], m['result']) == (FABLE + '@' + PB, FABLE + '@' + PA, 'ok'), m
    assert not kinds(j, 'chat_recover') and not kinds(j, 'event_drop'), j
    assert len(kinds(go(d, True)[2], 'chat_move')) == 1                # once: the pin points at a live account now
    d = setup(probe={'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}}, entries={K: PIN})
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text(json.dumps({'started': 0, 'limited': {FABLE + '@' + PB: ms() + 3600_000}}))
    m, = kinds(go(d, True)[2], 'chat_move')
    assert (m['to'], m['why']) == (FABLE + '@' + PA, 'held'), m       # every probe says ok: the hold decides
    # Left alone: a chat with a run in progress, a chat whose account OpenClaw chose itself, a worker session.
    auto = dict(PIN, modelOverrideSource='auto', authProfileOverrideSource='auto')
    for sessions, entries in (({K: {'hasActiveRun': True}}, {K: PIN}), ({}, {K: auto}), ({WEB: {'spawnDepth': 1}}, {WEB: PIN})):
        d = setup(probe=ONE_LIVE, sessions=sessions, entries=entries)
        assert not kinds(go(d, True)[2], 'chat_move'), (sessions, entries)


@offline
def test_a_limit_is_held_for_one_model_on_one_account_and_an_ok_probe_never_clears_it():
    health = F.Health({'primary': OPUS, 'chain': [OPUS], 'order': {'anthropic': [PB, PA]}},
                      {FABLE + '@' + PB: ms() + 10 * 3600_000})
    health.cache['anthropic'] = {PB: 'ok', PA: 'ok'}                  # the probe (of another model) says both answer
    assert health.alive(PB, OPUS) and not health.alive(PB, FABLE) and health.dead(PB, FABLE)
    assert health.target(FABLE) == FABLE + '@' + PA
    health.hold(PA, ms() + 3600_000, FABLE)
    assert health.target(FABLE) == OPUS + '@' + PB                    # Fable is out on both; Opus on PB is not


def test_work_does_not_go_back_to_a_model_still_inside_its_own_wait():
    # Every account answers its probe, but Opus is held on all of them until the time its provider named, longer than
    # any former cap: the parked job and chat stay on the fallback. A Fable hold does not keep Opus off an account,
    # and a whole-account hold left by an older version is dropped.
    grok = {'providerOverride': 'xai', 'modelOverride': 'grok-4.7', 'modelOverrideSource': 'user',
            'authProfileOverride': 'xai:x', 'authProfileOverrideSource': 'user'}
    d = setup({'a': ('xai/grok-4.7', 'xai:x', 'trusted')}, [], {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}, 'xai': {'xai:x': 'ok'}},
              entries={K: grok})
    later = ms() + 20 * 3600_000
    parked = {'model': OPUS, 'parked': 'xai/grok-4.7@xai:x', 'at': 0, 'wait': 0}
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text(json.dumps({'started': 0, 'home': {'a': parked}, 'chat_home': {K: parked},
        'limited': {OPUS + '@' + PB: later, OPUS + '@' + PA: later, OPUS + '@' + PC: later, PA: later}}))
    j = go(d, True)[2]
    assert binding(d, 'a') == 'xai/grok-4.7@xai:x' and not kinds(j, 'chat_return') and not kinds(j, 'job_move'), j
    s = json.loads((d / 'st' / 'state.json').read_text())
    assert sorted(s['limited']) == sorted([OPUS + '@' + PB, OPUS + '@' + PA, OPUS + '@' + PC]), s['limited']
    s['limited'] = {OPUS + '@' + PB: later, FABLE + '@' + PA: later}
    (d / 'st' / 'state.json').write_text(json.dumps(s))
    j = go(d, True)[2]
    assert binding(d, 'a') == OPUS + '@' + PA, j                      # Opus on PB is held; a Fable hold is not an Opus hold
    r, = kinds(j, 'chat_return')
    assert r['to'] == OPUS + '@' + PA, r


def test_the_log_alone_decides_a_limit_death_whatever_the_status_word_says():
    # The status word after a limit death differs by build and path ('failed', 'killed', 'aborted' or none). It neither
    # allows nor blocks a continuation; the run's log does. Every dropped event is journaled with its reason.
    for status in (None, 'failed', 'killed', 'aborted', 'running'):
        for went_on in (False, True):
            ses = {'key': K, 'hasActiveRun': False, 'lastRunId': 'L1'}
            if status:
                ses['status'] = status
            later = [line({'runId': 'L1', 'milestone': 'tool_execution_started'}, 'agent turn milestone', ago=10)] if went_on else []
            d = setup(log=[req(K, 'L1'), end('L1')] + later, probe={'anthropic': DEAD, 'xai': {'xai:x': 'ok'}},
                      sessions={K: ses}, entries={K: PIN})
            j = go(d)[2]
            state = json.loads((d / 'st' / 'state.json').read_text())
            assert 'L1' not in state['pending'], (status, went_on)
            if went_on:
                drop, = kinds(j, 'event_drop')
                assert not kinds(j, 'chat_recover') and drop['run'] == 'L1' and 'last word' in drop['why'], (status, j)
            else:
                r, = kinds(j, 'chat_recover')
                assert r['continue'] == 'started' and 'L1' in state['handled'], (status, r)


def test_stall_that_woke_up_or_was_stopped_by_a_person_is_left_alone():
    went_on = line({'runId': 'D3', 'milestone': 'model_call_started'}, 'agent turn milestone', ago=20)
    done = line({'runId': 'D3', 'runStatus': 'completed', 'outcome': 'sent'}, 'recorded run outcome', ago=10)
    for active, status, later in ((False, 'done', [went_on, done]),                   # woke up and answered
                                  (False, 'killed', [went_on] + aborted_lines('D3', ago=10)),   # went on, then stopped
                                  (False, 'killed', aborted_lines('D3', by='chat.abort')),     # a person pressed stop
                                  (False, 'killed', aborted_lines('D3', actor='openclaw-control-ui')),  # Control UI Stop
                                  (True, None, [went_on])):                           # woke up, still working
        ses = {'key': K, 'hasActiveRun': active, 'lastRunId': 'D3', 'status': status}
        d = setup(log=[req(K, 'D3'), end('D3'), stall(FABLE, 'D3')] + later, probe=ONE_LIVE,
                  sessions={K: ses}, entries={K: PIN})
        j = go(d)[2]
        state = json.loads((d / 'st' / 'state.json').read_text())
        assert not kinds(j, 'chat_recover') and 'D3' not in state['pending'] and not state['limited'], (status, j)
        assert [r['run'] for r in kinds(j, 'event_drop')] == ['D3'], j                 # dropped with its reason
        # The pinned chat still leaves its probed-out account once idle; it is moved, not continued.
        assert [m['to'] for m in kinds(j, 'chat_move')] == ([] if active else [FABLE + '@' + PA]), (status, j)


def test_check_ignores_a_turn_that_rotated_accounts_and_ended_normally_after_a_limit():
    # 21 Nov 04:27: run 747f56b7 hit a 429, the Gateway rotated the profile, finished with an isolated finalization and
    # logged only the plain-text "[agent] run ... ended with stopReason=stop"; the check called the answered chat unanswered.
    ended = line('[agent] run D5 ended with stopReason=stop', ago=3300)
    d = setup(log=[req(K, 'D5', 3600), end('D5', 3600), ended], probe=ONE_LIVE,
              sessions={K: {'key': K, 'hasActiveRun': False, 'lastRunId': 'D5', 'status': 'done'}}, entries={K: PIN})
    rc, err, j = go(d)
    assert 'still unanswered' not in err and not kinds(j, 'chat_recover'), (rc, err)
    d = setup(log=[req(K, 'D6', 3600), end('D6', 3600), line('[agent] run D5 ended with stopReason=stop', ago=3300)],
              probe=ONE_LIVE, sessions={K: {'key': K, 'hasActiveRun': False, 'lastRunId': 'D6', 'status': 'failed'}},
              entries={K: PIN})
    rc, err, j = go(d)
    assert rc == 1 and 'still unanswered' in err, (rc, err)          # another run's end does not count


def test_check_reports_a_turn_killed_after_a_limit_that_is_still_unanswered():
    d = setup(log=[req(K, 'D4', 3600), end('D4', 3600)] + aborted_lines('D4', ago=3590), probe=ONE_LIVE,
              sessions={K: killed('D4')}, entries={K: PIN})
    rc, err, j = go(d)
    assert not kinds(j, 'chat_recover') and rc == 1 and 'still unanswered' in err, (rc, err)


@offline
def test_ended_stall_is_patched_and_continued_without_an_abort_call():
    run, e, s, p, state, health, out = recovery_fixture()
    ended = dict(s, hasActiveRun=False, status='killed')
    later = [line('sessions.abort requested: actor=cli session=%s runId=%s' % (WEB, run), ago=5)]
    with patch.object(F, 'APPLY', True), patch.object(F, 'FAKE', None), \
            patch.object(F, 'entry', return_value=(e, False)), patch.object(F, 'session', return_value=ended), \
            patch.object(F, 'log_lines', return_value=later), \
            patch.object(health, 'target', return_value=FABLE + '@' + PA), \
            patch.object(F, 'gw', return_value=({'ok': True}, '')) as mutate, \
            patch.object(F, 'continue_turn', return_value=('started', None)) as send:
        assert F.recover_chat(state, health, WEB, ended, run, p, out)
        assert [c.args[0] for c in mutate.call_args_list] == [F.RPC_PATCH]
        send.assert_called_once()
        assert run in state['handled'] and health.held(PB, FABLE)


@offline
def test_stall_being_stopped_by_someone_else_during_the_probe_waits_for_its_end():
    run, e, s, p, state, health, out = recovery_fixture()
    later = [line('sessions.abort requested: actor=cli session=%s runId=%s' % (WEB, run), ago=5)]
    seen = []
    def lines(run_id):                                                # quiet at the first look, stopping after the probe
        seen.append(run_id)
        return [] if len(seen) == 1 else later
    with patch.object(F, 'APPLY', True), patch.object(F, 'FAKE', None), \
            patch.object(F, 'entry', return_value=(e, False)), patch.object(F, 'session', return_value=s), \
            patch.object(F, 'log_lines', side_effect=lines), \
            patch.object(health, 'target', return_value=FABLE + '@' + PA), \
            patch.object(F, 'gw') as mutate, patch.object(F, 'continue_turn') as send:
        assert F.recover_chat(state, health, WEB, s, run, p, out) is False
        mutate.assert_not_called()
        send.assert_not_called()
        assert run not in state['handled']



def test_strict_chat_changes_account_but_never_model_and_its_failed_turn_waits():
    strict = {'OCF_STRICT_CHATS': 'agent:main:other:direct:x, ' + K}
    # Another account of its own model is live: an account-only move, like any pinned chat.
    d = setup(probe=ONE_LIVE, entries={K: PIN})
    m, = kinds(go(d, True, env=strict)[2], 'chat_move')
    assert m['to'] == FABLE + '@' + PA, m
    # Every account of its model is out while other models are live: no move, no continuation, no problem raised.
    ses = {K: {'key': K, 'hasActiveRun': False, 'lastRunId': 'R2', 'status': 'failed'}}
    d = setup(log=[req(K, 'R2'), end('R2')], probe={'anthropic': DEAD, 'xai': {'xai:x': 'ok'}, 'openai': {'openai:default': 'ok'}},
              sessions=ses, entries={K: PIN})
    for _ in range(2):
        rc, err, j = go(d, True, env=strict)
        assert not kinds(j, 'chat_move') and not kinds(j, 'chat_recover') and not kinds(j, 'event_drop'), j
        assert rc == 0 and 'no live account' not in err, err
    # Another account of its own model comes back: the waiting turn is continued there, with no model-change line.
    fake(d, probe={'anthropic': {PB: 'rate_limit', PA: 'ok', PC: 'rate_limit'}})
    r, = kinds(go(d, True, env=strict)[2], 'chat_recover')
    assert (r['to'], r['continue']) == (FABLE + '@' + PA, 'started'), r
    # Without the setting the same chat goes down the chain as before.
    d = setup(log=[req(K, 'R2'), end('R2')], probe={'anthropic': DEAD, 'xai': {'xai:x': 'ok'}}, sessions=ses, entries={K: PIN})
    r, = kinds(go(d)[2], 'chat_recover')
    assert r['to'] == 'xai/grok-4.7@xai:x', r



def test_job_bound_to_an_account_without_naming_it_moves_too():
    # payload.model names only the model; the account is recorded in the job's binding, which cron list does not show.
    d = setup({'u': (OPUS, PA, 'trusted'), 'a': (OPUS, PB, 'trusted')},
              probe={'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}})
    c = sqlite3.connect(d / 'state.sqlite')
    j = json.loads(c.execute("select job_json from cron_jobs where job_id='u'").fetchone()[0])
    j['payload']['model'] = OPUS                                      # no '@account'
    c.execute("update cron_jobs set job_json=? where job_id='u'", (json.dumps(j),))
    c.commit()
    c.close()
    m, = kinds(go(d, True)[2], 'job_move')
    assert (m['job'], m['frm'], m['to']) == ('u', OPUS + '@' + PA, OPUS + '@' + PB), m
    # Two limit failures in a row of such a job, while the probe still says ok: held and moved, no crash.
    d = setup({'u': (OPUS, PA, 'trusted'), 'a': (OPUS, PB, 'trusted')}, [('u', 'error', ERR, 120), ('u', 'error', ERR, 60)],
              {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}})
    c = sqlite3.connect(d / 'state.sqlite')
    c.execute("update cron_jobs set job_json=json_set(job_json, '$.payload.model', ?) where job_id='u'", (OPUS,))
    c.commit()
    c.close()
    rc, err, j = go(d, True)
    assert 'Traceback' not in err, err
    m, = kinds(j, 'job_move')
    assert (m['job'], m['to']) == ('u', OPUS + '@' + PB), (err, j)



def test_disabled_job_started_by_hand_moves_after_enabled_ones_and_is_never_a_problem():
    # A disabled job started only by 'cron run' stayed on an exhausted account: its next manual run failed.
    d = setup({'x': (OPUS, PA, 'trusted'), 'e': (OPUS, PA, 'trusted'), 'l': (OPUS, PA, 'account')},
              probe={'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}}, sched={'x': ('cron', 0), 'e': ('cron', 600)})
    c = sqlite3.connect(d / 'state.sqlite')
    c.execute("update cron_jobs set enabled=0 where job_id in ('x','l')")
    c.commit()
    c.close()
    rc, err, j = go(d, True)
    assert [(m['job'], m['to']) for m in kinds(j, 'job_move')] == [('e', OPUS + '@' + PB), ('x', OPUS + '@' + PB)], j
    assert rc == 0 and 'legacy' not in err, err                     # a disabled legacy job is skipped silently
    c = sqlite3.connect(d / 'state.sqlite')
    assert c.execute("select enabled from cron_jobs where job_id='x'").fetchone()[0] == 0   # still disabled
    c.close()
    # Nowhere to go: an enabled job is a problem, a disabled one is not.
    dead = {'anthropic': DEAD, 'xai': {'xai:x': 'rate_limit'}, 'openai': {'openai:default': 'rate_limit'}}
    d = setup({'x': (OPUS, PA, 'trusted')}, probe=dead)
    c = sqlite3.connect(d / 'state.sqlite')
    c.execute("update cron_jobs set enabled=0")
    c.commit()
    c.close()
    rc, err, j = go(d, True)
    assert rc == 0 and 'no live account' not in err and not kinds(j, 'job_move'), (rc, err)


def test_disabled_exclusions_and_edit_budget_preserve_enabled_priority():
    # Neither a disabled one-shot nor a legacy account-mode job warrants discovery or a move.
    d = setup({'e': (OPUS, PA, 'trusted'), 'x': ('xai/grok-4.7', 'xai:x', 'account'),
               'o': ('openai/gpt-6.1-sol', OA, 'trusted')},
              probe={'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}}, sched={'o': ('at', 0)})
    c = sqlite3.connect(d / 'state.sqlite')
    c.execute("update cron_jobs set enabled=0 where job_id in ('x','o')")
    c.commit()
    c.close()
    rc, err, j = go(d, True)
    assert [m['job'] for m in kinds(j, 'job_move')] == ['e'], j
    assert {r['provider'] for r in kinds(j, 'probe')} == {'anthropic'}, kinds(j, 'probe')
    assert binding(d, 'x') == 'xai/grok-4.7@xai:x' and binding(d, 'o') == 'openai/gpt-6.1-sol@' + OA
    assert rc == 0, err
    # Past DISABLED_BUDGET_S (here: spent at once) a disabled move waits for the next pass; an enabled one still goes.
    d = setup({'e': (OPUS, PA, 'trusted'), 'x': (OPUS, PA, 'trusted')},
              probe={'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}})
    c = sqlite3.connect(d / 'state.sqlite')
    c.execute("update cron_jobs set enabled=0 where job_id='x'")
    c.commit()
    c.close()
    fake(d, slow_edit=0.6)
    rc, err, j = go(d, True, env={'OCF_DISABLED_BUDGET_S': '-1'})
    assert [m['job'] for m in kinds(j, 'job_move')] == ['e'], j
    assert binding(d, 'x') == OPUS + '@' + PA
    assert not json.loads((d / 'st' / 'state.json').read_text())['retry']   # its wait does not make the next pass probe
    rc, err, j = go(d, True)                                          # next probing pass: the disabled job moves
    assert [m['job'] for m in kinds(j, 'job_move')] == ['e', 'x'], j   # the journal holds both passes
    assert binding(d, 'x') == OPUS + '@' + PB


def test_disabled_recurring_coverage_discovers_missing_providers_and_refreshes_each_pass():
    # Cold, unrelated-provider cache, and source-provider cache. None may depend on enabled work discovering xAI.
    for seed in ('cold', 'unrelated', 'source', 'saved'):
        enabled = {} if seed in ('cold', 'saved') else {'e': (OPUS, PB, 'trusted')} if seed == 'source' else {
            'e': ('openai/gpt-6.1-sol', OA, 'trusted')}
        probe = {'anthropic': {PA: 'rate_limit', PB: 'ok', PC: 'ok'},
                 'xai': {'xai:x': 'ok'}, 'openai': {OA: 'ok'}}
        d = setup(dict(enabled, x=(FABLE, PA, 'trusted')), probe=probe)
        if seed == 'saved':
            cfg = json.loads((d / 'cfg.json').read_text())
            cfg['auth']['order'].pop('xai')
            (d / 'cfg.json').write_text(json.dumps(cfg))
            fake(d, models_status=models_status([('xai:x', 'oauth')]))
        c = sqlite3.connect(d / 'state.sqlite')
        c.execute("update cron_jobs set enabled=0 where job_id='x'")
        c.commit()
        c.close()
        (d / 'st').mkdir()
        initial = {'started': 0, 'checked_at': ms(), 'limited': {
            FABLE + '@' + PB: ms() + 1800_000, FABLE + '@' + PC: ms() + 1800_000}}
        (d / 'st' / 'state.json').write_text(json.dumps(initial))
        rc, err, j = go(d, True)
        moves = kinds(j, 'job_move')
        assert rc == 0 and [(m['job'], m['to']) for m in moves] == [('x', 'xai/grok-4.7@xai:x')], (seed, err, j)
        st = state_of(d)
        assert st['home']['x']['model'] == FABLE and st['bindings']['x']['model'] == binding(d, 'x')
        assert st['limited'] == initial['limited'] and not st.get('verified')
        assert not kinds(j, 'chat_recover') and not kinds(j, 'cron_stall_abort')
        # The following invocation has no fresh evidence: no rediscovery until the usual cadence is due.
        assert len(kinds(go(d)[2], 'probe')) == len(kinds(j, 'probe'))
        # A fresh pass must not reuse the old live answer. It moves off a newly exhausted fallback.
        fake(d, probe={'anthropic': DEAD, 'xai': {'xai:x': 'rate_limit'}, 'openai': {OA: 'ok'}})
        rc, err, j = go(d, True)
        assert rc == 0 and binding(d, 'x') == 'openai/gpt-6.1-sol@' + OA, (seed, err, j)
        # Own-model return still honors its model-specific holds, then returns without running the job.
        fake(d, probe=probe)
        rc, err, j = go(d, True)
        assert binding(d, 'x') == 'openai/gpt-6.1-sol@' + OA
        fake(d, probe=dict(probe, anthropic={PA: 'ok', PB: 'ok', PC: 'ok'}))
        st = state_of(d)
        st['home']['x']['at'] = 0               # the repeated-park wait has elapsed
        (d / 'st' / 'state.json').write_text(json.dumps(st))
        rc, err, j = go(d, True)
        assert rc == 0 and binding(d, 'x') == FABLE + '@' + PA and 'x' not in state_of(d)['home'], (seed, err, j)
        c = sqlite3.connect(d / 'state.sqlite')
        assert c.execute("select enabled from cron_jobs where job_id='x'").fetchone()[0] == 0
        c.close()


def test_disabled_uncertainty_does_not_hide_an_enabled_no_target_alert():
    probe = {'anthropic': DEAD, 'xai': {'xai:x': 'rate_limit'},
             'openai': {'openai:default': 'rate_limit', OA: 'rate_limit', 'openai:b': 'rate_limit'},
             'custom': {'custom:a': 'timeout'}}
    for disabled in (False, True):
        jobs = {'e': (OPUS, PA, 'trusted')}
        if disabled:
            jobs['x'] = ('custom/model', 'custom:a', 'trusted')
        d = setup(jobs, probe=probe)
        cfg = json.loads((d / 'cfg.json').read_text())
        cfg['auth']['order']['custom'] = ['custom:a']
        (d / 'cfg.json').write_text(json.dumps(cfg))
        c = sqlite3.connect(d / 'state.sqlite')
        c.execute("update cron_jobs set enabled=0 where job_id='x'")
        c.commit()
        c.close()
        rc, err, j = go(d, True)
        assert rc == 1 and 'no live account or fallback model' in err, (disabled, rc, err)
        assert 'no_target:' + OPUS in state_of(d)['problems']
        assert not kinds(j, 'job_move')


@offline
def test_slow_disabled_discovery_reaches_a_healthy_fallback_on_repeated_passes():
    # Advance elapsed time by the actual timeout a slow transport would consume. Each fresh pass
    # starts with the same two slow providers, not a lucky cache, provider order, or later recovery.
    conf = {'primary': 'source/model', 'chain': ['slow/model', 'healthy/model'],
            'order': {'source': ['source:a', 'source:b'], 'slow': ['slow:a'], 'healthy': ['healthy:a']}}
    base = 1700000000.0
    for start in (25, 180):
        for attempt in range(3):
            clock, requests, edits = [base + start], [], []
            def gateway(method, params, timeout=120, cli_allowance=5):
                assert method == F.RPC_PROBE
                provider = params['provider']
                requests.append((provider, clock[0] - base, timeout))
                if provider != 'healthy':
                    clock[0] += timeout
                    return None, 'timeout'
                clock[0] += min(1, timeout)
                return {'results': [{'profileId': 'healthy:a', 'status': 'ok'}]}, ''
            def cli(argv, timeout=120):
                assert argv[1:3] == ['cron', 'edit'], argv
                edits.append((argv[3], argv[5], clock[0] - base, timeout))
                clock[0] += timeout     # even an edit taking its entire reservation must fit
                return 0, '', ''
            state = {'limited': {'source/model@source:a': int((base + 3600) * 1000)}}
            jobs = {'x': {'enabled': False, 'mode': 'trusted', 'kind': 'cron', 'next': 0,
                          'model': 'source/model@source:a', 'profile': 'source:a', 'name': 'x'}}
            out = {'moved': [], 'no_target': [], 'problems': [], 'later': 0, 'deferred': 0}
            with patch.object(F, 'FAKE', None), patch.object(F, 'APPLY', True), \
                    patch.object(F, 'T0', base), patch.object(F.time, 'time', lambda: clock[0]), \
                    patch.object(F, 'default_agent', return_value='fixture'), \
                    patch.object(F, 'gw', side_effect=gateway), patch.object(F, 'run', side_effect=cli):
                health = F.Health(conf, state['limited'])
                F.jobs_pass(conf, jobs, health, state, base, out, disabled=True)
            assert clock[0] - base <= F.PASS_SOFT_S, (start, attempt, clock, requests, edits)
            assert out['moved'] == [('x', 'healthy/model@healthy:a')], (start, attempt, out, requests)
            assert [p for p, _, _ in requests] == ['source', 'slow', 'healthy']
            assert state['home']['x']['model'] == 'source/model'
            assert not jobs['x']['enabled'] and not out['problems'] and not out['no_target']


@offline
def test_disabled_discovery_bounds_real_slow_transports_including_saved_accounts():
    # Distinct transport proof: subprocesses really sleep, and only their supplied timeouts stop them.
    # The saved-store read is slow too; a declared subscription still lets the healthy fallback serve.
    with tempfile.TemporaryDirectory(prefix='ocf-probes-') as tmp:
        executable = pathlib.Path(tmp) / 'openclaw'
        executable.write_text('#!%s\n' % sys.executable + r'''
import json, sys, time
a = sys.argv[1:]
if a[:2] == ['cron', 'edit']:
    sys.exit(0)
if a[:2] == ['models', 'status']:
    time.sleep(3)
    print(json.dumps({'auth': {'oauth': {'profiles': []}}}))
else:
    assert a[:3] == ['gateway', 'call', 'models.probe'], a
    assert float(a[a.index('--timeout') + 1]) > 0
    provider = json.loads(a[a.index('--params') + 1])['provider']
    if provider != 'healthy':
        time.sleep(3)
    else:
        # A healthy provider needs real response time inside the RPC deadline, not just the outer process timeout.
        if float(a[a.index('--timeout') + 1]) < 50:
            print(json.dumps({'ok': False, 'error': 'timeout'}))
            sys.exit(0)
        time.sleep(0.05)
    print(json.dumps({'results': [{'profileId': provider + ':a',
                                    'status': 'ok' if provider == 'healthy' else 'rate_limit'}]}))
''')
        executable.chmod(0o755)
        conf = {'primary': 'source/model', 'chain': ['slow/model', 'healthy/model'],
                'order': {'source': ['source:a', 'source:b'], 'slow': ['slow:a']},
                'profiles': {'healthy:a': {'provider': 'healthy', 'mode': 'oauth'}}}
        jobs = {'x': {'enabled': False, 'mode': 'trusted', 'kind': 'cron', 'next': 0, 'name': 'x',
                      'model': 'source/model@source:a', 'profile': 'source:a'}}
        start = time.time()
        state = {'limited': {'source/model@source:a': ms() + 3600000}}
        out = {'moved': [], 'no_target': [], 'problems': [], 'later': 0, 'deferred': 0}
        with patch.dict(os.environ, PATH=tmp + os.pathsep + os.environ.get('PATH', '')), \
                patch.object(F, 'FAKE', None), patch.object(F, 'APPLY', True), \
                patch.object(F, 'run', _run), patch.object(F, '_CACHE', {}), \
                patch.object(F, 'T0', start - 0.5), patch.object(F, 'PASS_SOFT_S', 6), \
                patch.object(F, 'PASS_HARD_S', 30), patch.object(F, 'DISABLED_BUDGET_S', 1), \
                patch.object(F, 'default_agent', return_value='fixture'):
            health = F.Health(conf, state['limited'])
            F.jobs_pass(conf, jobs, health, state, F.T0, out, disabled=True)
            elapsed = time.time() - F.T0
            assert elapsed <= F.PASS_SOFT_S - F.DISABLED_BUDGET_S, elapsed
            assert out['moved'] == [('x', 'healthy/model@healthy:a')], out
            assert set(health.failed) == {'source', 'slow'} and health.saved_err == 'timeout'


@offline
def test_disabled_work_past_the_pass_stop_leaves_unasked_accounts_and_bindings_alone():
    conf = {'primary': OPUS, 'chain': [], 'order': {'anthropic': [PA, PB]}}
    # Both discovery and a move using already-known health must respect the stop.
    for cached in (False, True):
        jobs = {'x': {'enabled': False, 'mode': 'trusted', 'kind': 'cron', 'next': 0,
                      'model': OPUS + '@' + PA, 'profile': PA, 'name': 'x'}}
        state, out = {}, {'moved': [], 'no_target': [], 'problems': [], 'later': 0, 'deferred': 0}
        health = F.Health(conf)
        if cached:
            health.cache['anthropic'] = {PA: 'rate_limit', PB: 'ok'}
        def gateway(method, params, timeout=120):
            return {'results': [{'profileId': PA, 'status': 'rate_limit'}, {'profileId': PB, 'status': 'ok'}]}, ''
        with patch.object(F, 'FAKE', None), patch.object(F, 'APPLY', True), \
                patch.object(F, 'T0', 1000), patch.object(F.time, 'time', return_value=1201), \
                patch.object(F, 'default_agent', return_value='fixture'), \
                patch.object(F, 'gw', side_effect=gateway) as probe, \
                patch.object(F, 'run', return_value=(0, '', '')) as edit:
            F.jobs_pass(conf, jobs, health, state, 1000, out, disabled=True)
            assert not probe.called and not edit.called, (cached, probe.call_args_list, edit.call_args_list)
        assert jobs['x']['model'] == OPUS + '@' + PA and not out['moved']
        if not cached:
            assert 'anthropic' not in health.cache and not health.failed


def test_disabled_recurring_probe_failure_waits_and_later_pass_repairs_without_a_streak():
    # No enabled jobs or run history can seed discovery. Unknown source health is not an outage decision.
    d = setup({'x': (OPUS, PA, 'trusted')}, probe={'anthropic': {PA: 'timeout'}})
    c = sqlite3.connect(d / 'state.sqlite')
    c.execute('update cron_jobs set enabled=0')
    c.commit()
    c.close()
    rc, err, j = go(d, True)
    assert rc == 0 and not kinds(j, 'job_move') and binding(d, 'x') == OPUS + '@' + PA
    # Failed first fallback is independent: the next configured provider can still repair the binding.
    fake(d, probe={'anthropic': DEAD, 'xai': {'xai:x': 'timeout'}, 'openai': {OA: 'ok'}})
    rc, err, j = go(d, True)
    assert rc == 0 and binding(d, 'x') == 'openai/gpt-6.1-sol@' + OA, (err, j)


def test_disabled_discovery_does_not_spend_edit_budget_and_deferred_work_progresses():
    d = setup({'e': ('openai/gpt-6.1-sol', 'openai:default', 'trusted'),
               'x': (OPUS, PA, 'trusted'), 'y': (OPUS, PA, 'trusted')},
              probe={'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'},
                     'openai': {'openai:default': 'rate_limit', OA: 'ok'}})
    c = sqlite3.connect(d / 'state.sqlite')
    c.execute("update cron_jobs set enabled=0 where job_id in ('x','y')")
    c.commit()
    c.close()
    fake(d, slow_probe=0.04, slow_edit=0.04)
    rc, err, j = go(d, True, env={'OCF_DISABLED_BUDGET_S': '0.01'})
    assert rc == 0 and [m['job'] for m in kinds(j, 'job_move')] == ['e', 'x'], (err, j)
    assert binding(d, 'y') == OPUS + '@' + PA and not state_of(d)['retry']
    rc, err, j = go(d, True, env={'OCF_DISABLED_BUDGET_S': '0.01'})
    assert rc == 0 and [m['job'] for m in kinds(j, 'job_move')] == ['e', 'x', 'y'], (err, j)


def test_disabled_late_edit_fits_its_budget_and_uncertain_result_is_reconciled():
    # Less than 180 s remain after enabled work: a disabled edit can still fit its own 10 s allowance.
    # The timeout is uncertain: the Gateway applied the edit, and the next pass must adopt it once.
    d = setup({'x': (OPUS, PA, 'trusted')}, probe={'anthropic': DEAD, 'xai': {'xai:x': 'ok'}})
    c = sqlite3.connect(d / 'state.sqlite')
    c.execute('update cron_jobs set enabled=0')
    c.commit()
    c.close()
    fake(d, edit_result='rc=124 timeout')
    rc, err, j = go(d, True, env={'OCF_PASS_HARD_S': '100'})
    assert rc == 1 and binding(d, 'x') == 'xai/grok-4.7@xai:x', (err, j)
    st = state_of(d)
    assert st['unsure']['x']['tgt'] == binding(d, 'x') and 'rebind:x' in st['problems']
    assert 'x' not in st['home']
    fake(d, edit_result='ok')
    rc, err, j = go(d, env={'OCF_PASS_HARD_S': '100'})
    st = state_of(d)
    assert rc == 0 and not st['unsure'] and 'rebind:x' not in st['problems'], (err, st)
    assert st['home']['x']['model'] == OPUS and st['home']['x']['parked'] == binding(d, 'x')
    assert [m['result'] for m in kinds(j, 'job_move')] == ['rc=124 timeout', 'ok']
    # No room for even the bounded edit plus the existing safety margin: leave the job and state unparked.
    d = setup({'x': (OPUS, PA, 'trusted')}, probe={'anthropic': DEAD, 'xai': {'xai:x': 'ok'}})
    c = sqlite3.connect(d / 'state.sqlite')
    c.execute('update cron_jobs set enabled=0')
    c.commit()
    c.close()
    rc, err, j = go(d, True, env={'OCF_PASS_HARD_S': '15'})
    assert rc == 0 and binding(d, 'x') == OPUS + '@' + PA and not kinds(j, 'job_move'), (err, j)
    assert not state_of(d)['home'] and not state_of(d)['unsure']


@offline
def test_cron_edit_bounds_a_stalled_cli_call_by_the_supplied_timeout():
    # Exercise the real subprocess timeout, independently of the fixture edit backend.
    with tempfile.TemporaryDirectory(prefix='ocf-edit-') as tmp:
        executable = pathlib.Path(tmp) / 'openclaw'
        executable.write_text('#!%s\nimport time\ntime.sleep(1)\n' % sys.executable)
        executable.chmod(0o755)
        with patch.dict(os.environ, PATH=tmp + os.pathsep + os.environ.get('PATH', '')), \
                patch.object(F, 'FAKE', None), patch.object(F, 'APPLY', True):
            assert F.cron_edit('x', OPUS + '@' + PB, timeout=0.01) == 'rc=124 timeout'


def test_stalled_subagent_run_is_aborted_not_moved_or_continued():
    # A subagent sleeping out a 2-day retry: its parent must get the failure now. Not continued, not moved.
    S = 'agent:main:subagent:00000000-0000-4000-8000-000000000009'
    d = setup(log=[req(S, 'R9'), stall(OPUS, 'R9', key=S, delay=211552000)],
              probe={'anthropic': {PB: 'rate_limit', PA: 'rate_limit', PC: 'rate_limit'}},
              sessions={S: {'key': S, 'hasActiveRun': True, 'lastRunId': 'R9'}},
              entries={S: {'providerOverride': 'anthropic', 'modelOverride': 'claude-opus-5-5', 'authProfileOverride': PA}})
    rc, err, j = go(d, True)
    held = json.loads((d / 'st' / 'state.json').read_text())['limited']
    assert OPUS + '@' + PA in held, held                                # its route stays out until the provider's time
    a, = kinds(j, 'subagent_stall_abort')
    assert a['session'] == S and a['result'] == 'ok', (err, j)
    assert not kinds(j, 'chat_recover') and not kinds(j, 'chat_move'), j
    assert len(kinds(go(d, True)[2], 'subagent_stall_abort')) == 1      # handled once (the journal accumulates)


RT_OK = {'rc': 0, 'out': json.dumps({'ok': True, 'capability': 'model.run', 'transport': 'local', 'provider': 'anthropic',
                                      'outputs': [{'text': 'ok', 'mediaUrl': None}]})}
RT_LIMIT = {'rc': 1, 'out': json.dumps({'ok': False, 'error': {'type': 'cli_error', 'message': 'No text output returned for '
            'provider "anthropic" model "claude-opus-5-5": 429 {"type":"error","error":{"type":"rate_limit_error","message":'
            '"This request would exceed your account\'s rate limit. Please try again later."}}; Retry-After: 205922 seconds.'}}),
            'err': '[provider-error] Model request failed provider=anthropic model=claude-opus-5-5 status=429\n'}


def held_fixture(holds, retest):
    """Job 'a' parked on grok, its own model Opus held on the given accounts for 20 h; every probe says ok."""
    d = setup({'a': ('xai/grok-4.7', 'xai:x', 'trusted')}, [], {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}, 'xai': {'xai:x': 'ok'}})
    later = ms() + 20 * 3600_000
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text(json.dumps({'started': 0, 'home': {'a': {'model': OPUS, 'parked': 'xai/grok-4.7@xai:x',
                                                                                 'at': 0, 'wait': 0}},
                                                      'limited': {OPUS + '@' + p: later for p in holds}}))
    fake(d, retest=retest)
    return d, later


def state_of(d):
    return json.loads((d / 'st' / 'state.json').read_text())


def test_a_long_hold_is_cleared_when_a_real_request_of_that_model_on_that_account_answers():
    # The reported case: the provider reset its limit early. The probe (another model) cannot see it; a real request
    # of the held model on that account answers, so the hold goes and the next pass brings the parked job home.
    d, _ = held_fixture((PB, PA, PC), {OPUS + '@' + PB: RT_OK})
    rc, err, j = go(d, True)
    c, = kinds(j, 'hold_cleared')
    assert c['key'] == OPUS + '@' + PB and 'answered' in c['why'], c
    e, = kinds(j, 'hold_retest')                                       # PA: no answer -> kept, no problem
    assert e['key'] == OPUS + '@' + PA and e['result'] == 'error' and rc == 0, (e, err)
    s = state_of(d)
    assert OPUS + '@' + PB not in s['limited'] and OPUS + '@' + PA in s['limited'] and s['retry'], s
    assert binding(d, 'a') == 'xai/grok-4.7@xai:x'                     # moved by the next pass, not this one
    go(d)
    assert binding(d, 'a') == OPUS + '@' + PB


def test_a_re_test_that_hits_the_limit_keeps_the_hold_at_the_newly_named_time():
    d, later = held_fixture((PB,), {OPUS + '@' + PB: RT_LIMIT})
    rc, err, j = go(d, True)
    r, = kinds(j, 'hold_retest')
    assert r['result'] == 'limit' and 'rate_limit_error' in r['error'] and not kinds(j, 'hold_cleared'), r
    until = state_of(d)['limited'][OPUS + '@' + PB]
    assert abs(until - (ms() + 205922_000)) < 120_000 and until != later, until   # the provider's new time
    assert rc == 0, err
    # A limit that names no time keeps the old one; an error keeps the hold and is no problem the first time.
    for answer in ({'rc': 1, 'err': 'rate_limit_error'}, {'rc': 1, 'err': 'gateway timeout'}):
        d, later = held_fixture((PB,), {OPUS + '@' + PB: answer})
        rc, err, j = go(d, True)
        assert state_of(d)['limited'][OPUS + '@' + PB] == later and not kinds(j, 'hold_cleared'), j
        assert rc == 0 and not kinds(j, 'problem'), (err, j)


def test_holds_are_re_tested_at_most_hourly_a_few_per_pass_and_only_when_long():
    d, _ = held_fixture((PB, PA, PC), {})
    s = state_of(d)
    s['limited'][FABLE + '@' + PB] = ms() + 30 * 60_000                # under an hour left: waits out its own time
    (d / 'st' / 'state.json').write_text(json.dumps(s))
    n = lambda: sum(1 for x in open(d / 'calls.log') if x.startswith('infer model run'))
    j = go(d, True)[2]
    first = {r['key'] for r in kinds(j, 'hold_retest')}
    assert len(first) == F.RETEST_PER_PASS == 2 and n() == 2, first
    j = go(d, True)[2]
    assert {r['key'] for r in kinds(j, 'hold_retest')} - first == {OPUS + '@' + PC} and n() == 3, j
    go(d, True)
    assert n() == 3                                                    # each tested within the hour: none again
    s = state_of(d)
    assert set(s['retested']) == {OPUS + '@' + p for p in (PA, PB, PC)}, s['retested']
    s['retested'][OPUS + '@' + PA] -= F.RETEST_EVERY_MS
    (d / 'st' / 'state.json').write_text(json.dumps(s))
    j = go(d, True)[2]
    assert n() == 4 and kinds(j, 'hold_retest')[-1]['key'] == OPUS + '@' + PA, j
    assert all(r['in_a_row'] < F.RETEST_ERRORS for r in kinds(j, 'hold_retest')) and not kinds(j, 'problem'), j


# Per-model account preference (settings file): the owner's split is Opus on A, then B, then C; Fable on A, then C, then
# B. Accounts here: PA, PB, PC; auth.order in the fixture config is [PB, PA, PC].
PREFER = {'prefer': {OPUS: [PA, PB, PC], FABLE: [PA, PC, PB]}}
ALL_OK = {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}}


def prefer(d, settings=PREFER, state=None):
    (d / 'settings.json').write_text(json.dumps(settings))
    if state is not None:
        (d / 'st').mkdir(exist_ok=True)
        (d / 'st' / 'state.json').write_text(json.dumps(dict({'started': 0}, **state)))


def go_dry(d):
    env = dict(os.environ, OCF_STATE=str(d / 'st'), OCF_STATE_DB='file:%s?mode=ro' % (d / 'state.sqlite'),
               OCF_FAKE_DB=str(d / 'state.sqlite'), OCF_CONFIG=str(d / 'cfg.json'), OCF_LOG_DIR=str(d / 'logs'),
               OCF_FAKE=str(d / 'fake.json'), OCF_DIST=str(d / 'pkg' / 'dist'), OCF_OPENCLAW_DIR=str(d / 'oc'),
               OCF_FAKE_CALLS=str(d / 'calls.log'), OCF_SETTINGS=str(d / 'settings.json'),
               PATH=str(d / 'bin') + os.pathsep + os.environ.get('PATH', ''))
    p = subprocess.run([sys.executable, str(HERE / os.environ.get('OCF_SCRIPT', 'failover.py'))], env=env,
                       capture_output=True, text=True)
    assert p.returncode == 0, p.stderr
    return json.loads(p.stdout)


def requests(d):
    f = d / 'calls.log'
    return [x.split()[5] for x in open(f) if x.startswith('infer model run')] if f.exists() else []


def order(d):
    return json.loads((d / 'cfg.json').read_text())['auth']['order']['anthropic']


def test_stored_order_owns_targets_and_supplies_the_model_preference():
    person = dict(PIN, modelOverride=OPUS.split('/')[1], authProfileOverride=PA)
    d = setup({'j': (OPUS, PA, 'trusted')}, [], ALL_OK, entries={K: person})
    fake(d, saved_orders={'main': {'anthropic': [PC, PA]}}, retest={OPUS + '@' + PC: RT_OK})
    # Config names PB first. The saved override excludes PB and names PC first;
    # no manual settings copy is needed for a preferred-account return.
    rc, err, j = go(d, True)
    assert binding(d, 'j') == OPUS + '@' + PC, (err, j)
    assert kinds(j, 'prefer_order')[0]['order'] == [PC, PA], j
    assert order(d) == [PB, PA, PC] and not kinds(j, 'order_set'), j
    assert not kinds(j, 'chat_move') and json.loads((d / 'fake.json').read_text())['entries'][K] == person, j
    assert rc == 0, err
    st = subprocess.run([sys.executable, str(HERE / 'failover.py'), '--status'], capture_output=True, text=True,
                        env=dict(os.environ, OCF_STATE=str(d / 'st'), OCF_SETTINGS=str(d / 'settings.json')))
    status = json.loads(st.stdout)
    assert status['prefer'][OPUS] == [PC, PA] and status['prefer_settings'] == {}, status


def test_stored_order_limits_preferences_and_never_enables_an_unlisted_api_key():
    for saved, want in (([PA, PC], PC), ([PA, 'anthropic:k', PC], 'anthropic:k')):
        d = setup({'j': (OPUS, PA, 'trusted')}, [],
                  {'anthropic': {PA: 'rate_limit', PB: 'ok', PC: 'ok', 'anthropic:k': 'ok'}})
        prefer(d, {'prefer': {OPUS: [PB, 'anthropic:k', PC, PA]}})
        fake(d, saved_orders={'main': {'anthropic': saved}})
        rc, err, j = go(d, True)
        assert binding(d, 'j') == OPUS + '@' + want and rc == 0, (err, j)
        assert order(d) == [PB, PA, PC] and not kinds(j, 'order_set'), j


def test_empty_absent_and_malformed_persisted_orders_fall_back_to_config():
    for raw in (None, [], 'bad', {}, [None, 7, '  ']):
        d = setup({'j': (OPUS, PA, 'trusted')}, [], {'anthropic': {PA: 'rate_limit', PB: 'ok', PC: 'ok'}})
        fake(d, saved_orders={'main': {'anthropic': raw}})
        rc, err, j = go(d, True)
        assert binding(d, 'j') == OPUS + '@' + PB and rc == 0, (raw, err, j)
        assert not kinds(j, 'prefer_order') and not kinds(j, 'order_set'), (raw, j)
    # A normalized empty saved order does not erase the existing empty-config
    # subscription fallback, nor does it admit an undeclared API-key profile.
    d = setup({'j': (OPUS, PA, 'trusted')}, [], {'anthropic': {PA: 'rate_limit', PC: 'ok', 'anthropic:k': 'ok'}})
    c = json.loads((d / 'cfg.json').read_text())
    c['auth'] = {'order': {'anthropic': []}, 'profiles': SUBS}
    (d / 'cfg.json').write_text(json.dumps(c))
    fake(d, saved_orders={'main': {'anthropic': []}})
    assert binding_after_pass(d, 'j') == OPUS + '@' + PC


def binding_after_pass(d, jid):
    rc, err, j = go(d, True)
    assert rc == 0, (err, j)
    return binding(d, jid)


def test_order_read_is_scoped_to_the_selected_agent_and_reloaded_each_pass():
    d = setup({'j': (OPUS, PA, 'trusted')}, [], {'anthropic': {PA: 'rate_limit', PB: 'ok', PC: 'ok'}})
    c = json.loads((d / 'cfg.json').read_text())
    c['agents']['list'] = [{'id': 'main'}, {'id': 'worker', 'default': True}]
    (d / 'cfg.json').write_text(json.dumps(c))
    fake(d, saved_orders={'main': {'anthropic': [PB]}, 'worker': {'anthropic': [PC]}})
    assert binding_after_pass(d, 'j') == OPUS + '@' + PC
    assert not any(x.startswith('models auth') for x in calls(d)), calls(d)
    fake(d, saved_orders={'worker': {'anthropic': [PB]}}, probe={'anthropic': {PC: 'rate_limit', PB: 'ok'}})
    assert binding_after_pass(d, 'j') == OPUS + '@' + PB


def test_stored_order_covers_models_named_only_by_jobs_chats_or_parked_homes():
    for surface in ('job', 'chat', 'home'):
        d = setup({'j': (OPUS if surface == 'job' else SOL, PA if surface == 'job' else OA, 'trusted')}
                  if surface != 'chat' else {}, probe={'anthropic': {PA: 'rate_limit', PB: 'ok', PC: 'ok'}},
                  entries={K: dict(PIN, authProfileOverride=PA)} if surface == 'chat' else {})
        c = json.loads((d / 'cfg.json').read_text())
        c['agents']['defaults']['model'] = {'primary': SOL, 'fallbacks': []}
        c['auth'] = {'order': {'openai': [OA]}}
        (d / 'cfg.json').write_text(json.dumps(c))
        if surface == 'home':
            prefer(d, {}, state={'home': {'j': {'model': OPUS, 'parked': SOL + '@' + OA, 'at': 0, 'wait': 0}}})
        fake(d, saved_orders={'main': {'anthropic': [PA, PC]}}, models_status=models_status([(PB, 'oauth'), (PC, 'token')]))
        rc, err, j = go(d, True)
        moved = kinds(j, 'chat_move' if surface == 'chat' else 'job_move')
        assert rc == 0 and moved and moved[-1]['to'] == (FABLE if surface == 'chat' else OPUS) + '@' + PC, (surface, err, j)


def test_unavailable_order_metadata_blocks_mutation_and_never_selects_an_unlisted_api_key():
    for broken in ('bad-json', 'wrong-owner', 'wrong-provider', 'wrong-table', 'missing-owner-table'):
        d = setup({'j': (OPUS, PA, 'trusted')}, [], {'anthropic': {PA: 'rate_limit', PB: 'ok', PC: 'ok'}})
        c = json.loads((d / 'cfg.json').read_text())
        c['auth']['order']['anthropic'] = ['anthropic:k', PB]
        (d / 'cfg.json').write_text(json.dumps(c))
        fake(d, probe={'anthropic': {PA: 'rate_limit', PB: 'ok', PC: 'ok', 'anthropic:k': 'ok'}},
             saved_orders={'main': {'anthropic': [PC, PA]}})
        with sqlite3.connect(d / 'oc/agents/main/agent/openclaw-agent.sqlite') as db:
            if broken == 'bad-json':
                db.execute('update auth_profile_state set state_json=?', ('broken ' + SECRET,))
            elif broken == 'wrong-provider':
                db.execute('update auth_profile_state set state_json=?', (json.dumps({'order': {'anthropic': [OA]}}),))
            elif broken == 'wrong-table':
                db.execute('drop table auth_profile_state')
                db.execute('create view auth_profile_state as select 1 as state_key, 2 as state_json')
        if broken == 'wrong-owner':
            fake(d, saved_orders={})
            with sqlite3.connect(d / 'state.sqlite') as db:
                db.execute('update config_machine_state set value_json=?', (json.dumps({'location': SECRET}),))
        elif broken == 'missing-owner-table':
            with sqlite3.connect(d / 'state.sqlite') as db:
                db.execute('drop table config_machine_state')
        rc, err, j = go(d, False, '--check')
        assert rc == 1 and 'saved auth order' in err and SECRET not in err, (err, j)
        rc, err, j = go(d, True)
        assert rc == 1 and binding(d, 'j') == OPUS + '@' + PA and not kinds(j, 'order_set'), (err, j)
        assert SECRET not in err + (d / 'st' / 'journal.jsonl').read_text(), err


def test_online_order_reader_uses_committed_wal_metadata_while_auth_cli_is_offline_only():
    d = setup({'j': (OPUS, PA, 'trusted')}, [], {'anthropic': {PA: 'rate_limit', PB: 'ok', PC: 'ok'}})
    fake(d, saved_orders={'main': {'anthropic': [PB, PA]}})
    path = d / 'oc/agents/main/agent/openclaw-agent.sqlite'
    with sqlite3.connect(path) as writer:
        writer.execute('pragma journal_mode=wal')
        writer.execute('update auth_profile_state set state_json=?', (json.dumps({'order': {'anthropic': [PC, PA]}}),))
        writer.commit()                      # the committed order is in WAL, beyond the main database image
        writer.execute('begin immediate')
        writer.execute('update auth_profile_state set state_json=?', (json.dumps({'order': {'anthropic': [PB]}}),))
        # A concurrent online reader must see committed PC, not an uncommitted
        # operator edit, an offline admission error, or the config's unlisted PB.
        rc, err, j = go(d, True)
        assert rc == 0 and binding(d, 'j') == OPUS + '@' + PC, (err, j)
        assert not any(x.startswith('models auth') for x in calls(d)), calls(d)
        writer.rollback()


@offline
def test_explicit_saved_order_cannot_be_replaced_by_stale_settings():
    conf = {'primary': OPUS, 'chain': [], 'allowed': {}, 'profiles': {}, 'order': {'anthropic': [PB, PA, PC]},
            'prefer_static': {OPUS: [PB, PA, PC]}, 'settings_err': None}
    # Isolate precedence from transport; usage behavior is exercised end to end.
    with patch.object(F, 'stored_order', return_value=[PC, PA]), \
            patch.object(F, 'load_usage', return_value=({}, None)):
        result = F.with_stored_orders(conf)
    assert result['prefer'][OPUS] == [PC, PA] and result['prefer_from'][OPUS] == 'stored', result
    assert conf['prefer_static'][OPUS] == [PB, PA, PC], conf


@offline
def test_order_metadata_uses_native_shared_owner_local_precedence_and_configured_agent_directory():
    d = setup()
    fake(d, saved_orders={'main': {'anthropic': [PB]}, 'worker': {'anthropic': [PC]}})
    custom = d / 'custom-agent'
    custom.mkdir()
    with sqlite3.connect(custom / 'openclaw-agent.sqlite') as db:
        db.execute('create table auth_profile_state(state_key primary key, state_json)')
        db.execute('insert into auth_profile_state values(?,?)', ('primary', json.dumps({'order': {'anthropic': [PA]}})))
    c = json.loads((d / 'cfg.json').read_text())
    c['agents']['list'] = [{'id': 'worker', 'default': True, 'agentDir': ' ' + str(custom) + ' '}]
    (d / 'cfg.json').write_text(json.dumps(c))
    with sqlite3.connect(d / 'state.sqlite') as db:
        db.execute('insert into config_machine_state values(?,?)', ('authProfiles.state', json.dumps({'order': {'anthropic': [PC]}})))
    with patch.object(F, 'CONFIG', str(d / 'cfg.json')), patch.object(F, 'OC_DIR', d / 'oc'), \
            patch.object(F, '_CACHE', {}), patch.dict(os.environ, {'OCF_STATE_DB': 'file:%s?mode=ro' % (d / 'state.sqlite')}):
        assert F.stored_order('anthropic') == [PA]                    # configured directory, not worker's default path
        with sqlite3.connect(custom / 'openclaw-agent.sqlite') as db:
            db.execute('update auth_profile_state set state_json=?', ('{"order":{"anthropic":[]}}',))
        F._CACHE.clear()
        assert F.stored_order('anthropic') == [PC]                    # empty local list inherits current shared state
        with sqlite3.connect(d / 'state.sqlite') as db:
            db.execute('update config_machine_state set value_json=? where state_key=?',
                       ('{"location":"legacy-main"}', 'auth.sharedStore'))
        F._CACHE.clear()
        assert F.stored_order('anthropic') == [PB]                    # native legacy owner, ignoring relocated state


@offline
def test_order_reader_never_reads_credential_cells_or_changes_database_bytes():
    d = setup()
    fake(d, saved_orders={'main': {'anthropic': [PC, PA]}})
    path = d / 'oc/agents/main/agent/openclaw-agent.sqlite'
    with sqlite3.connect(path) as db:
        db.execute('create table auth_profile_store(store_key primary key, store_json)')
        db.execute('insert into auth_profile_store values(?,?)', ('primary', SECRET))
    before = path.read_bytes()
    native_connect = sqlite3.connect
    def guarded_connect(db_uri, **kw):
        assert kw.get('uri') and 'mode=ro' in db_uri, db_uri
        db = native_connect(db_uri, **kw)
        db.set_authorizer(lambda action, table, *_: sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_READ
                          and table == 'auth_profile_store' else sqlite3.SQLITE_OK)
        return db
    with patch.object(F, 'CONFIG', str(d / 'cfg.json')), patch.object(F, 'OC_DIR', d / 'oc'), \
            patch.object(F, '_CACHE', {}), patch.object(F.sqlite3, 'connect', side_effect=guarded_connect), \
            patch.dict(os.environ, {'OCF_STATE_DB': 'file:%s?mode=ro' % (d / 'state.sqlite')}):
        assert F.stored_order('anthropic') == [PC, PA]
    assert path.read_bytes() == before


def test_clearing_saved_order_restores_the_existing_config_compare_and_set_writer():
    d = setup({'j': (OPUS, PB, 'trusted')}, [], ALL_OK)
    prefer(d)
    fake(d, saved_orders={'main': {'anthropic': [PA, PB, PC]}}, retest={OPUS + '@' + PA: RT_OK})
    j = go(d, True)[2]
    assert binding(d, 'j') == OPUS + '@' + PA and not kinds(j, 'order_set') and order(d) == [PB, PA, PC], j
    fake(d, saved_orders={})
    j = go(d, True)[2]
    assert order(d) == [PA, PB, PC] and kinds(j, 'order_set')[-1]['result'] == 'ok', j
    writes = [x for x in calls(d) if x.startswith('config set auth.order.anthropic')]
    assert len(writes) == 1 and '--expect-current-json' in writes[0], writes


def test_a_move_off_a_held_account_goes_to_the_most_preferred_live_one():
    # Fable is out on PA. auth.order would pick PB next; the preference puts Fable on PC before PB.
    d = setup(probe=ALL_OK, entries={K: dict(PIN, authProfileOverride=PA)})
    prefer(d, state={'limited': {FABLE + '@' + PA: ms() + 5 * 86400_000}})
    m, = kinds(go(d, True)[2], 'chat_move')
    assert (m['frm'], m['to'], m['why']) == (FABLE + '@' + PA, FABLE + '@' + PC, 'held'), m


def test_a_job_returns_to_its_preferred_account_once_a_real_request_there_answers():
    # The job was moved to PB while Opus was held on PA. The hold ran out (the weekly reset came): the first pass
    # proves Opus answers on PA with one real request and moves the job back; no second request within the hour.
    d = setup({'a': (OPUS, PB, 'trusted'), 'b': (OPUS, PA, 'trusted')}, [], ALL_OK)
    prefer(d, state={'limited': {OPUS + '@' + PA: ms() - 1000}})
    fake(d, retest={OPUS + '@' + PA: RT_OK})
    rc, err, j = go(d)
    c, = kinds(j, 'return_check')
    m, = kinds(j, 'job_move')
    assert c['key'] == OPUS + '@' + PA and c['result'] == 'ok', c
    assert (m['job'], m['frm'], m['to'], m['why']) == ('a', OPUS + '@' + PB, OPUS + '@' + PA, 'preference'), m
    assert binding(d, 'a') == OPUS + '@' + PA and binding(d, 'b') == OPUS + '@' + PA and rc == 0, err
    assert len(kinds(go(d, True)[2], 'job_move')) == 1 and requests(d) == [OPUS + '@' + PA]


def test_no_return_while_the_preferred_account_is_held_or_does_not_answer():
    # Held there (a weekly limit): no request, no move. Its re-test is the hold logic's, not the return's.
    d = setup({'a': (OPUS, PB, 'trusted')}, [], ALL_OK)
    prefer(d, state={'limited': {OPUS + '@' + PA: ms() + 40 * 60_000}})
    j = go(d, True)[2]
    assert binding(d, 'a') == OPUS + '@' + PB and not kinds(j, 'job_move') and not requests(d), j
    # The request hits the limit: held until the provider's time, the job stays, and later passes ask nothing.
    d = setup({'a': (OPUS, PB, 'trusted')}, [], ALL_OK)
    prefer(d)
    fake(d, retest={OPUS + '@' + PA: RT_LIMIT})
    rc, err, j = go(d, True)
    c, = kinds(j, 'return_check')
    assert c['result'] == 'limit' and binding(d, 'a') == OPUS + '@' + PB and rc == 0, (c, err)
    assert abs(state_of(d)['limited'][OPUS + '@' + PA] - (ms() + 205922_000)) < 120_000
    go(d, True)
    assert requests(d) == [OPUS + '@' + PA] and binding(d, 'a') == OPUS + '@' + PB
    # No answer at all (a timeout): no move, no problem, asked again only after an hour.
    d = setup({'a': (OPUS, PB, 'trusted')}, [], ALL_OK)
    prefer(d)
    rc, err, j = go(d, True)
    assert kinds(j, 'return_check')[0]['result'] == 'error' and binding(d, 'a') == OPUS + '@' + PB and rc == 0, err
    go(d, True)
    assert len(requests(d)) == 1
    s = state_of(d)
    s['verify_tried'][OPUS + '@' + PA] -= F.RETEST_EVERY_MS
    (d / 'st' / 'state.json').write_text(json.dumps(s))
    fake(d, retest={OPUS + '@' + PA: RT_OK})
    go(d, True)
    assert len(requests(d)) == 2 and binding(d, 'a') == OPUS + '@' + PA


def test_without_a_preference_nothing_returns_and_auth_order_is_left_alone():
    d = setup({'a': (OPUS, PB, 'trusted')}, [], ALL_OK, entries={K: dict(PIN, authProfileOverrideSource='auto')})
    prefer(d, {'prefer': {'anthropic/claude-sonnet-5-5': [PA]}})
    j = go(d, True)[2]
    assert not kinds(j, 'job_move') and not kinds(j, 'chat_move') and not requests(d) and order(d) == [PB, PA, PC], j


def test_chats_return_only_where_no_person_chose_the_account():
    # Opus on PB everywhere, PA preferred and answering. The chat this script moved, the one the operator adopted and
    # the one parked on a fallback model move. Left alone: the chat a person pinned, the ones OpenClaw placed itself
    # (auto), a chat whose person changed the account after our move, and one replaced by a new session under its
    # key. A chat with a run in progress waits.
    opus_b = {'modelOverrideSource': 'default', 'authProfileOverride': PB, 'authProfileOverrideSource': 'user'}
    keys = {n: 'agent:main:telegram:group:-100%d' % i for i, n in enumerate(
        ('person', 'ours', 'adopted', 'auto', 'parked', 'busy', 'changed', 'auto_but_model_picked', 'new_session'))}
    entries = {k: dict(opus_b, sessionId='s-' + k[-1]) for k in keys.values()}
    entries[keys['new_session']]['sessionId'] = 'replaced'
    entries[keys['auto']]['authProfileOverrideSource'] = 'auto'
    entries[keys['auto_but_model_picked']].update(providerOverride='anthropic', modelOverride='claude-opus-5-5',
                                                  modelOverrideSource='user', authProfileOverrideSource='auto')
    entries[keys['changed']]['authProfileOverride'] = PC
    sessions = {keys['busy']: {'hasActiveRun': True}}
    d = setup(probe=ALL_OK, entries=entries, sessions=sessions)
    ob = OPUS + '@' + PB
    mine = lambda n: {'ref': ob, 'session': 's-' + keys[n][-1]}
    prefer(d, state={'owned': {keys[n_]: mine(n_) for n_ in ('ours', 'busy', 'changed', 'new_session')},
                     'chat_home': {keys['parked']: {'model': FABLE, 'parked': ob}},
                     'limited': {FABLE + '@' + p: ms() + 30 * 60_000 for p in (PA, PB, PC)}})
    fake(d, retest={OPUS + '@' + PA: RT_OK})
    rc, err, j = go(d, False, '--adopt', keys['adopted'])
    assert rc == 0 and kinds(j, 'chat_adopt')[0]['ref'] == ob, (err, j)
    rc, err, j = go(d, True)
    moved = {m['session']: m for m in kinds(j, 'chat_move')}
    assert set(moved) == {keys[n] for n in ('ours', 'adopted', 'parked')}, moved
    assert all(m['to'] == OPUS + '@' + PA and m['why'] == 'preference' for m in moved.values()), moved
    s = state_of(d)
    assert s['chat_home'][keys['parked']]['parked'] == OPUS + '@' + PA and s['chat_home'][keys['parked']]['model'] == FABLE
    assert set(s['owned']) == {keys[n] for n in ('ours', 'adopted', 'parked', 'busy')}, s['owned']
    assert s['owned'][keys['adopted']] == {'ref': OPUS + '@' + PA, 'session': 's-' + keys['adopted'][-1]}, s['owned']
    assert len(requests(d)) == 1 and rc == 0, err
    # The busy chat goes once its run is over; nothing moves twice.
    fake(d, sessions={k: {'key': k, 'hasActiveRun': False} for k in keys.values()})
    j = go(d, True)[2]
    assert [m['session'] for m in kinds(j, 'chat_move')][3:] == [keys['busy']], j


def test_preference_moves_are_rate_limited_per_item():
    d = setup({'a': (OPUS, PB, 'trusted')}, [], ALL_OK)
    prefer(d, state={'verified': {OPUS + '@' + PA: ms()}, 'pref_moved': {'a': ms() - 30 * 60_000}})
    assert not kinds(go(d, True)[2], 'job_move')                       # moved by the preference half an hour ago
    s = state_of(d)
    s['pref_moved']['a'] -= F.RETURN_EVERY_MS
    (d / 'st' / 'state.json').write_text(json.dumps(s))
    m, = kinds(go(d, True)[2], 'job_move')
    assert m['why'] == 'preference' and not requests(d), m             # proof less than an hour old: no new request


def test_auth_order_follows_the_preferred_live_account_of_the_most_used_models():
    # The reported state: Opus work on PA, Fable out on PA and PC, auth.order still [PB, PA, PC]. New unpinned work
    # must start on PA: Opus (2 jobs + the primary) votes PA, Fable (1 pinned chat) votes PB.
    fable_b = dict(PIN, authProfileOverride=PB)
    d = setup({'a': (OPUS, PA, 'trusted'), 'b': (OPUS, PA, 'trusted')}, [], ALL_OK, entries={K: fable_b})
    prefer(d, state={'limited': {FABLE + '@' + PA: ms() + 5 * 86400_000, FABLE + '@' + PC: ms() + 86400_000}})
    out = go_dry(d)
    assert out['order'] == [['anthropic', [PA, PB, PC]]] and order(d) == [PB, PA, PC], out   # a dry run only says so
    rc, err, j = go(d, True)
    o, = kinds(j, 'order_set')
    assert (o['frm'], o['to'], o['result'], o['votes']) == ([PB, PA, PC], [PA, PB, PC], 'ok', {PA: 3, PB: 1}), o
    assert order(d) == [PA, PB, PC] and rc == 0, err
    assert len(kinds(go(d, True)[2], 'order_set')) == 1                # already first: nothing to write
    # Opus is then held on PA: it votes PB, which wins. Not before ORDER_EVERY_MS has passed since the last write.
    s = state_of(d)
    s['limited'][OPUS + '@' + PA] = ms() + 86400_000
    (d / 'st' / 'state.json').write_text(json.dumps(s))
    assert len(kinds(go(d, True)[2], 'order_set')) == 1 and order(d) == [PA, PB, PC]
    s = state_of(d)
    s['order_set']['anthropic'] -= F.ORDER_EVERY_MS
    (d / 'st' / 'state.json').write_text(json.dumps(s))
    o = kinds(go(d, True)[2], 'order_set')[-1]
    assert o['to'] == [PB, PA, PC] and order(d) == [PB, PA, PC], o


def test_order_compare_and_set_never_overwrites_a_newer_order():
    d = setup({'a': (OPUS, PA, 'trusted')}, [], ALL_OK)
    prefer(d)
    wrapper = d / 'bin' / 'openclaw'                                   # the order changes between read and write
    wrapper.write_text(wrapper.read_text().replace("elif a[:2] == ['config', 'set']:\n", "elif a[:2] == ['config', 'set']:\n"
        "    c0 = json.load(open(os.environ['OCF_CONFIG'])); c0['auth']['order']['anthropic'] = "
        "['anthropic:c', 'anthropic:b', 'anthropic:a']; json.dump(c0, open(os.environ['OCF_CONFIG'], 'w'))\n"))
    rc, err, j = go(d, True)
    o, = kinds(j, 'order_set')
    assert o['result'] == F.ORDER_CHANGED and order(d) == [PC, PB, PA], o
    assert rc == 0 and not kinds(j, 'problem'), err                   # a lost race is no problem: next pass decides
    wrapper.write_text(wrapper.read_text().replace("elif a[:2] == ['config', 'set']:\n", "elif a[:2] == ['config', 'set']:\n"
        "    sys.stderr.write('error: unknown option --expect-current-json\\n'); sys.exit(1)\n"))
    s = state_of(d)
    s['order_set']['anthropic'] -= F.ORDER_EVERY_MS
    (d / 'st' / 'state.json').write_text(json.dumps(s))
    rc, err, j = go(d, True)
    assert rc == 1 and 'auth.order.anthropic could not be set' in err and order(d) == [PC, PB, PA], err


def test_a_broken_settings_file_is_a_problem_and_prefers_nothing():
    for bad in ('{not json', json.dumps({'prefer': {OPUS: ['openai:a']}}), json.dumps({'prefer': {OPUS: []}})):
        d = setup({'a': (OPUS, PB, 'trusted')}, [], ALL_OK)
        (d / 'settings.json').write_text(bad)
        rc, err, j = go(d, True)
        assert rc == 1 and 'settings' in err and not kinds(j, 'job_move') and order(d) == [PB, PA, PC], (bad, err)


def test_dry_run_makes_no_request_and_no_edit():
    d = setup({'a': (OPUS, PB, 'trusted')}, [], ALL_OK)
    prefer(d)
    fake(d, retest={OPUS + '@' + PA: RT_OK})
    out = go_dry(d)
    assert out['verify_due'] == [OPUS + '@' + PA] and not out['moved'] and not requests(d), out
    assert binding(d, 'a') == OPUS + '@' + PB


def test_disabled_recurring_dry_run_plans_binding_without_writing_state_or_dispatching():
    d = setup({'x': (OPUS, PA, 'trusted')}, probe={'anthropic': DEAD, 'xai': {'xai:x': 'ok'}})
    c = sqlite3.connect(d / 'state.sqlite')
    c.execute('update cron_jobs set enabled=0')
    c.commit()
    c.close()
    (d / 'st').mkdir()
    st = d / 'st' / 'state.json'
    st.write_text(json.dumps({'started': 0, 'probed_at': 0}))
    before = {p: p.read_bytes() for p in (st, d / 'state.sqlite', d / 'cfg.json')}
    out = go_dry(d)
    assert out['moved'] == [['x', 'xai/grok-4.7@xai:x']], out
    assert all(p.read_bytes() == data for p, data in before.items())
    assert not (d / 'st' / 'journal.jsonl').exists() and not requests(d)
    calls = (d / 'calls.log').read_text().splitlines()
    assert not any(call.startswith(('cron run ', 'cron edit ', 'agent ', 'config set ')) for call in calls)


def test_disabled_jobs_return_only_on_proof_the_pass_already_has():
    d = setup({'a': (OPUS, PB, 'trusted'), 'b': (OPUS, PA, 'trusted')}, [], ALL_OK)   # b: the pass asks anthropic
    c = sqlite3.connect(d / 'state.sqlite')
    c.execute("update cron_jobs set enabled=0 where job_id='a'")
    c.commit()
    c.close()
    prefer(d)
    fake(d, retest={OPUS + '@' + PA: RT_OK})
    go(d, True)
    assert binding(d, 'a') == OPUS + '@' + PB and not requests(d)      # no request of its own for a disabled job
    s = state_of(d)
    s['verified'] = {OPUS + '@' + PA: ms()}
    (d / 'st' / 'state.json').write_text(json.dumps(s))
    m, = kinds(go(d, True)[2], 'job_move')
    assert m['why'] == 'preference' and binding(d, 'a') == OPUS + '@' + PA, m


def test_a_return_check_that_keeps_failing_is_a_problem_once():
    d = setup({'a': (OPUS, PB, 'trusted')}, [], ALL_OK)
    prefer(d)
    for n in range(F.RETEST_ERRORS):
        rc, err, j = go(d, True)
        s = state_of(d)
        s['verify_tried'][OPUS + '@' + PA] -= F.RETEST_EVERY_MS            # an hour later the next check is due
        (d / 'st' / 'state.json').write_text(json.dumps(s))
    assert rc == 1 and 'its check failed 3 times in a row' in err and binding(d, 'a') == OPUS + '@' + PB, err


def test_moves_off_a_held_account_come_before_returns_and_their_checks():
    # 'r' runs first and could return to PA; 'h' sits on a held account. The held one is edited first, and the
    # real request of the return comes after it, so a slow check never delays a move that must happen.
    d = setup({'r': (OPUS, PB, 'trusted'), 'h': (OPUS, PC, 'trusted')}, [], ALL_OK, sched={'r': ('cron', 60), 'h': ('cron', 3600)})
    prefer(d, state={'limited': {OPUS + '@' + PC: ms() + 86400_000}})
    fake(d, retest={OPUS + '@' + PA: RT_OK})
    j = [r for r in go(d, True)[2] if r['kind'] in ('job_move', 'return_check')]
    assert [(r['kind'], r.get('job'), r.get('why')) for r in j] == [('job_move', 'h', 'out'), ('return_check', None, None),
                                                                    ('job_move', 'r', 'preference')], j
    assert binding(d, 'h') == OPUS + '@' + PA and binding(d, 'r') == OPUS + '@' + PA


def test_a_proof_from_before_a_limit_does_not_count_after_it_and_failed_checks_never_lead_the_order():
    # Opus on PA answered 10 min ago, then hit a 30-min limit that has just ended: the return needs a new answer.
    d = setup({'a': (OPUS, PB, 'trusted')}, [], ALL_OK)
    prefer(d, state={'verified': {OPUS + '@' + PA: ms() - 600_000}, 'limited': {OPUS + '@' + PA: ms() - 1000}})
    j = go(d)[2]
    assert [r['key'] for r in kinds(j, 'return_check')] == [OPUS + '@' + PA] and binding(d, 'a') == OPUS + '@' + PB, j
    # That check found nothing (no answer): the order is not put on PA either, though its probe says ok.
    assert order(d) == [PB, PA, PC] and not kinds(j, 'order_set'), j


def test_a_pass_with_fresh_limits_makes_no_return_check():
    d = setup({'a': (OPUS, PB, 'trusted')}, [], ALL_OK, log=[req(K, 'R1'), end('R1')], entries={K: dict(PIN, authProfileOverride=PC)})
    prefer(d)
    fake(d, retest={OPUS + '@' + PA: RT_OK})
    go(d)
    assert not requests(d) and binding(d, 'a') == OPUS + '@' + PB


# Account limits read from Claude (claude_usage.py) decide the order (Alex 6885: fill the earliest-reset account first).
# The fixture is a real GET /api/oauth/usage?cedar_ember=1 answer of a Max 20x account at its weekly limit, 18 Nov 2026.
import claude_usage as CU
REAL = json.loads((HERE / 'fixtures' / 'usage-max20x-at-weekly-limit.json').read_text())
T18 = CU.iso_ms('2026-11-18T10:00:00+00:00')
DAY = 86400_000


def usage_answer(weekly, wreset, fable=None, freset=None, grants=(1,)):
    """An answer of the real shape with other numbers."""
    d = json.loads(json.dumps(REAL))
    iso = lambda t: time.strftime('%Y-%m-%dT%H:%M:%S+00:00', time.gmtime(t / 1000))
    d['seven_day'].update(utilization=float(weekly), resets_at=iso(wreset))
    d['limits'][0].update(percent=weekly, resets_at=iso(wreset))
    d['limits'][1].update(percent=fable if fable is not None else 0, resets_at=iso(freset or wreset))
    d['cedar_ember']['grants'] = [dict(d['cedar_ember']['grants'][0], id='g%d' % i, resets_left=n,
                                       ends_at=iso(ms() + 20 * DAY)) for i, n in enumerate(grants)]
    return d


def test_usage_reading_of_the_real_answer():
    r = CU.parse(REAL, T18)
    assert r['weekly']['pct'] == 100 and abs(r['weekly']['resets_at'] - CU.iso_ms('2026-11-19T04:00:00+00:00')) < 1000, r
    assert r['scoped']['Fable']['pct'] == 98 and r['five_hour'] is None, r
    assert r['resets'] == 1, r['grants']                 # the weekly reset counts; the used Fable reset (left 0) not
    assert CU.parse(REAL, CU.iso_ms('2026-12-11T00:00:00+00:00'))['resets'] == 0   # expired grants do not count
    assert not CU.live(r, OPUS) and not CU.live(r, FABLE)
    assert CU.scoped_for(r, FABLE) is r['scoped']['Fable'] and CU.scoped_for(r, OPUS) is None


def test_usage_order_fills_the_earliest_reset_first():
    def entry(*a, **kw):
        return dict(CU.parse(usage_answer(*a, **kw), T18), ok=True, email_ok=True, t=T18, source='own')
    snap = {'profiles': {PA: entry(85, T18 + 2 * DAY, 30), PB: entry(40, T18 + 5 * DAY, 10),
                         PC: entry(100, T18 + DAY, 98)}}
    assert CU.order([PB, PC, PA], snap, OPUS, T18, 20 * 60_000) == [PA, PB, PC]   # live by reset, then the full one
    snap['profiles'][PA] = entry(85, T18 + 2 * DAY, 100, T18 + 2 * DAY)          # Fable out on PA, room for Opus
    assert CU.order([PA, PB, PC], snap, FABLE, T18, 20 * 60_000) == [PB, PC, PA]  # full ones: back soonest first
    assert CU.order([PA, PB, PC], snap, OPUS, T18, 20 * 60_000) == [PA, PB, PC]
    assert CU.order([PA, PB, PC], snap, OPUS, T18 + 21 * 60_000, 20 * 60_000) is None   # stale: the caller's order
    snap['profiles'][PB]['source'] = 'borrowed'
    assert CU.order([PA, PB, PC], snap, OPUS, T18, 20 * 60_000) is None              # a borrowed login comes and goes
    snap['profiles'][PB].update(source='own', email_ok=None)
    assert CU.order([PA, PB, PC], snap, OPUS, T18, 20 * 60_000) is None              # an unverified login is not used


def usage_setup(answers, emails=None, expiring=()):
    """Logins for PA, PB, PC in a fixture dir, and the settings naming them; answers: {account: usage answer}."""
    d = setup({'a': (OPUS, PB, 'trusted'), 'b': (OPUS, PB, 'trusted')}, [], ALL_OK)
    usage, api = {}, {'refresh': {}}
    for p in (PA, PB, PC):
        n = p.split(':')[1]
        ld = d / 'logins' / n
        ld.mkdir(parents=True)
        exp = ms() + (60_000 if p in expiring else 3600_000)
        (ld / '.credentials.json').write_text(json.dumps({'claudeAiOauth': {
            'accessToken': 'tok-' + n, 'refreshToken': 'ref-' + n, 'expiresAt': exp, 'scopes': ['user:profile']}}))
        usage[p] = {'dir': str(ld), 'email': n + '@example.com'}
        acct = {'email_address': (emails or {}).get(p, n + '@example.com')}
        if p in answers:
            api['tok-' + n] = api['tok-' + n + '2'] = {'account': acct, 'usage': answers[p]}
        api['refresh']['ref-' + n] = {'access_token': 'tok-' + n + '2', 'refresh_token': 'ref-' + n + '2', 'expires_in': 28800}
    prefer(d, {'prefer': {OPUS: [PB, PA, PC]}, 'usage': usage})
    fake(d, usage_api=api, retest={OPUS + '@' + PA: RT_OK})
    return d


def test_saved_override_precedes_computed_usage_and_clear_restores_dynamic_order():
    now = ms()
    for complete in (True, False):
        answers = {PA: usage_answer(85, now + 2 * DAY), PB: usage_answer(40, now + 5 * DAY)}
        if complete:
            answers[PC] = usage_answer(100, now + DAY, 98)
        d = usage_setup(answers)
        settings = json.loads((d / 'settings.json').read_text())
        settings['prefer'] = {OPUS: [PC, PB, PA]}
        prefer(d, settings)
        fake(d, saved_orders={'main': {'anthropic': [PB, PA, PC]}})
        rc, err, j = go(d, True)
        o, = [r for r in kinds(j, 'prefer_order') if r['model'] == OPUS]
        want = [PB, PA, PC]
        assert rc == 0 and o['order'] == want and o['source'] == 'stored', (err, j)
        assert binding(d, 'a') == OPUS + '@' + want[0] and order(d) == [PB, PA, PC], j
        assert not kinds(j, 'order_set') and not any(x.startswith('models auth order set') for x in calls(d)), j
        st = subprocess.run([sys.executable, str(HERE / 'failover.py'), '--status'], capture_output=True, text=True,
                            env=dict(os.environ, OCF_STATE=str(d / 'st'), OCF_SETTINGS=str(d / 'settings.json')))
        assert st.returncode == 0 and json.loads(st.stdout)['prefer'][OPUS] == want, st.stderr
        fake(d, saved_orders={})
        before = len(j)
        rc, err, j = go(d, True)
        o, = [r for r in kinds(j[before:], 'prefer_order') if r['model'] == OPUS]
        assert rc == 0 and o['order'] == ([PA, PB, PC] if complete else [PC, PB, PA]), (err, j)
        assert o['source'] == ('usage' if complete else 'settings'), o
        if complete:
            assert binding(d, 'a') == OPUS + '@' + PA and order(d)[0] == PA, j


def test_failover_follows_the_usage_order_and_returns_work_to_the_earliest_reset():
    now = ms()
    d = usage_setup({PA: usage_answer(85, now + 2 * DAY), PB: usage_answer(40, now + 5 * DAY),
                     PC: usage_answer(100, now + DAY, 98)}, expiring=(PA,))
    rc, err, j = go(d, True)
    o, = [r for r in kinds(j, 'prefer_order') if r['model'] == OPUS]
    assert o['order'] == [PA, PB, PC] and o['source'] == 'usage', o            # settings said PB first
    assert {(m['job'], m['to'], m['why']) for m in kinds(j, 'job_move')} == {('a', OPUS + '@' + PA, 'preference'),
                                                                            ('b', OPUS + '@' + PA, 'preference')}, j
    assert order(d)[0] == PA, order(d)
    creds = json.loads((d / 'logins' / 'a' / '.credentials.json').read_text())['claudeAiOauth']
    assert creds['accessToken'] == 'tok-a2' and creds['refreshToken'] == 'ref-a2' and creds['scopes'] == ['user:profile']
    assert 'tok-' not in (d / 'st' / 'journal.jsonl').read_text()             # no token in the journal
    snap = json.loads((d / 'st' / 'usage.json').read_text())
    assert snap['profiles'][PC]['resets'] == 1 and snap['profiles'][PA]['weekly']['pct'] == 85, snap
    st = subprocess.run([sys.executable, str(HERE / 'failover.py'), '--status'], capture_output=True, text=True,
                        env=dict(os.environ, OCF_STATE=str(d / 'st'), OCF_SETTINGS=str(d / 'settings.json')))
    s = json.loads(st.stdout)
    assert s['prefer'][OPUS] == [PA, PB, PC] and s['prefer_from'] == {OPUS: 'usage'}, s
    assert [r['account'] for r in s['usage']] == [PA, PB, PC] and 'weekly 85%' in s['usage'][0]['text'], s['usage']
    n = len(j)
    j = go(d)[2][n:]                                                      # a minute later: nothing is read again
    assert not kinds(j, 'usage') and not kinds(j, 'prefer_order'), j
    assert json.loads((d / 'st' / 'usage.json').read_text())['t'] == snap['t']


def test_without_a_usable_reading_the_settings_order_stands():
    now = ms()
    # PC has no answer (its login does not work): the order is the settings' one, and no problem is raised for a
    # login that never read.
    d = usage_setup({PA: usage_answer(85, now + 2 * DAY), PB: usage_answer(40, now + 5 * DAY)})
    rc, err, j = go(d, True)
    assert rc == 0 and kinds(j, 'prefer_order')[0]['source'] == 'settings', (err, j)
    assert any(r['account'] == PC and not r['ok'] for r in kinds(j, 'usage')), j
    assert not kinds(j, 'job_move') and order(d) == [PB, PA, PC]
    # A login of another account is not used either.
    d = usage_setup({p: usage_answer(50, now + DAY) for p in (PA, PB, PC)}, emails={PA: 'someone@example.com'})
    rc, err, j = go(d, True)
    assert kinds(j, 'prefer_order')[0]['source'] == 'settings', j
    assert 'not a@example.com' in json.loads((d / 'st' / 'usage.json').read_text())['profiles'][PA]['error']


def test_one_failed_read_keeps_the_order_and_a_reading_that_never_works_is_raised_once_it_lasts():
    now = ms()
    d = usage_setup({PA: usage_answer(85, now + 2 * DAY), PB: usage_answer(40, now + 5 * DAY),
                     PC: usage_answer(100, now + DAY, 98)})
    go(d, True)
    s = json.loads((d / 'st' / 'usage.json').read_text())
    for p in s['profiles'].values():
        p['t'] -= 6 * 60_000                                              # due for a new read
    (d / 'st' / 'usage.json').write_text(json.dumps(s))
    api = json.loads((d / 'fake.json').read_text())['usage_api']
    api.pop('tok-b')                                                      # PB's next read fails once
    fake(d, usage_api=api)
    j = go(d, True)[2]
    assert [r['order'] for r in kinds(j, 'prefer_order')] == [[PA, PB, PC]], j   # no flip back to the settings
    assert json.loads((d / 'st' / 'usage.json').read_text())['profiles'][PB]['last_ok']['weekly']['pct'] == 40
    # ab-style account: a login of no verified account (no email set) never decides the order; after a day: problem.
    d = usage_setup({p: usage_answer(50, now + DAY) for p in (PA, PB, PC)})
    st = json.loads((d / 'settings.json').read_text())
    st['usage'][PA]['email'] = None
    (d / 'settings.json').write_text(json.dumps(st))
    rc, err, j = go(d, True)
    assert rc == 0 and kinds(j, 'prefer_order')[0]['source'] == 'settings', (err, j)
    s = json.loads((d / 'st' / 'state.json').read_text())
    s['usage_inert'][PA] = ms() - 25 * 3600_000
    s['problems'] = {}
    (d / 'st' / 'state.json').write_text(json.dumps(s))
    for _ in range(2):                                                    # a problem fails the second pass in a row
        rc, err, j = go(d, True)
    assert rc == 1 and 'anthropic:a' in err and 'email' in err, err


def test_usage_rules_for_full_accounts_five_hour_window_and_changed_answers():
    def entry(*a, **kw):
        return dict(CU.parse(usage_answer(*a, **kw), T18), ok=True, email_ok=True, t=T18, source='own')
    F20 = 20 * 60_000
    # Fable has room on PA and resets tomorrow, but PA's weekly is full for three more days: PA is back in 3 days,
    # after PB (full until tomorrow).
    snap = {'profiles': {PA: entry(100, T18 + 3 * DAY, 40, T18 + DAY), PB: entry(100, T18 + DAY, 100, T18 + DAY),
                         PC: entry(30, T18 + 6 * DAY, 10, T18 + 6 * DAY)}}
    assert CU.order([PA, PB, PC], snap, FABLE, T18, F20) == [PC, PB, PA]
    # A full 5-hour window takes an account out until it resets.
    snap['profiles'][PC]['five_hour'] = {'pct': 100.0, 'resets_at': T18 + 3600_000}
    assert not CU.live(snap['profiles'][PC], OPUS) and CU.order([PA, PB, PC], snap, OPUS, T18, F20)[0] == PC
    # A changed answer: a percentage that is not a number is no reading; a Fable limit gone from the answer stops the
    # computed Fable order, not the Opus one; grants not shown to this surface are an unknown count, not 0.
    bad = usage_answer(50, T18 + DAY)
    bad['limits'][0]['percent'] = bad['seven_day']['utilization'] = '50'
    assert CU.parse(bad, T18)['weekly']['pct'] is None and not CU.usable(dict(CU.parse(bad, T18), ok=True,
                                                                                  email_ok=True, t=T18, source='own'), T18, F20)
    nofable = usage_answer(50, T18 + DAY)
    nofable['limits'] = nofable['limits'][:1]
    snap['profiles'][PA] = dict(CU.parse(nofable, T18), ok=True, email_ok=True, t=T18, source='own')
    assert CU.order([PA, PB, PC], snap, FABLE, T18, F20) is None and CU.order([PA, PB, PC], snap, OPUS, T18, F20)
    hidden = dict(REAL, cedar_ember={'eligible': False, 'ineligible_reason': 'surface', 'grants': []})
    assert CU.parse(hidden, T18)['resets'] is None and 'resets available ?' in CU.rows(
        {'profiles': {PA: dict(CU.parse(hidden, T18), ok=True, t=T18)}})[0]['text']


def test_a_failed_config_check_drops_the_live_order_once_and_says_why():
    """Fresh-eyes 20 Nov: a pass whose config check fails stamps last_pass but follows no order, so the order others
    read (prefer_eff, the SLA judge) is dropped with the reason, not left at an old value; a good pass writes it again."""
    d = setup(JOBS, probe={'anthropic': {PA: 'ok', PB: 'ok', PC: 'ok'}})
    prefer(d, {'prefer': {OPUS: [PB, PA, PC]}})
    j = go(d, True)[2]
    st = lambda: json.loads((d / 'st' / 'state.json').read_text())
    assert st()['prefer_eff'][OPUS] == [[PB, PA, PC], 'settings'] and not kinds(j, 'prefer_order_dropped'), j
    good = (d / 'cfg.json').read_text()
    c = json.loads(good)
    del c['agents']['defaults']['model']['primary']
    (d / 'cfg.json').write_text(json.dumps(c))
    n, before = len(j), st()['last_pass']
    time.sleep(0.01)
    j = go(d, True)[2]
    dropped = kinds(j[n:], 'prefer_order_dropped')
    assert len(dropped) == 1 and 'primary' in dropped[0]['error'] and 'settings' in dropped[0]['why'], j[n:]
    assert 'prefer_eff' not in st() and st()['last_pass'] > before, st()
    n = len(j)
    j = go(d, True)[2]
    assert not kinds(j[n:], 'prefer_order_dropped') and 'prefer_eff' not in st(), j[n:]   # journaled once
    (d / 'cfg.json').write_text(good)
    n = len(j)
    j = go(d, True)[2]
    assert [r['order'] for r in kinds(j[n:], 'prefer_order')] == [[PB, PA, PC]], j[n:]
    assert st()['prefer_eff'][OPUS] == [[PB, PA, PC], 'settings']


def test_an_unreadable_config_also_drops_the_live_order_once():
    d = setup(JOBS, probe={'anthropic': {PA: 'ok', PB: 'ok', PC: 'ok'}})
    prefer(d, {'prefer': {OPUS: [PB, PA, PC]}})
    j = go(d, True)[2]
    st = lambda: json.loads((d / 'st' / 'state.json').read_text())
    assert 'prefer_eff' in st()
    good = (d / 'cfg.json').read_text()
    (d / 'cfg.json').write_text(good[:40])                                 # half written
    n = len(j)
    rc, err, j = go(d, True)
    dropped = kinds(j[n:], 'prefer_order_dropped')
    assert len(dropped) == 1 and 'not readable' in dropped[0]['error'] and 'prefer_eff' not in st(), j[n:]
    n = len(j)
    j = go(d, True)[2]
    assert not kinds(j[n:], 'prefer_order_dropped'), j[n:]
    (d / 'cfg.json').write_text(good)
    n = len(j)
    j = go(d, True)[2]
    assert [r['order'] for r in kinds(j[n:], 'prefer_order')] == [[PB, PA, PC]] and 'prefer_eff' in st(), j[n:]


def native_limit(run, profile=PB, ago=45):
    import hashlib
    return line({'event': F.EV_AUTH_LIMIT, 'runId': run, 'provider': 'openai', 'reason': 'rate_limit',
                 'profileId': 'sha256:' + hashlib.sha256(profile.encode()).hexdigest()[:12]}, ago=ago)


def native_outcome(run, status='errored', ago=40):
    return line({'runId': run, 'sessionKey': K, 'provider': 'openai', 'model': 'gpt-6-astra',
                 'outcome': 'mute' if status == 'errored' else 'tool_delivered', 'runStatus': status},
                F.NATIVE_OUTCOME, ago=ago)


def test_native_quota_terminal_is_detected_across_pass_boundaries_and_never_replayed():
    # No Model request failed or embedded_run_agent_end: the native Codex path from the real incident.
    account, other = 'openai:b', 'openai:a'
    pin = dict(PIN, providerOverride='openai', modelOverride='gpt-6-astra', authProfileOverride=account)
    d = setup(log=[native_limit('N1', account)], entries={K: pin},
              sessions={K: {'key': K, 'hasActiveRun': False, 'lastRunId': 'N1'}},
              probe={'openai': {account: 'ok', other: 'ok'}})
    # Explicit subscription targets avoid relying on any real store.
    config = json.loads((d / 'cfg.json').read_text())
    config.setdefault('auth', {}).setdefault('order', {})['openai'] = [account, other]
    (d / 'cfg.json').write_text(json.dumps(config))
    go(d)
    logfile = next((d / 'logs').glob('*.log'))
    with logfile.open('a') as f:
        f.write(json.dumps(native_outcome('N1')) + '\n')
    rc, err, j = go(d)
    moves = kinds(j, 'chat_move')
    assert moves and moves[-1]['to'] == 'openai/gpt-6-astra@' + other, (rc, err, j)
    recover, = kinds(j, 'chat_recover')
    assert recover['continue'].startswith('not replayed:'), recover
    assert not json.loads((d / 'st/state.json').read_text()).get('continued'), j
    assert len(kinds(go(d)[2], 'chat_recover')) == 1


@offline
def test_native_rotation_success_and_nonquota_errors_are_not_failed_limit_turns():
    quota, completed = native_limit('N2'), native_outcome('N2', 'completed')
    with patch.object(F, 'log_lines', return_value=[quota, completed]):
        assert F.native_limit_terminal(completed) is None
    unrelated = native_outcome('N3')
    with patch.object(F, 'log_lines', return_value=[quota, unrelated]):
        assert F.native_limit_terminal(unrelated) is None
    failed = native_outcome('N2')
    with patch.object(F, 'log_lines', return_value=[quota, failed]):
        result = F.native_limit_terminal(failed)
        assert result['model'] == 'openai/gpt-6-astra' and result['no_continue'] is True
    success = line('post-run auth-profile success bookkeeping completed after 1ms: runId=N2 provider=openai', ago=42)
    with patch.object(F, 'log_lines', return_value=[quota, success, failed]):
        assert F.native_limit_terminal(failed) is None
    nonlimit = line({'event': F.EV_END, 'runId': 'N2', 'isError': True, 'failoverReason': 'tool_error'}, ago=42)
    with patch.object(F, 'log_lines', return_value=[quota, nonlimit, failed]):
        assert F.native_limit_terminal(failed) is None
    # A successful terminal receipt wins even when another error receipt also exists for the same run.
    with patch.object(F, 'log_lines', return_value=[quota, failed, completed]):
        assert F.native_limit_terminal(failed) is None


@offline
def test_local_model_checks_can_be_disabled_without_accepting_unverified_returns():
    state = {'limited': {FABLE + '@' + PA: ms() + 86400000}}
    health = F.Health({'primary': OPUS, 'chain': [], 'order': {}})
    with patch.object(F, 'LOCAL_MODEL_CHECKS', False), patch.object(F, 'APPLY', True), \
            patch.object(F, 'retest_one', side_effect=AssertionError('must not run a local request')):
        F.retest_holds(state, health, {})
        assert not F.verify(state, health, FABLE + '@' + PA, {})
        assert state['limited'][FABLE + '@' + PA] > ms()


# OpenClaw 2026.11.3's refusal of an isolated --local request for a model only an agent runtime serves (synthetic names).
RUNTIME_REF = 'openai/example-runtime-model@openai:example'
RUNTIME_REFUSAL = ('Unknown model: openai/example-runtime-model. Found agents.defaults.models["openai/example-runtime-'
                   'model"] bound to the "codex" agent runtime. Models served by an agent runtime come from that runtime '
                   'and its linked account, not from models.providers["openai"].models[] — registering it there will not '
                   'make it usable. Confirm "example-runtime-model" is still offered by the "codex" runtime.')
CODEX_REFUSAL = ('The codex provider is served by the Codex app-server agent runtime, not the local simple-completion '
                 'transport. Use an openai/<model> ref with provider/model agentRuntime.id: "codex", run through the '
                 'gateway, or use /codex commands.')


@offline
def test_a_runtime_bound_model_check_is_a_named_blocker_never_a_proof():
    # Incident 25 Nov 2026: return checks of codex-runtime models failed every hour as plain errors ('Unknown model').
    # OpenClaw serves such a model only through its agent runtime, and no isolated request can lock it to one account,
    # so --local refuses it before any provider request. That is no answer and no limit: the pair stays unverified,
    # its hold stays, and the problem names the missing check instead of a generic failure.
    def page(message):
        error = {'type': 'cli_error', 'message': message}
        return {'rc': 1, 'out': 'log line\n' + json.dumps({'ok': False, 'error': error})}

    env = {'OCF_FAKE_CALLS': str(F.SDIR / 'calls')}
    for refusal in (RUNTIME_REFUSAL, CODEX_REFUSAL):
        with patch.object(F, 'FAKE', {'retest': {RUNTIME_REF: page(refusal)}}), patch.dict(os.environ, env):
            res, until, why = F.retest_one(RUNTIME_REF)
        assert (res, until) == ('unsupported', None) and 'runtime-bound' in why, (res, why)


@offline
def test_exclusive_offline_ownership_refusal_is_unsupported_not_repeated_retest_error():
    # OpenClaw 2026.11.10 refuses `infer model run --local` whenever a live Gateway owns the state root.
    ref = 'anthropic/example-model@anthropic:example'
    msg = ('Cannot admit infer model run for agent model credentials in state root /tmp/example: '
           'This operation requires exclusive offline state ownership. Omit --local to run it through the Gateway.')
    env = {'OCF_FAKE_CALLS': str(F.SDIR / 'calls')}
    with patch.object(F, 'FAKE', {'retest': {ref: {'rc': 1, 'err': msg}}}), patch.dict(os.environ, env):
        res, until, why = F.retest_one(ref)
    assert (res, until) == ('unsupported', None) and 'while Gateway owns state' in why, (res, why)
    state = {'selfcheck': {'version': '2026.11.10'}, 'problems': {F.RETEST_PREFIX + ref: {'raised': 1, 'left': 1, 'text': 'old'}}}
    out = {'problems': []}
    with patch.object(F, 'FAKE', {'retest': {ref: {'rc': 1, 'err': msg}}}), patch.dict(os.environ, env), \
            patch.object(F, 'APPLY', True), patch.object(F, 'load_state', return_value={}), patch.object(F, 'save'):
        res2, _, _ = F.exact_check(state, ref, out)
    assert res2 == 'unsupported'
    assert F.RETEST_PREFIX + ref not in state['problems']
    assert 'while Gateway owns state' in state['problems'][F.RUNTIME_CHECK_PREFIX + ref]['text']
    assert state['problems'][F.RUNTIME_CHECK_PREFIX + ref]['once'] is True


def runtime_problems(d):
    env = dict(os.environ, OCF_STATE=str(d / 'st'), OCF_SETTINGS=str(d / 'settings.json'),
               OCF_CONFIG=str(d / 'cfg.json'), OCF_FAKE=str(d / 'fake.json'))
    status = subprocess.check_output([sys.executable, str(HERE / 'failover.py'), '--status'], env=env)
    return json.loads(status)['open_problems']


def test_runtime_refusal_blocks_returns_until_version_change_and_closes_on_proof():
    ref = OPUS + '@' + PA
    d = setup({'a': (OPUS, PB, 'trusted')}, [], ALL_OK)
    prefer(d)
    fake(d, retest={ref: {'rc': 1, 'err': CODEX_REFUSAL}})
    rc, err, j = go(d, True)
    assert rc == 1 and 'runtime-bound' in err and requests(d) == [ref], (rc, err)
    assert binding(d, 'a') == OPUS + '@' + PB and not state_of(d).get('verified')
    assert go(d, True)[0] == 0 and requests(d) == [ref]
    # A different version may still refuse. It gets an immediate check, in the same open episode.
    rc, err, j = go(d, True, env={'OCF_FAKE_VERSION': '2026.11.4'})
    assert rc == 0 and requests(d) == [ref, ref] and len(kinds(j, 'problem')) == 1, (err, j)
    assert 'runtime-bound' in runtime_problems(d)[0]['text']
    fake(d, retest={ref: RT_OK})
    rc, err, j = go(d, True, env={'OCF_FAKE_VERSION': '2026.11.5'})
    assert rc == 0 and requests(d) == [ref] * 3 and binding(d, 'a') == ref
    assert not runtime_problems(d)


@offline
def test_runtime_refusal_shares_daily_retry_across_hold_and_return_without_starving_other_pairs():
    ref = RUNTIME_REF
    other = ref + '-other'
    hour = 3600_000
    start = ms()
    state = {'selfcheck': {'version': '2026.11.3'}}
    health = F.Health({}, {})
    out = {'problems': [], 'cleared': []}
    with patch.object(F, 'FAKE', {'retest': {ref: {'rc': 1, 'err': CODEX_REFUSAL}, other: RT_OK}}), \
            patch.dict(os.environ, OCF_FAKE_CALLS=str(F.SDIR / 'calls')), patch.object(F, 'APPLY', True), \
            patch.dict(F._VERIFY, n=0, failed=set()), patch.object(F, 'NOW', return_value=start) as clock:
        assert not F.verify(state, health, ref, out)
        assert len(F.raise_due(state, out)) == 1
        health.limited.update({ref: start + 48 * hour, other: start + 48 * hour})
        clock.return_value = start + 24 * hour - 1
        out['problems'].clear()
        F.cleanup_prefer(state)
        with patch.object(F, 'RETEST_PER_PASS', 1):
            F.retest_holds(state, health, out)
        assert out['cleared'] == [other] and ref in health.limited
        assert not F.raise_due(state, out)
        clock.return_value += 1
        F.retest_holds(state, health, out)
        assert not F.raise_due(state, out) and ref in health.limited
    checks = [json.loads(line) for line in F.JOURNAL.read_text().splitlines()]
    assert [r['key'] for r in kinds(checks, 'hold_retest')] == [ref]
    assert len(kinds(checks, 'problem')) == 1


def test_runtime_refusal_survives_a_later_gateway_failure_without_advancing_log_offsets():
    ref = OPUS + '@' + PA
    d = setup({'a': (OPUS, PB, 'trusted')}, [], ALL_OK, log=[{'message': 'new log entry'}])
    prefer(d)
    fake(d, retest={ref: {'rc': 1, 'err': CODEX_REFUSAL}})
    runner = d / 'interrupted.py'
    runner.write_text(f"""import sys
from unittest.mock import patch
sys.path.insert(0, {str(HERE.resolve())!r})
import failover as F

def unavailable(*args):
    assert not F.load_state().get('offsets'), 'an incomplete pass was already checkpointed'
    raise F.Unreachable('gateway unavailable')

with patch.object(F, 'check', unavailable):
    F.main()
""")
    with patch.dict(os.environ, OCF_SCRIPT=str(runner)):
        rc, err, _ = go(d, True)
    assert rc == 1 and 'runtime-bound' in err and 'Traceback' not in err, err
    assert not state_of(d).get('offsets') and requests(d) == [ref]
    rc, err, j = go(d, True)
    assert rc == 0 and any(state_of(d)['offsets'].values()) and requests(d) == [ref], err
    assert len(kinds(j, 'problem')) == 1 and len(runtime_problems(d)) == 1


def test_ending_a_runtime_bound_hold_closes_its_problem_and_preserves_the_retry_delay():
    ref = OPUS + '@' + PA
    d = setup({'a': (OPUS, PB, 'trusted')}, [], ALL_OK)
    prefer(d, state={'limited': {ref: ms() + 86400_000}})
    fake(d, retest={ref: {'rc': 1, 'err': CODEX_REFUSAL}})
    rc, err, j = go(d, True)
    assert rc == 1 and requests(d) == [ref] and ref in state_of(d)['limited'], (err, j)
    s = state_of(d)
    s['limited'][ref] = ms() - 1
    (d / 'st/state.json').write_text(json.dumps(s))
    assert not runtime_problems(d)  # status also sees expiry between scheduled passes
    rc, err, j = go(d, True)
    assert rc == 0 and requests(d) == [ref] and binding(d, 'a') == OPUS + '@' + PB
    assert not runtime_problems(d)
    assert not state_of(d).get('verified')


def test_not_found_edit_error_survives_cli_diagnostic_truncation():
    reply = (1, 'Automation not found', 'gateway diagnostic ' * 20)
    with patch.object(F, 'APPLY', True), patch.object(F, 'FAKE', None), patch.object(F, 'run', return_value=reply):
        assert F.cron_edit('once', OPUS + '@' + PA) == 'rc=1 Automation not found'


def test_one_shot_disappears_during_preference_edit():
    d = setup({'once': (OPUS, PB, 'trusted')}, probe=ALL_OK, sched={'once': ('at', 60)})
    prefer(d, state={'verified': {OPUS + '@' + PA: ms()}})
    with sqlite3.connect(d / 'state.sqlite') as c:
        c.execute("""create trigger expire_once before update on cron_jobs begin
                     delete from cron_jobs where job_id=old.job_id; select raise(ignore); end""")
    fake(d, edit_result='rc=1 Automation not found')
    rc, err, journal = go(d)
    assert rc == 0 and not err, err
    assert [r['result'] for r in kinds(journal, 'job_move')] == ['rc=1 Automation not found']
    assert [r['job'] for r in kinds(journal, 'job_gone')] == ['once']
    assert not runtime_problems(d) and not state_of(d)['unsure']
    with sqlite3.connect(d / 'state.sqlite') as c:
        assert c.execute('select count(*) from cron_jobs').fetchone()[0] == 0


def test_only_complete_inventory_retires_job_records():
    d = setup({'disabled': (OPUS, PA, 'trusted')}, sched={'disabled': ('at', 0)})
    with sqlite3.connect(d / 'state.sqlite') as c:
        c.execute('update cron_jobs set enabled=0')
    problems = {'rebind:' + jid: {'text': jid, 'raised': ms(), 'left': 0}
                for jid in ('missing', 'disabled')}
    problems['rebind:unseen'] = {'text': 'unseen', 'raised': ms() - 3 * F.RERAISE_MS, 'left': 0}
    prefer(d, state={'problems': problems, 'home': {'missing': {'model': OPUS}},
                     'unsure': {'missing': {}}, 'pref_moved': {'missing': ms(), K: ms()}})
    path = d / 'st' / 'state.json'
    before = path.read_bytes()
    go_dry(d)
    assert path.read_bytes() == before and not (path.parent / 'journal.jsonl').exists()
    _, _, journal = go(d, env={'OCF_FAKE_MORE': '1'})
    state = state_of(d)
    assert not kinds(journal, 'job_gone') and problems.keys() <= state['problems'].keys()
    assert all('missing' in state[k] for k in ('home', 'unsure', 'pref_moved'))
    go(d)
    _, _, journal = go(d)
    state = state_of(d)
    assert [r['job'] for r in kinds(journal, 'job_gone')] == ['missing', 'unseen']
    assert all('missing' not in state[k] for k in ('home', 'unsure', 'pref_moved'))
    assert K in state['pref_moved'] and 'rebind:missing' not in state['problems']
    visible = {p['text'] for p in runtime_problems(d)}
    assert 'disabled' in visible and 'missing' not in visible


def test_store_read_error_keeps_job_state_until_next_complete_pass():
    d = setup({'live': (OPUS, PA, 'trusted'), 'other': (OPUS, PB, 'trusted')}, probe=ALL_OK)
    go(d)
    with sqlite3.connect(d / 'state.sqlite') as c:
        c.execute("update cron_jobs set job_json=json_set(job_json, '$.payload.model', ?) where job_id='live'", (OPUS,))
    # The CLI still lists both jobs when the local binding store cannot decode a row.
    broken = d / 'broken.sqlite'
    broken.write_bytes((d / 'state.sqlite').read_bytes())
    with sqlite3.connect(broken) as c:
        c.execute("update cron_jobs set job_json='{' where job_id='live'")
    patch_state(d, problems={'rebind:missing': {'raised': ms(), 'text': 'missing', 'left': 0}})
    _, err, journal = go(d, env={'OCF_STATE_DB': 'file:%s?mode=ro' % broken})
    assert 'cron store' in err and 'rebind:missing' in state_of(d)['problems']
    assert not kinds(journal, 'job_gone')
    go(d)
    assert 'rebind:missing' not in state_of(d)['problems']


def test_not_found_edit_with_unconfirmed_absence_keeps_its_problem():
    for partial in (False, True):
        d = setup({'j': (OPUS, PB, 'trusted')}, probe=ALL_OK)
        prefer(d, state={'verified': {OPUS + '@' + PA: ms()}})
        fake(d, edit_result='rc=1 Automation not found')
        rc, err, journal = go(d, env={'OCF_FAKE_MORE': '1'} if partial else {})
        assert rc == 1 and 'Automation not found' in err
        assert 'rebind:j' in state_of(d)['problems'] and not kinds(journal, 'job_gone')


def test_failed_rebind_retried_ok_next_pass_does_not_fail_that_pass():
    # 23 Nov 08:08: a rebind timed out while the Gateway was busy and did not land; the next pass rebound the job
    # fine but still exited 1 for the ledger's second pass, a failure streak for a problem that pass had fixed.
    d = setup({'x': (OPUS, PA, 'trusted')}, probe={'anthropic': DEAD, 'xai': {'xai:x': 'ok'}})
    fake(d, edit_result='rc=124 timeout')
    rc, err, j = go(d, True)
    assert rc == 1 and 'rebind:x' in state_of(d)['problems'], (err, j)
    c = sqlite3.connect(d / 'state.sqlite')                           # the timed-out edit did not land after all
    jj = json.loads(c.execute('select job_json from cron_jobs where job_id=?', ('x',)).fetchone()[0])
    jj['payload']['model'], jj['inferenceBinding'] = OPUS + '@' + PA, {'profileId': PA}
    c.execute('update cron_jobs set job_json=? where job_id=?', (json.dumps(jj), 'x'))
    c.commit()
    c.close()
    fake(d, edit_result='ok')
    rc, err, j = go(d, True)
    st = state_of(d)
    assert binding(d, 'x') == 'xai/grok-4.7@xai:x' and kinds(j, 'job_move')[-1]['result'] == 'ok', (err, j)
    assert rc == 0 and not err and 'rebind:x' not in st['problems'], (rc, err, st)


SOL, SOL_A = 'openai/gpt-6.1-sol', 'openai:default'
OPENAI_OUT = {SOL_A: 'rate_limit', OA: 'rate_limit', 'openai:b': 'rate_limit'}
OPENAI_OK = {SOL_A: 'ok', OA: 'ok', 'openai:b': 'ok'}
ANTHROPIC_OK = {PA: 'ok', PB: 'ok', PC: 'ok'}
XAI_OUT = {'xai:x': 'rate_limit'}


def rebind_outside(d, jid, ref):
    """A change of the job's binding that this script did not make (the operator, or an edit that landed late)."""
    c = sqlite3.connect(d / 'state.sqlite')
    jj = json.loads(c.execute('select job_json from cron_jobs where job_id=?', (jid,)).fetchone()[0])
    jj['payload']['model'], jj['inferenceBinding'] = ref, {'profileId': ref.split('@', 1)[1]}
    c.execute('update cron_jobs set job_json=? where job_id=?', (json.dumps(jj), jid))
    c.commit()
    c.close()


def patch_state(d, **kw):
    s = state_of(d)
    s.update(kw)
    (d / 'st' / 'state.json').write_text(json.dumps(s))


def parked_job(**kw):
    """Job 'j' of SOL, moved across providers to Opus on PB by a real pass while every OpenAI account is out."""
    d = setup({'j': (SOL, SOL_A, 'trusted')}, [], {'openai': OPENAI_OUT, 'anthropic': ANTHROPIC_OK, 'xai': XAI_OUT}, **kw)
    go(d)
    assert binding(d, 'j') == OPUS + '@' + PB and state_of(d)['home']['j']['model'] == SOL, state_of(d)
    return d


def test_job_keeps_its_home_through_repeated_moves_until_it_returns():
    # Production 17-18 Nov: a job parked on Opus had its Opus account changed outside this script, and the next pass
    # took that for a new home. Each go() is a new process, so every step also proves the home survives a restart.
    d = parked_job()
    rebind_outside(d, 'j', OPUS + '@' + PA)                            # same fallback model, another account
    go(d, True)
    h = state_of(d)['home'].get('j')
    assert h and h['model'] == SOL and h['parked'] == OPUS + '@' + PA, state_of(d)['home']
    assert binding(d, 'j') == OPUS + '@' + PA                          # nothing to do while OpenAI is out
    prefer(d, {'prefer': {OPUS: [PC, PA, PB]}})                        # the account preference moves it on the fallback
    fake(d, retest={OPUS + '@' + PC: RT_OK})
    go(d, True)
    assert binding(d, 'j') == OPUS + '@' + PC, binding(d, 'j')
    assert state_of(d)['home']['j']['model'] == SOL and state_of(d)['home']['j']['parked'] == OPUS + '@' + PC
    fake(d, probe={'openai': OPENAI_OUT, 'anthropic': DEAD, 'xai': {'xai:x': 'ok'}})   # a second provider move
    go(d, True)
    assert binding(d, 'j') == 'xai/grok-4.7@xai:x' and state_of(d)['home']['j']['model'] == SOL, state_of(d)['home']
    go(d, True)
    assert binding(d, 'j') == 'xai/grok-4.7@xai:x' and state_of(d)['home']['j']['model'] == SOL
    fake(d, probe={'openai': OPENAI_OK, 'anthropic': DEAD, 'xai': {'xai:x': 'ok'}})
    patch_state(d, home=dict(state_of(d)['home'], j=dict(state_of(d)['home']['j'], at=0, wait=0)))
    rc, err, j = go(d, True)
    assert binding(d, 'j') == SOL + '@' + SOL_A and not state_of(d)['home'], (err, j)
    assert kinds(j, 'job_move')[-1]['why'] == 'home', j
    prefer(d, {'prefer': {SOL: [OA, SOL_A]}})                          # once home, the preference only changes account
    fake(d, retest={SOL + '@' + OA: RT_OK})
    patch_state(d, pref_moved={})
    go(d, True)
    assert binding(d, 'j') == SOL + '@' + OA and not state_of(d)['home'], binding(d, 'j')


def test_operator_choice_of_another_model_for_a_parked_job_still_stands():
    d = parked_job()
    rebind_outside(d, 'j', FABLE + '@' + PA)
    go(d, True)
    assert 'j' not in state_of(d)['home'], state_of(d)['home']
    fake(d, probe={'openai': OPENAI_OK, 'anthropic': ANTHROPIC_OK, 'xai': XAI_OUT})
    go(d, True)
    assert binding(d, 'j') == FABLE + '@' + PA


def test_job_home_survives_an_ambiguous_rebind_that_lands_or_lands_late():
    # A cross-provider edit times out but landed: the next pass adopts it with the original home and clears the problem.
    d = parked_job()
    fake(d, probe={'openai': OPENAI_OUT, 'anthropic': DEAD, 'xai': {'xai:x': 'ok'}}, edit_result='rc=124 timeout')
    rc, err, j = go(d, True)
    assert rc == 1 and state_of(d)['unsure']['j']['own'] == SOL, (err, j)
    fake(d, edit_result='ok')
    rc, err, j = go(d)
    s = state_of(d)
    assert rc == 0 and not s['unsure'] and 'rebind:j' not in s['problems'], (err, s)
    assert s['home']['j']['model'] == SOL and s['home']['j']['parked'] == 'xai/grok-4.7@xai:x', s['home']
    # An account edit not visible on the next pass, which lands after that pass gave up on it: still on its way home.
    d = parked_job()
    patch_state(d, unsure={'j': {'own': SOL, 'tgt': OPUS + '@' + PA, 'frm': OPUS + '@' + PB, 'why': 'preference'}})
    go(d)
    assert not state_of(d)['unsure'] and binding(d, 'j') == OPUS + '@' + PB
    rebind_outside(d, 'j', OPUS + '@' + PA)
    go(d, True)
    h = state_of(d)['home'].get('j')
    assert h and h['model'] == SOL and h['parked'] == OPUS + '@' + PA, state_of(d)['home']


def test_adopt_job_returns_an_operator_moved_job_through_the_normal_path():
    # The job's record lost its model; the operator names only the MODEL. The account comes from normal selection.
    d = setup({'j': (OPUS, PB, 'trusted')}, [], {'openai': OPENAI_OUT, 'anthropic': ANTHROPIC_OK, 'xai': XAI_OUT})
    go(d)
    assert not state_of(d)['home']
    rc, err, j = go(d, False, '--adopt-job', 'j=' + SOL)
    a, = kinds(j, 'job_adopt')
    assert rc == 0 and (a['job'], a['ref'], a['home']) == ('j', OPUS + '@' + PB, SOL), (err, a)
    assert state_of(d)['home']['j']['parked'] == OPUS + '@' + PB and binding(d, 'j') == OPUS + '@' + PB
    go(d, True)                                                        # its model is still out: it waits, no edit
    assert binding(d, 'j') == OPUS + '@' + PB and state_of(d)['home']['j']['model'] == SOL
    fake(d, probe={'openai': {SOL_A: 'rate_limit', OA: 'ok', 'openai:b': 'ok'}, 'anthropic': ANTHROPIC_OK, 'xai': XAI_OUT})
    rc, err, j = go(d, True)
    m = kinds(j, 'job_move')[-1]
    assert (m['to'], m['why']) == (SOL + '@' + OA, 'home') and not state_of(d)['home'], (err, m)


def test_adopt_job_refuses_what_the_normal_path_could_not_return():
    d = setup({'j': (OPUS, PB, 'trusted'), 'legacy': (OPUS, PB, 'account'), 'once': (OPUS, PB, 'trusted')}, [],
              {'openai': OPENAI_OUT, 'anthropic': ANTHROPIC_OK, 'xai': XAI_OUT}, sched={'once': ('at', 60)})
    c = sqlite3.connect(d / 'state.sqlite')
    c.execute("update cron_jobs set enabled=0 where job_id='once'")
    c.commit()
    c.close()
    rc, err, j = go(d, False, '--adopt-job', 'j=' + SOL + '@' + SOL_A)  # no allowlist to catch it: an account is refused
    assert rc == 0 and not kinds(j, 'job_adopt') and not state_of(d).get('home'), (err, j)
    cfg_ = json.loads((d / 'cfg.json').read_text())
    cfg_['agents']['defaults']['models'] = {m: {} for m in (OPUS, FABLE, SOL, 'xai/grok-4.7')}
    (d / 'cfg.json').write_text(json.dumps(cfg_))
    go(d)
    for arg in ('j', 'j=gpt-6.1-sol', 'j=' + SOL + '@' + SOL_A, 'j=openai/old-model', 'legacy=' + SOL, 'once=' + SOL,
                'missing=' + SOL):
        rc, err, j = go(d, False, '--adopt-job', arg)
        assert rc == 0 and not kinds(j, 'job_adopt') and not state_of(d)['home'], (arg, err, j)
    # While a rebind's outcome is unknown, its reconciliation owns the home: an adoption would be overwritten by it.
    patch_state(d, unsure={'j': {'own': OPUS, 'tgt': 'xai/grok-4.7@xai:x', 'frm': OPUS + '@' + PB, 'why': 'out'}})
    rc, err, j = go(d, False, '--adopt-job', 'j=' + SOL)
    assert rc == 0 and not kinds(j, 'job_adopt') and not state_of(d)['home'] and state_of(d)['unsure'], (err, j)
    patch_state(d, unsure={})
    # MODEL equal to the current model: the operator says it is home now, so an older return is dropped.
    go(d, False, '--adopt-job', 'j=' + SOL)
    assert state_of(d)['home']['j']['model'] == SOL
    rc, err, j = go(d, False, '--adopt-job', 'j=' + OPUS)
    assert rc == 0 and 'j' not in state_of(d)['home'] and 'home' not in kinds(j, 'job_adopt')[-1], (err, j)


def test_adopted_job_yields_to_a_later_operator_model_choice():
    d = setup({'j': (OPUS, PB, 'trusted')}, [], {'openai': OPENAI_OUT, 'anthropic': ANTHROPIC_OK, 'xai': XAI_OUT})
    go(d)
    go(d, False, '--adopt-job', 'j=' + SOL)
    rebind_outside(d, 'j', FABLE + '@' + PA)
    fake(d, probe={'openai': OPENAI_OK, 'anthropic': ANTHROPIC_OK, 'xai': XAI_OUT})
    go(d, True)
    assert binding(d, 'j') == FABLE + '@' + PA and 'j' not in state_of(d)['home'], state_of(d)['home']


def test_adopt_job_with_an_unreadable_job_list_records_nothing():
    d = setup({'j': (OPUS, PB, 'trusted')}, [], {'openai': OPENAI_OUT, 'anthropic': ANTHROPIC_OK, 'xai': XAI_OUT})
    go(d)
    before = state_of(d)
    env = dict(os.environ, OCF_STATE=str(d / 'st'), OCF_STATE_DB='file:%s?mode=ro' % (d / 'state.sqlite'),
               OCF_FAKE_DB=str(d / 'state.sqlite'), OCF_CONFIG=str(d / 'cfg.json'), OCF_LOG_DIR=str(d / 'logs'),
               OCF_FAKE=str(d / 'fake.json'), OCF_DIST=str(d / 'pkg' / 'dist'), OCF_OPENCLAW_DIR=str(d / 'oc'),
               OCF_FAKE_CALLS=str(d / 'calls.log'), OCF_SETTINGS=str(d / 'settings.json'), OCF_FAKE_CLI_DOWN='1',
               PATH=str(d / 'bin') + os.pathsep + os.environ.get('PATH', ''))
    p = subprocess.run([sys.executable, str(HERE / os.environ.get('OCF_SCRIPT', 'failover.py')), '--adopt-job',
                        'j=' + SOL], env=env, capture_output=True, text=True)
    assert p.returncode == 1 and 'nothing recorded' in p.stderr and 'Traceback' not in p.stderr, p.stderr
    assert state_of(d) == before


def test_chat_policy_retires_the_home_recorded_by_an_earlier_failover():
    sol = dict(PIN, providerOverride='openai', modelOverride='gpt-6.1-sol', authProfileOverride=SOL_A)
    d = setup(entries={K: sol}, probe={'openai': OPENAI_OUT, 'xai': {'xai:x': 'ok'}, **ALL_OK})
    go(d, env={'OCF_CHAT_BANNED_MODELS': ''})  # reproduce a saved Sol home under the old policy
    assert state_of(d)['chat_home'][K]['model'] == SOL
    fake(d, probe={'openai': OPENAI_OK, **ALL_OK})
    rc, err, j = go(d, True)
    assert rc == 0 and [r['to'] for r in kinds(j, 'chat_policy')] == [OPUS + '@' + PB], (err, j)
    assert K not in state_of(d)['chat_home'] and not kinds(go(d, True)[2], 'chat_return')


def test_chat_policy_repairs_inherited_models_between_probes_and_skips_jobs():
    sol = {'providerOverride': 'openai', 'modelOverride': 'gpt-7-sol'}
    d = setup({'audit': (SOL, SOL_A, 'trusted')}, entries={K: sol}, probe=ALL_OK)
    prefer(d, {}, state={'probed_at': ms(), 'limited': {OPUS + '@' + PB: ms() + 300_000}})
    rc, err, j = go(d)
    assert rc == 0 and [r['to'] for r in kinds(j, 'chat_policy')] == [OPUS + '@' + PA], (err, j)
    assert binding(d, 'audit') == SOL + '@' + SOL_A
    assert len(kinds(go(d)[2], 'chat_policy')) == 1


def test_chat_policy_dry_run_only_previews_the_primary():
    sol = {'providerOverride': 'openai', 'modelOverride': 'gpt-6-luna'}
    d = setup(entries={K: sol}, probe=ALL_OK)
    preview = go_dry(d)
    assert preview['chats'] == [[K[-40:], OPUS + '@' + PB]]
    assert json.loads((d / 'fake.json').read_text())['entries'][K] == sol
    assert not (d / 'st/state.json').exists() and not (d / 'st/journal.jsonl').exists()


def test_strict_chat_policy_waits_for_primary_then_stays_on_it():
    strict = {'OCF_STRICT_CHATS': K}
    sol = dict(PIN, providerOverride='openai', modelOverride='gpt-6.1-sol', authProfileOverride=SOL_A)
    d = setup(entries={K: sol}, probe={'anthropic': DEAD, 'openai': OPENAI_OK, 'xai': {'xai:x': 'ok'}})
    assert go(d, env=strict)[0] == 1
    fake(d, probe=ALL_OK)
    rc, err, j = go(d, env=strict)
    assert rc == 0 and [r['to'] for r in kinds(j, 'chat_policy')] == [OPUS + '@' + PB], (err, j)
    fake(d, probe={'anthropic': DEAD, 'openai': OPENAI_OK, 'xai': {'xai:x': 'ok'}})
    assert not kinds(go(d, True, env=strict)[2], 'chat_move')


def test_chat_skips_banned_fallback_that_a_job_can_use():
    d = setup({'audit': (FABLE, PA, 'trusted')}, entries={K: PIN},
              probe={'anthropic': DEAD, 'xai': XAI_OUT, 'openai': OPENAI_OK})
    _, _, j = go(d)
    assert not kinds(j, 'chat_move') and binding(d, 'audit') == SOL + '@' + SOL_A


def test_chat_policy_reports_busy_chat_once_and_closes_after_repair():
    d = setup(entries={K: {'providerOverride': 'openai', 'modelOverride': 'gpt-6.1-terra'}}, probe=ALL_OK)
    fake(d, queued={K: True})
    assert any(K in p['text'] for p in runtime_problems(d))
    assert go(d)[0] == 1 and go(d)[0] == 0
    assert any(K in p['text'] for p in runtime_problems(d))
    fake(d, queued={})
    rc, err, j = go(d)
    assert rc == 0 and not runtime_problems(d), (err, j)
    assert len([r for r in kinds(j, 'problem') if r['key'] == 'chat_policy:' + K]) == 1



# Infrastructure kills. Shape of the live case of 26 Nov 2026 (Listings topic): a person's message arrived while a
# Codex run worked; the run failed before it replied with 'Plugin openai was reloaded or disabled'; an inter-session
# message then started a newer run that answered NO_REPLY (hidden from chat.history); the Gateway restarted and was
# ready again 16 min after the failure. Text is paraphrased, ids are synthetic.
IRUN, ILATER = '00000000-0000-4000-8000-0000000000c1', '00000000-0000-4000-8000-0000000000c2'
PLUGIN_ERR = 'Plugin openai was reloaded or disabled; use its current tools.'


def infra_history(after=()):
    meta = lambda run: {'runId': run}
    return [
        {'role': 'assistant', 'content': [{'type': 'text', 'text': 'Earlier answer.'}], 'stopReason': 'stop',
         'model': 'delivery-mirror'},
        {'role': 'user', 'content': 'Keep the deposit for this house, please.',
         '__openclaw': {'senderIsOwner': True, 'senderIdentity': {'senderKind': 'human'}}},
        {'role': 'custom', 'customType': F.H_FAILED, 'content': "Your request couldn't be completed: " + PLUGIN_ERR,
         'openclawRunError': {'runId': IRUN, 'summary': "Your request couldn't be completed: " + PLUGIN_ERR},
         '__openclaw': meta(IRUN)},
        {'role': 'assistant', 'content': '[Inter-session message] a status note from another topic',
         'provenance': {'kind': 'inter_session'}, '__openclaw': dict(meta(ILATER), senderIsOwner=False)},
        {'role': 'assistant', 'content': [{'type': 'toolCall', 'name': 'bash', 'arguments': {}}], 'stopReason': 'toolUse',
         '__openclaw': meta(ILATER)},
        {'role': 'toolResult', 'content': [{'type': 'text', 'text': 'ok'}], '__openclaw': meta(ILATER)},
    ] + list(after)


def infra_setup(failed_ago=21 * 60, ready_ago=5 * 60, key=K, history=None, active=False, extra_log=()):
    out = line({'runId': IRUN, 'sessionKey': key, 'provider': 'openai', 'model': 'gpt-6-astra', 'outcome': 'mute',
                'runStatus': 'errored'}, F.NATIVE_OUTCOME, ago=failed_ago)
    ready = line('gateway startup outcomes: internal-hooks=loaded', ago=ready_ago)
    d = setup(log=[out, ready] + list(extra_log), probe=ALL_OK,
              sessions={key: {'key': key, 'hasActiveRun': active, 'lastRunId': ILATER}},
              entries={key: {'lastRunId': ILATER, 'status': 'done', 'lastInteractionAt': ms() - 20 * 60_000}})
    fake(d, history={key: infra_history() if history is None else history})
    return d


def test_turn_killed_by_a_plugin_reload_is_continued_once_on_its_own_model():
    d = infra_setup()
    rc, err, j = go(d)
    c, = kinds(j, 'turn_continue')
    assert (c['session'], c['run'], c['reason'], c['result']) == (K, IRUN, 'infra', 'started'), j
    assert PLUGIN_ERR in c['error'] and not kinds(j, 'chat_move'), j
    assert json.loads((d / 'st/state.json').read_text())['infra'][IRUN]['why'] == 'continued'
    assert len(kinds(go(d)[2], 'turn_continue')) == 1                 # a second pass: no duplicate
    assert rc == 0, err


def test_infra_continuation_prompt_names_the_restart_and_no_model_line():
    calls = []
    with patch.object(F, 'APPLY', True), patch.object(F, 'FAKE', None), \
            patch.object(F, 'run', lambda argv, timeout=120: calls.append(argv) or (0, '', '')):
        res, unit = F.continue_turn(K, None, False, IRUN, infra=True)
    msg = calls[0][calls[0].index('--message') + 1]
    assert res == 'started' and unit and '--deliver' in calls[0] and '--session-key' in calls[0], calls
    assert 'cut off by a Gateway restart before it answered' in msg and IRUN in msg, msg
    assert 'usage limit' not in msg and 'now runs on' not in msg, msg


def test_infra_kill_left_alone_when_a_person_wrote_again():
    d = infra_setup(history=infra_history([{'role': 'user', 'content': 'Never mind.'}]))
    j = go(d)[2]
    drop, = [x for x in kinds(j, 'event_drop') if x.get('event') == 'infra']
    assert drop['why'] == 'a person wrote again' and not kinds(j, 'turn_continue'), j


def test_infra_kill_left_alone_after_a_stop():
    d = infra_setup(history=infra_history([{'role': 'user', 'content': '/stop'}]))
    assert not kinds(go(d)[2], 'turn_continue')
    stop = line('chat.abort requested runId=%s' % IRUN, ago=20 * 60)
    d = infra_setup(extra_log=[stop])
    j = go(d)[2]
    assert [x['why'] for x in kinds(j, 'event_drop')] == ['a person stopped it'] and not kinds(j, 'turn_continue'), j


def test_infra_kill_waits_while_a_run_is_active():
    d = infra_setup(active=True)
    j = go(d)[2]
    assert not kinds(j, 'turn_continue') and not kinds(j, 'event_drop'), j
    assert IRUN not in json.loads((d / 'st/state.json').read_text()).get('infra', {})   # looked at again later


def test_infra_kill_of_a_cron_run_is_never_continued():
    ck = 'agent:main:cron:00000000-0000-4000-8000-00000000000a:run:00000000-0000-4000-8000-0000000000f1'
    j = go(infra_setup(key=ck))[2]
    assert not kinds(j, 'turn_continue'), j


def test_infra_kill_answered_or_not_infra_is_not_continued():
    answered = infra_history([{'role': 'assistant', 'content': [{'type': 'text', 'text': 'Done.'}], 'stopReason': 'stop'}])
    assert not kinds(go(infra_setup(history=answered))[2], 'turn_continue')
    limit = infra_history()
    limit[2] = dict(limit[2], openclawRunError={'runId': IRUN, 'summary': 'The AI service needs a short break.'})
    assert not kinds(go(infra_setup(history=limit))[2], 'turn_continue')


def test_infra_window_counts_from_gateway_ready_not_from_the_failure():
    # Failed 66 min ago, but the Gateway was down until 10 min ago: inside the window.
    assert kinds(go(infra_setup(failed_ago=66 * 60, ready_ago=10 * 60))[2], 'turn_continue')
    # Just over 60 min after ready: too old.
    assert not kinds(go(infra_setup(failed_ago=80 * 60, ready_ago=61 * 60))[2], 'turn_continue')
    # No restart after the failure: the failure itself starts the window.
    assert not kinds(go(infra_setup(failed_ago=61 * 60, ready_ago=2 * 3600))[2], 'turn_continue')


def test_gateway_restart_interrupt_in_the_stored_entry_is_continued():
    # WebChat has no message-tool receipt: the stored entry names the interrupted last run.
    main = 'agent:main:main'
    d = setup(probe=ALL_OK, sessions={main: {'key': main, 'hasActiveRun': False, 'lastRunId': IRUN}},
              entries={main: {'lastRunId': IRUN, 'status': 'interrupted', 'endedAt': ms() - 5 * 60_000,
                              'lastRunError': 'Run interrupted by a Gateway restart.'}})
    fake(d, history={main: [{'role': 'user', 'content': 'Check the report.', '__openclaw': {'senderIsOwner': True}},
                            {'role': 'assistant', 'content': [{'type': 'toolCall', 'name': 'bash'}], 'stopReason': 'toolUse',
                             '__openclaw': {'runId': IRUN}}]})
    c, = kinds(go(d)[2], 'turn_continue')
    assert c['run'] == IRUN and 'Gateway restart' in c['error'], c


def test_log_dir_prefers_newest_between_state_logs_and_tmp_dirs():
    with tempfile.TemporaryDirectory(prefix='ocf-logdir-') as tmp:
        root = pathlib.Path(tmp)
        oc_dir = root / 'state'
        state_logs = oc_dir / 'logs'
        tmp_logs = root / 'tmp-openclaw'
        state_logs.mkdir(parents=True)
        tmp_logs.mkdir(parents=True)
        old_tmp = tmp_logs / 'openclaw-2026-11-22.log'
        new_state = state_logs / 'openclaw-2026-11-26.log'
        old_tmp.write_text('{}\n')
        new_state.write_text('{}\n')
        os.utime(old_tmp, (1700000000, 1700000000))
        os.utime(new_state, (1700000500, 1700000500))
        cfg_path = str(root / 'openclaw.json')
        open(cfg_path, 'w').write('{}')
        env_clean = {k: v for k, v in os.environ.items() if k != 'OCF_LOG_DIR'}
        with patch.dict(os.environ, env_clean, clear=True), \
                patch.object(F, 'OC_DIR', oc_dir), \
                patch.object(F, 'CONFIG', cfg_path), \
                patch.object(F, 'LOG_TMP_DIRS', (str(tmp_logs),)):
            F._CACHE.pop('log_dir', None)
            assert F.log_dir() == str(state_logs)
            os.utime(old_tmp, (1700001000, 1700001000))
            F._CACHE.pop('log_dir', None)
            assert F.log_dir() == str(tmp_logs)
            F._CACHE.pop('log_dir', None)


ASTRA = 'openai/gpt-6.2-astra'
ASTRA_ACCOUNTS = (OA, 'openai:b', 'openai:default')
COOLDOWN_UNKNOWN = {'status': 'unknown', 'error': 'The connection check failed.'}
PREPARED_COOLDOWN = 'Prepared auth profiles are temporarily unavailable for ' + ASTRA + '.'


def cooldown_status(accounts, until):
    answer = models_status([(p, 'oauth') for p in ASTRA_ACCOUNTS])
    answer['auth']['unusableProfiles'] = [
        {'profileId': p, 'provider': 'openai', 'kind': 'cooldown', 'until': until,
         'remainingMs': until - ms(), 'reason': 'auth' if p == ASTRA_ACCOUNTS[-1] else 'rate_limit'} for p in accounts
    ]
    return answer


def astra_pin(account=OA, source='user'):
    return dict(PIN, providerOverride='openai', modelOverride=ASTRA.split('/')[1],
                authProfileOverride=account, authProfileOverrideSource=source)


def test_auth_cooldown_unknown_fails_over_continues_and_returns_on_the_next_pass():
    a, b, c = ASTRA_ACCOUNTS
    user, auto, strict = ('agent:main:dashboard:' + name for name in ('user-fixture', 'auto-fixture', 'strict-fixture'))
    entries = {user: astra_pin(a), auto: astra_pin(a, 'auto'), strict: astra_pin(a)}
    entries[auto]['modelOverrideSource'] = 'auto'
    # The embedded runner logs only the failure text. The stored terminal notice names the run but masks its cause.
    embedded = 'Embedded agent failed before reply: Auth profile "%s" is temporarily unavailable for %s.' % (b, ASTRA)
    entries[user].update(lastRunId='run-user', endedAt=ms() - 40_000, lastRunError=F.COOLDOWN_TERMINAL_NOTICE)
    logs = [line(embedded), line({
        'runId': 'run-auto', 'sessionKey': auto, 'provider': 'openai', 'model': ASTRA.split('/')[1],
        'message': 'Auth profile "%s" is temporarily unavailable for %s.' % (a, ASTRA),
        'code': 'auth_profile_cooldown',
    })]
    entries[strict].update(lastRunId='run-strict', endedAt=ms() - 40_000, lastRunError=PREPARED_COOLDOWN)
    probe = {'openai': dict.fromkeys(ASTRA_ACCOUNTS, COOLDOWN_UNKNOWN), 'anthropic': {PB: 'ok'}, 'xai': XAI_OUT}
    d = setup({'job': (ASTRA, a, 'trusted')}, [('job', 'error', PREPARED_COOLDOWN, 40)],
              probe=probe, entries=entries, log=logs)
    until = ms() + 43_200_000
    answer = cooldown_status(ASTRA_ACCOUNTS, until)
    answer['auth']['unusableProfiles'][-1]['classification'] = 'wham_token_expired'
    fake(d, models_status=answer, saved_orders={'main': {'openai': [c, b, a]}})
    prefer(d, {}, state={'parks': {k: {'n': 1, 't': ms()} for k in (user, auto)}})
    env = {'OCF_FAKE_VERSION': '2026.11.10', 'OCF_LOCAL_MODEL_CHECKS': '0', 'OCF_STRICT_CHATS': strict}

    rc, err, journal = go(d, env=env)
    assert rc == 0 and not err, (err, journal)
    opened, = [p for p in kinds(journal, 'probe') if p['provider'] == 'openai']
    assert opened['result'] == dict.fromkeys(ASTRA_ACCOUNTS, 'unknown'), opened
    assert opened['cooldowns'] == dict.fromkeys(ASTRA_ACCOUNTS, until), opened
    assert {(m['session'], m['to']) for m in kinds(journal, 'chat_move')} == {(user, OPUS + '@' + PB), (auto, OPUS + '@' + PB)}
    assert {(r['session'], r['to'], r['continue']) for r in kinds(journal, 'chat_recover')} == {
        (user, OPUS + '@' + PB, 'started'), (auto, OPUS + '@' + PB, 'started')}
    assert binding(d, 'job') == OPUS + '@' + PB
    state = state_of(d)
    assert not state['limited'] and all(state['chat_home'][k]['wait'] == F.HOLD_MS for k in (user, auto)), state

    probe['openai'][c] = {'status': 'ok', 'latencyMs': 4000}
    fake(d, probe=probe, models_status=cooldown_status((a, b), until))
    rc, err, journal = go(d, env=env)  # regular next pass, within the old return wait
    assert rc == 0 and not err and not requests(d), (err, journal)
    returned = {(r['session'], r['to'], r['result']) for r in kinds(journal, 'chat_return')}
    assert returned == {(user, ASTRA + '@' + c, 'ok'), (auto, ASTRA + '@' + c, 'ok')}, returned
    recovered = kinds(journal, 'chat_recover')
    assert len(recovered) == 3 and (recovered[-1]['session'], recovered[-1]['to'], recovered[-1]['continue']) == (
        strict, ASTRA + '@' + c, 'started'), recovered
    assert binding(d, 'job') == ASTRA + '@' + c
    assert state_of(d)['cooldowns'] == dict.fromkeys((a, b), until) and not state_of(d)['chat_home']
    assert all(e['modelOverride'] == ASTRA.split('/')[1] and e['authProfileOverride'] == c
               for e in json.loads((d / 'fake.json').read_text())['entries'].values())


def test_saved_order_change_probes_on_the_next_pass_and_moves_only_the_out_pin():
    a, b, c = ASTRA_ACCOUNTS
    out_pin, live_pin = 'agent:main:dashboard:out-fixture', 'agent:main:dashboard:live-fixture'
    d = setup(probe={'openai': dict.fromkeys(ASTRA_ACCOUNTS, 'ok')},
              entries={out_pin: astra_pin(a, 'auto'), live_pin: astra_pin(b)})
    fake(d, saved_orders={'main': {'openai': [a, b, c]}})
    assert not kinds(go(d)[2], 'chat_move')
    fake(d, saved_orders={'main': {'openai': [c, b, a]}}, models_status=cooldown_status((a,), ms() + 600_000),
         probe={'openai': {a: COOLDOWN_UNKNOWN, b: 'ok', c: 'ok'}})
    rc, err, journal = go(d)
    moved, = kinds(journal, 'chat_move')
    assert rc == 0 and (moved['session'], moved['to']) == (out_pin, ASTRA + '@' + c), (err, journal)
    assert kinds(journal, 'prefer_order')[-1]['order'] == [c, b, a]
    assert kinds(journal, 'prefer_order')[-1]['source'] == 'stored'
    assert not kinds(journal, 'chat_recover') and not requests(d)


def test_cooldown_remains_definite_until_expiry_or_a_fresh_ok():
    a, b, c = ASTRA_ACCOUNTS
    probe = {'openai': dict.fromkeys(ASTRA_ACCOUNTS, COOLDOWN_UNKNOWN), 'anthropic': DEAD, 'xai': XAI_OUT}
    d = setup({'job': (ASTRA, a, 'trusted')}, probe=probe)
    until = ms() + 600_000
    fake(d, models_status=cooldown_status((a, b), until))
    rc, err, _ = go(d)
    assert rc == 0 and not err   # a genuine connection failure for C still leaves the outage unproven
    fake(d, models_status=cooldown_status(ASTRA_ACCOUNTS, until))
    rc, err, _ = go(d)
    assert rc == 1 and 'no live account' in err
    fake(d, models_status_rc=1)
    rc, err, _ = go(d)
    assert rc == 1 and 'no live account' in err and state_of(d)['cooldowns'][a] == until

    fake(d, models_status_rc=0, probe=dict(probe, openai=dict(probe['openai'], **{c: 'ok'})))
    rc, err, _ = go(d)   # the CLI still lists C, but its fresh provider probe takes precedence
    assert rc == 0 and not err and binding(d, 'job') == ASTRA + '@' + c
    assert c not in state_of(d)['cooldowns']

    elapsed = ms() - 1000
    fake(d, probe=probe, models_status=cooldown_status(ASTRA_ACCOUNTS, elapsed))
    patch_state(d, cooldowns=dict.fromkeys(ASTRA_ACCOUNTS, elapsed))
    rc, err, _ = go(d)
    assert rc == 0 and not err and not state_of(d)['cooldowns']


def test_cooldown_receipts_prompt_probes_and_refusals_do_not():
    a, b, c = ASTRA_ACCOUNTS
    key = 'agent:main:dashboard:receipt-fixture'
    for error, is_outage in ((PREPARED_COOLDOWN, True),
                             ('Model not allowed: ' + ASTRA + ' | auth_profile_cooldown', False),
                             (ERR + ' | auth_profile_cooldown', False),
                             ('Embedded agent failed before reply: invalid request', False)):
        d = setup({'job': (ASTRA, a, 'trusted')}, [('job', 'error', error, 40)],
                  probe={'openai': {a: COOLDOWN_UNKNOWN, b: 'ok', c: 'ok'}},
                  entries={key: dict(astra_pin(), lastRunId='run-fixture', lastRunError=error, endedAt=ms() - 40_000)})
        if is_outage:   # only the cron receipt carries the outage signal in this case
            fake(d, entries={key: astra_pin()})
        fake(d, models_status=cooldown_status((a,), ms() + 600_000))
        prefer(d, {}, state={'probed_at': ms()})
        rc, err, journal = go(d)
        assert rc == 0 and not err and not kinds(journal, 'chat_recover'), (err, journal)
        assert bool(kinds(journal, 'probe')) == is_outage
        assert binding(d, 'job') == ASTRA + '@' + (c if is_outage else a)
        assert not state_of(d)['limited']


def test_selfcheck_requires_the_cooldown_error_and_log_anchors():
    d = setup()
    removed = (*F.COOLDOWN_TEXTS, F.EMBEDDED_FAILED)
    pkg_strings(d / 'pkg', tuple(a for a in F.ANCHORS + F.ERROR_TEXTS if a not in removed) +
                (F.RPC_PROBE, F.RPC_DESCRIBE, F.RPC_PATCH, F.RPC_ABORT))
    rc, err, journal = go(d)
    assert rc == 1 and all(a in err for a in removed), (err, journal)


if __name__ == '__main__':
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    for t in tests:
        t()
        print('ok', t.__name__)
    print('%d passed' % len(tests))
