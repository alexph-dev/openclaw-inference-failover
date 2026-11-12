#!/usr/bin/env python3
"""Fixture tests for failover.py: no Gateway, no real edits, no messages.
Run: python3 test_failover.py"""
import functools, json, os, pathlib, sqlite3, subprocess, sys, tempfile, time
from unittest.mock import patch

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
import failover as F                                                  # names of the OpenClaw surface only


def offline(test):
    """Unit tests that mock APPLY must never write to the operator's actual state/journal."""
    @functools.wraps(test)
    def wrapped():
        with tempfile.TemporaryDirectory(prefix='ocf-unit-') as tmp:
            d = pathlib.Path(tmp)
            with patch.object(F, 'SDIR', d), patch.object(F, 'STATE', d / 'state.json'), \
                    patch.object(F, 'JOURNAL', d / 'journal.jsonl'):
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
               OCF_FAKE=str(d / 'fake.json'), OCF_DIST=str(d / 'pkg' / 'dist'), OCF_OPENCLAW_DIR=str(d / 'oc'), OCF_FAKE_CALLS=str(d / 'calls.log'),
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


def test_failed_continuation_is_reported():
    d = setup(probe={'anthropic': {PB: 'ok'}})
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text(json.dumps({'continued': {'ocf-continue-1': K, 'ocf-continue-2': K}, 'probed_at': ms()}))
    fake(d, units={'ocf-continue-1': 'failed', 'ocf-continue-2': 'active'})
    rc, err, j = go(d)
    assert rc == 1 and 'continued turn failed' in err and 'ocf-continue-1' in err
    assert list(json.loads((d / 'st' / 'state.json').read_text())['continued']) == ['ocf-continue-2']


def test_log_format_drift_after_an_update_is_reported():
    d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'ok', PC: 'ok'}})
    pkg_strings(d / 'pkg', tuple(s for s in F.ANCHORS + F.ERROR_TEXTS if s != F.EV_END))
    rc, err, j = go(d, False, '--check')
    assert rc == 1 and 'anchor "embedded_run_agent_end"' in err and 'Model request failed' not in err, err


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


def test_second_park_within_a_day_waits_before_going_home():
    d = setup(JOBS, [], {'anthropic': DEAD, 'xai': {'xai:x': 'ok'}})
    (d / 'st').mkdir()
    (d / 'st' / 'state.json').write_text(json.dumps({'parks': {'a': {'n': 1, 't': ms() - 3600_000}}}))
    go(d)                                                             # a: second park today, b: first
    fake(d, probe={'anthropic': {PB: 'rate_limit', PA: 'ok', PC: 'rate_limit'}, 'xai': {'xai:x': 'ok'}})
    go(d, True)
    assert binding(d, 'b') == OPUS + '@' + PA and binding(d, 'a') == 'xai/grok-4.7@xai:x'


def test_slow_gateway_pass_is_cut_off_and_saved_before_the_job_timeout():
    d = setup(JOBS, [], {'anthropic': {PB: 'ok', PA: 'rate_limit', PC: 'rate_limit'}})
    fake(d, slow_probe=5)
    t = time.time()
    rc, err, j = go(d, False, env={'OCF_PASS_HARD_S': '2'})
    assert rc == 1 and 'cut off' in err and time.time() - t < 5, (rc, err)
    assert any(k.startswith('slow') for k in json.loads((d / 'st' / 'state.json').read_text())['problems'])


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
    # without an auth.order entry yields no target at all, not a guessed account.
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


if __name__ == '__main__':
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    for t in tests:
        t()
        print('ok', t.__name__)
    print('%d passed' % len(tests))
