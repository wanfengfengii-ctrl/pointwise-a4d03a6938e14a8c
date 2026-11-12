#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""计量数仓质量判定与证据（偏序时间增量关联 × 历史压缩）。

子命令：
  verify       重跑原断言与全部质量验证，重算并核对 evidence.json；
               发现未解决业务偏差或证据不符时非零退出并保留失败材料。
  demo         真实展示压缩前后、重开恢复与旧请求重放的结果。
  record       运行完整套件并把证据写入 evidence.json（交付基线）。
  replay-case  重放留存的失败用例文件，核对同一语义偏差是否复现。

公开判定入口（合同要求）：
  certify(history, snapshot)                → 等价证书或反例（可 JSON 往返）
  check_certificate(history, snapshot, result) → 独立重算核对

判定器只使用公开输入、inspect 返回数据与 qasem 中自行实现的语义；
不导入或复制生产的前沿、压缩、聚合函数（lakeflow 仅作为被测对象调用）。
"""

import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import os
import platform
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import tracemalloc

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import qagen
import qasem
import lakeflow
from lakeflow import FlowError, Store

SCHEMA = 1
CROSS_CHECK_BUDGET = 20_000_000  # 前缀权重交叉自检的初等运算预算

DERIVATION = (
    '合法截点 q 满足 since 某元素≤q，即 q∈↑since。本证书证明：快照迹与历史原始'
    '记录在 ↑since 的每个截点上逐（端口,行）前缀权重相等——经由上集生成元范式'
    '（记录时间 t 映为反链 {t∨s : s∈since}，按（端口,生成元,行）合并权重去零）'
    '逐点相等；有限偏序集上前缀和与系数相互唯一决定，故范式相等当且仅当全部'
    '截点前缀权重相等。read 在截点 q 的输出完全由各行前缀权重决定：L 每 key 的'
    '重数与金额加权和、R 每 (key,region) 的重数，经整数双线性关联（同 key 重数'
    '相乘、按 region 求和）得到 rows。前缀权重逐项相等蕴含同一合法截点的关联'
    '输出逐项相等，故压缩前后全部合法查询截点的账一致。'
)


# ---------------------------------------------------------------- 判定核心

def _digest(obj):
    return 'sha256:' + hashlib.sha256(
        qasem.canon(obj).encode('utf-8')).hexdigest()


def _snapshot_parts(snapshot):
    """结构校验并取出 (维数, state, 快照记录流, 回执表)。"""
    if not isinstance(snapshot, dict) or set(snapshot) != {'state', 'receipts'}:
        raise qasem.ReplayError('snapshot.shape', '恰含 state/receipts 的对象',
                                sorted(snapshot) if isinstance(snapshot, dict)
                                else type(snapshot).__name__)
    st = snapshot['state']
    need = {'dimensions', 'generation', 'since', 'upper', 'traces'}
    if not isinstance(st, dict) or not need <= set(st):
        raise qasem.ReplayError('snapshot.state', '含 %s 的对象' % sorted(need), st)
    d = st['dimensions']
    if not qasem.strict_int(d) or not 2 <= d <= 6:
        raise qasem.ReplayError('state.dimensions', '2..6 的整数', d)
    if not qasem.strict_int(st['generation']) or st['generation'] < 0:
        raise qasem.ReplayError('state.generation', '非负整数', st['generation'])
    try:
        qasem.check_front(st['since'], d, allow_empty=False)
    except qasem.SemanticsError:
        raise qasem.ReplayError('state.since', '非空反链', st['since'])
    up = st['upper']
    if not isinstance(up, dict) or set(up) != {'L', 'R'}:
        raise qasem.ReplayError('state.upper', '恰含 L/R 的对象', up)
    for p in ('L', 'R'):
        try:
            qasem.check_front(up[p], d, allow_empty=True)
        except qasem.SemanticsError:
            raise qasem.ReplayError('state.upper.' + p, '反链或空前沿', up[p])
    tr = st['traces']
    if not isinstance(tr, dict) or set(tr) != {'L', 'R'}:
        raise qasem.ReplayError('state.traces', '恰含 L/R 的对象', tr)
    records = []
    for p in ('L', 'R'):
        if not isinstance(tr[p], list):
            raise qasem.ReplayError('state.traces.' + p, '记录数组', tr[p])
        for rec in tr[p]:
            ok = (isinstance(rec, list) and len(rec) == 3
                  and qasem.is_time(rec[0], d) and qasem.strict_int(rec[2]))
            if ok:
                row = rec[1]
                ok = (isinstance(row, list) and len(row) == 2
                      and isinstance(row[0], str) and row[0]
                      and ((p == 'L' and qasem.strict_int(row[1]))
                           or (p == 'R' and isinstance(row[1], str))))
            if not ok:
                raise qasem.ReplayError('state.traces.' + p,
                                        '[时间,行,整数权重]', rec)
            records.append((p, list(rec[0]), list(rec[1]), rec[2]))
    receipts = snapshot['receipts']
    if not isinstance(receipts, list):
        raise qasem.ReplayError('snapshot.receipts', '数组', receipts)
    by_id = {}
    for entry in receipts:
        if (not isinstance(entry, list) or len(entry) != 3
                or not isinstance(entry[0], str) or not entry[0]
                or not isinstance(entry[1], dict)
                or set(entry[1]) != {'changes', 'upper', 'since'}
                or not isinstance(entry[2], dict)
                or set(entry[2]) != {'request', 'generation'}):
            raise qasem.ReplayError('receipt.shape', '[请求号,身份载荷,响应]', entry)
        if entry[0] in by_id:
            raise qasem.ReplayError('receipt.duplicate', '请求号唯一', entry[0])
        by_id[entry[0]] = entry
    return d, st, records, by_id


def _evaluate(history, snapshot):
    """重放历史并独立重算元数据与范式；结构/元数据不符抛 ReplayError。"""
    if not isinstance(history, list) or any(not isinstance(r, dict) for r in history):
        raise qasem.ReplayError('history.shape', '请求对象数组', None)
    d, st, snap_records, receipts = _snapshot_parts(snapshot)
    meta, raw_records = qasem.replay(history, d)
    if st['generation'] != meta['generation']:
        raise qasem.ReplayError('state.generation', meta['generation'],
                                st['generation'])
    if qasem.canon(st['since']) != qasem.canon(meta['since']):
        raise qasem.ReplayError('state.since', meta['since'], st['since'])
    if qasem.canon(st['upper']) != qasem.canon(meta['upper']):
        raise qasem.ReplayError('state.upper', meta['upper'], st['upper'])
    for i, req in enumerate(history):
        rid = req['request']
        if rid not in receipts:
            raise qasem.ReplayError('receipt.missing', rid, None)
        payload, response = receipts[rid][1], receipts[rid][2]
        ident = {k: req[k] for k in ('changes', 'upper', 'since')}
        if qasem.canon(payload) != qasem.canon(ident):
            raise qasem.ReplayError('receipt.payload', ident, payload)
        want = {'request': rid, 'generation': i + 1}
        if response != want:
            raise qasem.ReplayError('receipt.response', want, response)
    extra = sorted(set(receipts) - {r['request'] for r in history})
    if extra:
        raise qasem.ReplayError('receipt.extra', '回执集合与历史一致', extra)
    return {'dimensions': d, 'meta': meta, 'raw': raw_records,
            'snap': snap_records,
            'nf_raw': qasem.normal_form(raw_records, meta['since']),
            'nf_snap': qasem.normal_form(snap_records, meta['since'])}


def _first_divergence(ev):
    """范式不等时，在范式支持集截点上定位首个前缀权重分歧（保证找到）。"""
    for port in ('L', 'R'):
        cuts = sorted({qasem.canon(e[1])
                       for e in ev['nf_raw'] + ev['nf_snap'] if e[0] == port})
        for cu in cuts:
            u = json.loads(cu)
            pm_raw = qasem.prefix_weights(ev['raw'], port, u)
            pm_snap = qasem.prefix_weights(ev['snap'], port, u)
            if pm_raw != pm_snap:
                for rk in sorted(set(pm_raw) | set(pm_snap)):
                    a = pm_raw.get(rk, (None, 0))[1]
                    b = pm_snap.get(rk, (None, 0))[1]
                    if a != b:
                        row = pm_raw.get(rk, pm_snap.get(rk))[0]
                        return {'port': port, 'row': row, 'cut': u,
                                'expected_weight': a, 'actual_weight': b,
                                'since': ev['meta']['since']}
    raise AssertionError('范式不等但未找到分歧截点')


def _cross_check(ev):
    """在范式支持集截点上逐一核对前缀权重（预算内穷举，判定器自检）。"""
    cuts_by_port = {}
    ops = 0
    for port in ('L', 'R'):
        cuts = sorted({qasem.canon(e[1]) for e in ev['nf_raw'] if e[0] == port})
        n = max(1, sum(1 for r in ev['raw'] if r[0] == port))
        ops += len(cuts) * n
        cuts_by_port[port] = cuts
    if ops > CROSS_CHECK_BUDGET:
        return {'status': 'skipped-budget', 'cuts': 0, 'ok': None}
    total = 0
    for port in ('L', 'R'):
        for cu in cuts_by_port[port]:
            total += 1
            u = json.loads(cu)
            if (qasem.prefix_weights(ev['raw'], port, u)
                    != qasem.prefix_weights(ev['snap'], port, u)):
                return {'status': 'exhaustive', 'cuts': total, 'ok': False}
    return {'status': 'exhaustive', 'cuts': total, 'ok': True}


def certify(history, snapshot):
    """对全部合法查询截点给出压缩等价证书，或给出定位反例。

    history：按首次提交顺序排列的成功请求（不含重放）；
    snapshot：inspect 返回对象。返回值可 JSON 往返。
    """
    try:
        ev = _evaluate(history, snapshot)
    except qasem.ReplayError as e:
        return {'kind': 'metadata', 'schema': SCHEMA, 'field': e.field,
                'expected': e.expected, 'actual': e.actual}
    if ev['nf_raw'] != ev['nf_snap']:
        ce = _first_divergence(ev)
        ce.update({'kind': 'counterexample', 'schema': SCHEMA})
        return ce
    cross = _cross_check(ev)
    if cross['status'] == 'exhaustive' and not cross['ok']:
        raise AssertionError('判定器自检矛盾：范式相等但前缀权重不等')
    meta = ev['meta']
    nfs = {'L': [e[1:] for e in ev['nf_raw'] if e[0] == 'L'],
           'R': [e[1:] for e in ev['nf_raw'] if e[0] == 'R']}
    cert = {
        'kind': 'certificate', 'schema': SCHEMA, 'equivalent': True,
        'dimensions': ev['dimensions'], 'generation': meta['generation'],
        'since': meta['since'], 'upper': meta['upper'],
        'normal_forms': nfs,
        'raw_records': {'L': sum(1 for r in ev['raw'] if r[0] == 'L'),
                        'R': sum(1 for r in ev['raw'] if r[0] == 'R')},
        'history_digest': _digest(history),
        'snapshot_digest': _digest(snapshot),
        'cross_check': {'status': cross['status'], 'cuts': cross['cuts']},
        'derivation': DERIVATION,
    }
    cert['certificate_digest'] = _digest(
        {k: cert[k] for k in ('dimensions', 'generation', 'since', 'upper',
                              'normal_forms')})
    return cert


def check_certificate(history, snapshot, result):
    """独立重算核对判定结果；不读取 equivalent 标志作结论。"""
    try:
        rt = json.loads(json.dumps(result, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError):
        return False
    if rt != result or not isinstance(result, dict) or result.get('schema') != SCHEMA:
        return False
    kind = result.get('kind')
    try:
        again = certify(history, snapshot)
    except Exception:
        return False
    if again.get('kind') != kind:
        return False
    if kind == 'certificate':
        for field in ('dimensions', 'generation', 'since', 'upper',
                      'normal_forms', 'raw_records', 'history_digest',
                      'snapshot_digest', 'cross_check', 'derivation',
                      'certificate_digest'):
            if result.get(field) != again.get(field):
                return False
        return again.get('equivalent') is True
    if kind == 'counterexample':
        try:
            ev = _evaluate(history, snapshot)
        except qasem.ReplayError:
            return False
        port, row, cut = result.get('port'), result.get('row'), result.get('cut')
        if port not in ('L', 'R') or not qasem.is_time(cut, ev['dimensions']):
            return False
        if not qasem.admitted(ev['meta']['since'], cut):
            return False
        if not (isinstance(row, list) and len(row) == 2):
            return False
        key = qasem.canon(row)
        exp = qasem.prefix_weights(ev['raw'], port, cut).get(key, (row, 0))[1]
        act = qasem.prefix_weights(ev['snap'], port, cut).get(key, (row, 0))[1]
        return (exp == result.get('expected_weight')
                and act == result.get('actual_weight') and exp != act)
    if kind == 'metadata':
        return result == again
    return False


# ---------------------------------------------------------------- 差分运行器

def _probe_read(mod, store, meta, raw_records, q):
    devs = []
    st = qasem.cut_status(meta['upper'], meta['since'], q)
    try:
        out = store.read(q)
        err = None
    except mod.FlowError as e:
        out, err = None, str(e)
    if st != 'ok':
        if err is None or err.lower() != st:
            devs.append({'type': 'read', 'cut': q, 'expected': st,
                         'actual': err if err else out})
        return devs
    if err is not None:
        devs.append({'type': 'read', 'cut': q, 'expected': 'ok', 'actual': err})
        return devs
    want = qasem.join_rows(raw_records, q)
    if out.get('generation') != meta['generation'] or out.get('rows') != want:
        devs.append({'type': 'read', 'cut': q,
                     'expected': {'generation': meta['generation'], 'rows': want},
                     'actual': out})
    return devs


def _replay_probes(mod, store, history, dims):
    devs = []
    idx = len(history) // 2
    req = history[idx]
    want = {'request': req['request'], 'generation': idx + 1}
    before = qasem.canon(store.inspect())
    r1 = copy.deepcopy(req)
    r1['expected'] = 999  # 旧 expected 不影响回执
    try:
        resp = store.apply(r1)
        if resp != want:
            devs.append({'type': 'replay', 'expected': want, 'actual': resp})
    except mod.FlowError as e:
        devs.append({'type': 'replay', 'expected': want, 'actual': str(e)})
    r2 = copy.deepcopy(req)
    r2['changes'] = r2['changes'] + [['L', [0] * dims, ['conflict-probe', 0], 1]]
    try:
        store.apply(r2)
        devs.append({'type': 'conflict', 'expected': 'REQUEST_CONFLICT',
                     'actual': 'accepted'})
    except mod.FlowError as e:
        if str(e) != 'REQUEST_CONFLICT':
            devs.append({'type': 'conflict', 'expected': 'REQUEST_CONFLICT',
                         'actual': str(e)})
    if qasem.canon(store.inspect()) != before:
        devs.append({'type': 'replay_state_changed'})
    return devs


def _negative_probes(mod, store, meta, dims):
    devs = []
    g = meta['generation']
    zero = [0] * dims
    probes = [
        ({'request': 'neg-generation', 'expected': g + 1, 'changes': [],
          'upper': {}, 'since': None}, 'GENERATION'),
        ({'request': 'neg-antichain', 'expected': g, 'changes': [],
          'upper': {'L': [zero, zero]}, 'since': None}, 'ANTICHAIN'),
        ({'request': 'neg-envelope', 'expected': g, 'changes': [],
          'upper': {}, 'since': None, 'extra': 1}, 'REQUEST'),
    ]
    if meta['upper']['L']:
        top = [max(u[j] for u in meta['upper']['L']) + 1 for j in range(dims)]
        t0 = meta['upper']['L'][0]
        probes += [
            ({'request': 'neg-since', 'expected': g, 'changes': [],
              'upper': {}, 'since': [top]}, 'SINCE'),
            ({'request': 'neg-weight', 'expected': g,
              'changes': [['L', t0, ['nk', 1], 0]], 'upper': {},
              'since': None}, 'WEIGHT'),
            ({'request': 'neg-row', 'expected': g,
              'changes': [['L', t0, ['nk', 'x'], 1]], 'upper': {},
              'since': None}, 'ROW'),
            ({'request': 'neg-bool', 'expected': g,
              'changes': [['L', [True] * dims, ['nk', 1], 1]], 'upper': {},
              'since': None}, 'TIME'),
            ({'request': 'neg-port', 'expected': g,
              'changes': [['X', t0, ['nk', 1], 1]], 'upper': {},
              'since': None}, 'PORT'),
        ]
    if not qasem.admitted(meta['upper']['L'], zero):
        probes.append(({'request': 'neg-late', 'expected': g,
                       'changes': [['L', zero, ['nk', 1], 1]], 'upper': {},
                       'since': None}, 'LATE'))
    for req, code in probes:
        before = qasem.canon(store.inspect())
        try:
            store.apply(req)
            devs.append({'type': 'negative', 'probe': req['request'],
                         'expected': code, 'actual': 'accepted'})
        except mod.FlowError as e:
            if str(e) != code:
                devs.append({'type': 'negative', 'probe': req['request'],
                             'expected': code, 'actual': str(e)})
        if qasem.canon(store.inspect()) != before:
            devs.append({'type': 'negative_state_changed',
                         'probe': req['request']})
    return devs


def evaluate_history(mod, history, dims, probe_cuts=None, db_path=None,
                     do_reopen=False, do_replay=True, do_negative=True):
    """对给定历史运行差分检查，返回 (deviations, info)。mod 为生产或变异模块。"""
    deviations = []
    meta, raw_records = qasem.replay(history, dims)
    store = mod.Store(db_path if db_path else ':memory:', dimensions=dims)
    responses = []
    for i, req in enumerate(history):
        try:
            resp = store.apply(req)
        except mod.FlowError as e:
            deviations.append({'type': 'apply_rejected', 'index': i,
                               'error': str(e)})
            store.close()
            return deviations, {'meta': meta}
        responses.append(resp)
        want = {'request': req['request'], 'generation': i + 1}
        if resp != want:
            deviations.append({'type': 'response', 'index': i,
                               'expected': want, 'actual': resp})
    for q in (probe_cuts or []):
        deviations.extend(_probe_read(mod, store, meta, raw_records, q))
    snap = store.inspect()
    cert = certify(history, snap)
    if cert['kind'] != 'certificate':
        deviations.append({'type': 'certificate', 'result': cert})
    elif not check_certificate(history, snap, cert):
        deviations.append({'type': 'check_certificate'})
    info = {'meta': meta, 'snapshot': snap, 'certificate': cert,
            'responses': responses}
    if do_replay and history:
        deviations.extend(_replay_probes(mod, store, history, dims))
    if do_negative:
        deviations.extend(_negative_probes(mod, store, meta, dims))
    if do_reopen and db_path:
        store.close()
        store = mod.Store(db_path, dimensions=dims)
        if qasem.canon(store.inspect()) != qasem.canon(snap):
            deviations.append({'type': 'reopen'})
        info['reopened'] = True
    store.close()
    return deviations, info


def probe_cuts_for(meta, raw_records, dims, rng, count):
    pool = [list(t) for _, t, _, _ in raw_records]
    pool += [list(s) for s in meta['since']]
    for f in meta['upper'].values():
        pool += [list(u) for u in f]
    cuts = []
    if pool:
        for _ in range(count // 2):
            a = rng.choice(pool)
            if rng.random() < 0.5:
                cuts.append(qasem.join2(a, rng.choice(pool)))
            else:
                cuts.append(list(a))
    maxc = max([max(t) for t in pool] + [1]) + 1 if pool else 2
    while len(cuts) < count:
        cuts.append([rng.randint(0, maxc) for _ in range(dims)])
    return cuts[:count]


def grid_cuts_for(meta, raw_records, dims, cap=4000):
    vals = [set() for _ in range(dims)]
    for _, t, _, _ in raw_records:
        for j, x in enumerate(t):
            vals[j].add(x)
    for f in (meta['since'], meta['upper']['L'], meta['upper']['R']):
        for t in f:
            for j, x in enumerate(t):
                vals[j].add(x)
    lists = []
    for j in range(dims):
        xs = sorted(vals[j]) or [0]
        lists.append(xs + [xs[-1] + 1])
    grid = qasem.grid_points(lists)
    if len(grid) > cap:
        raise ValueError('小域网格过大: %d' % len(grid))
    return grid


def exhaustive_case(mod, history, dims):
    """小域穷举：网格全部截点上互核证书、前缀权重与真实 read。"""
    meta, raw = qasem.replay(history, dims)
    grid = grid_cuts_for(meta, raw, dims)
    store = mod.Store(':memory:', dimensions=dims)
    devs = []
    for i, req in enumerate(history):
        try:
            store.apply(req)
        except mod.FlowError as e:
            devs.append({'type': 'apply_rejected', 'index': i, 'error': str(e)})
            store.close()
            return devs, {'grid_cuts': len(grid)}
    snap = store.inspect()
    _, _, snap_records, _ = _snapshot_parts(snap)
    for q in grid:
        devs.extend(_probe_read(mod, store, meta, raw, q))
        st = qasem.cut_status(meta['upper'], meta['since'], q)
        if st == 'ok':
            a = qasem.join_rows(raw, q)
            b = qasem.join_rows_explicit(raw, q)
            if a != b:
                devs.append({'type': 'join_constructions', 'cut': q})
        if st != 'compacted':
            for port in ('L', 'R'):
                if (qasem.prefix_weights(raw, port, q)
                        != qasem.prefix_weights(snap_records, port, q)):
                    devs.append({'type': 'prefix', 'cut': q, 'port': port})
    cert = certify(history, snap)
    if cert['kind'] != 'certificate':
        devs.append({'type': 'certificate', 'result': cert})
    elif not check_certificate(history, snap, cert):
        devs.append({'type': 'check_certificate'})
    store.close()
    return devs, {'grid_cuts': len(grid), 'certificate_digest':
                  cert.get('certificate_digest'),
                  'cross_check': cert.get('cross_check')}


# ---------------------------------------------------------------- 并发

def _write_json(path, obj):
    with open(path, 'w') as f:
        json.dump(obj, f, ensure_ascii=False)


def _wait_file(path, timeout):
    t0 = time.monotonic()
    while not os.path.exists(path):
        if time.monotonic() - t0 > timeout:
            raise TimeoutError(path)
        time.sleep(0.02)


def _spawn(args):
    return subprocess.Popen([sys.executable,
                             os.path.join(HERE, 'qa_worker.py')] + args,
                            cwd=HERE)


def concurrency_suite(work):
    os.makedirs(work, exist_ok=True)
    # ---- 屏障编排的代次竞争：A 持写事务阻塞，B 到达后重试 ----
    db = os.path.join(work, 'race.db')
    base_req = {'request': 'base', 'expected': 0, 'changes': [],
                'upper': {}, 'since': None}
    s = Store(db)
    s.apply(base_req)
    s.close()
    req_a = {'request': 'race-a', 'expected': 1,
             'changes': [['L', [0, 0], ['ra', 1], 1]], 'upper': {}, 'since': None}
    req_b = {'request': 'race-b', 'expected': 1,
             'changes': [['L', [0, 0], ['rb', 2], 1]], 'upper': {}, 'since': None}
    pa_file = os.path.join(work, 'race-a.json')
    pb_file = os.path.join(work, 'race-b.json')
    _write_json(pa_file, req_a)
    _write_json(pb_file, req_b)
    ready_a = os.path.join(work, 'race-a.ready')
    release_a = os.path.join(work, 'race-a.release')
    out_a = os.path.join(work, 'race-a.out.json')
    out_b = os.path.join(work, 'race-b.out.json')
    proc_a = _spawn(['race-a', db, pa_file, ready_a, release_a, out_a])
    _wait_file(ready_a, 30)
    proc_b = _spawn(['race-b', db, pb_file, out_b])
    time.sleep(1.0)  # 让 B 在 A 持有写锁期间到达
    _write_json(release_a, {'go': True})
    proc_a.wait(30)
    proc_b.wait(30)
    a = json.load(open(out_a))
    b = json.load(open(out_b))
    s = Store(db)
    snap = s.inspect()
    replay = s.apply(base_req)  # 旧回执不得回退进度
    gen_after = s.inspect()['state']['generation']
    s.close()
    race_ok = (a.get('response') == {'request': 'race-a', 'generation': 2}
               and b.get('first') == 'GENERATION'
               and b.get('retry', {}).get('response')
               == {'request': 'race-b', 'generation': 3}
               and snap['state']['generation'] == 3
               and replay == {'request': 'base', 'generation': 1}
               and gen_after == 3)
    race = {'ok': race_ok, 'a': a, 'b': b,
            'generation': snap['state']['generation'], 'replay': replay}

    # ---- 自由竞争：多线程独立连接，核对实际历史可解释 ----
    db2 = os.path.join(work, 'free.db')
    # 先由父进程完成 WAL 初始化，避免建库竞态（SQLite 层瞬态，非业务语义）
    Store(db2).close()
    n_threads, per = 4, 5
    results = [[] for _ in range(n_threads)]
    errors = [[] for _ in range(n_threads)]

    def worker(w):
        s = Store(db2)
        k = 0
        while k < per:
            gen = s.inspect()['state']['generation']
            req = {'request': 'w%d-%d' % (w, k), 'expected': gen,
                   'changes': [['L', [0, 0], ['w%d' % w, 1], 1]],
                   'upper': {}, 'since': None}
            t0 = time.monotonic_ns()
            try:
                resp = s.apply(req)
                results[w].append({'start': t0, 'end': time.monotonic_ns(),
                                   'generation': resp['generation'],
                                   'id': req['request']})
                k += 1
            except FlowError as e:
                if str(e) != 'GENERATION':
                    errors[w].append(str(e))
                    k += 1
                time.sleep(0.01)
        s.close()

    threads = [threading.Thread(target=worker, args=(w,))
               for w in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    allrec = [r for rs in results for r in rs]
    violations = 0
    for x in allrec:
        for y in allrec:
            if x['end'] < y['start'] and not x['generation'] < y['generation']:
                violations += 1
    s = Store(db2)
    snap2 = s.inspect()
    s.close()
    total = n_threads * per
    free_ok = (not any(errors) and len(allrec) == total
               and snap2['state']['generation'] == total
               and len(snap2['receipts']) == total
               and violations == 0
               and sorted(r['generation'] for r in allrec)
               == list(range(1, total + 1)))
    free = {'ok': free_ok, 'threads': n_threads, 'per_thread': per,
            'successes': len(allrec), 'generation': snap2['state']['generation'],
            'receipts': len(snap2['receipts']),
            'ordering_violations': violations, 'errors': any(errors) and errors or [],
            'calls': allrec}
    return {'race': race, 'free_run': free,
            'ok': race_ok and free_ok}


# ---------------------------------------------------------------- 故障

def fault_suite(work):
    os.makedirs(work, exist_ok=True)
    cases = []
    for point in ('after_begin', 'after_state', 'after_receipt',
                  'after_commit', 'idle'):
        db = os.path.join(work, 'fault-%s.db' % point)
        base = [
            {'request': 'f0-%s' % point, 'expected': 0,
             'changes': [['L', [0, 0], ['fa', 2], 1]],
             'upper': {'L': [[1, 1]]}, 'since': None},
            {'request': 'f1-%s' % point, 'expected': 1,
             'changes': [['R', [0, 0], ['fa', 'e'], 1]],
             'upper': {'R': [[1, 1]]}, 'since': None},
        ]
        s = Store(db)
        for r in base:
            s.apply(r)
        s.close()
        victim = {'request': 'victim-%s' % point, 'expected': 2,
                  'changes': [['L', [1, 1], ['fb', 3], 1]],
                  'upper': {'L': [[2, 2]]}, 'since': None}
        req_file = os.path.join(work, 'victim-%s.json' % point)
        _write_json(req_file, victim)
        ready = os.path.join(work, 'victim-%s.ready' % point)
        release = os.path.join(work, 'victim-%s.release' % point)  # 永不创建
        out = os.path.join(work, 'victim-%s.out.json' % point)
        if point == 'idle':
            proc = _spawn(['idle', db, req_file, ready, out])
        else:
            proc = _spawn(['killpoint', db, req_file, point, ready, release, out])
        _wait_file(ready, 30)
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()
        response_lost = not os.path.exists(out)
        # 重开后核对：状态、回执、迹的联合结果须恰好居两种原子结局之一
        s = Store(db)
        snap = s.inspect()
        committed = certify(base + [victim], snap)['kind'] == 'certificate'
        aborted = certify(base, snap)['kind'] == 'certificate'
        atomic = committed != aborted
        expect_committed = point in ('after_commit', 'idle')
        # 重放受害者请求：已提交则返回旧回执且不再触发 after_commit
        log = []
        resp = s.apply(victim, lambda pt: log.append(pt))
        want_resp = {'request': victim['request'], 'generation': 3}
        if committed:
            reapply_ok = resp == want_resp and log == ['after_begin']
        else:
            reapply_ok = (resp == want_resp and log ==
                          ['after_begin', 'after_state', 'after_receipt',
                           'after_commit'])
        replay = s.apply(base[0])
        gen_now = s.inspect()['state']['generation']
        no_rollback = (replay == {'request': base[0]['request'], 'generation': 1}
                       and gen_now == 3)
        s.close()
        cases.append({
            'kill_point': point,
            'expect': 'committed' if expect_committed else 'aborted',
            'outcome': 'committed' if committed else (
                'aborted' if aborted else 'broken'),
            'atomic': atomic,
            'outcome_ok': committed == expect_committed,
            'response_lost': response_lost,
            'reapply_ok': reapply_ok,
            'no_rollback': no_rollback,
        })
    ok = all(c['atomic'] and c['outcome_ok'] and c['reapply_ok']
             and c['no_rollback'] for c in cases)
    return {'ok': ok, 'cases': cases}


# ---------------------------------------------------------------- 变异

def _mut(old, new):
    def fn(src):
        assert src.count(old) == 1, '变异定位失败: %r' % old
        return src.replace(old, new, 1)
    return fn


def _t_advance_sup():
    return ([{'request': 't0', 'expected': 0,
              'changes': [['L', [1, 0], ['k', 5], 1]],
              'upper': {'L': [[2, 1], [1, 2]], 'R': [[2, 1], [1, 2]]},
              'since': None},
             {'request': 't1', 'expected': 1, 'changes': [], 'upper': {},
              'since': [[2, 1], [1, 2]]}],
            2, [[0, 0], [1, 1], [2, 1], [1, 2], [2, 2]])


def _t_weight_abs():
    return ([{'request': 't0', 'expected': 0,
              'changes': [['L', [0, 0], ['k', 5], -1]],
              'upper': {'L': [[1, 1]]}, 'since': None}],
            2, [[0, 0], [1, 1]])


def _t_drop_total_only():
    return ([{'request': 't0', 'expected': 0,
              'changes': [['L', [0, 0], ['k', 10], 1],
                          ['L', [0, 0], ['k', 20], -1],
                          ['R', [0, 0], ['k', 'e'], 1]],
              'upper': {'L': [[1, 1]], 'R': [[1, 1]]}, 'since': None}],
            2, [[0, 0], [1, 1]])


def _t_generation():
    return _t_weight_abs()


def _t_incomplete_all():
    return ([{'request': 't0', 'expected': 0,
              'changes': [['L', [0, 0], ['k', 7], 1]],
              'upper': {'L': [[0, 0]], 'R': [[1, 1]]}, 'since': None}],
            2, [[0, 0], [0, 1], [1, 0]])


def _t_replay_blind():
    return _t_weight_abs()


def _t_le_strict():
    return _t_weight_abs()


def _t_keep_zero():
    return ([{'request': 't0', 'expected': 0,
              'changes': [['L', [0, 0], ['k', 5], 1],
                          ['L', [0, 0], ['k', 5], -1]],
              'upper': {'L': [[1, 1]]}, 'since': None}],
            2, [[0, 0], [1, 1]])


MUTANTS = [
    {'id': 'advance-sup',
     'desc': '压缩推进取逐分量上确界(max)而非下确界(min)',
     'expect': 'certificate',
     'fn': _mut('return [min(max(t[j], x[j]) for x in f) for j in range(len(t))]',
                'return [max(max(t[j], x[j]) for x in f) for j in range(len(t))]'),
     'trigger': _t_advance_sup},
    {'id': 'weight-abs',
     'desc': '合并权重时取绝对值，丢弃负号（抵消失效）',
     'expect': 'certificate',
     'fn': _mut('weights[k] = weights.get(k, 0) + w',
                'weights[k] = weights.get(k, 0) + abs(w)'),
     'trigger': _t_weight_abs},
    {'id': 'drop-total-only',
     'desc': 'read 丢弃 count=0 但 total≠0 的分组',
     'expect': 'read',
     'fn': _mut('if count or total]', 'if count]'),
     'trigger': _t_drop_total_only},
    {'id': 'generation-plus2',
     'desc': '代次每次 +2 而非 +1',
     'expect': 'response',
     'fn': _mut("generation=s['generation'] + 1",
                "generation=s['generation'] + 2"),
     'trigger': _t_generation},
    {'id': 'incomplete-all',
     'desc': 'INCOMPLETE 判定由 any 改为 all',
     'expect': 'read',
     'fn': _mut("if any(admits(f, t) for f in s['upper'].values()):",
                "if all(admits(f, t) for f in s['upper'].values()):"),
     'trigger': _t_incomplete_all},
    {'id': 'replay-blind',
     'desc': '重放不核对身份内容，异内容同号也返回旧回执',
     'expect': 'conflict',
     'fn': _mut('if old[0] != payload:', 'if False:'),
     'trigger': _t_replay_blind},
    {'id': 'le-strict',
     'desc': '偏序 ≤ 改为严格 <（边界不再自反）',
     'expect': 'apply_rejected',
     'fn': _mut('return all(x <= y for x, y in zip(a, b))',
                'return all(x < y for x, y in zip(a, b))'),
     'trigger': _t_le_strict},
    {'id': 'keep-zero',
     'desc': '对照变异：合并后保留零权重记录（对合同可观察语义等价，不应检出）',
     'expect': 'none',
     'fn': _mut(' if w]', ' if True]'),
     'trigger': _t_keep_zero},
]

_MUTANT_CACHE = {}


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _mutant_module(work, mutant_id):
    if mutant_id in _MUTANT_CACHE:
        return _MUTANT_CACHE[mutant_id]
    src = open(os.path.join(HERE, 'lakeflow.py'), encoding='utf-8').read()
    m = next(m for m in MUTANTS if m['id'] == mutant_id)
    msrc = m['fn'](src)
    assert msrc != src
    mdir = os.path.join(work, 'mutants')
    os.makedirs(mdir, exist_ok=True)
    path = os.path.join(mdir, 'lakeflow_%s.py' % mutant_id)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(msrc)
    mod = _load_module('lakeflow_mut_' + mutant_id, path)
    _MUTANT_CACHE[mutant_id] = mod
    return mod


def _norm_deviation(d):
    if d['type'] == 'certificate':
        r = d['result']
        if r['kind'] == 'counterexample':
            return qasem.canon(['certificate', 'counterexample', r['port'],
                                r['row'], r['cut']])
        return qasem.canon(['certificate', r['kind'], r.get('field')])
    if d['type'] == 'read':
        return qasem.canon(['read', d['cut']])
    if d['type'] == 'apply_rejected':
        return qasem.canon(['apply_rejected', d['error']])
    return qasem.canon([d['type']])


def _signature(mod, history, dims, probe_cuts):
    devs, _ = evaluate_history(mod, history, dims, probe_cuts=probe_cuts,
                               do_replay=False, do_negative=False)
    return _norm_deviation(devs[0]) if devs else None


def mutation_suite(work):
    results = []
    seeded_params = dict(dims=2, coord_max=3, width=2, n_keys=2, neg_ratio=0.5,
                         n_requests=10, max_changes=3, amount_max=6,
                         weight_max=3)
    for m in MUTANTS:
        mod = _mutant_module(work, m['id'])
        trig_history, trig_dims, trig_probes = m['trigger']()
        qasem.replay(trig_history, trig_dims)  # 触发输入本身合法
        # 正常对照：生产实现跑同一触发输入须无偏差
        ctrl_devs, _ = evaluate_history(lakeflow, trig_history, trig_dims,
                                        probe_cuts=trig_probes)
        control_pass = not ctrl_devs
        devs, _ = evaluate_history(mod, trig_history, trig_dims,
                                   probe_cuts=trig_probes)
        detected = bool(devs)
        # 附加一次带种子运行（含抵消与压缩），核对不是只会报固定输入
        history, dims = gen_case(555, seeded_params)
        meta, raw = qasem.replay(history, dims)
        cuts = probe_cuts_for(meta, raw, dims, random.Random(556), 6)
        seeded_devs, _ = evaluate_history(mod, history, dims, probe_cuts=cuts)
        rec = {'id': m['id'], 'desc': m['desc'], 'expect': m['expect'],
               'control_pass': control_pass,
               'detected': detected,
               'detection_path': devs[0]['type'] if devs else None,
               'seeded_deviations': len(seeded_devs),
               'first_deviation': devs[0] if devs else None,
               'trigger_digest': _digest(trig_history)}
        rec['ok'] = (control_pass and (detected if m['expect'] != 'none'
                                       else not detected))
        results.append(rec)
    return results


# ---------------------------------------------------------------- 缩减

def _renumber(history):
    out = copy.deepcopy(history)
    for i, req in enumerate(out):
        req['expected'] = i
    return out


def _shrink(mod, history, dims, probe_cuts_fn, target):
    """贪心缩减：删除请求、清空变更、撤销上界推进；保持同一语义偏差。"""
    def same(h):
        try:
            return _signature(mod, h, dims, probe_cuts_fn(h)) == target
        except Exception:
            return False

    cur = history
    improved = True
    while improved:
        improved = False
        for i in range(len(cur)):
            cand = _renumber(cur[:i] + cur[i + 1:])
            if same(cand):
                cur = cand
                improved = True
                break
        if improved:
            continue
        for i in range(len(cur)):
            if not cur[i]['changes']:
                continue
            cand = copy.deepcopy(cur)
            cand[i]['changes'] = []
            if same(cand):
                cur = cand
                improved = True
                break
        if improved:
            continue
        for i in range(len(cur)):
            if not cur[i]['upper']:
                continue
            cand = copy.deepcopy(cur)
            cand[i]['upper'] = {}
            if same(cand):
                cur = cand
                improved = True
                break
    return cur


def _hist_size(h):
    return {'requests': len(h),
            'changes': sum(len(r['changes']) for r in h)}


def shrink_demo(work, runtime, mutant_id, params, max_seeds=80):
    """在变异体上制造随机失败并自动缩减，留存种子与重放输入。"""
    os.makedirs(work, exist_ok=True)
    mod = _mutant_module(runtime, mutant_id)
    for i in range(max_seeds):
        seed = 9000 + i
        history, dims = gen_case(seed, params)
        meta, raw = qasem.replay(history, dims)

        def cuts_fn(h, _meta=meta, _raw=raw, _dims=dims):
            m2, r2 = qasem.replay(h, _dims)
            return grid_cuts_for(m2, r2, _dims)

        sig = _signature(mod, history, dims, cuts_fn(history))
        if sig is None:
            continue
        shrunk = _shrink(mod, history, dims, cuts_fn, sig)
        devs, _ = evaluate_history(mod, shrunk, dims,
                                   probe_cuts=cuts_fn(shrunk),
                                   do_replay=False, do_negative=False)
        case = {'mutant': mutant_id, 'seed': seed, 'dims': dims,
                'history': shrunk, 'signature': sig,
                'deviation': devs[0] if devs else None,
                'replay': 'python3 qa.py replay-case %s'}
        case_file = os.path.join(work, 'shrunk-%s.json' % mutant_id)
        rel = os.path.relpath(case_file, HERE)
        case['replay'] = case['replay'] % rel
        _write_json(case_file, case)
        return {'mutant': mutant_id, 'seed': seed,
                'before': _hist_size(history), 'after': _hist_size(shrunk),
                'signature': sig, 'case_file': rel,
                'deviation': case['deviation'],
                'reproduced': _signature(mod, shrunk, dims,
                                         cuts_fn(shrunk)) == sig}
    return None


# ---------------------------------------------------------------- 压力

STRESS_BASE = dict(dims=3, coord_max=60, width=2, n_keys=8, neg_ratio=0.3,
                   n_requests=30, max_changes=20)


def _cfg(**kw):
    p = dict(STRESS_BASE)
    p.update(kw)
    return p


STRESS_CONFIGS = [
    ('base', 7001, _cfg()),
    ('dims2', 7002, _cfg(dims=2)),
    ('dims6', 7003, _cfg(dims=6)),
    ('width1', 7004, _cfg(width=1)),
    ('width6', 7005, _cfg(width=6)),
    ('skew1', 7006, _cfg(n_keys=1)),
    ('keys128', 7007, _cfg(n_keys=128)),
    ('noneg', 7008, _cfg(neg_ratio=0.0)),
    ('halfneg', 7009, _cfg(neg_ratio=0.5)),
    ('huge-coords', 7010, _cfg(coord_max=10 ** 12)),
    ('scale', 7011, _cfg(n_requests=100, max_changes=25)),
]
MEMORY_CONFIGS = {'base', 'width6', 'keys128', 'halfneg', 'scale'}


def _first_legal_cut(meta):
    cands = [list(s) for s in meta['since']]
    for a in meta['since']:
        for b in meta['since']:
            cands.append(qasem.join2(a, b))
    for q in cands:
        if qasem.cut_status(meta['upper'], meta['since'], q) == 'ok':
            return q
    return None


def stress_suite():
    out = []
    for name, seed, params in STRESS_CONFIGS:
        history, dims = gen_case(seed, params)
        meta, raw = qasem.replay(history, dims)
        t0 = time.perf_counter()
        store = Store(':memory:', dimensions=dims)
        for req in history:
            store.apply(req)
        t1 = time.perf_counter()
        q = _first_legal_cut(meta)
        read_ms = None
        if q is not None:
            r0 = time.perf_counter()
            for _ in range(3):
                store.read(q)
            read_ms = (time.perf_counter() - r0) * 1e3 / 3
        snap = store.inspect()
        t2 = time.perf_counter()
        cert = certify(history, snap)
        t3 = time.perf_counter()
        ok = cert['kind'] == 'certificate' and check_certificate(
            history, snap, cert)
        t4 = time.perf_counter()
        store.close()
        rec = {'name': name, 'seed': seed, 'params': params,
               'counts': {'requests': len(history),
                          'changes': sum(len(r['changes']) for r in history),
                          'trace_L': len(snap['state']['traces']['L']),
                          'trace_R': len(snap['state']['traces']['R']),
                          'nf_L': len(cert['normal_forms']['L']) if ok else None,
                          'nf_R': len(cert['normal_forms']['R']) if ok else None},
               'production': {'apply_ms': (t1 - t0) * 1e3, 'read_ms': read_ms},
               'judge': {'certify_ms': (t3 - t2) * 1e3,
                         'check_ms': (t4 - t3) * 1e3,
                         'cross_check': cert.get('cross_check'),
                         'ok': ok}}
        out.append(rec)
    # 内存轮：tracemalloc 分别测生产与判定器的 Python 峰值分配
    for rec in out:
        if rec['name'] not in MEMORY_CONFIGS:
            continue
        history, dims = gen_case(rec['seed'], rec['params'])
        tracemalloc.start()
        store = Store(':memory:', dimensions=dims)
        for req in history:
            store.apply(req)
        snap = store.inspect()
        _, peak_prod = tracemalloc.get_traced_memory()
        tracemalloc.reset_peak()
        cert = certify(history, snap)
        check_certificate(history, snap, cert)
        _, peak_judge = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        store.close()
        rec['production']['peak_bytes'] = peak_prod
        rec['judge']['peak_bytes'] = peak_judge
    return out


# ---------------------------------------------------------------- 套件

FUNCTIONAL_CASES = [
    ('f-d2', 101, dict(dims=2, coord_max=6, width=2, n_keys=4, neg_ratio=0.3,
                       n_requests=30, max_changes=4), 8),
    ('f-d3', 102, dict(dims=3, coord_max=8, width=3, n_keys=6, neg_ratio=0.4,
                       n_requests=35, max_changes=5), 8),
    ('f-d4', 103, dict(dims=4, coord_max=6, width=2, n_keys=3, neg_ratio=0.2,
                       n_requests=30, max_changes=4), 6),
    ('f-d6', 104, dict(dims=6, coord_max=5, width=2, n_keys=4, neg_ratio=0.3,
                       n_requests=25, max_changes=4), 6),
    ('f-wide', 105, dict(dims=2, coord_max=10, width=5, n_keys=4, neg_ratio=0.3,
                         n_requests=30, max_changes=4), 8),
    ('f-skew', 106, dict(dims=2, coord_max=6, width=2, n_keys=1, neg_ratio=0.5,
                         n_requests=35, max_changes=6), 8),
    ('f-noneg', 107, dict(dims=3, coord_max=6, width=2, n_keys=4, neg_ratio=0.0,
                          n_requests=25, max_changes=4), 6),
    ('f-close', 108, dict(dims=2, coord_max=6, width=2, n_keys=3, neg_ratio=0.3,
                          n_requests=35, max_changes=4, close_prob=0.15), 8),
    ('f-cancel', 109, dict(dims=2, coord_max=4, width=2, n_keys=2,
                           neg_ratio=0.5, n_requests=40, max_changes=6,
                           amount_max=6, weight_max=3), 10),
    ('f-emptyregion', 110, dict(dims=3, coord_max=6, width=2, n_keys=5,
                                neg_ratio=0.3, n_requests=25, max_changes=4,
                                regions=('', 'e')), 6),
]

EXHAUSTIVE_CASES = [
    ('x-d2', 201, dict(dims=2, coord_max=3, width=2, n_keys=3, neg_ratio=0.4,
                       n_requests=16, max_changes=3)),
    ('x-d2b', 202, dict(dims=2, coord_max=4, width=3, n_keys=2, neg_ratio=0.5,
                        n_requests=18, max_changes=3, amount_max=5,
                        weight_max=3)),
    ('x-d3', 203, dict(dims=3, coord_max=2, width=2, n_keys=3, neg_ratio=0.4,
                       n_requests=12, max_changes=3)),
    ('x-d3b', 204, dict(dims=3, coord_max=3, width=3, n_keys=1, neg_ratio=0.5,
                        n_requests=12, max_changes=4, amount_max=4)),
]

LARGE_COORD_CASE = ('f-huge', 301, dict(dims=3, coord_max=10 ** 12, width=3,
                                        n_keys=4, neg_ratio=0.4, n_requests=25,
                                        max_changes=4), 4)


def gen_case(seed, params):
    gen = qagen.HistoryGen(seed=seed, **params)
    return gen.history(), gen.dims


def run_all(work):
    """运行全部套件。work 为 JSON 证据材料目录；SQLite 数据库与进程间
    协调文件一律放在正规文件系统的运行时临时目录（/workspace 为宿主机
    共享挂载，POSIX 锁语义不可靠，不能承载跨进程 WAL 数据库）。"""
    if os.path.exists(work):
        shutil.rmtree(work)
    os.makedirs(work)
    runtime = tempfile.mkdtemp(prefix='qa-runtime-')
    try:
        return _run_all_impl(work, runtime)
    finally:
        shutil.rmtree(runtime, ignore_errors=True)


def _run_all_impl(work, runtime):
    det = {}
    unresolved = []

    # 0. 原断言（checks.py normal 原文）
    import checks
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        checks.normal()
    assert 'NORMAL_REGRESSION_OK' in buf.getvalue()
    det['normal_regression'] = 'NORMAL_REGRESSION_OK'

    # 1. 功能差分（含重开、重放、拒绝探针、读探针）
    func = []
    for name, seed, params, n_probes in FUNCTIONAL_CASES:
        history, dims = gen_case(seed, params)
        meta, raw = qasem.replay(history, dims)
        cuts = probe_cuts_for(meta, raw, dims, random.Random('p%d' % seed),
                              n_probes)
        db = os.path.join(runtime, 'case-%s.db' % name)
        devs, info = evaluate_history(lakeflow, history, dims, probe_cuts=cuts,
                                      db_path=db, do_reopen=True)
        cert = info['certificate']
        func.append({'name': name, 'seed': seed, 'dims': dims,
                     'requests': len(history),
                     'changes': sum(len(r['changes']) for r in history),
                     'probes': len(cuts),
                     'certificate_digest': cert.get('certificate_digest'),
                     'cross_check': cert.get('cross_check'),
                     'deviations': len(devs)})
        for d in devs:
            unresolved.append({'case': name, 'deviation': d})
    det['functional'] = func

    # 2. 小域穷举互核
    exh = []
    for name, seed, params in EXHAUSTIVE_CASES:
        history, dims = gen_case(seed, params)
        devs, info = exhaustive_case(lakeflow, history, dims)
        exh.append({'name': name, 'seed': seed, 'dims': dims,
                    'grid_cuts': info['grid_cuts'],
                    'certificate_digest': info.get('certificate_digest'),
                    'cross_check': info.get('cross_check'),
                    'deviations': len(devs)})
        for d in devs:
            unresolved.append({'case': name, 'deviation': d})
    det['exhaustive'] = exh

    # 3. 大坐标量级（证书不得依赖坐标大小）
    name, seed, params, n_probes = LARGE_COORD_CASE
    history, dims = gen_case(seed, params)
    meta, raw = qasem.replay(history, dims)
    cuts = probe_cuts_for(meta, raw, dims, random.Random('p%d' % seed), n_probes)
    devs, info = evaluate_history(lakeflow, history, dims, probe_cuts=cuts)
    det['large_coords'] = {'name': name, 'seed': seed, 'dims': dims,
                           'coord_max': params['coord_max'],
                           'certificate_digest':
                               info['certificate'].get('certificate_digest'),
                           'cross_check': info['certificate'].get('cross_check'),
                           'deviations': len(devs)}
    for d in devs:
        unresolved.append({'case': name, 'deviation': d})

    # 4. 并发
    conc = concurrency_suite(os.path.join(runtime, 'conc'))
    det['concurrency'] = {
        'race': {'ok': conc['race']['ok'],
                 'generation': conc['race']['generation'],
                 'replay': conc['race']['replay']},
        'free_run': {'ok': conc['free_run']['ok'],
                     'successes': conc['free_run']['successes'],
                     'generation': conc['free_run']['generation'],
                     'receipts': conc['free_run']['receipts'],
                     'ordering_violations':
                         conc['free_run']['ordering_violations']},
    }
    if not conc['ok']:
        unresolved.append({'case': 'concurrency', 'deviation': '见 observed'})

    # 5. 故障（真实杀进程）
    faults = fault_suite(os.path.join(runtime, 'fault'))
    det['faults'] = faults['cases']
    if not faults['ok']:
        unresolved.append({'case': 'faults', 'deviation': '见 cases'})

    # 6. 语义变异检出能力
    muts = mutation_suite(os.path.join(runtime, 'mut'))
    det['mutations'] = [{k: r[k] for k in
                         ('id', 'expect', 'control_pass', 'detected',
                          'detection_path', 'seeded_deviations',
                          'trigger_digest', 'ok')} for r in muts]
    for r in muts:
        if not r['ok']:
            unresolved.append({'case': 'mutation', 'deviation': r})

    # 7. 反例自动缩减（在变异体上演示）
    shrinks = []
    for mid, params in (
            ('advance-sup', dict(dims=2, coord_max=3, width=2, n_keys=2,
                                 neg_ratio=0.4, n_requests=14, max_changes=3,
                                 compact_prob=0.6)),
            ('drop-total-only', dict(dims=2, coord_max=2, width=2, n_keys=1,
                                     neg_ratio=0.5, n_requests=12,
                                     max_changes=4, amount_max=6))):
        rec = shrink_demo(os.path.join(work, 'shrink'), runtime, mid, params)
        if rec is None:
            unresolved.append({'case': 'shrink',
                               'deviation': '未找到 %s 的随机失败' % mid})
        else:
            shrinks.append(rec)
    det['shrinks'] = shrinks

    # 8. 压力与测量（大域判定不枚举网格、不展开关联对、不逐点 read）
    stress = stress_suite()
    for rec in stress:
        if not rec['judge']['ok']:
            unresolved.append({'case': 'stress-' + rec['name'],
                               'deviation': '判定失败'})

    evidence = {
        'schema': SCHEMA,
        'tool': 'qa.py',
        'python': platform.python_version(),
        'deterministic': det,
        'measurements': {
            'method': ('耗时为 wall clock（time.perf_counter），内存为 '
                       'tracemalloc 的 Python 堆峰值分配（非 RSS）；计时轮不'
                       '开 tracemalloc，内存轮单独重放同一种子。大域判定只经'
                       '证书范式与预算内交叉自检，不枚举坐标网格、不展开关联'
                       '对、不逐点调用 read。'),
            'stress': stress,
        },
        'observed': {
            'concurrency_raw': {'race': conc['race'], 'free_run_calls':
                                conc['free_run']['calls']},
            'note': '本区为上一次运行的原始记录（含调用区间与原始响应），'
                    '每次运行重采；判定结论以 deterministic 区为准。',
        },
        'unresolved': unresolved,
    }
    return evidence


# ---------------------------------------------------------------- 命令

def _load_evidence():
    with open(os.path.join(HERE, 'evidence.json'), encoding='utf-8') as f:
        return json.load(f)


def cmd_record():
    work = os.path.join(HERE, 'qa_artifacts')
    ev = run_all(work)
    path = os.path.join(HERE, 'evidence.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(ev, f, ensure_ascii=False, indent=1)
        f.write('\n')
    print('evidence.json written; unresolved=%d' % len(ev['unresolved']))
    return 0 if not ev['unresolved'] else 1


def cmd_verify():
    work = os.path.join(HERE, 'qa_artifacts')
    ev = run_all(work)
    failures = []
    if ev['unresolved']:
        failures.append({'kind': 'unresolved', 'items': ev['unresolved']})
    try:
        recorded = _load_evidence()
    except (OSError, ValueError) as e:
        recorded = None
        failures.append({'kind': 'evidence_missing', 'error': str(e)})
    if recorded is not None:
        if qasem.canon(recorded.get('deterministic')) != qasem.canon(ev['deterministic']):
            failures.append({'kind': 'evidence_mismatch',
                             'section': 'deterministic'})
        if recorded.get('unresolved') != []:
            failures.append({'kind': 'evidence_unresolved',
                             'items': recorded.get('unresolved')})
        # 测量区：配置一致且数值健全（耗时/内存依机器而异，不做等值核对）
        got = {r['name'] for r in ev['measurements']['stress']}
        want = {r['name'] for r in recorded['measurements']['stress']}
        if got != want:
            failures.append({'kind': 'evidence_mismatch',
                             'section': 'measurements.configs'})
        for r in ev['measurements']['stress']:
            nums = [r['production']['apply_ms'], r['judge']['certify_ms'],
                    r['judge']['check_ms']]
            if not all(isinstance(x, (int, float)) and x >= 0 for x in nums):
                failures.append({'kind': 'measurement_insane',
                                 'config': r['name']})
    if failures:
        fail_dir = os.path.join(work, 'verify-failure')
        os.makedirs(fail_dir, exist_ok=True)
        _write_json(os.path.join(fail_dir, 'failures.json'), failures)
        _write_json(os.path.join(fail_dir, 'evidence.actual.json'), ev)
        print('VERIFY FAILED (%d); 失败材料留存于 %s'
              % (len(failures), fail_dir))
        for f in failures[:5]:
            print(' -', f.get('kind'), str(f)[:200])
        return 1
    n_cases = (len(ev['deterministic']['functional'])
               + len(ev['deterministic']['exhaustive']) + 1)
    muts = ev['deterministic']['mutations']
    n_detect = sum(1 for m in muts if m['expect'] != 'none' and m['detected'])
    n_expect = sum(1 for m in muts if m['expect'] != 'none')
    print('VERIFY OK: 原断言通过；功能/穷举/大坐标用例 %d 个；'
          '并发与故障核对通过；语义变异检出 %d/%d（另有等价对照变异'
          '按预期未检出）；缩减用例 %d 个；压力配置 %d 个；'
          '证据与 evidence.json 一致。'
          % (n_cases, n_detect, n_expect,
             len(ev['deterministic']['shrinks']),
             len(ev['measurements']['stress'])))
    return 0


def cmd_demo():
    import samples
    work = tempfile.mkdtemp(prefix='qa-demo-')
    db = os.path.join(work, 'demo.db')
    out = []

    s = Store(db)
    r1 = s.apply(samples.initial())
    before = s.inspect()
    out.append({'step': 'apply-initial', 'response': r1,
                'read[2,2]': s.read([2, 2]),
                'traces': before['state']['traces']})
    r2 = s.apply(samples.compact())
    mid = s.inspect()
    out.append({'step': 'compact', 'response': r2,
                'since': mid['state']['since'],
                'traces_after_compaction': mid['state']['traces'],
                'read[2,2]': s.read([2, 2])})
    try:
        s.read([0, 0])
    except FlowError as e:
        out.append({'step': 'read-old-cut', 'error': str(e)})
    r3 = s.apply(samples.correction())
    out.append({'step': 'correction', 'response': r3,
                'read[2,2]': s.read([2, 2]), 'read[3,3]': s.read([3, 3])})
    s.close()

    # 重开恢复：压缩后的账与代次原样恢复
    s2 = Store(db)
    snap = s2.inspect()
    out.append({'step': 'reopen', 'generation': snap['state']['generation'],
                'since': snap['state']['since'],
                'read[2,2]': s2.read([2, 2]), 'read[3,3]': s2.read([3, 3])})
    replay = s2.apply(samples.initial())
    out.append({'step': 'replay-initial', 'response': replay,
                'generation_after': s2.inspect()['state']['generation']})
    cert = certify([samples.initial(), samples.compact(), samples.correction()],
                   s2.inspect())
    out.append({'step': 'certify', 'kind': cert['kind'],
                'certificate_digest': cert.get('certificate_digest')})
    s2.close()

    # 真实杀进程：提交后响应丢失，重开按回执恢复
    db2 = os.path.join(work, 'crash.db')
    victim = {'request': 'victim', 'expected': 0,
              'changes': [['L', [0, 0], ['vk', 3], 1]],
              'upper': {'L': [[1, 1]]}, 'since': None}
    req_file = os.path.join(work, 'victim.json')
    _write_json(req_file, victim)
    ready = os.path.join(work, 'victim.ready')
    release = os.path.join(work, 'victim.release')
    proc = _spawn(['killpoint', db2, req_file, 'after_commit', ready,
                   release, os.path.join(work, 'victim.out.json')])
    _wait_file(ready, 30)
    os.kill(proc.pid, signal.SIGKILL)
    proc.wait()
    s3 = Store(db2)
    snap3 = s3.inspect()
    reapply = s3.apply(victim)
    out.append({'step': 'kill-after-commit',
                'response_lost': True,
                'reopen_generation': snap3['state']['generation'],
                'receipts': [[r[0], r[2]] for r in snap3['receipts']],
                'reapply_returns': reapply})
    s3.close()
    for line in out:
        print(json.dumps(line, ensure_ascii=False))
    return 0


def cmd_replay_case(path):
    with open(path, encoding='utf-8') as f:
        case = json.load(f)
    mod = lakeflow
    if case.get('mutant'):
        mod = _mutant_module(tempfile.mkdtemp(prefix='qa-mutants-'),
                             case['mutant'])
    history, dims = case['history'], case['dims']
    meta, raw = qasem.replay(history, dims)
    cuts = grid_cuts_for(meta, raw, dims)
    sig = _signature(mod, history, dims, cuts)
    ok = sig == case['signature']
    print(json.dumps({'reproduced': ok, 'signature': sig,
                      'expected': case['signature']}, ensure_ascii=False))
    return 0 if ok else 1


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'verify'
    if cmd == 'verify':
        return cmd_verify()
    if cmd == 'demo':
        return cmd_demo()
    if cmd == 'record':
        return cmd_record()
    if cmd == 'replay-case':
        return cmd_replay_case(sys.argv[2])
    raise SystemExit('未知子命令: %r（verify/demo/record/replay-case）' % cmd)


if __name__ == '__main__':
    sys.exit(main())
