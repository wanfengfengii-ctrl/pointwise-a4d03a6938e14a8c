"""故障与并发测试的子进程工人：真实进程、真实 SIGKILL、真实重开。

父进程通过就绪/放行文件做进程间屏障；工人在指定钩子点阻塞等待，
父进程随后真正 kill。工人把原始响应与调用区间（单调时钟，纳秒）
写入结果文件，供父进程判定实际历史。不以抛异常代替杀进程。
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lakeflow import FlowError, Store


def _wait_file(path, timeout=60.0):
    t0 = time.monotonic()
    while not os.path.exists(path):
        if time.monotonic() - t0 > timeout:
            return False
        time.sleep(0.02)
    return True


def _write(path, obj):
    with open(path, 'w') as f:
        json.dump(obj, f, ensure_ascii=False)


def _load(path):
    with open(path) as f:
        return json.load(f)


def mode_killpoint(argv):
    # 在指定钩子点阻塞，等待被杀；若被放行则正常完成。
    db, req_file, point, ready, release, out = argv
    req = _load(req_file)
    store = Store(db)

    def hook(p):
        if p == point:
            _write(ready, {'point': p, 'pid': os.getpid(),
                           'at': time.monotonic_ns()})
            _wait_file(release)

    rec = {'mode': 'killpoint', 'point': point}
    try:
        resp = store.apply(req, hook)
        rec['response'] = resp
    except FlowError as e:
        rec['error'] = str(e)
    _write(out, rec)
    store.close()


def mode_idle(argv):
    # 正常完成请求，随后在两次请求之间被杀（无钩子注入）。
    db, req_file, ready, out = argv
    req = _load(req_file)
    store = Store(db)
    rec = {'mode': 'idle'}
    try:
        resp = store.apply(req)
        rec['response'] = resp
    except FlowError as e:
        rec['error'] = str(e)
    _write(out, rec)
    _write(ready, {'pid': os.getpid(), 'at': time.monotonic_ns()})
    while True:
        time.sleep(0.5)


def mode_race_a(argv):
    # 屏障持有者：在 after_begin 阻塞（写事务已开启），等待放行后提交。
    db, req_file, ready, release, out = argv
    req = _load(req_file)
    store = Store(db)

    def hook(p):
        if p == 'after_begin':
            _write(ready, {'pid': os.getpid(), 'at': time.monotonic_ns()})
            _wait_file(release)

    rec = {'mode': 'race-a', 'start': time.monotonic_ns()}
    try:
        resp = store.apply(req, hook)
        rec['response'] = resp
    except FlowError as e:
        rec['error'] = str(e)
    rec['end'] = time.monotonic_ns()
    _write(out, rec)
    store.close()


def mode_race_b(argv):
    # 竞争者：expected 冲突时按当前代次重试一次；记录两次调用的区间。
    db, req_file, out = argv
    req = _load(req_file)
    store = Store(db)
    rec = {'mode': 'race-b', 'attempts': []}
    t0 = time.monotonic_ns()
    try:
        resp = store.apply(req)
        rec['attempts'].append({'start': t0, 'end': time.monotonic_ns(),
                                'response': resp})
        rec['first'] = 'ok'
    except FlowError as e:
        rec['attempts'].append({'start': t0, 'end': time.monotonic_ns(),
                                'error': str(e)})
        rec['first'] = str(e)
        if str(e) == 'GENERATION':
            req['expected'] = store.inspect()['state']['generation']
            t1 = time.monotonic_ns()
            try:
                resp = store.apply(req)
                rec['attempts'].append(
                    {'start': t1, 'end': time.monotonic_ns(), 'response': resp})
                rec['retry'] = {'response': resp}
            except FlowError as e2:
                rec['attempts'].append(
                    {'start': t1, 'end': time.monotonic_ns(), 'error': str(e2)})
                rec['retry'] = {'error': str(e2)}
    _write(out, rec)
    store.close()


if __name__ == '__main__':
    {'killpoint': mode_killpoint, 'idle': mode_idle,
     'race-a': mode_race_a, 'race-b': mode_race_b}[sys.argv[1]](sys.argv[2:])
