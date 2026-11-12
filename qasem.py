"""独立语义判定器：只依据 CONTRACT.md 的公开规范重新实现。

本模块不导入、不复制 lakeflow 的前沿、压缩或聚合函数。全部语义从合同
条款独立构造：

- 偏序 ≤ 为逐分量比较；反链、前沿推进关系按合同定义直接判定。
- 压缩等价证书使用「上集生成元范式」：记录时间 t 在合法截点集 ↑since
  上的可见性，等于反链 {t∨s : s∈since} 生成的上集。两个迹在 ↑since 的
  每个截点上逐 (端口,行) 前缀权重相等，当且仅当它们的范式（按
  (端口,生成元,行) 合并权重并去零）相等。这是与生产的逐分量下确界
  推进不同的构造，且与坐标数值大小无关。
- 关联账按合同的显式多重集语义直接求值（小域另提供逐单位展开版本，
  与映射版本互核）。
"""

import json


class SemanticsError(ValueError):
    pass


class ReplayError(SemanticsError):
    """历史或快照与独立语义不一致；携带可 JSON 化的定位信息。"""

    def __init__(self, field, expected, actual):
        super().__init__(field)
        self.field = field
        self.expected = expected
        self.actual = actual


def canon(value):
    """规范 JSON：键排序、紧凑分隔符、拒绝非标准数值。用于身份与摘要。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False)


def strict_int(v):
    return type(v) is int  # 布尔不算整数


def is_time(t, d):
    return (isinstance(t, list) and len(t) == d
            and all(strict_int(x) and x >= 0 for x in t))


def at_most(a, b):
    """偏序 ≤：逐分量比较。"""
    return all(x <= y for x, y in zip(a, b))


def admitted(front, t):
    """前沿中存在元素 ≤ t。"""
    return any(at_most(f, t) for f in front)


def front_at_most(old, new):
    """old ≤ new：new 的每个元素都 ≥ old 的某个元素。空 new 恒真。"""
    return all(any(at_most(o, n) for o in old) for n in new)


def check_front(front, d, allow_empty):
    if not isinstance(front, list) or (not front and not allow_empty):
        raise SemanticsError('FRONTIER')
    for t in front:
        if not is_time(t, d):
            raise SemanticsError('FRONTIER')
    for i, a in enumerate(front):
        for b in front[i + 1:]:
            if at_most(a, b) or at_most(b, a):
                raise SemanticsError('ANTICHAIN')


def join2(a, b):
    return [max(x, y) for x, y in zip(a, b)]


def upset_generators(t, since):
    """t 在 ↑since 上可见性的生成元反链：{t∨s : s∈since} 的极小元。"""
    gen = []
    for s in since:
        u = join2(t, s)
        if any(at_most(g, u) for g in gen):
            continue
        gen = [g for g in gen if not at_most(u, g)]
        gen.append(u)
    return sorted(gen, key=canon)


def normal_form(records, since):
    """把 (port,time,row,weight) 记录流映射到 ↑since 上的范式带符号多重集。

    返回按规范序排列的 [port, 生成元, row, 权重和] 列表（权重和为零删除）。
    定理：两个迹在 ↑since 全部截点上逐 (端口,行) 前缀权重相等，当且仅当
    范式相等（有限偏序集上前缀和与系数的相互唯一决定）。
    """
    acc = {}
    for port, t, row, w in records:
        for u in upset_generators(t, since):
            key = (port, tuple(u), canon(row))
            if key in acc:
                acc[key][2] += w
            else:
                acc[key] = [list(u), list(row), w]
    flat = [[port] + val for (port, tu, cr), val in acc.items() if val[2]]
    flat.sort(key=lambda e: canon([e[0], e[1], e[2]]))
    return flat


def prefix_weights(records, port, q):
    """截点 q 处某端口每行的前缀权重：所有 time≤q 的该行 weight 之和。

    返回 {canon(row): (row, 权重)}，零权重行省略。Python 整数精确计算。
    """
    out = {}
    for p, t, row, w in records:
        if p == port and at_most(t, q):
            k = tuple(row)
            if k in out:
                out[k][1] += w
            else:
                out[k] = [row, w]
    return {canon(row): (row, w) for (rk, (row, w)) in
            ((k, v) for k, v in out.items()) if w}


def join_rows(records, q):
    """显式多重集关联（映射构造）：行重数=前缀权重和；关联重数=两侧重数
    之积；region 的 count=关联重数和，total=amount×关联重数之和。
    仅省略 count 与 total 同为零的分组。"""
    left = {}
    right = {}
    for p, t, row, w in records:
        if not at_most(t, q):
            continue
        if p == 'L':
            slot = left.setdefault(row[0], [0, 0])
            slot[0] += w
            slot[1] += row[1] * w
        else:
            rk = (row[0], row[1])
            right[rk] = right.get(rk, 0) + w
    groups = {}
    for (key, region), rw in right.items():
        lc, amount = left.get(key, [0, 0])
        g = groups.setdefault(region, [0, 0])
        g[0] += lc * rw
        g[1] += amount * rw
    return [[region, c, tot] for region, (c, tot) in sorted(groups.items())
            if c or tot]


def join_rows_explicit(records, q):
    """显式多重集关联（逐单位展开构造，仅供小域穷举互核）：每行展开为
    |w| 个带符号单位项，左右同 key 单位逐对相乘贡献到 region 分组。"""
    left_units = {}
    right_units = {}
    for p, t, row, w in records:
        if not at_most(t, q):
            continue
        sign = 1 if w > 0 else -1
        for _ in range(abs(w)):
            if p == 'L':
                left_units.setdefault(row[0], []).append((sign, row[1]))
            else:
                right_units.setdefault(row[0], []).append((sign, row[1]))
    groups = {}
    for key, runs in right_units.items():
        for sr, region in runs:
            for sl, amount in left_units.get(key, []):
                g = groups.setdefault(region, [0, 0])
                g[0] += sl * sr
                g[1] += amount * sl * sr
    return [[region, c, tot] for region, (c, tot) in sorted(groups.items())
            if c or tot]


def cut_status(upper, since, q):
    """合法查询截点判定：先 COMPACTED 后 INCOMPLETE。空 upper 不阻止读取。"""
    if not admitted(since, q):
        return 'compacted'
    if admitted(upper.get('L', []), q) or admitted(upper.get('R', []), q):
        return 'incomplete'
    return 'ok'


def replay(history, d):
    """按合同逐条核对合法请求并独立推进元数据。

    返回 (meta, records)：meta 含 generation/upper/since/dimensions；
    records 为按首次提交顺序的 (port,time,row,weight) 原始记录流。
    任何不合法或与「成功请求」身份不符之处抛 ReplayError。
    """
    generation = 0
    upper = {'L': [[0] * d], 'R': [[0] * d]}
    since = [[0] * d]
    records = []
    seen = set()
    for index, req in enumerate(history):
        if (not isinstance(req, dict)
                or set(req) != {'request', 'expected', 'changes', 'upper', 'since'}):
            raise ReplayError('envelope', '恰含五字段的对象', req)
        rid = req['request']
        if not isinstance(rid, str) or not rid:
            raise ReplayError('envelope.request', '非空字符串', rid)
        if rid in seen:
            raise ReplayError('history.duplicate', '首次提交历史不含重复请求号', rid)
        seen.add(rid)
        if not strict_int(req['expected']) or req['expected'] < 0:
            raise ReplayError('envelope.expected', '非负整数', req['expected'])
        if req['expected'] != generation:
            raise ReplayError('expected', generation, req['expected'])
        if not isinstance(req['changes'], list):
            raise ReplayError('envelope.changes', '数组', req['changes'])
        if not isinstance(req['upper'], dict) or set(req['upper']) - {'L', 'R'}:
            raise ReplayError('envelope.upper', '只含 L/R 可选键的对象', req['upper'])
        new_upper = {'L': [list(t) for t in upper['L']],
                     'R': [list(t) for t in upper['R']]}
        for port, f in req['upper'].items():
            try:
                check_front(f, d, allow_empty=True)
            except SemanticsError:
                raise ReplayError('upper.frontier', '合法反链', f)
            if not front_at_most(upper[port], f):
                raise ReplayError('upper.rewind', upper[port], f)
            new_upper[port] = sorted([list(t) for t in f], key=canon)
        if req['since'] is None:
            new_since = [list(t) for t in since]
        else:
            try:
                check_front(req['since'], d, allow_empty=False)
            except SemanticsError:
                raise ReplayError('since.frontier', '非空反链', req['since'])
            new_since = sorted([list(t) for t in req['since']], key=canon)
        if not front_at_most(since, new_since):
            raise ReplayError('since.rewind', since, new_since)
        for port in ('L', 'R'):
            if not front_at_most(new_since, new_upper[port]):
                raise ReplayError('since.above_upper', new_upper[port], new_since)
        for change in req['changes']:
            if not isinstance(change, list) or len(change) != 4:
                raise ReplayError('change.shape', '四元数组', change)
            port, t, row, w = change
            if port not in ('L', 'R'):
                raise ReplayError('change.port', 'L 或 R', port)
            if not is_time(t, d):
                raise ReplayError('change.time', '%d 维非负整数数组' % d, t)
            if not admitted(upper[port], t):
                raise ReplayError('change.late', upper[port], change)
            if (not isinstance(row, list) or len(row) != 2
                    or not isinstance(row[0], str) or not row[0]):
                raise ReplayError('change.row', '[非空key,值]', row)
            if port == 'L' and not strict_int(row[1]):
                raise ReplayError('change.row', 'L 行金额为整数', row)
            if port == 'R' and not isinstance(row[1], str):
                raise ReplayError('change.row', 'R 行归属为字符串', row)
            if not strict_int(w) or not w:
                raise ReplayError('change.weight', '非零整数', w)
            records.append((port, list(t), list(row), w))
        upper = new_upper
        since = new_since
        generation += 1
    meta = {'dimensions': d, 'generation': generation,
            'upper': upper, 'since': since}
    return meta, records


def grid_points(dim_values):
    """每维候选坐标的笛卡尔积（仅小域穷举使用）。"""
    pts = [[]]
    for vals in dim_values:
        pts = [p + [v] for p in pts for v in vals]
    return pts
