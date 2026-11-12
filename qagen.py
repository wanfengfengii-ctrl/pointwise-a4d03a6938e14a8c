"""带种子的合法历史生成器：只产出满足合同全部约束的请求序列。

维数、反链宽度、同键偏斜、正负抵消比例、坐标量级均可独立调节。
生成器自检：产出的历史须通过 qasem.replay 的独立合法性判定。
"""

import random

import qasem


def _add(a, b):
    return [x + y for x, y in zip(a, b)]


def _max2(a, b):
    return [max(x, y) for x, y in zip(a, b)]


def make_antichain(candidates):
    """保序过滤为反链：丢弃被已有元素支配者，移除被新元素支配者。"""
    front = []
    for c in candidates:
        if any(qasem.at_most(g, c) for g in front):
            continue
        front = [g for g in front if not qasem.at_most(c, g)]
        front.append(c)
    return front


class HistoryGen:
    def __init__(self, seed, dims, coord_max=6, width=2, n_keys=4,
                 neg_ratio=0.3, n_requests=30, max_changes=4,
                 close_prob=0.01, compact_prob=0.35, bump_prob=0.06,
                 regions=('east', 'west', 'north', ''), amount_max=20,
                 weight_max=5):
        self.rng = random.Random(seed)
        self.seed = seed
        self.dims = dims
        self.coord_max = coord_max
        self.width = width
        self.n_keys = n_keys
        self.neg_ratio = neg_ratio
        self.n_requests = n_requests
        self.max_changes = max_changes
        self.close_prob = close_prob
        self.compact_prob = compact_prob
        self.bump_prob = bump_prob
        self.regions = list(regions)
        self.amount_max = amount_max
        self.weight_max = weight_max
        self.step = max(1, coord_max // 4)
        self.key_weights = [1.0 / (i + 1) for i in range(n_keys)]

    def _offsets(self, bound=None):
        b = self.step if bound is None else bound
        return [self.rng.randint(0, b) for _ in range(self.dims)]

    def _front_above(self, old, lowers, target):
        """生成新前沿：每个元素 ≥ old 的某元素且 ≥ lowers 的某元素。

        小坐标空间中反链容量有限，补宽设尝试上限，达不到目标宽度时
        返回已得到的（非空）反链。"""
        cand = []
        for _ in range(target * 5):
            base = self.rng.choice(old)
            lo = self.rng.choice(lowers)
            cand.append(_add(_max2(base, lo), self._offsets()))
        front = make_antichain(cand)
        attempts = 0
        while len(front) < target and attempts < 60:
            attempts += 1
            front = make_antichain(
                front + [_add(_max2(self.rng.choice(old),
                                    self.rng.choice(lowers)),
                              self._offsets())])
        return front[:target]

    def _key(self):
        return 'k%d' % self.rng.choices(range(self.n_keys),
                                        weights=self.key_weights)[0]

    def _change(self, port):
        t = _add(self.rng.choice(self.upper[port]), self._offsets())
        if port == 'L':
            row = [self._key(),
                   self.rng.randint(-self.amount_max, self.amount_max)]
        else:
            row = [self._key(), self.rng.choice(self.regions)]
        w = self.rng.randint(1, self.weight_max)
        if self.rng.random() < self.neg_ratio:
            w = -w
        return [port, t, row, w]

    def history(self):
        self.upper = {'L': [[0] * self.dims], 'R': [[0] * self.dims]}
        self.since = [[0] * self.dims]
        reqs = []
        for i in range(self.n_requests):
            reqs.append(self._request(i))
        meta, _ = qasem.replay(reqs, self.dims)  # 生成器自检
        return reqs

    def _request(self, i):
        rng = self.rng
        open_ports = [p for p in ('L', 'R') if self.upper[p]]
        changes = []
        if open_ports and rng.random() >= self.bump_prob:
            for _ in range(rng.randint(0, self.max_changes)):
                changes.append(self._change(rng.choice(open_ports)))
        # 压缩：先定新 since，再让两路新前沿支配它
        compact = rng.random() < self.compact_prob
        if compact:
            m = rng.randint(1, self.width)
            new_since = make_antichain(
                [_add(rng.choice(self.since), self._offsets())
                 for _ in range(m * 4)])[:m]
            lowers = new_since
        else:
            new_since = None
            lowers = self.since
        upper_update = {}
        for port in ('L', 'R'):
            if not self.upper[port]:
                continue  # 端口已永久关闭
            r = rng.random()
            if r < self.close_prob:
                upper_update[port] = []  # 永久关闭端口
            elif compact or r < 0.75:
                # 压缩时每个开放端口必须推进到支配新 since 的前沿
                upper_update[port] = self._front_above(
                    self.upper[port], lowers, rng.randint(1, self.width))
        req = {'request': 'r%d-%d' % (self.seed, i), 'expected': i,
               'changes': changes, 'upper': upper_update,
               'since': new_since}
        # 更新生成器状态（与 qasem.replay 同语义）
        for port, f in upper_update.items():
            self.upper[port] = f
        if new_since is not None:
            self.since = new_since
        return req


def probe_cuts(seed, meta, records, count):
    """确定性截点探针：记录时间、since/upper 元素、其 join 与随机点混合。"""
    rng = random.Random(('probe', seed))
    d = meta['dimensions']
    maxc = 1
    for _, t, _, _ in records:
        maxc = max(maxc, max(t) + 1)
    for f in (meta['since'], meta['upper']['L'], meta['upper']['R']):
        for t in f:
            maxc = max(maxc, max(t) + 1)
    pool = [list(t) for _, t, _, _ in records]
    pool += [list(t) for t in meta['since']]
    pool += [list(t) for t in meta['upper']['L']]
    pool += [list(t) for t in meta['upper']['R']]
    cuts = []
    if pool:
        for _ in range(count // 2):
            a = rng.choice(pool)
            if rng.random() < 0.5 and pool:
                b = rng.choice(pool)
                cuts.append([max(x, y) for x, y in zip(a, b)])
            else:
                cuts.append(list(a))
    while len(cuts) < count:
        cuts.append([rng.randint(0, maxc + 1) for _ in range(d)])
    return cuts[:count]
